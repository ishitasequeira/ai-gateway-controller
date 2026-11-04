/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/publisher"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
)

const (
	withdrawalDrainAnnotation       = "inference.opendatahub.io/provider-withdrawal-drain"
	withdrawalRequestTimeoutSeconds = 300
	withdrawalDrainSafetyMarginSecs = 10
	extprocConfigHashAnnotation     = "ai-gateway-controller.opendatahub.io/extproc-config-sha256"
	externalModelExtProcContainer   = "payload-processing"
	reasonProviderInUse             = "ProviderInUse"
)

// errServingRevisionUnknown blocks cleanup without invalidating a proven drain.
var errServingRevisionUnknown = errors.New("serving revision is absent from the bounded pod log tail")

// errWithdrawnProviderGone identifies a stale candidate with no applied path.
var errWithdrawnProviderGone = errors.New("withdrawn provider has no applied route")

var extprocServingRevisionPattern = regexp.MustCompile(`(?:^|\s)accepted_revision=(?:"([0-9a-f]{64})"|([0-9a-f]{64})(?:\s|$)).*?\bserving_revision=(?:"([0-9a-f]{64})"|([0-9a-f]{64})(?:\s|$))`)
var terminalEscapePattern = regexp.MustCompile(`\x1b\[[0-?]*[ -/]*[@-~]`)

var (
	// Each generated provider route has a 300 second request timeout. Keep the
	// withdrawn backend available for that full interval after the last old
	// ExtProc endpoint disappears, plus a margin for reconciliation and clock
	// skew, so an already selected upstream request can finish before pruning.
	withdrawalDrainDuration   = time.Duration(withdrawalRequestTimeoutSeconds+withdrawalDrainSafetyMarginSecs) * time.Second
	withdrawalRequeueInterval = 5 * time.Second
)

type withdrawalDrainState struct {
	Revision  string    `json:"revision"`
	StartedAt time.Time `json:"startedAt"`
}

type pendingWithdrawalError struct {
	After time.Duration
}

func (e *pendingWithdrawalError) Error() string {
	return "ExternalModel provider withdrawal is waiting for serving endpoints and in-flight requests to drain"
}

// readPublishedEnvelope reads and verifies the last published routing state
// before any resource mutation. The previous candidates are the durable input
// needed to reconstruct a withdrawn route after a controller restart.
func (r *Reconciler) readPublishedEnvelope(ctx context.Context, namespace string) (*envelope.Envelope, error) {
	reader := r.APIReader
	if reader == nil {
		return nil, errors.New("APIReader is required to stage provider withdrawal")
	}
	var cm corev1.ConfigMap
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: r.configMapName()}, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read published routing overlay %s/%s: %w", namespace, r.configMapName(), err)
	}
	if cm.Labels["app.kubernetes.io/managed-by"] != "ai-gateway-controller" {
		return nil, fmt.Errorf("routing overlay %s/%s is not owned by ai-gateway-controller", namespace, cm.Name)
	}
	raw := cm.Data[publisher.DefaultDataKey]
	if raw == "" {
		return nil, fmt.Errorf("%w: %s/%s has no %q key", publisher.ErrNoEnvelope, namespace, cm.Name, publisher.DefaultDataKey)
	}
	var env envelope.Envelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		return nil, fmt.Errorf("%w: %s/%s: %w", publisher.ErrEnvelopeTampered, namespace, cm.Name, err)
	}
	digest, err := envelope.ComputeDigestFromWire([]byte(raw))
	if err != nil {
		return nil, fmt.Errorf("%w: %s/%s: %w", publisher.ErrEnvelopeTampered, namespace, cm.Name, err)
	}
	if digest != env.Revision.Value {
		return nil, fmt.Errorf("%w: %s/%s declares %s, content hashes to %s", publisher.ErrEnvelopeTampered, namespace, cm.Name, env.Revision.Value, digest)
	}
	return &env, nil
}

// transportRoutesForPublication adds candidates from the previously
// published overlay and any already applied HTTPRoute to the desired routes.
// Additions therefore reach Envoy before publication, while withdrawals keep
// their old provider rule until the serving workload has drained.
func (r *Reconciler) transportRoutesForPublication(ctx context.Context, namespace string, desired []resolver.Route, previous *envelope.Envelope, apiTranslationEnabled bool) ([]resolver.Route, bool, error) {
	byID := make(map[string]resolver.Route, len(desired))
	for _, route := range desired {
		for _, id := range desiredCandidateIDs(route) {
			byID[id] = route
		}
	}
	union := append([]resolver.Route(nil), desired...)

	transportRoutes, err := r.readAppliedModelRoutes(ctx, namespace)
	if err != nil {
		return nil, false, err
	}
	previousCandidates := map[string]envelope.Candidate{}
	if previous != nil {
		for _, candidate := range previous.Overlay.Candidates {
			if candidate.Kind == "inference_model" && candidate.StableID != "" {
				previousCandidates[candidate.StableID] = candidate
			}
		}
	}
	for id, route := range transportRoutes {
		if _, current := byID[id]; current {
			continue
		}
		// HTTPRoute parsing recovers transport and matching identity, but not
		// the full serving profile. Restore the API format, exact path,
		// translation permission, and credential reference from the prior
		// envelope before using this rule to stage a withdrawal.
		if candidate, found := previousCandidates[id]; found {
			modelName, providerNamespace, providerName, belongsToTenant := splitTenantWithdrawalStableID(id, namespace)
			if !belongsToTenant {
				return nil, false, fmt.Errorf("cannot associate applied route %q with this tenant's prior candidate", id)
			}
			restored, err := r.routeForWithdrawnCandidate(ctx, namespace, candidate, modelName, providerNamespace, providerName, transportRoutes, apiTranslationEnabled)
			if errors.Is(err, errWithdrawnProviderGone) {
				continue
			}
			if err != nil {
				return nil, false, err
			}
			route = restored
		}
		byID[id] = route
		union = append(union, route)
	}

	if previous != nil {
		for _, candidate := range previous.Overlay.Candidates {
			if candidate.Kind != "inference_model" {
				continue
			}
			if candidate.StableID == "" {
				return nil, false, fmt.Errorf("cannot stage inference_model candidate without stable_id in cluster %q", candidate.Cluster)
			}
			modelName, providerNamespace, providerName, belongsToTenant := splitTenantWithdrawalStableID(candidate.StableID, namespace)
			// An attached model has an independent lifecycle in its source
			// namespace. A tenant-local model may still use a cross-namespace
			// provider; that route is part of this withdrawal and must remain
			// available until the replacement ExtProc revision is serving.
			if envelope.IsScopedStableID(candidate.StableID) && !belongsToTenant {
				continue
			}
			if _, current := byID[candidate.StableID]; current {
				continue
			}
			if modelName == "" || providerName == "" || candidate.Cluster == "" {
				return nil, false, fmt.Errorf("cannot stage stale candidate %q with cluster %q", candidate.StableID, candidate.Cluster)
			}
			route, err := r.routeForWithdrawnCandidate(ctx, namespace, candidate, modelName, providerNamespace, providerName, transportRoutes, apiTranslationEnabled)
			if errors.Is(err, errWithdrawnProviderGone) {
				continue
			}
			if err != nil {
				return nil, false, err
			}
			byID[candidate.StableID] = route
			union = append(union, route)
		}
	}

	currentIDs := make(map[string]bool, len(desired)*2)
	for _, route := range desired {
		for _, id := range desiredCandidateIDs(route) {
			currentIDs[id] = true
		}
	}
	stale := false
	for id := range byID {
		if !currentIDs[id] {
			stale = true
			break
		}
	}
	return union, stale, nil
}

func desiredCandidateIDs(route resolver.Route) []string {
	ids := []string{envelope.RouteStableID(route), envelope.CandidateStableID(route.Model, route.Provider)}
	if route.CrossNamespace() {
		ids = append(ids, envelope.ScopedStableID(route.Namespace, route.Model, route.ProviderNamespace, route.Provider))
	}
	return ids
}

// splitTenantWithdrawalStableID returns the model and provider identity for a
// candidate owned by a model in namespace. Legacy local IDs have the form
// model/provider-name; scoped IDs for a cross-namespace provider have the
// form namespace/model/provider-namespace/provider-name. Scoped candidates
// whose model namespace differs belong to independently reconciled
// attachments and are not part of this tenant's withdrawal.
func splitTenantWithdrawalStableID(stableID, namespace string) (model, providerNamespace, provider string, belongsToTenant bool) {
	if !envelope.IsScopedStableID(stableID) {
		model, provider, ok := splitCandidateStableID(stableID)
		return model, "", provider, ok
	}
	parts := strings.Split(stableID, "/")
	if len(parts) != 4 || parts[0] != namespace {
		return "", "", "", false
	}
	return parts[1], parts[2], parts[3], true
}

func (r *Reconciler) providerNamespaceForService(ctx context.Context, modelNamespace, modelName, providerName, serviceName string) (string, bool, error) {
	if serviceName == providerServicePrefix+providerName {
		return modelNamespace, true, nil
	}
	var providers v1alpha1.ExternalProviderList
	if err := r.List(ctx, &providers); err != nil {
		return "", false, fmt.Errorf("list ExternalProviders to identify withdrawn transport %s: %w", serviceName, err)
	}
	for i := range providers.Items {
		provider := &providers.Items[i]
		if provider.Name != providerName {
			continue
		}
		route := resolver.Route{Model: modelName, Namespace: modelNamespace, Provider: providerName, ProviderNamespace: provider.Namespace}
		if route.ServiceName() == serviceName {
			return provider.Namespace, true, nil
		}
	}
	return "", false, nil
}

func splitCandidateStableID(stableID string) (model, provider string, ok bool) {
	index := strings.LastIndex(stableID, "/provider-")
	if index <= 0 || index+len("/provider-") >= len(stableID) {
		return "", "", false
	}
	return stableID[:index], stableID[index+len("/provider-"):], true
}

func (r *Reconciler) routeForWithdrawnCandidate(
	ctx context.Context,
	namespace string,
	candidate envelope.Candidate,
	modelName, providerNamespace, providerName string,
	applied map[string]resolver.Route,
	apiTranslationEnabled bool,
) (resolver.Route, error) {
	stableID := candidate.StableID
	route, appliedRoute := applied[stableID]
	var model v1alpha1.ExternalModel
	if !appliedRoute {
		if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: modelName}, &model); err != nil && !apierrors.IsNotFound(err) {
			return resolver.Route{}, fmt.Errorf("read ExternalModel %s/%s for withdrawn route: %w", namespace, modelName, err)
		}
		if providerNamespace == "" {
			providerNamespace = namespace
		}
		if providerNamespace == namespace {
			providerNamespace = ""
		}
		if providerNamespace == "" && candidate.Cluster != providerServicePrefix+providerName {
			resolvedNamespace, found, err := r.providerNamespaceForService(ctx, namespace, modelName, providerName, candidate.Cluster)
			if err != nil {
				return resolver.Route{}, err
			}
			if !found {
				return resolver.Route{}, fmt.Errorf("cannot identify provider namespace for withdrawn candidate %q and cluster %q", stableID, candidate.Cluster)
			}
			providerNamespace = resolvedNamespace
		}
		if providerNamespace == "" {
			providerNamespace = namespace
		}
		clientName := candidate.Name
		if clientName == "" {
			clientName = model.Spec.ModelName
			if clientName == "" {
				clientName = modelName
			}
		}
		route = resolver.Route{
			Model: modelName, ClientName: clientName, Namespace: namespace,
			Provider: providerName, ProviderNamespace: providerNamespace,
			APITranslationEnabled: apiTranslationEnabled,
			StableID:              candidate.StableID,
		}
	}
	if appliedRoute {
		route.ClientName = candidate.Name
		route.Cluster = candidate.Cluster
	}
	var provider v1alpha1.ExternalProvider
	if err := r.Get(ctx, client.ObjectKey{Namespace: route.ProviderNamespace, Name: providerName}, &provider); err != nil {
		if apierrors.IsNotFound(err) {
			if !appliedRoute {
				return resolver.Route{}, fmt.Errorf("%w: %s/%s", errWithdrawnProviderGone, route.ProviderNamespace, providerName)
			}
			// An applied snapshot carries the credential/header semantics that
			// cannot safely be guessed from an absent provider. Legacy public
			// routes need no credential reconstruction; other legacy routes hold.
			if route.ProviderType != "" || candidate.Credential == nil {
				route.APITranslationEnabled = apiTranslationEnabled
				route.StableID = candidate.StableID
				return route, nil
			}
		}
		return resolver.Route{}, fmt.Errorf("read ExternalProvider %s/%s for withdrawn route: %w", route.ProviderNamespace, providerName, err)
	}
	if candidate.APIFormat != "" || !appliedRoute {
		route.APIFormat, route.Path, route.TargetModel = candidate.APIFormat, candidate.Path, candidate.TargetModel
	}
	route.APITranslationEnabled = apiTranslationEnabled
	route.ProviderType = provider.Spec.Provider
	route.TLSCACertificates = strings.TrimSpace(provider.Spec.Config["tls.caCertificates"])
	if route.Cluster == "" {
		route.Cluster = candidate.Cluster
		if route.Cluster == "" {
			route.Cluster = providerServicePrefix + providerName
		}
	}
	if route.Endpoint == "" {
		route.Endpoint = provider.Spec.Endpoint
	}
	if candidate.Credential != nil {
		// The prior serving envelope is authoritative during drain. Its
		// SecretRef may name a model override or an attachment copy rather than
		// the provider's default credential.
		route.AuthType = "apikey"
		route.SecretName = candidate.Credential.SecretRef.Name
		route.SecretNamespace = candidate.Credential.SecretRef.Namespace
		route.SecretKey = candidate.Credential.SecretRef.Key
	} else {
		route.AuthType = provider.Spec.Auth.Type
		route.SecretName = provider.Spec.Auth.SecretRef.Name
		route.SecretNamespace = provider.Namespace
		route.SecretKey = "api-key"
	}
	route.StableID = candidate.StableID
	return route, nil
}

// readAppliedModelRoutes parses only controller-owned model HTTPRoutes in the
// tenant namespace. These rules may outlive the published overlay while a
// staged withdrawal is draining.
func (r *Reconciler) readAppliedModelRoutes(ctx context.Context, namespace string) (map[string]resolver.Route, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRouteList"})
	if err := r.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels{
		"app.kubernetes.io/managed-by": "ai-gateway-controller",
	}); err != nil {
		return nil, fmt.Errorf("list applied ExternalModel routes in %s: %w", namespace, err)
	}
	result := map[string]resolver.Route{}
	for i := range list.Items {
		route := &list.Items[i]
		modelName := route.GetLabels()["inference.opendatahub.io/external-model"]
		if modelName == "" {
			continue
		}
		rules, _, err := unstructured.NestedSlice(route.Object, "spec", "rules")
		if err != nil {
			return nil, fmt.Errorf("read rules from HTTPRoute %s/%s: %w", namespace, route.GetName(), err)
		}
		profiles, err := tenant.HomeRuntimeRouteProfiles(route, namespace)
		if err != nil {
			return nil, err
		}
		profileByID := map[string]resolver.Route{}
		for _, profile := range profiles {
			profileByID[envelope.RouteStableID(profile)] = profile
		}
		clientName := routeClientName(rules, namespace, modelName)
		for _, rawRule := range rules {
			rule, ok := rawRule.(map[string]any)
			if !ok {
				continue
			}
			stableID := providerStableIDFromRule(rule)
			refs, _, _ := unstructured.NestedSlice(rule, "backendRefs")
			if stableID == "" || len(refs) == 0 {
				continue
			}
			backend, ok := refs[0].(map[string]any)
			if !ok {
				continue
			}
			serviceName, _ := backend["name"].(string)
			if !strings.HasPrefix(serviceName, providerServicePrefix) || strings.HasPrefix(serviceName, providerSelectionSinkPrefix) {
				continue
			}
			candidateModel, providerNamespace, providerName, belongsToTenant := splitTenantWithdrawalStableID(stableID, namespace)
			if !belongsToTenant || candidateModel != modelName || providerName == "" {
				continue
			}
			resolvedNamespace := providerNamespace
			if resolvedNamespace != "" {
				probe := resolver.Route{Model: modelName, Namespace: namespace, Provider: providerName, ProviderNamespace: resolvedNamespace}
				if probe.ServiceName() != serviceName {
					continue
				}
			} else {
				var found bool
				var resolveErr error
				resolvedNamespace, found, resolveErr = r.providerNamespaceForService(ctx, namespace, modelName, providerName, serviceName)
				if resolveErr != nil {
					return nil, resolveErr
				}
				if !found {
					continue
				}
			}
			resolvedRoute := resolver.Route{Model: modelName, Namespace: namespace, Provider: providerName, ProviderNamespace: resolvedNamespace, Cluster: serviceName}
			endpoint, err := providerEndpointFromRule(rule, backend)
			if err != nil {
				return nil, fmt.Errorf("read provider endpoint from HTTPRoute %s/%s: %w", namespace, route.GetName(), err)
			}
			if endpoint == "" {
				continue
			}
			resolvedRoute.ClientName, resolvedRoute.Cluster, resolvedRoute.Endpoint = clientName, serviceName, endpoint
			if profile, found := profileByID[stableID]; found {
				profile.Endpoint, profile.Cluster = endpoint, serviceName
				resolvedRoute = profile
			}
			result[stableID] = resolvedRoute
		}
	}
	return result, nil
}

func (r *Reconciler) providerTransportStillInUse(ctx context.Context, namespace, providerName string) (bool, error) {
	env, err := r.readPublishedEnvelope(ctx, namespace)
	if err != nil {
		return false, err
	}
	cluster := providerServicePrefix + providerName
	if env != nil {
		for _, candidate := range env.Overlay.Candidates {
			if candidate.Cluster == cluster {
				return true, nil
			}
		}
	}
	routes, err := r.readAppliedModelRoutes(ctx, namespace)
	if err != nil {
		return false, err
	}
	for _, route := range routes {
		if route.Provider == providerName {
			return true, nil
		}
	}
	// Attached routes live in the source ExternalModel namespace and do not
	// carry the home-route label parsed above. They still use this namespace's
	// provider Service, so the provider finalizer must wait for their staged
	// route withdrawal too.
	attachments, err := r.listAttachmentRoutes(ctx, r.APIReader)
	if err != nil {
		return false, err
	}
	for i := range attachments {
		route := resolver.Route{Namespace: attachments[i].GetNamespace(), ProviderNamespace: namespace, Provider: providerName}
		if routeBackends(attachments[i : i+1])[route.ServiceName()] {
			return true, nil
		}
	}
	return false, nil
}

// providerWithdrawalWaitMessage makes the safe provider-deletion contract
// visible while a provider finalizer waits for model references and serving
// route state to be withdrawn. The controller deliberately does not discard a
// last-known-good route just because its provider object is terminating.
func (r *Reconciler) providerWithdrawalWaitMessage(ctx context.Context, namespace, providerName string) (string, error) {
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models); err != nil {
		return "", fmt.Errorf("list ExternalModels while reporting provider withdrawal: %w", err)
	}
	var references []string
	for i := range models.Items {
		model := &models.Items[i]
		for _, ref := range model.Spec.ExternalProviderRefs {
			refNamespace := ref.Ref.Namespace
			if refNamespace == "" {
				refNamespace = model.Namespace
			}
			if refNamespace == namespace && ref.Ref.Name == providerName {
				references = append(references, model.Namespace+"/"+model.Name)
				break
			}
		}
	}
	sort.Strings(references)
	if len(references) > 0 {
		return "provider deletion is waiting for referencing ExternalModel resources to be updated or deleted before the serving route and in-flight request drain can complete: " + strings.Join(references, ", "), nil
	}
	return "provider deletion is waiting for controller-owned serving routes to complete their reduced-runtime rollout and in-flight request drain", nil
}

func providerStableIDFromRule(rule map[string]any) string {
	matches, _, _ := unstructured.NestedSlice(rule, "matches")
	for _, rawMatch := range matches {
		match, ok := rawMatch.(map[string]any)
		if !ok {
			continue
		}
		headers, _, _ := unstructured.NestedSlice(match, "headers")
		for _, rawHeader := range headers {
			header, ok := rawHeader.(map[string]any)
			if !ok {
				continue
			}
			name, _ := header["name"].(string)
			value, _ := header["value"].(string)
			if strings.EqualFold(name, selectedProviderHeader) {
				return value
			}
		}
	}
	return ""
}

func providerEndpointFromRule(rule, backend map[string]any) (string, error) {
	filters, _, _ := unstructured.NestedSlice(rule, "filters")
	host := ""
	for _, rawFilter := range filters {
		filter, ok := rawFilter.(map[string]any)
		if !ok {
			continue
		}
		if hostname, found, _ := unstructured.NestedString(filter, "urlRewrite", "hostname"); found && hostname != "" {
			host = hostname
			break
		}
	}
	if host == "" {
		return "", nil
	}
	port, found, err := unstructured.NestedInt64(backend, "port")
	if err != nil {
		return "", fmt.Errorf("read backendRef port: %w", err)
	}
	if !found {
		port = 443
	}
	if port < 1 || port > 65535 {
		return "", fmt.Errorf("backendRef port %d is outside the valid range", port)
	}
	if port == 443 {
		return host, nil
	}
	return net.JoinHostPort(strings.Trim(host, "[]"), strconv.FormatInt(port, 10)), nil
}

func routeClientName(rules []any, namespace, model string) string {
	prefix := "/" + namespace + "/"
	for _, rawRule := range rules {
		rule, ok := rawRule.(map[string]any)
		if !ok {
			continue
		}
		matches, _, _ := unstructured.NestedSlice(rule, "matches")
		for _, rawMatch := range matches {
			match, ok := rawMatch.(map[string]any)
			if !ok {
				continue
			}
			path, found, _ := unstructured.NestedString(match, "path", "value")
			if found && strings.HasPrefix(path, prefix) {
				return strings.TrimPrefix(path, prefix)
			}
		}
	}
	return model
}

func (r *Reconciler) externalModelRolloutSafe(ctx context.Context, namespace, tenantID, expectedConfigHash string) (bool, error) {
	if r.readPodLogs == nil {
		return false, errors.New("pod log reader is required to verify ExternalModel serving overlay revisions")
	}
	var overlayRevision string
	env, err := r.readPublishedEnvelope(ctx, namespace)
	if err != nil {
		return false, err
	}
	if env == nil || env.Revision.Value == "" {
		return false, nil
	}
	overlayRevision = env.Revision.Value
	deployment, replicas, safe, err := r.externalModelDeploymentSafe(ctx, namespace, tenantID, expectedConfigHash)
	if err != nil || !safe {
		return safe, err
	}
	currentPods, safe, err := r.externalModelPodsSafe(ctx, namespace, deployment, replicas, expectedConfigHash, overlayRevision)
	if err != nil || !safe {
		return safe, err
	}
	return r.externalModelEndpointsSafe(ctx, namespace, tenantID, currentPods)
}

func (r *Reconciler) finishProviderWithdrawal(ctx context.Context, namespace, tenantID, gatewayName, gatewayNamespace string,
	models []*v1alpha1.ExternalModel, owners map[string]*v1alpha1.ExternalModel, desired []resolver.Route,
	attached []attachedModel, revision string, pending, apiTranslationEnabled bool, state map[string]attachedModel) (time.Duration, error) {
	if !pending {
		return 0, r.clearWithdrawalDrainStateIfNoAttachmentWithdrawal(ctx, attachmentTenant{
			namespace: namespace, tenantID: tenantID,
			gateway:               v1alpha1.NamespacedObjectReference{Name: gatewayName, Namespace: gatewayNamespace},
			apiTranslationEnabled: apiTranslationEnabled,
		})
	}
	runtimeHash, err := runtimeConfigHash(ctx, r, models, namespace, apiTranslationEnabled)
	if err != nil {
		return 0, err
	}
	safe, err := r.externalModelRolloutSafe(ctx, namespace, tenantID, runtimeHash)
	if errors.Is(err, errServingRevisionUnknown) {
		return withdrawalRequeueInterval, nil
	}
	if err != nil {
		return 0, err
	}
	remaining, err := r.withdrawalDrainRemaining(ctx, namespace, revision, safe, time.Now())
	if err != nil || remaining > 0 {
		return remaining, err
	}
	if err := r.applyTransport(ctx, desired, namespace, tenantID, gatewayName, gatewayNamespace, owners, attached, state, apiTranslationEnabled); err != nil {
		return 0, err
	}
	if err := r.cleanupExternalModelRouteFilters(ctx, namespace, tenantID, gatewayName, gatewayNamespace); err != nil {
		return 0, err
	}
	if err := r.cleanupModelTransport(ctx, namespace, desired); err != nil {
		return 0, err
	}
	return 0, r.clearWithdrawalDrainStateIfNoAttachmentWithdrawal(ctx, attachmentTenant{
		namespace: namespace, tenantID: tenantID,
		gateway:               v1alpha1.NamespacedObjectReference{Name: gatewayName, Namespace: gatewayNamespace},
		apiTranslationEnabled: apiTranslationEnabled,
	})
}

func (r *Reconciler) clearWithdrawalDrainStateIfNoAttachmentWithdrawal(ctx context.Context, t attachmentTenant) error {
	attachmentWithdrawalPending, err := r.hasPendingAttachmentWithdrawal(ctx, t)
	if err != nil {
		return err
	}
	if attachmentWithdrawalPending {
		// The drain annotation is shared by the tenant's runtime. A home-model
		// reconcile must not clear another attachment's active drain state.
		return nil
	}
	return r.clearWithdrawalDrainState(ctx, t.namespace)
}

func (r *Reconciler) hasPendingAttachmentWithdrawal(ctx context.Context, t attachmentTenant) (bool, error) {
	candidates, err := r.attachmentCandidates(ctx, t)
	if err != nil {
		return false, err
	}
	for _, candidate := range candidates {
		withdrawn, err := r.withdrawnAttachmentCandidates(ctx, candidate, t)
		if err != nil {
			return false, err
		}
		if len(withdrawn) > 0 {
			return true, nil
		}
	}
	return false, nil
}

func (r *Reconciler) externalModelDeploymentSafe(ctx context.Context, namespace, tenantID, expectedConfigHash string) (*appsv1.Deployment, int32, bool, error) {
	reader := r.APIReader
	if reader == nil {
		return nil, 0, false, errors.New("APIReader is required to verify ExternalModel serving endpoints")
	}
	deploymentName := tenant.PayloadProcessingExternalModelDeploymentName(tenantID)
	deployment := &appsv1.Deployment{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: deploymentName}, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, 0, false, nil
		}
		return nil, 0, false, fmt.Errorf("read ExternalModel ExtProc Deployment %s/%s: %w", namespace, deploymentName, err)
	}
	if deployment.Labels["app.kubernetes.io/managed-by"] != "ai-gateway-controller" || deployment.Labels["app"] != tenant.PayloadProcessingExternalModelName {
		return nil, 0, false, nil
	}
	templateHash := deployment.Spec.Template.Annotations[extprocConfigHashAnnotation]
	if expectedConfigHash == "" || templateHash != expectedConfigHash {
		return nil, 0, false, nil
	}
	var configMap corev1.ConfigMap
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: tenant.PayloadProcessingExternalModelPluginsConfigMapForTenant(tenantID)}, &configMap); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, 0, false, nil
		}
		return nil, 0, false, fmt.Errorf("read ExternalModel ExtProc config: %w", err)
	}
	if configMap.Labels["app.kubernetes.io/managed-by"] != "ai-gateway-controller" {
		return nil, 0, false, nil
	}
	extprocConfig := configMap.Data["extproc.yaml"]
	if extprocConfig == "" {
		return nil, 0, false, nil
	}
	configDigest := sha256.Sum256([]byte(extprocConfig))
	if hex.EncodeToString(configDigest[:]) != expectedConfigHash {
		return nil, 0, false, nil
	}
	if deployment.Status.ObservedGeneration != deployment.Generation {
		return nil, 0, false, nil
	}
	replicas := int32(1)
	if deployment.Spec.Replicas != nil {
		replicas = *deployment.Spec.Replicas
	}
	if replicas <= 0 || deployment.Status.Replicas != replicas || deployment.Status.UpdatedReplicas != replicas ||
		deployment.Status.ReadyReplicas != replicas || deployment.Status.AvailableReplicas != replicas || deployment.Status.UnavailableReplicas != 0 {
		return nil, 0, false, nil
	}
	if deployment.Status.TerminatingReplicas != nil && *deployment.Status.TerminatingReplicas != 0 {
		return nil, 0, false, nil
	}
	if deployment.Spec.Selector == nil || len(deployment.Spec.Selector.MatchLabels) == 0 {
		return nil, 0, false, nil
	}
	return deployment, replicas, true, nil
}

func (r *Reconciler) externalModelPodsSafe(ctx context.Context, namespace string, deployment *appsv1.Deployment, replicas int32, expectedConfigHash, expectedOverlayRevision string) (map[types.UID]bool, bool, error) {
	reader := r.APIReader
	selector := client.MatchingLabels(deployment.Spec.Selector.MatchLabels)
	var replicaSets appsv1.ReplicaSetList
	if err := reader.List(ctx, &replicaSets, client.InNamespace(namespace), selector); err != nil {
		return nil, false, fmt.Errorf("list ExternalModel ExtProc ReplicaSets: %w", err)
	}
	ownedReplicaSets := make([]*appsv1.ReplicaSet, 0, len(replicaSets.Items))
	for i := range replicaSets.Items {
		rs := &replicaSets.Items[i]
		if metav1.GetControllerOf(rs) != nil && metav1.GetControllerOf(rs).UID == deployment.UID {
			ownedReplicaSets = append(ownedReplicaSets, rs)
		}
	}
	var currentRS *appsv1.ReplicaSet
	var currentRevision int64 = -1
	for _, rs := range ownedReplicaSets {
		revision, _ := strconv.ParseInt(rs.Annotations["deployment.kubernetes.io/revision"], 10, 64)
		if revision > currentRevision {
			currentRevision, currentRS = revision, rs
		}
	}
	if currentRS == nil || currentRS.Spec.Template.Annotations[extprocConfigHashAnnotation] != expectedConfigHash ||
		currentRS.Status.Replicas != replicas || currentRS.Status.ReadyReplicas != replicas || currentRS.Status.AvailableReplicas != replicas {
		return nil, false, nil
	}
	for _, rs := range ownedReplicaSets {
		if rs.UID == currentRS.UID {
			continue
		}
		if rs.Status.Replicas != 0 || rs.Status.ReadyReplicas != 0 || rs.Status.AvailableReplicas != 0 {
			return nil, false, nil
		}
	}

	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.InNamespace(namespace), selector); err != nil {
		return nil, false, fmt.Errorf("list ExternalModel ExtProc pods: %w", err)
	}
	currentPods := map[types.UID]bool{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			continue
		}
		owner := metav1.GetControllerOf(pod)
		if owner == nil {
			return nil, false, nil
		}
		belongsToDeployment := false
		for _, rs := range ownedReplicaSets {
			if owner.UID == rs.UID {
				belongsToDeployment = true
				if rs.UID != currentRS.UID || !pod.DeletionTimestamp.IsZero() || !podReady(pod) {
					return nil, false, nil
				}
				servesExpectedRevision, err := r.podServesOverlayRevision(ctx, pod, expectedOverlayRevision)
				if err != nil {
					return nil, false, err
				}
				if !servesExpectedRevision {
					return nil, false, nil
				}
				currentPods[pod.UID] = true
				break
			}
		}
		if !belongsToDeployment {
			return nil, false, nil
		}
	}
	if len(currentPods) != int(replicas) {
		return nil, false, nil
	}
	return currentPods, true, nil
}

func (r *Reconciler) podServesOverlayRevision(ctx context.Context, pod *corev1.Pod, expectedRevision string) (bool, error) {
	logs, err := r.readPodLogs(ctx, pod.Namespace, pod.Name, externalModelExtProcContainer)
	if err != nil {
		return false, fmt.Errorf("%w: read pod %s/%s logs: %w", errServingRevisionUnknown, pod.Namespace, pod.Name, err)
	}
	lines := strings.Split(logs, "\n")
	for index := len(lines) - 1; index >= 0; index-- {
		line := terminalEscapePattern.ReplaceAllString(lines[index], "")
		if !strings.Contains(line, "accepted_revision") {
			continue
		}
		match := extprocServingRevisionPattern.FindStringSubmatch(line)
		if len(match) != 5 {
			return false, errServingRevisionUnknown
		}
		accepted := match[1] + match[2]
		serving := match[3] + match[4]
		return accepted == expectedRevision && serving == expectedRevision, nil
	}
	return false, errServingRevisionUnknown
}

func (r *Reconciler) externalModelEndpointsSafe(ctx context.Context, namespace, tenantID string, currentPods map[types.UID]bool) (bool, error) {
	reader := r.APIReader
	serviceName := tenant.PayloadProcessingExternalModelServiceName(tenantID)
	var slices discv1.EndpointSliceList
	if err := reader.List(ctx, &slices, client.InNamespace(namespace), client.MatchingLabels{"kubernetes.io/service-name": serviceName}); err != nil {
		return false, fmt.Errorf("list ExternalModel ExtProc EndpointSlices: %w", err)
	}
	if len(slices.Items) == 0 {
		return false, nil
	}
	servingPods := map[types.UID]bool{}
	for i := range slices.Items {
		for _, endpoint := range slices.Items[i].Endpoints {
			readyOrUnknown := endpoint.Conditions.Ready == nil || *endpoint.Conditions.Ready
			serving := endpoint.Conditions.Serving != nil && *endpoint.Conditions.Serving
			if !readyOrUnknown && !serving {
				continue
			}
			if endpoint.TargetRef == nil || endpoint.TargetRef.Kind != "Pod" || endpoint.TargetRef.UID == "" || !currentPods[endpoint.TargetRef.UID] {
				return false, nil
			}
			servingPods[endpoint.TargetRef.UID] = true
		}
	}
	if len(servingPods) != len(currentPods) {
		return false, nil
	}
	return true, nil
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (r *Reconciler) withdrawalDrainRemaining(ctx context.Context, namespace, revision string, safe bool, now time.Time) (time.Duration, error) {
	var cm corev1.ConfigMap
	key := client.ObjectKey{Namespace: namespace, Name: r.configMapName()}
	if err := r.APIReader.Get(ctx, key, &cm); err != nil {
		return 0, fmt.Errorf("read withdrawal drain state %s: %w", key, err)
	}
	if cm.Labels["app.kubernetes.io/managed-by"] != "ai-gateway-controller" {
		return 0, fmt.Errorf("withdrawal drain state ConfigMap %s is not controller-owned", key)
	}
	var state withdrawalDrainState
	stateExists := false
	if raw := cm.Annotations[withdrawalDrainAnnotation]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &state); err == nil && state.Revision != "" && !state.StartedAt.IsZero() {
			stateExists = true
		}
	}
	if !safe {
		if stateExists {
			if err := r.patchWithdrawalDrainState(ctx, &cm, ""); err != nil {
				return 0, err
			}
		}
		return withdrawalRequeueInterval, nil
	}
	if !stateExists || state.Revision != revision {
		state = withdrawalDrainState{Revision: revision, StartedAt: now.UTC()}
		encoded, err := json.Marshal(state)
		if err != nil {
			return 0, fmt.Errorf("encode withdrawal drain state: %w", err)
		}
		if err := r.patchWithdrawalDrainState(ctx, &cm, string(encoded)); err != nil {
			return 0, err
		}
		return withdrawalDrainDuration, nil
	}
	remaining := withdrawalDrainDuration - now.Sub(state.StartedAt)
	if remaining > 0 {
		return remaining, nil
	}
	return 0, nil
}

func (r *Reconciler) clearWithdrawalDrainState(ctx context.Context, namespace string) error {
	var cm corev1.ConfigMap
	key := client.ObjectKey{Namespace: namespace, Name: r.configMapName()}
	if err := r.APIReader.Get(ctx, key, &cm); err != nil {
		return client.IgnoreNotFound(err)
	}
	if cm.Annotations[withdrawalDrainAnnotation] == "" {
		return nil
	}
	if cm.Labels["app.kubernetes.io/managed-by"] != "ai-gateway-controller" {
		return fmt.Errorf("withdrawal drain state ConfigMap %s is not controller-owned", key)
	}
	return r.patchWithdrawalDrainState(ctx, &cm, "")
}

func (r *Reconciler) patchWithdrawalDrainState(ctx context.Context, cm *corev1.ConfigMap, value string) error {
	base := cm.DeepCopy()
	if cm.Annotations == nil {
		cm.Annotations = map[string]string{}
	}
	if value == "" {
		delete(cm.Annotations, withdrawalDrainAnnotation)
	} else {
		cm.Annotations[withdrawalDrainAnnotation] = value
	}
	if err := r.Patch(ctx, cm, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("persist withdrawal drain state on %s/%s: %w", cm.Namespace, cm.Name, err)
	}
	return nil
}

func runtimeConfigHash(ctx context.Context, r *Reconciler, models []*v1alpha1.ExternalModel, namespace string, apiTranslationEnabled bool) (string, error) {
	var providerList v1alpha1.ExternalProviderList
	if err := r.List(ctx, &providerList, client.InNamespace(namespace)); err != nil {
		return "", fmt.Errorf("list ExternalProviders for ExtProc runtime config: %w", err)
	}
	providers := make([]*v1alpha1.ExternalProvider, 0, len(providerList.Items))
	for i := range providerList.Items {
		if providerList.Items[i].DeletionTimestamp.IsZero() {
			providers = append(providers, &providerList.Items[i])
		}
	}
	routes, err := resolver.ResolveAllWithAPITranslation(models, providers, apiTranslationEnabled)
	if errors.Is(err, resolver.ErrNoRoutes) {
		// The tenant's ExtProc runtime also serves scoped attachment
		// candidates. An empty home candidate set is valid when those scoped
		// candidates define the complete runtime.
		routes = &resolver.ResolvedRouteSet{}
	} else if err != nil {
		return "", fmt.Errorf("resolve current ExternalModel ExtProc runtime routes: %w", err)
	}
	candidates, err := tenant.ExternalModelRuntimeCandidates(routes, apiTranslationEnabled)
	if err != nil {
		return "", err
	}
	candidates, err = tenant.RetainHomeRuntimeCandidates(ctx, r.Client, namespace, candidates, apiTranslationEnabled)
	if err != nil {
		return "", err
	}
	// The tenant runtime appends scoped candidates from the published overlay.
	// Include them so the expected hash matches a Deployment serving both home
	// models and attachments.
	if published, err := r.readPublishedEnvelope(ctx, namespace); err != nil {
		return "", err
	} else if published != nil {
		for _, candidate := range published.Overlay.Candidates {
			if !envelope.IsScopedStableID(candidate.StableID) {
				continue
			}
			candidate = cloneCandidate(candidate)
			if candidate.Credential != nil {
				ref := candidate.Credential.SecretRef
				if r.APIReader == nil {
					return "", errors.New("APIReader is required to verify attachment credential references")
				}
				secret := &metav1.PartialObjectMetadata{}
				secret.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
				if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, secret); err != nil {
					if apierrors.IsNotFound(err) {
						continue
					}
					return "", fmt.Errorf("read attachment credential copy %s/%s: %w", ref.Namespace, ref.Name, err)
				}
				if secret.GetLabels()[tenant.LabelCredentialCopy] == "true" {
					if header := secret.GetAnnotations()[tenant.AnnotationCredentialHeader]; envelope.IsQualifiedAuthHeader(header) {
						candidate.Credential.Header = header
					}
				}
			}
			candidates = append(candidates, candidate)
		}
	}
	hash, err := tenant.ExternalModelExtProcConfigHash(namespace, candidates, apiTranslationEnabled)
	if err != nil {
		return "", fmt.Errorf("compute current ExternalModel ExtProc config hash: %w", err)
	}
	return hash, nil
}

func cloneCandidate(candidate envelope.Candidate) envelope.Candidate {
	cloned := candidate
	if candidate.Credential != nil {
		credential := *candidate.Credential
		cloned.Credential = &credential
	}
	return cloned
}

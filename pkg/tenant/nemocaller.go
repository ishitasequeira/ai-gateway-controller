/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tenant

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/yaml"

	aigatewayv1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/aigateway/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/guardrail"
)

const (
	// nemoCalloutConfigAnnotation rolls the post-auth pod when the compiled
	// callouts change. Praxis reads extproc.yaml only at start.
	nemoCalloutConfigAnnotation = "aigateway.opendatahub.io/nemo-callouts-sha256"

	// nemoServerPort is the NeMo server's pod port. With auth off its Service
	// maps port 80 to it; OpenShift matches NetworkPolicy egress against the
	// pod port.
	nemoServerPort int64 = 8000
	// nemoProxyPort is kube-rbac-proxy's pod port. With auth on the Service
	// maps port 443 to it.
	nemoProxyPort int64 = 8443
)

// errUncompilableCallout is returned, joined with one error per policy, when
// an accepted AIGuardrail cannot be compiled. The callouts returned alongside
// it are the policies that compiled; one bad policy does not drop the others.
var errUncompilableCallout = errors.New("accepted AIGuardrail cannot be compiled")

// errMalformedAttachment means one guardrails entry could not be fully read.
// The catalog fails closed for what that entry could reference; every other
// entry is still used.
var errMalformedAttachment = errors.New("guardrail attachment is malformed")

// errStaleBinding means an accepted policy's spec changed and its status has
// not caught up. Publishing now would remove checks that are still selected.
var errStaleBinding = errors.New("accepted guardrail status is behind the spec")

// errProviderOutsideInfra means an accepted policy's NeMo Service is not in
// the tenant's infrastructure namespace, so its checks fail closed.
var errProviderOutsideInfra = errors.New("guardrail provider is outside the infrastructure namespace")

const (
	// reasonProviderOutsideInfra is the reason of the Warning Event
	// reportNemoOutsideInfra publishes on the AIGuardrail.
	reasonProviderOutsideInfra = "ProviderOutsideInfraNamespace"
	// compileGuardrailAction is that Event's action. The events API requires one.
	compileGuardrailAction = "CompileGuardrail"
)

// nemoCallout is one accepted AIGuardrail. Each of its checks is compiled
// as its own ai_guardrails filter on the shared post-auth ExtProc.
type nemoCallout struct {
	name     string
	endpoint string
	// serviceNamespace is where the NeMo Service runs; egress is opened to it.
	serviceNamespace string
	// serviceName is the NeMo Service. Egress uses its pod selector when the
	// Service can be read; NetworkPolicy cannot name a Service directly. It is
	// also the Service kube-rbac-proxy authorizes callers against.
	serviceName string
	// podLabels come from that Service's selector. Empty means the egress
	// rule covers the whole namespace on the NeMo port.
	podLabels map[string]string
	// authenticated means the Service is behind kube-rbac-proxy: egress opens
	// its port, and the call carries the pod's ServiceAccount token and trusts
	// the service CA.
	authenticated bool
	// closed rejects a request that selects this policy instead of calling NeMo.
	// The policy is accepted, but this controller cannot compile a callout for it.
	closed  bool
	timeout time.Duration
	checks  []nemoCheck
}

// nemoCheck is one AIGuardrail check. Request and response are the Input
// and Output phases. A check with both phases is compiled twice, and both
// phases keep the same catalog order.
type nemoCheck struct {
	name     string
	configID string
	// header turns the check on for a request when its value is "true".
	header   string
	request  bool
	response bool
	// closed rejects the request when the selection header is "true".
	closed bool
}

// nemoFilter is one compiled ai_guardrails entry. Exactly one of request
// or response is set.
type nemoFilter struct {
	endpoint      string
	configID      string
	header        string
	authenticated bool
	timeout       time.Duration
	request       bool
	response      bool
	closed        bool
}

// acceptedNemoCallouts returns the checks this tenant's attachments select, in
// catalog order. A policy no attachment references is ignored, and only the
// checks an attachment selects are returned.
// An attached policy whose accepted status is behind its spec fails the call:
// publishing would drop checks that are still selected. Every other attached
// policy that is not a current call — refused, deleting, or accepted but
// impossible to call — is returned as a closed check, so a request that
// selects it is rejected. With outputUnsupported, a policy with an Output
// check is also closed rather than compiled with a response hook that would
// not run; see OutputChecksUnsupported. A policy whose endpoint is not its
// NeMo Service in an AI Gateway infrastructure namespace is closed too;
// pinNemoCalloutsToInfra narrows that to this tenant's.
func acceptedNemoCallouts(ctx context.Context, reader client.Reader, tenantNamespace string, outputUnsupported bool) ([]nemoCallout, error) {
	refs, attachErr := catalogAttachments(ctx, reader, tenantNamespace)
	if attachErr != nil && !errors.Is(attachErr, errMalformedAttachment) {
		return nil, attachErr
	}
	attached, all := attachedPolicies(refs, tenantNamespace)
	if !all && len(attached) == 0 {
		return nil, attachErr
	}
	var policies aigatewayv1alpha1.AIGuardrailList
	if err := reader.List(ctx, &policies, client.InNamespace(tenantNamespace)); err != nil {
		return nil, fmt.Errorf("list AIGuardrails in %s: %w", tenantNamespace, err)
	}
	callouts := make([]nemoCallout, 0, len(policies.Items))
	var rejected []error
	for i := range policies.Items {
		policy := &policies.Items[i]
		if !all && !attached[policy.Name] {
			continue
		}
		if guardrail.BindingStale(policy) {
			return nil, fmt.Errorf("%w: AIGuardrail %s/%s", errStaleBinding, policy.Namespace, policy.Name)
		}
		if !guardrail.BindingAccepted(policy) {
			if closed, ok := closedCallout(policy); ok {
				callouts = append(callouts, closed)
			}
			continue
		}
		if policy.Status.ProviderEndpoint == "" {
			if closed, ok := closedCallout(policy); ok {
				callouts = append(callouts, closed)
			}
			continue
		}
		callout, err := nemoCalloutFrom(policy)
		if err == nil && outputUnsupported && slices.ContainsFunc(callout.checks, func(c nemoCheck) bool { return c.response }) {
			err = fmt.Errorf("AIGuardrail %s/%s has Output checks, which this tenant cannot enforce", policy.Namespace, policy.Name)
		}
		if err != nil {
			rejected = append(rejected, err)
			if closed, ok := closedCallout(policy); ok {
				callouts = append(callouts, closed)
			}
			continue
		}
		callouts = append(callouts, callout)
	}
	ordered := applyAttachmentOrder(callouts, refs, tenantNamespace)
	var soft []error
	if attachErr != nil {
		soft = append(soft, attachErr)
	}
	if len(rejected) > 0 {
		soft = append(soft, fmt.Errorf("%w: %w", errUncompilableCallout, errors.Join(rejected...)))
	}
	if len(soft) > 0 {
		return ordered, errors.Join(soft...)
	}
	return ordered, nil
}

// softCalloutErr is a problem with one policy or one list entry.
// The checks that could be built are still applied.
func softCalloutErr(err error) bool {
	return errors.Is(err, errUncompilableCallout) || errors.Is(err, errMalformedAttachment)
}

// hasNemoCallouts reports whether the tenant has NeMo checks compiled into
// its post-auth config. A stale acceptance still counts - that check is in
// effect, and failing the model reconcile would mark the model failed for a
// status lag. The tenant reconcile is what waits to publish. A closed check
// counts like a compiled one, so the Output gate cannot change the answer.
func hasNemoCallouts(ctx context.Context, reader client.Reader, tenantNamespace string) (bool, error) {
	callouts, err := acceptedNemoCallouts(ctx, reader, tenantNamespace, false)
	if errors.Is(err, errStaleBinding) {
		return true, nil
	}
	if err != nil && !softCalloutErr(err) {
		return false, err
	}
	return len(callouts) > 0, nil
}

// nemoCalloutInputs is everything acceptedNemoCallouts reads from a policy.
type nemoCalloutInputs struct {
	generation int64
	deleting   bool
	accepted   bool
	revision   string
	endpoint   string
}

func calloutInputs(policy *aigatewayv1alpha1.AIGuardrail) nemoCalloutInputs {
	return nemoCalloutInputs{
		generation: policy.Generation,
		deleting:   !policy.DeletionTimestamp.IsZero(),
		accepted:   guardrail.BindingAccepted(policy),
		revision:   policy.Status.BindingRevision,
		endpoint:   policy.Status.ProviderEndpoint,
	}
}

// nemoCalloutChanged drops AIGuardrail updates that cannot change the
// compiled callouts, such as condition timestamps and finalizer patches.
func nemoCalloutChanged() predicate.Funcs {
	return predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
		before, okBefore := e.ObjectOld.(*aigatewayv1alpha1.AIGuardrail)
		after, okAfter := e.ObjectNew.(*aigatewayv1alpha1.AIGuardrail)
		if !okBefore || !okAfter {
			return true
		}
		return calloutInputs(before) != calloutInputs(after)
	}}
}

func nemoCalloutFrom(policy *aigatewayv1alpha1.AIGuardrail) (nemoCallout, error) {
	if guardrail.ProviderTimeoutTooFine(policy.Spec.Provider.Timeout.Duration) {
		return nemoCallout{}, fmt.Errorf("AIGuardrail %s/%s timeout %s is below 1ms", policy.Namespace, policy.Name, policy.Spec.Provider.Timeout.Duration)
	}
	ref := policy.Spec.Provider.Nemo.Ref
	providerNamespace := ref.Namespace
	if providerNamespace == "" {
		providerNamespace = policy.Namespace
	}
	service, err := guardrail.ParseNemoEndpoint(policy.Status.ProviderEndpoint, providerNamespace, ref.Name)
	if err != nil {
		return nemoCallout{}, fmt.Errorf("AIGuardrail %s/%s provider endpoint: %w", policy.Namespace, policy.Name, err)
	}
	callout := nemoCallout{
		name:             policy.Name,
		endpoint:         policy.Status.ProviderEndpoint,
		serviceName:      service.Name,
		serviceNamespace: service.Namespace,
		authenticated:    service.Authenticated,
		timeout:          policy.Spec.Provider.Timeout.Duration,
	}
	for _, check := range policy.Spec.Checks {
		if check.ConfigID == "" {
			return nemoCallout{}, fmt.Errorf("AIGuardrail %s/%s check %q has an empty configId", policy.Namespace, policy.Name, check.Name)
		}
		compiled := nemoCheck{
			name:     check.Name,
			configID: check.ConfigID,
			header:   guardrail.CheckHeaderName(policy.Namespace, policy.Name, check.Name),
		}
		for _, phase := range check.Phases {
			switch phase {
			case aigatewayv1alpha1.GuardrailPhaseInput:
				compiled.request = true
			case aigatewayv1alpha1.GuardrailPhaseOutput:
				compiled.response = true
			default:
				return nemoCallout{}, fmt.Errorf("AIGuardrail %s/%s check %q has unknown phase %q", policy.Namespace, policy.Name, check.Name, phase)
			}
		}
		if !compiled.request && !compiled.response {
			return nemoCallout{}, fmt.Errorf("AIGuardrail %s/%s check %q has no compilable phase", policy.Namespace, policy.Name, check.Name)
		}
		callout.checks = append(callout.checks, compiled)
	}
	if len(callout.checks) == 0 {
		return nemoCallout{}, fmt.Errorf("AIGuardrail %s/%s has no compilable checks", policy.Namespace, policy.Name)
	}
	return callout, nil
}

// closedCallout is a stand-in for an accepted policy this controller cannot call.
// Each named check rejects the request when its selection header is "true".
func closedCallout(policy *aigatewayv1alpha1.AIGuardrail) (nemoCallout, bool) {
	callout := nemoCallout{name: policy.Name, closed: true}
	for _, check := range policy.Spec.Checks {
		if check.Name == "" {
			continue
		}
		callout.checks = append(callout.checks, closedCheck(check.Name, guardrail.CheckHeaderName(policy.Namespace, policy.Name, check.Name)))
	}
	if len(callout.checks) == 0 {
		return nemoCallout{}, false
	}
	return callout, true
}

// closedCheck rejects the request when its selection header is "true". It
// runs on the request only: a rejected request has no response to check.
func closedCheck(name, header string) nemoCheck {
	return nemoCheck{name: name, header: header, request: true, closed: true}
}

// pinNemoCalloutsToInfra closes, in place, every callout whose NeMo Service is
// not in infraNamespace, the MaasTenantConfig's status.infraNamespace. ODH and
// RHOAI run the NeMo workload only there, and egress and the caller grant
// follow the Service's namespace, so a policy pointing anywhere else is
// rejected rather than opened.
//
// This is the only check that a Service is this tenant's: the guardrail
// reconciler and ParseNemoEndpoint accept either AI Gateway infrastructure
// namespace, so a policy closed here can still be Accepted with
// ProviderReady=True. It returns those callouts as they were before
// closing. While infraNamespace is unpublished every callout is closed and
// none is returned: the reconcile already reports and requeues that state.
func pinNemoCalloutsToInfra(callouts []nemoCallout, infraNamespace string) []nemoCallout {
	var outside []nemoCallout
	for i := range callouts {
		callout := &callouts[i]
		if callout.closed || (infraNamespace != "" && callout.serviceNamespace == infraNamespace) {
			continue
		}
		if infraNamespace != "" {
			outside = append(outside, *callout)
		}
		closed := nemoCallout{name: callout.name, closed: true}
		for _, check := range callout.checks {
			closed.checks = append(closed.checks, closedCheck(check.name, check.header))
		}
		*callout = closed
	}
	return outside
}

// reportNemoOutsideInfra logs the callouts pinNemoCalloutsToInfra closed and
// publishes a Warning Event on each one's AIGuardrail. The guardrail
// reconciler accepts such a policy, so without this its owner, who can read
// the AIGuardrail but not this controller's logs, sees no reason requests are
// rejected. Both fire only when the set changes: a tenant is reconciled every
// resync, and reporting each time would flood the namespace with Events.
func (r *Reconciler) reportNemoOutsideInfra(ctx context.Context, log logr.Logger, tenantNamespace, infraNamespace string, outside []nemoCallout) {
	messages := make([]string, 0, len(outside))
	for _, callout := range outside {
		messages = append(messages, fmt.Sprintf(
			"NeMo Service %s/%s is not in the infrastructure namespace %q, so this tenant rejects requests that select the checks of AIGuardrail %s",
			callout.serviceNamespace, callout.serviceName, infraNamespace, callout.name))
	}
	key := strings.Join(messages, "\n")
	if previous, _ := r.nemoOutsideInfra.Load(tenantNamespace); previous == key || (previous == nil && key == "") {
		return
	}
	if key == "" {
		r.nemoOutsideInfra.Delete(tenantNamespace)
		return
	}
	r.nemoOutsideInfra.Store(tenantNamespace, key)
	log.Error(errProviderOutsideInfra, "guardrail checks fail closed", "infraNamespace", infraNamespace, "details", messages)
	if r.Recorder == nil {
		return
	}
	for i, callout := range outside {
		policy := &aigatewayv1alpha1.AIGuardrail{}
		if err := r.Client.Get(ctx, client.ObjectKey{Namespace: tenantNamespace, Name: callout.name}, policy); err != nil {
			log.Error(err, "read AIGuardrail to report its provider namespace", "name", callout.name)
			continue
		}
		r.Recorder.Eventf(policy, nil, corev1.EventTypeWarning, reasonProviderOutsideInfra, compileGuardrailAction, "%s", messages[i])
	}
}

// bindNemoPodSelectors copies each NeMo Service's pod selector onto the callout
// so egress can target those pods. A Service that cannot be read, or that has
// no selector, leaves the callout covering the whole namespace.
func bindNemoPodSelectors(ctx context.Context, reader client.Reader, callouts []nemoCallout) {
	if reader == nil {
		return
	}
	seen := map[string]map[string]string{}
	for i := range callouts {
		callout := &callouts[i]
		if callout.serviceName == "" || callout.serviceNamespace == "" {
			continue
		}
		key := callout.serviceNamespace + "/" + callout.serviceName
		labels, ok := seen[key]
		if !ok {
			svc := &corev1.Service{}
			err := reader.Get(ctx, client.ObjectKey{Namespace: callout.serviceNamespace, Name: callout.serviceName}, svc)
			if err == nil && len(svc.Spec.Selector) > 0 {
				labels = make(map[string]string, len(svc.Spec.Selector))
				for k, v := range svc.Spec.Selector {
					labels[k] = v
				}
			}
			seen[key] = labels
		}
		callout.podLabels = labels
	}
}

// checksEndpoint is the NeMo checks URL Praxis posts to. status.providerEndpoint
// is the server base URL; a value that already ends at /v1/checks is kept.
func checksEndpoint(endpoint string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	parsed.Path = guardrail.NemoChecksPath
	parsed.RawPath = ""
	return parsed.String(), nil
}

// configureNemoCallouts compiles the callouts into the post-auth extproc.yaml
// and opens egress to each NeMo namespace. For an authenticated provider it
// also mounts the caller token and service CA on the post-auth pod and
// appends the Role and RoleBinding that let that pod call the provider,
// labeled for tenantNamespace. With no callouts the rendered resources are
// returned as they are.
func configureNemoCallouts(resources []unstructured.Unstructured, tenantID, gatewayNamespace, tenantNamespace string, callouts []nemoCallout) ([]unstructured.Unstructured, error) {
	if len(callouts) == 0 {
		return resources, nil
	}
	deploymentName := PayloadProcessingDeploymentName(tenantID)
	deployment := findResource(resources, "Deployment", deploymentName, gatewayNamespace)
	if deployment == nil {
		return nil, fmt.Errorf("payload-processing Deployment %s is missing", deploymentName)
	}
	configMapName := PayloadProcessingPluginsConfigMapForTenant(tenantID)
	configMap := findResource(resources, "ConfigMap", configMapName, gatewayNamespace)
	if configMap == nil {
		return nil, fmt.Errorf("payload-processing ConfigMap %s is missing", configMapName)
	}
	data, found, err := unstructured.NestedStringMap(configMap.Object, "data")
	if err != nil || !found {
		if err == nil {
			err = errors.New("data is missing")
		}
		return nil, fmt.Errorf("read payload-processing ConfigMap: %w", err)
	}
	compiled, err := compileNemoExtproc(data["extproc.yaml"], callouts)
	if err != nil {
		return nil, err
	}
	data["extproc.yaml"] = compiled
	if err := unstructured.SetNestedStringMap(configMap.Object, data, "data"); err != nil {
		return nil, fmt.Errorf("write payload-processing ConfigMap: %w", err)
	}
	if err := ensureNemoEgress(resources, tenantID, gatewayNamespace, callouts); err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(compiled))
	if err := unstructured.SetNestedField(deployment.Object, hex.EncodeToString(sum[:]), "spec", "template", "metadata", "annotations", nemoCalloutConfigAnnotation); err != nil {
		return nil, fmt.Errorf("write NeMo callout config hash: %w", err)
	}
	if !slices.ContainsFunc(callouts, func(c nemoCallout) bool { return c.authenticated }) {
		return resources, nil
	}
	if err := mountNemoAuth(deployment); err != nil {
		return nil, err
	}
	rbac := nemoCallerRBAC(callouts, tenantNamespace, gatewayNamespace, PayloadProcessingPostServiceAccountName(tenantID))
	return append(resources, rbac...), nil
}

// compileNemoExtproc inserts the checks right after the post-auth chain's
// request_id filter and opts the config into private upstreams, which the
// in-cluster NeMo Service needs. Each check runs only when its selection
// header is "true". A headers filter after the checks removes those
// headers so they are not forwarded to the model.
func compileNemoExtproc(config string, callouts []nemoCallout) (string, error) {
	if len(callouts) == 0 {
		return config, nil
	}
	filters, err := expandNemoChecks(callouts)
	if err != nil {
		return "", err
	}
	compiled := make([]any, 0, len(filters)+1)
	seen := map[string]bool{}
	headers := make([]any, 0, len(filters))
	for _, filter := range filters {
		compiled = append(compiled, nemoFilterEntry(filter))
		if !seen[filter.header] {
			seen[filter.header] = true
			headers = append(headers, filter.header)
		}
	}
	compiled = append(compiled, map[string]any{"filter": "headers", "request_remove": headers})

	var doc map[string]any
	if err := yaml.Unmarshal([]byte(config), &doc); err != nil {
		return "", fmt.Errorf("parse post-auth extproc.yaml: %w", err)
	}
	if !insertAfterRequestID(doc, compiled) {
		return "", errors.New("post-auth extproc.yaml has no request_id filter")
	}
	insecure, _ := doc["insecure_options"].(map[string]any)
	if insecure == nil {
		insecure = map[string]any{}
		doc["insecure_options"] = insecure
	}
	insecure["allow_private_upstreams"] = true
	out, err := yaml.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("write post-auth extproc.yaml: %w", err)
	}
	return string(out), nil
}

func insertAfterRequestID(doc map[string]any, entries []any) bool {
	chains, _ := doc["filter_chains"].([]any)
	for _, item := range chains {
		chain, ok := item.(map[string]any)
		if !ok {
			continue
		}
		filters, _ := chain["filters"].([]any)
		for i, filter := range filters {
			if entry, ok := filter.(map[string]any); !ok || entry["filter"] != "request_id" {
				continue
			}
			out := make([]any, 0, len(filters)+len(entries))
			out = append(out, filters[:i+1]...)
			out = append(out, entries...)
			chain["filters"] = append(out, filters[i+1:]...)
			return true
		}
	}
	return false
}

// nemoFilterEntry runs one check when its selection header is "true". A
// request that skips the check also skips its response hook. The model is
// omitted so NeMo calls the model named in the selected configuration
// rather than a name this controller invents. Praxis refuses token_file and
// tls on a plaintext endpoint, so only an authenticated provider gets them.
// token_file needs praxis-proxy/ai with #1507; v0.9.0 rejects it as an
// unknown field and the post-auth ExtProc would not start.
// A closed check rejects that request instead of calling NeMo.
func nemoFilterEntry(filter nemoFilter) map[string]any {
	if filter.closed {
		return map[string]any{
			"filter": "static_response",
			"status": int64(403),
			"body":   `{"error":"guardrail_unavailable"}`,
			"conditions": []any{map[string]any{
				"when": map[string]any{"headers": map[string]any{filter.header: "true"}},
			}},
		}
	}
	provider := map[string]any{
		"type":       "nemo",
		"endpoint":   filter.endpoint,
		"guardrails": map[string]any{"config_ids": []any{filter.configID}},
	}
	if filter.authenticated {
		provider["token_file"] = nemoTokenFile
		provider["tls"] = map[string]any{"ca_file": nemoServiceCAFile}
	}
	// A positive duration below 1ms truncates to 0, which Praxis rejects.
	// nemoCalloutFrom already excludes that policy; this keeps a missed
	// check from writing timeout_ms: 0 into the shared config.
	if ms := filter.timeout.Milliseconds(); ms > 0 {
		provider["timeout_ms"] = ms
	}
	return map[string]any{
		"filter": "ai_guardrails",
		"conditions": []any{map[string]any{
			"when": map[string]any{"headers": map[string]any{filter.header: "true"}},
		}},
		"provider": provider,
		"phase":    map[string]any{"request": filter.request, "response": filter.response},
	}
}

// expandNemoChecks emits one filter per phase. Request checks keep the
// callout order. Praxis then walks response hooks from the end of the chain
// back toward the start, so response checks are placed in reverse. That walk
// runs them in the same catalog order as the request checks. A closed check
// rejects the request and has no response hook.
func expandNemoChecks(callouts []nemoCallout) ([]nemoFilter, error) {
	forward := make([]nemoFilter, 0, len(callouts))
	output := make([]nemoFilter, 0, len(callouts))
	for _, callout := range callouts {
		if callout.closed {
			for _, check := range callout.checks {
				forward = append(forward, nemoFilter{header: check.header, request: true, closed: true})
			}
			continue
		}
		endpoint, err := checksEndpoint(callout.endpoint)
		if err != nil {
			return nil, fmt.Errorf("AIGuardrail %s: %w", callout.name, err)
		}
		for _, check := range callout.checks {
			filter := nemoFilter{endpoint: endpoint, configID: check.configID, header: check.header, authenticated: callout.authenticated, timeout: callout.timeout}
			if check.request {
				input := filter
				input.request = true
				forward = append(forward, input)
			}
			if check.response {
				response := filter
				response.response = true
				output = append(output, response)
			}
		}
	}
	slices.Reverse(output)
	forward = append(forward, output...)
	if len(forward) == 0 {
		return nil, errors.New("accepted guardrails have no compilable checks")
	}
	return forward, nil
}

// ensureNemoEgress opens the NeMo pod port from the post-auth pod: TCP 8000
// for an auth-off server and TCP 8443 for kube-rbac-proxy. NetworkPolicy
// matches pods, not Services. A callout that learned its Service's pod
// selector is limited to those pods. Otherwise the rule is the whole NeMo
// namespace on that port, which is the only identity the endpoint gives us.
// The vendored NetworkPolicy allows the API server ports only. A missing
// policy is left alone: SkipNetworkPolicy removes it before this runs.
func ensureNemoEgress(resources []unstructured.Unstructured, tenantID, gatewayNamespace string, callouts []nemoCallout) error {
	policy := findResource(resources, "NetworkPolicy", PayloadProcessingNetworkPolicyName(tenantID), gatewayNamespace)
	if policy == nil {
		return nil
	}
	egress, _, err := unstructured.NestedSlice(policy.Object, "spec", "egress")
	if err != nil {
		return fmt.Errorf("read payload-processing NetworkPolicy egress: %w", err)
	}
	byPort := map[int64]map[string]nemoEgressPeer{}
	for _, callout := range callouts {
		if callout.serviceNamespace == "" {
			continue
		}
		port := nemoServerPort
		if callout.authenticated {
			port = nemoProxyPort
		}
		if byPort[port] == nil {
			byPort[port] = map[string]nemoEgressPeer{}
		}
		peer := nemoEgressPeer{namespace: callout.serviceNamespace, labels: callout.podLabels}
		byPort[port][peer.key()] = peer
	}
	for _, port := range []int64{nemoServerPort, nemoProxyPort} {
		grouped := byPort[port]
		if len(grouped) == 0 {
			continue
		}
		sorted := make([]nemoEgressPeer, 0, len(grouped))
		for _, peer := range grouped {
			sorted = append(sorted, peer)
		}
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].key() < sorted[j].key() })
		peers := make([]any, 0, len(sorted))
		for _, peer := range sorted {
			peers = append(peers, peer.object())
		}
		egress = append(egress, map[string]any{
			"to":    peers,
			"ports": []any{map[string]any{"protocol": "TCP", "port": port}},
		})
	}
	if err := unstructured.SetNestedSlice(policy.Object, egress, "spec", "egress"); err != nil {
		return fmt.Errorf("write payload-processing NetworkPolicy egress: %w", err)
	}
	return nil
}

// nemoEgressPeer is one NetworkPolicy peer: a namespace, and the NeMo
// Service's pods when their labels are known.
type nemoEgressPeer struct {
	namespace string
	labels    map[string]string
}

func (p nemoEgressPeer) key() string {
	parts := make([]string, 0, 1+len(p.labels))
	parts = append(parts, p.namespace)
	keys := make([]string, 0, len(p.labels))
	for key := range p.labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts = append(parts, key+"="+p.labels[key])
	}
	return strings.Join(parts, "|")
}

func (p nemoEgressPeer) object() map[string]any {
	peer := map[string]any{
		"namespaceSelector": map[string]any{
			"matchLabels": map[string]any{"kubernetes.io/metadata.name": p.namespace},
		},
	}
	if len(p.labels) == 0 {
		return peer
	}
	match := map[string]any{}
	for key, value := range p.labels {
		match[key] = value
	}
	peer["podSelector"] = map[string]any{"matchLabels": match}
	return peer
}

func findResource(resources []unstructured.Unstructured, kind, name, namespace string) *unstructured.Unstructured {
	for i := range resources {
		resource := &resources[i]
		if resource.GetKind() == kind && resource.GetName() == name && resource.GetNamespace() == namespace {
			return resource
		}
	}
	return nil
}

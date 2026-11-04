package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
)

// Cross-namespace ExternalProvider references. A reference to a provider in
// another namespace is served only when a ReferenceGrant in the provider
// namespace permits ExternalModels of the model namespace to reference it.
// The provider then gets hashed transport in the model namespace and is
// otherwise served like an attachment's provider: a hashed Gateway
// DestinationRule and copies of credential Secrets outside the tenant
// namespace.
const (
	reasonRefNotPermitted = "RefNotPermitted"

	// labelProviderNamespace marks model-namespace transport for a provider
	// in another namespace. It replaces the local provider label, so the
	// reconciler of a same-named local provider never collects it.
	labelProviderNamespace = "inference.opendatahub.io/external-provider-namespace"
)

// ReferenceGrant v1beta1 is the storage version and is served by every
// standard Gateway API install.
var referenceGrantGVK = schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1beta1", Kind: "ReferenceGrant"}

func newReferenceGrant() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(referenceGrantGVK)
	return u
}

// referencePermitted reports whether a ReferenceGrant in the provider
// namespace permits ExternalModels of modelNamespace to reference provider.
func (r *Reconciler) referencePermitted(ctx context.Context, modelNamespace string, provider types.NamespacedName) (bool, error) {
	grants := &unstructured.UnstructuredList{}
	grants.SetGroupVersionKind(referenceGrantGVK.GroupVersion().WithKind("ReferenceGrantList"))
	if err := r.List(ctx, grants, client.InNamespace(provider.Namespace)); err != nil {
		return false, fmt.Errorf("list ReferenceGrants in %s: %w", provider.Namespace, err)
	}
	group := v1alpha1.GroupVersion.Group
	for i := range grants.Items {
		from, _, _ := unstructured.NestedSlice(grants.Items[i].Object, "spec", "from")
		to, _, _ := unstructured.NestedSlice(grants.Items[i].Object, "spec", "to")
		if slices.ContainsFunc(from, func(raw any) bool {
			f, _ := raw.(map[string]any)
			return f["group"] == group && f["kind"] == "ExternalModel" && f["namespace"] == modelNamespace
		}) && slices.ContainsFunc(to, func(raw any) bool {
			t, _ := raw.(map[string]any)
			name, _ := t["name"].(string)
			return t["group"] == group && t["kind"] == "ExternalProvider" && (name == "" || name == provider.Name)
		}) {
			return true, nil
		}
	}
	return false, nil
}

// foreignProviders returns the providers in other namespaces that the models
// reference and a ReferenceGrant permits the model namespace to reference.
// Like an attachment's providers they are validated here, not through their
// own namespace's tenant. A missing or deleting provider is omitted like an
// unpermitted one, so a reference never discloses whether a provider exists.
// A provider whose own credential is not ready, while a reference inherits
// it, is still returned: unready maps it to a message, and only the routes
// that inherit that credential are skipped (see skipUnreadyCredentials). An
// override's model-namespace Secret is validated with its route.
func (r *Reconciler) foreignProviders(ctx context.Context, modelNamespace string, models []*v1alpha1.ExternalModel) (
	providers []*v1alpha1.ExternalProvider, unready map[types.NamespacedName]string, err error) {
	if r.Namespace != "" {
		// A single-namespace controller reads no other namespace.
		return nil, nil, nil
	}
	// Whether a reference to each provider inherits its auth.
	refs := map[types.NamespacedName]bool{}
	for _, m := range models {
		for _, ref := range m.Spec.ExternalProviderRefs {
			if ref.Ref.Namespace != "" && ref.Ref.Namespace != modelNamespace {
				key := types.NamespacedName{Namespace: ref.Ref.Namespace, Name: ref.Ref.Name}
				refs[key] = refs[key] || ref.Auth == nil
			}
		}
	}
	providers = make([]*v1alpha1.ExternalProvider, 0, len(refs))
	unready = map[types.NamespacedName]string{}
	for key, inherited := range refs {
		if permitted, err := r.referencePermitted(ctx, modelNamespace, key); err != nil || !permitted {
			if err != nil {
				return nil, nil, err
			}
			continue
		}
		var provider v1alpha1.ExternalProvider
		if err := r.Get(ctx, key, &provider); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, nil, fmt.Errorf("get ExternalProvider %s: %w", key, err)
		}
		if !provider.DeletionTimestamp.IsZero() {
			continue
		}
		provider.Status.Phase = resolver.PhaseReady
		if r.validateProvider(ctx, &provider) != nil {
			provider.Status.Phase = providerPhaseFailed
		} else if inherited {
			if err := r.validateProviderCredential(ctx, &provider); transientReadError(err) {
				return nil, nil, err
			} else if err != nil {
				unready[key] = fmt.Sprintf("the credential of ExternalProvider %s is not ready for references that inherit its auth", key)
			}
		}
		providers = append(providers, &provider)
	}
	return providers, unready, nil
}

// skipUnreadyCredentials skips each route that inherits the credential of a
// provider in unready, as the resolver skips an unready provider; a route that
// overrides the credential is kept. It returns the models left with no route
// by it, with the message to report.
func skipUnreadyCredentials(set *resolver.ResolvedRouteSet, err error, unready map[types.NamespacedName]string) (
	*resolver.ResolvedRouteSet, map[string]string, error) {
	unserved := map[string]string{}
	if set == nil || len(unready) == 0 || (err != nil && !errors.Is(err, resolver.ErrNoRoutes)) {
		return set, unserved, err
	}
	routes := 0
	for i := range set.Models {
		model := &set.Models[i]
		kept, message := model.Routes[:0], ""
		for _, route := range model.Routes {
			if skip, ok := unready[types.NamespacedName{Namespace: route.ProviderNamespace, Name: route.Provider}]; ok && route.SecretRefNamespace() == route.ProviderNamespace {
				model.Skips = append(model.Skips, resolver.Skip{ModelRef: model.ModelRef, ProviderRef: route.Provider, Reason: resolver.SkipProviderNotReady, Message: skip})
				message = skip
				continue
			}
			kept = append(kept, route)
		}
		model.Routes = kept
		routes += len(kept)
		if len(kept) == 0 && message != "" {
			unserved[model.ModelRef] = message
		}
	}
	if routes == 0 {
		return set, unserved, resolver.ErrNoRoutes
	}
	return set, unserved, err
}

// deniedReferences maps each model with a provider reference that is not
// permitted to that reference's message.
func deniedReferences(set *resolver.ResolvedRouteSet) map[string]string {
	denied := map[string]string{}
	if set == nil {
		return denied
	}
	for _, model := range set.Models {
		for _, skip := range model.Skips {
			if _, seen := denied[model.ModelRef]; !seen && skip.Reason == resolver.SkipRefNotPermitted {
				denied[model.ModelRef] = skip.Message
			}
		}
	}
	return denied
}

// servesDeniedReference reports whether the tenant's published overlay still
// serves a provider reference of the models that is not permitted any more.
// Only then does a namespace that resolves no routes give up its last serving
// state; a reference that was never served keeps the existing retention.
func (r *Reconciler) servesDeniedReference(ctx context.Context, namespace string, models []*v1alpha1.ExternalModel, permitted []*v1alpha1.ExternalProvider) (bool, error) {
	allowed := map[types.NamespacedName]bool{}
	for _, p := range permitted {
		allowed[client.ObjectKeyFromObject(p)] = true
	}
	denied := map[string]bool{}
	for _, m := range models {
		for _, ref := range m.Spec.ExternalProviderRefs {
			key := types.NamespacedName{Namespace: ref.Ref.Namespace, Name: ref.Ref.Name}
			if key.Namespace != "" && key.Namespace != namespace && !allowed[key] {
				denied[envelope.ScopedStableID(namespace, m.Name, key.Namespace, key.Name)] = true
			}
		}
	}
	if len(denied) == 0 {
		return false, nil
	}
	published, err := r.ownOverlayCandidates(ctx, namespace)
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(published, func(c envelope.Candidate) bool { return denied[c.StableID] }), nil
}

// referenceGrantModels enqueues the models that reference a provider in the
// grant's namespace, so a granted reference recovers and a revoked one stops
// serving.
func (r *Reconciler) referenceGrantModels(ctx context.Context, grant *unstructured.Unstructured) []reconcile.Request {
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models); err != nil {
		r.Log.Error(err, "list models for ReferenceGrant event", "namespace", grant.GetNamespace())
		return nil
	}
	var requests []reconcile.Request
	for i := range models.Items {
		m := &models.Items[i]
		if m.Namespace != grant.GetNamespace() && r.namespaceAllowed(m.Namespace) &&
			slices.ContainsFunc(m.Spec.ExternalProviderRefs, func(ref v1alpha1.ExternalProviderRef) bool { return ref.Ref.Namespace == grant.GetNamespace() }) {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(m)})
		}
	}
	return requests
}

// providerIdentity identifies the provider of hashed model-namespace transport.
func providerIdentity(route resolver.Route) string {
	return route.ProviderNamespace + "/" + route.Provider
}

// verifyProviderTransport refuses to adopt an object created for another
// identity: hashed transport (or an attachment DestinationRule) is never taken
// over by a local provider whose name produces the same object name, nor
// local transport by hashed transport.
func (r *Reconciler) verifyProviderTransport(ctx context.Context, resources []unstructured.Unstructured) error {
	for i := range resources {
		identity := resources[i].GetAnnotations()[annotationAttachmentIdentity]
		existing := &unstructured.Unstructured{}
		existing.SetGroupVersionKind(resources[i].GroupVersionKind())
		if err := r.Get(ctx, client.ObjectKeyFromObject(&resources[i]), existing); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("get %s %s/%s: %w", resources[i].GetKind(), resources[i].GetNamespace(), resources[i].GetName(), err)
		}
		if existing.GetAnnotations()[annotationAttachmentIdentity] != identity {
			return fmt.Errorf("%s %s/%s exists with a different identity; refusing to adopt it", resources[i].GetKind(), resources[i].GetNamespace(), resources[i].GetName())
		}
	}
	return nil
}

// collectProviderTransport deletes the namespace's transport for providers in
// other namespaces that no HTTPRoute there uses any more. It runs after the
// reconcile withdrew the routes, which are read live, so a route stops using
// its backend before the backend is deleted.
func (r *Reconciler) collectProviderTransport(ctx context.Context, namespace string) error {
	var transport []unstructured.Unstructured
	for _, gvk := range []schema.GroupVersionKind{
		{Version: "v1", Kind: "ServiceList"},
		{Group: "networking.istio.io", Version: "v1", Kind: "ServiceEntryList"},
	} {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(gvk)
		if err := r.List(ctx, list, client.InNamespace(namespace),
			client.MatchingLabels{"app.kubernetes.io/managed-by": "ai-gateway-controller"}, client.HasLabels{labelProviderNamespace}); err != nil {
			return fmt.Errorf("list provider transport in %s: %w", namespace, err)
		}
		transport = append(transport, list.Items...)
	}
	if len(transport) == 0 {
		return r.releaseUnusedAttachmentProviders(ctx, namespace)
	}
	routes := &unstructured.UnstructuredList{}
	routes.SetGroupVersionKind(httpRouteGVK.GroupVersion().WithKind("HTTPRouteList"))
	if err := r.APIReader.List(ctx, routes, client.InNamespace(namespace)); err != nil {
		return fmt.Errorf("list HTTPRoutes in %s: %w", namespace, err)
	}
	used := map[string]bool{}
	for i := range routes.Items {
		rules, _, _ := unstructured.NestedSlice(routes.Items[i].Object, "spec", "rules")
		for _, raw := range rules {
			rule, _ := raw.(map[string]any)
			backends, _, _ := unstructured.NestedSlice(rule, "backendRefs")
			for _, raw := range backends {
				backend, _ := raw.(map[string]any)
				name, _, _ := unstructured.NestedString(backend, "name")
				used[name] = true
			}
		}
	}
	for i := range transport {
		if used[transport[i].GetName()] || transport[i].GetAnnotations()[annotationAttachmentIdentity] == "" {
			continue
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, &transport[i])); err != nil {
			return fmt.Errorf("delete provider transport %s/%s: %w", namespace, transport[i].GetName(), err)
		}
	}
	return r.releaseUnusedAttachmentProviders(ctx, namespace)
}

// releaseUnusedAttachmentProviders drops protection after the last route is gone.
func (r *Reconciler) releaseUnusedAttachmentProviders(ctx context.Context, namespace string) error {
	var providers v1alpha1.ExternalProviderList
	if err := r.List(ctx, &providers, client.InNamespace(namespace)); err != nil {
		return err
	}
	for i := range providers.Items {
		if !providers.Items[i].DeletionTimestamp.IsZero() {
			continue
		}
		if err := r.releaseUnusedAttachmentProvider(ctx, &providers.Items[i]); err != nil {
			return err
		}
	}
	return nil
}

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
	"errors"
	"fmt"
	"sort"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// attachmentRef is one entry in a guardrails list. An empty checks list means every check on that policy.
type attachmentRef struct {
	namespace string
	name      string
	checks    []string
	// unreadable is an entry whose policy could not be read. It may name any
	// tenant policy, so every one is attached rather than none.
	unreadable bool
}

// unreadableRef stands in for a guardrails entry, or a whole list, that could
// not be read.
var unreadableRef = attachmentRef{unreadable: true}

// attachedPolicies returns the names of the tenant's policies that at least
// one attachment references. all is true when an unreadable entry may
// reference any of them.
func attachedPolicies(refs []attachmentRef, tenantNamespace string) (names map[string]bool, all bool) {
	names = map[string]bool{}
	for _, ref := range refs {
		switch {
		case ref.unreadable:
			all = true
		case inTenant(ref, tenantNamespace):
			names[ref.name] = true
		}
	}
	return names, all
}

// inTenant reports whether ref names a policy in the tenant's namespace.
// A same-named policy stored next to the model is a different policy.
func inTenant(ref attachmentRef, tenantNamespace string) bool {
	return !ref.unreadable && (ref.namespace == "" || ref.namespace == tenantNamespace)
}

// applyAttachmentOrder returns only the checks the attachments select, ordered
// from the broadest list to the narrowest: AI Tenant, tenant config,
// subscription, model, then a subscription's model entry. Several objects at one level are ordered by
// namespace, then name; that does not reorder a list. A check named twice runs
// the first time. A check no attachment selects is not returned, unless an
// unreadable entry may select it: then every remaining check runs last, in
// policy name order.
func applyAttachmentOrder(callouts []nemoCallout, refs []attachmentRef, tenantNamespace string) []nemoCallout {
	byName := make(map[string]nemoCallout, len(callouts))
	for _, callout := range callouts {
		byName[callout.name] = callout
	}
	used := map[string]bool{}
	ordered := make([]nemoCallout, 0, len(callouts))
	add := func(src nemoCallout, checks []string) {
		selected := selectCalloutChecks(src, checks, used)
		if len(selected) == 0 {
			return
		}
		src.checks = selected
		ordered = append(ordered, src)
	}
	for _, ref := range refs {
		if !inTenant(ref, tenantNamespace) {
			continue
		}
		if src, ok := byName[ref.name]; ok {
			add(src, ref.checks)
		}
	}
	if _, all := attachedPolicies(refs, tenantNamespace); all {
		names := make([]string, 0, len(byName))
		for name := range byName {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			add(byName[name], nil)
		}
	}
	return ordered
}

// selectCalloutChecks keeps the policy's check order. An empty wanted list takes every check not already used.
func selectCalloutChecks(callout nemoCallout, wanted []string, used map[string]bool) []nemoCheck {
	allow := map[string]bool{}
	for _, name := range wanted {
		allow[name] = true
	}
	selected := make([]nemoCheck, 0, len(callout.checks))
	for _, check := range callout.checks {
		if len(wanted) > 0 && !allow[check.name] {
			continue
		}
		key := callout.name + "/" + check.name
		if used[key] {
			continue
		}
		used[key] = true
		selected = append(selected, check)
	}
	return selected
}

func catalogAttachments(ctx context.Context, reader client.Reader, tenantNamespace string) ([]attachmentRef, error) {
	var refs []attachmentRef
	var malformed []error
	keep := func(parsed []attachmentRef, err error) error {
		if err != nil && !errors.Is(err, errMalformedAttachment) {
			return err
		}
		refs = append(refs, parsed...)
		if err != nil {
			malformed = append(malformed, err)
		}
		return nil
	}

	mtc := NewMaasTenantConfig()
	err := reader.Get(ctx, client.ObjectKey{Namespace: tenantNamespace, Name: MaasTenantConfigInstanceName}, mtc)
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		return nil, fmt.Errorf("get MaasTenantConfig %s/%s: %w", tenantNamespace, MaasTenantConfigInstanceName, err)
	default:
		tenantRefs, err := aiTenantGuardrails(ctx, reader, mtc, tenantNamespace)
		if err := keep(tenantRefs, err); err != nil {
			return nil, err
		}
		configRefs, err := guardrailRefs(mtc, "spec", "guardrails")
		if err := keep(configRefs, err); err != nil {
			return nil, err
		}
	}

	subscriptions, err := listUnstructured(ctx, reader, MaaSSubscriptionGVK, tenantNamespace)
	if err != nil {
		return nil, err
	}
	sortResources(subscriptions)
	for i := range subscriptions {
		wide, err := guardrailRefs(&subscriptions[i], "spec", "guardrails")
		if err := keep(wide, err); err != nil {
			return nil, err
		}
	}

	models, err := modelRefsForTenant(ctx, reader, tenantNamespace, subscriptions)
	if err != nil {
		return nil, err
	}
	sortResources(models)
	for i := range models {
		modelGuardrails, err := guardrailRefs(&models[i], "spec", "guardrails")
		if err := keep(modelGuardrails, err); err != nil {
			return nil, err
		}
	}

	for i := range subscriptions {
		entries, err := modelRefGuardrails(&subscriptions[i])
		if err := keep(entries, err); err != nil {
			return nil, err
		}
	}
	if len(malformed) > 0 {
		return refs, errors.Join(malformed...)
	}
	return refs, nil
}

// modelRefsForTenant reads model refs in the tenant namespace, plus model refs
// in other namespaces that this tenant's subscriptions point at by name.
func modelRefsForTenant(ctx context.Context, reader client.Reader, tenantNamespace string, subscriptions []unstructured.Unstructured) ([]unstructured.Unstructured, error) {
	models, err := listUnstructured(ctx, reader, MaaSModelRefGVK, tenantNamespace)
	if err != nil {
		return nil, err
	}
	seen := make(map[types.NamespacedName]bool, len(models))
	for i := range models {
		seen[client.ObjectKeyFromObject(&models[i])] = true
	}
	for i := range subscriptions {
		for _, key := range subscriptionModelRefKeys(&subscriptions[i]) {
			if key.Namespace == "" || key.Namespace == tenantNamespace || seen[key] {
				continue
			}
			seen[key] = true
			model := newUnstructured(MaaSModelRefGVK)
			err := reader.Get(ctx, key, model)
			if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("get MaaSModelRef %s: %w", key, err)
			}
			models = append(models, *model)
		}
	}
	return models, nil
}

// subscriptionModelRefKeys reads each model entry's name and namespace.
// A missing namespace means the subscription's own namespace.
func subscriptionModelRefKeys(subscription *unstructured.Unstructured) []types.NamespacedName {
	entries, found, err := unstructured.NestedSlice(subscription.Object, "spec", "modelRefs")
	if err != nil || !found {
		return nil
	}
	keys := make([]types.NamespacedName, 0, len(entries))
	for _, entry := range entries {
		object, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		name, _ := object["name"].(string)
		if name == "" {
			continue
		}
		namespace, _ := object["namespace"].(string)
		if namespace == "" {
			namespace = subscription.GetNamespace()
		}
		keys = append(keys, types.NamespacedName{Namespace: namespace, Name: name})
	}
	return keys
}

// aiTenantGuardrails returns the baseline the owning AITenant carries, which
// is the first attachment level and the one a narrower list cannot displace.
// A config with no owning-AITenant annotations is not AITenant-managed and
// has no baseline. An owner that is named but does not resolve is a baseline
// this call cannot read, and reading no baseline is indistinguishable from
// reading an empty one: it attaches every tenant policy rather than none, the
// same way an unreadable guardrails entry does.
func aiTenantGuardrails(ctx context.Context, reader client.Reader, mtc *unstructured.Unstructured, tenantNamespace string) ([]attachmentRef, error) {
	name, namespace, ok := OwningAITenantRef(mtc)
	if !ok {
		return nil, nil
	}
	aitenant := NewAITenant()
	err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, aitenant)
	if apierrors.IsNotFound(err) {
		return []attachmentRef{unreadableRef}, fmt.Errorf("%w: owning AITenant %s/%s is gone", errMalformedAttachment, namespace, name)
	}
	if err != nil {
		return nil, fmt.Errorf("get AITenant %s/%s: %w", namespace, name, err)
	}
	if got, ok := ConfigNamespace(aitenant); !ok || got != tenantNamespace {
		return []attachmentRef{unreadableRef}, fmt.Errorf(
			"%w: AITenant %s/%s status.tenantNamespace %q does not own the config in %q",
			errMalformedAttachment, namespace, name, got, tenantNamespace)
	}
	return guardrailRefs(aitenant, "spec", "guardrails")
}

func modelRefGuardrails(subscription *unstructured.Unstructured) ([]attachmentRef, error) {
	entries, found, err := unstructured.NestedSlice(subscription.Object, "spec", "modelRefs")
	if err != nil {
		return []attachmentRef{unreadableRef}, fmt.Errorf("%w: MaaSSubscription %s spec.modelRefs: %w", errMalformedAttachment, subscription.GetName(), err)
	}
	if !found {
		return nil, nil
	}
	var refs []attachmentRef
	var problems []error
	owner := subscription.GetKind() + "/" + subscription.GetName()
	for i, entry := range entries {
		object, ok := entry.(map[string]any)
		if !ok {
			refs = append(refs, unreadableRef)
			problems = append(problems, fmt.Errorf("%s spec.modelRefs[%d] is not an object", owner, i))
			continue
		}
		raw, exists := object["guardrails"]
		if !exists || raw == nil {
			continue
		}
		list, ok := raw.([]any)
		if !ok {
			refs = append(refs, unreadableRef)
			problems = append(problems, fmt.Errorf("%s spec.modelRefs[%d].guardrails is not a list", owner, i))
			continue
		}
		parsed, err := parseGuardrailRefs(owner, list)
		refs = append(refs, parsed...)
		if err != nil {
			problems = append(problems, err)
		}
	}
	return refs, malformedError(problems)
}

func guardrailRefs(obj *unstructured.Unstructured, fields ...string) ([]attachmentRef, error) {
	raw, found, err := unstructured.NestedSlice(obj.Object, fields...)
	if err != nil {
		return []attachmentRef{unreadableRef}, fmt.Errorf("%w: %s %s guardrails: %w", errMalformedAttachment, obj.GetKind(), obj.GetName(), err)
	}
	if !found {
		return nil, nil
	}
	parsed, err := parseGuardrailRefs(obj.GetKind()+"/"+obj.GetName(), raw)
	if err != nil {
		return parsed, fmt.Errorf("%w: %w", errMalformedAttachment, err)
	}
	return parsed, nil
}

// parseGuardrailRefs fails closed on an entry it cannot fully read. An entry
// with no readable policy may name any policy; one whose checks cannot be read
// selects every check of the policy it names.
func parseGuardrailRefs(owner string, raw []any) ([]attachmentRef, error) {
	refs := make([]attachmentRef, 0, len(raw))
	var problems []error
	for i, item := range raw {
		entry, ok := item.(map[string]any)
		if !ok {
			refs = append(refs, unreadableRef)
			problems = append(problems, fmt.Errorf("%s guardrails[%d] is not an object", owner, i))
			continue
		}
		ref, _ := entry["ref"].(map[string]any)
		name, _ := ref["name"].(string)
		if name == "" {
			refs = append(refs, unreadableRef)
			problems = append(problems, fmt.Errorf("%s guardrails[%d] has no ref.name", owner, i))
			continue
		}
		namespace, _ := ref["namespace"].(string)
		checks, err := stringList(entry["checks"])
		if err != nil {
			problems = append(problems, fmt.Errorf("%s guardrails[%d]: %w", owner, i, err))
			checks = nil
		}
		refs = append(refs, attachmentRef{namespace: namespace, name: name, checks: checks})
	}
	return refs, errors.Join(problems...)
}

func malformedError(problems []error) error {
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %w", errMalformedAttachment, errors.Join(problems...))
}

func stringList(value any) ([]string, error) {
	if value == nil {
		return nil, nil
	}
	raw, ok := value.([]any)
	if !ok {
		return nil, errors.New("checks is not a list")
	}
	names := make([]string, 0, len(raw))
	for _, item := range raw {
		name, ok := item.(string)
		if !ok {
			return nil, errors.New("checks entry is not a string")
		}
		names = append(names, name)
	}
	return names, nil
}

// sortResources sorts objects at one attachment level by namespace, then name.
// The guardrails list on each object stays as written.
func sortResources(items []unstructured.Unstructured) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].GetNamespace() != items[j].GetNamespace() {
			return items[i].GetNamespace() < items[j].GetNamespace()
		}
		return items[i].GetName() < items[j].GetName()
	})
}

func newUnstructured(gvk schema.GroupVersionKind) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	return u
}

func newMaaSSubscription() *unstructured.Unstructured {
	return newUnstructured(MaaSSubscriptionGVK)
}

func newMaaSModelRef() *unstructured.Unstructured {
	return newUnstructured(MaaSModelRefGVK)
}

// subscriptionNamespacesNaming returns, once each, the namespaces of the
// subscriptions that point at the model.
func subscriptionNamespacesNaming(ctx context.Context, reader client.Reader, namespace, name string) ([]string, error) {
	subscriptions, err := listUnstructured(ctx, reader, MaaSSubscriptionGVK, "")
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var namespaces []string
	for i := range subscriptions {
		ns := subscriptions[i].GetNamespace()
		if seen[ns] || !subscriptionNamesModel(&subscriptions[i], namespace, name) {
			continue
		}
		seen[ns] = true
		namespaces = append(namespaces, ns)
	}
	return namespaces, nil
}

// subscriptionNamesModel reports whether this subscription points at the model.
func subscriptionNamesModel(subscription *unstructured.Unstructured, namespace, name string) bool {
	for _, key := range subscriptionModelRefKeys(subscription) {
		if key.Namespace == namespace && key.Name == name {
			return true
		}
	}
	return false
}

// listUnstructured lists one namespace. An empty namespace lists all of them.
// A missing CRD is an empty list.
func listUnstructured(ctx context.Context, reader client.Reader, gvk schema.GroupVersionKind, namespace string) ([]unstructured.Unstructured, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
	var opts []client.ListOption
	where := "all namespaces"
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
		where = namespace
	}
	if err := reader.List(ctx, list, opts...); err != nil {
		if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list %s in %s: %w", gvk.Kind, where, err)
	}
	return list.Items, nil
}

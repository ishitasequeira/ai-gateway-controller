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

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	nemoAuthVolumeName = "nemo-auth"
	nemoAuthMountPath  = "/var/run/secrets/nemo"
	nemoTokenFile      = nemoAuthMountPath + "/token"
	nemoServiceCAFile  = nemoAuthMountPath + "/service-ca.crt"
	// nemoTokenExpirationSeconds is how long each projected token lives. The
	// kubelet rotates the file before it expires, and Praxis watches
	// token_file and reloads it (praxis-proxy/ai#1507).
	nemoTokenExpirationSeconds int64 = 3600
	// openShiftServiceCAConfigMap is published in every namespace by the
	// OpenShift service CA operator. It holds the CA that signs the
	// kube-rbac-proxy serving certificate.
	openShiftServiceCAConfigMap = "openshift-service-ca.crt"
	openShiftServiceCAKey       = "service-ca.crt"

	// labelNemoCallerTenant names the tenant (MaasTenantConfig) namespace a
	// NeMo Role and RoleBinding were written for. Pruning by tenant rather
	// than by gateway namespace and ServiceAccount also removes the grants a
	// tenant left behind when it moved to another Gateway.
	labelNemoCallerTenant = "aigateway.opendatahub.io/nemo-caller-tenant"
)

// mountNemoAuth projects the pod's ServiceAccount token and the OpenShift
// service CA into the post-auth container. The token audience is left unset:
// TrustyAI's kube-rbac-proxy does not set --auth-token-audiences, so it
// accepts the API server's default audience and would reject any other. Such
// a token is also good against the API server, which is why the post-auth pod
// runs as PayloadProcessingPostServiceAccountName: the receiver can act as
// that account and nothing more.
func mountNemoAuth(deployment *unstructured.Unstructured) error {
	volumes, _, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "volumes")
	if err != nil {
		return fmt.Errorf("read payload-processing volumes: %w", err)
	}
	volumes = appendOrReplaceNamedVolume(volumes, nemoAuthVolumeName, map[string]any{
		"name": nemoAuthVolumeName,
		"projected": map[string]any{
			"sources": []any{
				map[string]any{"serviceAccountToken": map[string]any{
					"path":              "token",
					"expirationSeconds": nemoTokenExpirationSeconds,
				}},
				map[string]any{"configMap": map[string]any{
					"name":  openShiftServiceCAConfigMap,
					"items": []any{map[string]any{"key": openShiftServiceCAKey, "path": "service-ca.crt"}},
				}},
			},
		},
	})
	if err := unstructured.SetNestedSlice(deployment.Object, volumes, "spec", "template", "spec", "volumes"); err != nil {
		return fmt.Errorf("write payload-processing volumes: %w", err)
	}

	containers, found, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	if err != nil || !found || len(containers) == 0 {
		if err == nil {
			err = errors.New("containers are missing")
		}
		return fmt.Errorf("read payload-processing containers: %w", err)
	}
	container, ok := containers[0].(map[string]any)
	if !ok {
		return errors.New("payload-processing first container is malformed")
	}
	mounts, _, err := unstructured.NestedSlice(container, "volumeMounts")
	if err != nil {
		return fmt.Errorf("read payload-processing volume mounts: %w", err)
	}
	mounts = appendOrReplaceNamedVolume(mounts, nemoAuthVolumeName, map[string]any{
		"name":      nemoAuthVolumeName,
		"mountPath": nemoAuthMountPath,
		"readOnly":  true,
	})
	if err := unstructured.SetNestedSlice(container, mounts, "volumeMounts"); err != nil {
		return fmt.Errorf("write payload-processing volume mounts: %w", err)
	}
	containers[0] = container
	if err := unstructured.SetNestedSlice(deployment.Object, containers, "spec", "template", "spec", "containers"); err != nil {
		return fmt.Errorf("write payload-processing containers: %w", err)
	}
	return nil
}

// nemoCallerRBAC returns a Role and RoleBinding next to each authenticated
// NeMo Service. kube-rbac-proxy allows a call only when a SubjectAccessReview
// for get on that Service, in its own namespace, succeeds for the caller's
// token. TrustyAI names a Service placed outside the custom resource's
// namespace <cr-namespace>-<cr-name>, so the grant follows the Service the
// endpoint addresses, not the NemoGuardrails name. A closed callout has no
// Service and gets no grant.
func nemoCallerRBAC(callouts []nemoCallout, tenantNamespace, gatewayNamespace, serviceAccount string) []unstructured.Unstructured {
	type target struct{ namespace, service string }
	seen := map[target]bool{}
	var targets []target
	for _, callout := range callouts {
		t := target{callout.serviceNamespace, callout.serviceName}
		if !callout.authenticated || t.namespace == "" || t.service == "" || seen[t] {
			continue
		}
		seen[t] = true
		targets = append(targets, t)
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].namespace != targets[j].namespace {
			return targets[i].namespace < targets[j].namespace
		}
		return targets[i].service < targets[j].service
	})

	out := make([]unstructured.Unstructured, 0, 2*len(targets))
	for _, t := range targets {
		name := nemoCallerName(gatewayNamespace, serviceAccount, t.service)
		metadata := func() map[string]any {
			labels := map[string]any{}
			for key, value := range nemoCallerLabels(tenantNamespace) {
				labels[key] = value
			}
			return map[string]any{"name": name, "namespace": t.namespace, "labels": labels}
		}
		out = append(out,
			unstructured.Unstructured{Object: map[string]any{
				"apiVersion": rbacv1.SchemeGroupVersion.String(),
				"kind":       "Role",
				"metadata":   metadata(),
				"rules": []any{map[string]any{
					"apiGroups":     []any{""},
					"resources":     []any{"services"},
					"resourceNames": []any{t.service},
					"verbs":         []any{"get"},
				}},
			}},
			unstructured.Unstructured{Object: map[string]any{
				"apiVersion": rbacv1.SchemeGroupVersion.String(),
				"kind":       "RoleBinding",
				"metadata":   metadata(),
				"subjects": []any{map[string]any{
					"kind":      rbacv1.ServiceAccountKind,
					"name":      serviceAccount,
					"namespace": gatewayNamespace,
				}},
				"roleRef": map[string]any{"apiGroup": rbacv1.GroupName, "kind": "Role", "name": name},
			}},
		)
	}
	return out
}

// nemoCallerName is unique per caller and Service within the Service's
// namespace, since several tenants can call the same NeMo. It reads as
// nemo-caller.<gateway-namespace>.<service-account>.<service>. Each part is a
// DNS label of at most 63 characters, so the name stays within the 253 a Role
// name allows.
func nemoCallerName(gatewayNamespace, serviceAccount, service string) string {
	return "nemo-caller." + gatewayNamespace + "." + serviceAccount + "." + service
}

func nemoCallerLabels(tenantNamespace string) map[string]string {
	return map[string]string{
		LabelManagedBy:        ManagedByAIGatewayController,
		labelNemoCallerTenant: tenantNamespace,
	}
}

// pruneNemoCallerRBAC deletes the NeMo Roles and RoleBindings written for
// this tenant that are not in keep. Server-side apply only upserts, so a
// provider that is no longer called, or a Gateway the tenant left, would
// otherwise keep its grant. It reads the API server directly: caching these
// kinds would mean watching every Role and RoleBinding in the cluster.
func (r *Reconciler) pruneNemoCallerRBAC(ctx context.Context, tenantNamespace string, keep []unstructured.Unstructured) error {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	wanted := map[string]bool{}
	for i := range keep {
		wanted[keep[i].GetKind()+"/"+keep[i].GetNamespace()+"/"+keep[i].GetName()] = true
	}
	for _, kind := range []string{"RoleBinding", "Role"} {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind(kind + "List"))
		if err := reader.List(ctx, list, client.MatchingLabels(nemoCallerLabels(tenantNamespace))); err != nil {
			return fmt.Errorf("list NeMo caller %ss: %w", kind, err)
		}
		for i := range list.Items {
			item := &list.Items[i]
			if wanted[kind+"/"+item.GetNamespace()+"/"+item.GetName()] {
				continue
			}
			if err := client.IgnoreNotFound(r.Client.Delete(ctx, item)); err != nil {
				return fmt.Errorf("delete NeMo caller %s %s/%s: %w", kind, item.GetNamespace(), item.GetName(), err)
			}
		}
	}
	return nil
}

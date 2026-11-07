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
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/yaml"

	aigatewayv1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/aigateway/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/guardrail"
)

var updateNemoContract = flag.Bool("update", false, "rewrite test/testdata/nemo-callout-contract")

const postAuthExtproc = `server:
  grpc_address: "0.0.0.0:9004"
filter_chains:
  - name: ipp
    filters:
      - filter: request_id

insecure_options:
  allow_unbounded_body: true
`

func TestConfigureNemoCalloutsWithoutAcceptance(t *testing.T) {
	resources := nemoCalloutFixtures("red", "gw")
	resources, err := configureNemoCallouts(resources, "red", "gw", "red", nil)
	if err != nil {
		t.Fatalf("configureNemoCallouts: %v", err)
	}
	if len(resources) != 4 {
		t.Fatalf("resources = %d, want the four fixtures and no NeMo RBAC", len(resources))
	}
	if data, _, _ := unstructured.NestedStringMap(resources[3].Object, "data"); data["extproc.yaml"] != postAuthExtproc {
		t.Fatalf("extproc.yaml changed without accepted callouts:\n%s", data["extproc.yaml"])
	}
	if egress, _, _ := unstructured.NestedSlice(resources[2].Object, "spec", "egress"); len(egress) != 1 {
		t.Fatalf("egress = %v, want the rendered rule only", egress)
	}
	if _, found, _ := unstructured.NestedString(resources[0].Object, "spec", "template", "metadata", "annotations", nemoCalloutConfigAnnotation); found {
		t.Fatal("pod template was annotated without callouts")
	}
}

func TestConfigureNemoCalloutsCompilesAcceptedEndpoint(t *testing.T) {
	resources := nemoCalloutFixtures("red", "gw")
	callouts := []nemoCallout{
		{name: "zeta", endpoint: "http://zeta.other.svc", serviceNamespace: "other", checks: []nemoCheck{{configID: "zeta-v1", header: "h-zeta", response: true}}},
		{name: "alpha", endpoint: "http://tenant-a-nemo.odh-ai-gateway-infra.svc.cluster.local", serviceNamespace: "odh-ai-gateway-infra", timeout: 5 * time.Second, checks: []nemoCheck{
			{configID: "pii-v1", header: "h-pii", request: true},
			{configID: "jailbreak-v1", header: "h-jailbreak", request: true},
		}},
		{name: "beta", endpoint: "http://tenant-a-nemo.odh-ai-gateway-infra.svc.cluster.local/v1/checks", serviceNamespace: "odh-ai-gateway-infra", checks: []nemoCheck{
			{configID: "toxicity-v1", header: "h-toxicity", request: true, response: true},
		}},
	}
	resources, err := configureNemoCallouts(resources, "red", "gw", "red", callouts)
	if err != nil {
		t.Fatalf("configureNemoCallouts: %v", err)
	}
	if len(resources) != 4 {
		t.Fatalf("resources = %d, want no NeMo RBAC for auth-off providers", len(resources))
	}
	volumes, _, _ := unstructured.NestedSlice(resources[0].Object, "spec", "template", "spec", "volumes")
	if len(volumes) != 2 {
		t.Fatalf("volumes = %v, want plugins + TLS only (no NeMo auth volume for auth-off providers)", volumes)
	}
	for _, volume := range volumes {
		v, ok := volume.(map[string]any)
		if !ok {
			t.Fatalf("volume = %T, want object", volume)
		}
		if name, _, _ := unstructured.NestedString(v, "name"); name == nemoAuthVolumeName {
			t.Fatalf("volumes = %v, want no NeMo auth volume for auth-off providers", volumes)
		}
	}
	data, _, _ := unstructured.NestedStringMap(findResource(resources, "ConfigMap", PayloadProcessingPluginsConfigMapForTenant("red"), "gw").Object, "data")
	config := data["extproc.yaml"]
	if data["pre-extproc.yaml"] != "pre" {
		t.Fatal("pre-auth config was rewritten")
	}
	assertCompiledNemoAcceptedEndpoint(t, config)

	egress, _, _ := unstructured.NestedSlice(findResource(resources, "NetworkPolicy", PayloadProcessingNetworkPolicyName("red"), "gw").Object, "spec", "egress")
	if len(egress) != 2 {
		t.Fatalf("egress = %v, want the rendered rule plus one NeMo rule", egress)
	}
	rule, ok := egress[1].(map[string]any)
	if !ok {
		t.Fatalf("NeMo egress rule = %v, want a map", egress[1])
	}
	ports, _ := rule["ports"].([]any)
	port, _ := ports[0].(map[string]any)
	if len(ports) != 1 || port["port"] != nemoServerPort || port["protocol"] != "TCP" {
		t.Fatalf("NeMo egress ports = %v, want TCP %d", ports, nemoServerPort)
	}
	var namespaces []string
	peers, _ := rule["to"].([]any)
	for _, peer := range peers {
		peerMap, ok := peer.(map[string]any)
		if !ok {
			t.Fatalf("egress peer = %v, want a map", peer)
		}
		labels, _, err := unstructured.NestedStringMap(peerMap, "namespaceSelector", "matchLabels")
		if err != nil {
			t.Fatalf("egress peer labels: %v", err)
		}
		namespaces = append(namespaces, labels["kubernetes.io/metadata.name"])
	}
	if len(namespaces) != 2 || namespaces[0] != "odh-ai-gateway-infra" || namespaces[1] != "other" {
		t.Fatalf("NeMo egress namespaces = %v, want [odh-ai-gateway-infra other]", namespaces)
	}

	hash, _, _ := unstructured.NestedString(findResource(resources, "Deployment", PayloadProcessingDeploymentName("red"), "gw").Object,
		"spec", "template", "metadata", "annotations", nemoCalloutConfigAnnotation)
	if hash == "" {
		t.Fatal("post-auth pod template has no config hash; Praxis would keep the old extproc.yaml")
	}
	if _, found, _ := unstructured.NestedString(findResource(resources, "Deployment", PayloadPreProcessingDeploymentName("red"), "gw").Object,
		"spec", "template", "metadata", "annotations", nemoCalloutConfigAnnotation); found {
		t.Fatal("pre-auth pod was annotated")
	}
}

type compiledNemoCheck struct {
	configID, endpoint, header string
	request, response          bool
}

// assertCompiledNemoAcceptedEndpoint checks the compiled NeMo filter chain for
// TestConfigureNemoCalloutsCompilesAcceptedEndpoint. Extracted so the parent
// test stays under the gocyclo limit.
func assertCompiledNemoAcceptedEndpoint(t *testing.T, config string) {
	t.Helper()
	doc, filters := parseCompiled(t, config)
	if first, _ := filters[0].(map[string]any); first["filter"] != "request_id" {
		t.Fatalf("request_id is no longer the first filter:\n%s", config)
	}
	const infra = "http://tenant-a-nemo.odh-ai-gateway-infra.svc.cluster.local/v1/checks"
	want := []compiledNemoCheck{
		{"pii-v1", infra, "h-pii", true, false},
		{"jailbreak-v1", infra, "h-jailbreak", true, false},
		{"toxicity-v1", infra, "h-toxicity", true, false},
		// Response filters are listed last-to-first. Praxis walks them
		// backwards, so toxicity runs after zeta, the catalog order.
		{"toxicity-v1", infra, "h-toxicity", false, true},
		{"zeta-v1", "http://zeta.other.svc/v1/checks", "h-zeta", false, true},
	}
	if len(filters) != len(want)+2 {
		t.Fatalf("filters = %d, want request_id, %d checks and the header cleanup:\n%s", len(filters), len(want), config)
	}
	for i, w := range want {
		entry, _ := filters[i+1].(map[string]any)
		provider, _ := entry["provider"].(map[string]any)
		phase, _ := entry["phase"].(map[string]any)
		guardrails, _ := provider["guardrails"].(map[string]any)
		ids, _ := guardrails["config_ids"].([]any)
		if entry["filter"] != "ai_guardrails" || provider["type"] != "nemo" || len(ids) != 1 || ids[0] != w.configID ||
			provider["endpoint"] != w.endpoint || phase["request"] != w.request || phase["response"] != w.response {
			t.Fatalf("check %d = %v, want %+v (request checks in catalog order, response checks placed in reverse)", i, entry, w)
		}
		if _, set := provider["model"]; set {
			t.Fatalf("check %d sets model = %v; NeMo must keep the model from its configuration", i, provider["model"])
		}
		conditions, _ := entry["conditions"].([]any)
		var when map[string]any
		if len(conditions) == 1 {
			when, _ = conditions[0].(map[string]any)["when"].(map[string]any)
		}
		headers, _ := when["headers"].(map[string]any)
		if len(headers) != 1 || headers[w.header] != "true" {
			t.Fatalf("check %d conditions = %v, want it to run only when %s is \"true\"", i, entry["conditions"], w.header)
		}
		_, hasTimeout := provider["timeout_ms"]
		if hasTimeout != (w.endpoint == infra && w.configID != "toxicity-v1") {
			t.Fatalf("check %d timeout_ms = %v, want it only on the policy that sets one", i, provider["timeout_ms"])
		}
	}
	cleanup, _ := filters[len(filters)-1].(map[string]any)
	removed, _ := cleanup["request_remove"].([]any)
	wantRemoved := []any{"h-pii", "h-jailbreak", "h-toxicity", "h-zeta"}
	if cleanup["filter"] != "headers" || len(removed) != len(wantRemoved) {
		t.Fatalf("last filter = %v, want a headers filter removing %v", cleanup, wantRemoved)
	}
	for i := range wantRemoved {
		if removed[i] != wantRemoved[i] {
			t.Fatalf("request_remove = %v, want each selection header once: %v", removed, wantRemoved)
		}
	}
	insecure, _ := doc["insecure_options"].(map[string]any)
	if insecure["allow_private_upstreams"] != true || insecure["allow_unbounded_body"] != true {
		t.Fatalf("insecure_options = %v, want private upstreams added and unbounded body kept", insecure)
	}
}

func TestConfigureNemoCalloutsWithoutNetworkPolicy(t *testing.T) {
	resources := nemoCalloutFixtures("red", "gw")
	resources = append(resources[:2], resources[3])
	callouts := []nemoCallout{{name: "safety", endpoint: "http://tenant-a-nemo.odh-ai-gateway-infra.svc", serviceNamespace: "odh-ai-gateway-infra", checks: []nemoCheck{{configID: "pii-v1", request: true}}}}
	if _, err := configureNemoCallouts(resources, "red", "gw", "red", callouts); err != nil {
		t.Fatalf("configureNemoCallouts without a NetworkPolicy: %v", err)
	}
}

func TestClosedCheckRejectsItsSelection(t *testing.T) {
	broken := acceptedPolicy("broken", 3, 3, "http://attacker.example")
	callouts, err := calloutsFor(t, broken)
	if !errors.Is(err, errUncompilableCallout) || len(callouts) != 1 || !callouts[0].closed {
		t.Fatalf("callouts = %#v, err = %v, want one closed policy", callouts, err)
	}
	compiled, err := compileNemoExtproc(postAuthExtproc, callouts)
	if err != nil {
		t.Fatalf("compileNemoExtproc: %v", err)
	}
	if !strings.Contains(compiled, "filter: static_response") || !strings.Contains(compiled, "status: 403") {
		t.Fatalf("compiled config = %s, want the selected check rejected", compiled)
	}
	header := guardrail.CheckHeaderName("tenant-a", "broken", "pii")
	if !strings.Contains(compiled, header+": \"true\"") {
		t.Fatalf("compiled config = %s, want the check's selection header", compiled)
	}
}

func TestNemoEgressUsesServicePodSelector(t *testing.T) {
	resources := nemoCalloutFixtures("red", "gw")
	callouts := []nemoCallout{{
		name: "safety", endpoint: "http://tenant-a-nemo.odh-ai-gateway-infra.svc", serviceNamespace: "odh-ai-gateway-infra",
		podLabels: map[string]string{"app": "nemo"},
		checks:    []nemoCheck{{configID: "pii-v1", header: "h", request: true}},
	}}
	resources, err := configureNemoCallouts(resources, "red", "gw", "red", callouts)
	if err != nil {
		t.Fatalf("configureNemoCallouts: %v", err)
	}
	egress, _, err := unstructured.NestedSlice(findResource(resources, "NetworkPolicy", PayloadProcessingNetworkPolicyName("red"), "gw").Object, "spec", "egress")
	if err != nil || len(egress) != 2 {
		t.Fatalf("egress = %v, err = %v, want the rendered rule plus the NeMo pods", egress, err)
	}
	rule, _ := egress[1].(map[string]any)
	peers, _ := rule["to"].([]any)
	peer, _ := peers[0].(map[string]any)
	labels, _, err := unstructured.NestedStringMap(peer, "podSelector", "matchLabels")
	if err != nil || labels["app"] != "nemo" {
		t.Fatalf("pod selector = %v, err = %v, want app=nemo", labels, err)
	}
	ns, _, _ := unstructured.NestedStringMap(peer, "namespaceSelector", "matchLabels")
	if ns["kubernetes.io/metadata.name"] != "odh-ai-gateway-infra" {
		t.Fatalf("namespace selector = %v, want infra", ns)
	}
}

func TestBindNemoPodSelectorsCopiesServiceSelector(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "nemo", Namespace: "odh-ai-gateway-infra"},
		Spec:       corev1.ServiceSpec{Selector: map[string]string{"app": "nemo"}},
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(svc).Build()
	callouts := []nemoCallout{
		{serviceName: "nemo", serviceNamespace: "odh-ai-gateway-infra"},
		{serviceName: "missing", serviceNamespace: "odh-ai-gateway-infra"},
	}
	bindNemoPodSelectors(context.Background(), reader, callouts)
	if callouts[0].podLabels["app"] != "nemo" {
		t.Fatalf("pod labels = %v, want the Service selector", callouts[0].podLabels)
	}
	if len(callouts[1].podLabels) != 0 {
		t.Fatalf("missing Service pod labels = %v, want the whole namespace", callouts[1].podLabels)
	}
}

func TestAcceptedNemoCalloutsUsesCrossNamespaceServiceName(t *testing.T) {
	policy := acceptedPolicy("safety", 3, 3, "http://tenant-a-nemo.odh-ai-gateway-infra.svc.cluster.local")
	callouts, err := calloutsFor(t, policy)
	if err != nil {
		t.Fatalf("acceptedNemoCallouts: %v", err)
	}
	if len(callouts) != 1 || callouts[0].serviceName != "tenant-a-nemo" || callouts[0].serviceNamespace != "odh-ai-gateway-infra" || callouts[0].closed {
		t.Fatalf("callouts = %#v, want Service tenant-a-nemo in odh-ai-gateway-infra", callouts)
	}
}

func TestAcceptedNemoCalloutsAcceptsHTTPSEndpoint(t *testing.T) {
	policy := acceptedPolicy("safety", 3, 3, "https://tenant-a-nemo.odh-ai-gateway-infra.svc.cluster.local")
	callouts, err := calloutsFor(t, policy)
	if err != nil {
		t.Fatalf("acceptedNemoCallouts: %v", err)
	}
	if len(callouts) != 1 || !callouts[0].authenticated || callouts[0].serviceNamespace != "odh-ai-gateway-infra" || callouts[0].serviceName != "tenant-a-nemo" || callouts[0].endpoint != policy.Status.ProviderEndpoint {
		t.Fatalf("callouts = %#v, want the authenticated tenant-a-nemo Service in odh-ai-gateway-infra", callouts)
	}
}

// TestCalloutInTheOtherInfraNamespaceIsReported pins the composition
// reconcilePraxis runs. The guardrail reconciler accepts a NeMo Service in
// either AI Gateway infrastructure namespace, so acceptedNemoCallouts must
// compile it and leave rejecting it to pinNemoCalloutsToInfra, which is what
// hands it to reportNemoOutsideInfra. Closing it any earlier would reject the
// policy's requests with nothing on the AIGuardrail to say why.
func TestCalloutInTheOtherInfraNamespaceIsReported(t *testing.T) {
	policy := acceptedPolicy("safety", 3, 3, "https://tenant-a-nemo.redhat-ai-gateway-infra.svc.cluster.local")
	callouts, err := calloutsFor(t, policy)
	if err != nil || len(callouts) != 1 || callouts[0].closed {
		t.Fatalf("callouts = %#v, %v; want the policy compiled", callouts, err)
	}
	outside := pinNemoCalloutsToInfra(callouts, "odh-ai-gateway-infra")
	if !callouts[0].closed {
		t.Fatalf("callout = %#v, want it closed outside the tenant's infrastructure namespace", callouts[0])
	}
	if len(outside) != 1 || outside[0].name != "safety" || outside[0].serviceNamespace != "redhat-ai-gateway-infra" {
		t.Fatalf("outside = %#v, want safety reported in redhat-ai-gateway-infra", outside)
	}
}

// TestPinNemoCalloutsToInfra pins that only a provider Service in the
// infrastructure namespace gets egress and a grant. Any other namespace, or an
// unpublished infrastructure namespace, closes the policy's checks, and only
// the first is reported: the reconcile reports the second itself.
func TestPinNemoCalloutsToInfra(t *testing.T) {
	callout := func(namespace string) nemoCallout {
		return nemoCallout{
			name: "safety", endpoint: "https://tenant-a-nemo." + namespace + ".svc.cluster.local",
			serviceName: "tenant-a-nemo", serviceNamespace: namespace, authenticated: true,
			checks: []nemoCheck{{name: "pii", configID: "pii-v1", header: "x-pii", request: true, response: true}},
		}
	}
	closed := nemoCallout{name: "safety", closed: true, checks: []nemoCheck{{name: "pii", header: "x-pii", request: true, closed: true}}}
	for _, c := range []struct {
		name, serviceNamespace, infraNamespace string
		wantClosed, wantReported               bool
	}{
		{"in the infrastructure namespace", "odh-ai-gateway-infra", "odh-ai-gateway-infra", false, false},
		{"in another namespace", "kube-system", "odh-ai-gateway-infra", true, true},
		{"infrastructure namespace unpublished", "odh-ai-gateway-infra", "", true, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := []nemoCallout{callout(c.serviceNamespace)}
			outside := pinNemoCalloutsToInfra(got, c.infraNamespace)
			want := callout(c.serviceNamespace)
			if c.wantClosed {
				want = closed
			}
			if !reflect.DeepEqual(got, []nemoCallout{want}) {
				t.Fatalf("callouts = %+v, want %+v", got, want)
			}
			var wantOutside []nemoCallout
			if c.wantReported {
				wantOutside = []nemoCallout{callout(c.serviceNamespace)}
			}
			if !reflect.DeepEqual(outside, wantOutside) {
				t.Fatalf("outside = %+v, want %+v", outside, wantOutside)
			}
			if rbac := nemoCallerRBAC(got, "tenant-a", "gw", "sa"); c.wantClosed && len(rbac) != 0 {
				t.Fatalf("closed callout still granted: %v", rbac)
			}
		})
	}
}

// TestReportNemoOutsideInfraEventsOnlyOnChange pins that the owner of an
// AIGuardrail the tenant fails closed gets one Warning Event per change, not
// one per resync, and a new one if the problem clears and comes back.
func TestReportNemoOutsideInfraEventsOnlyOnChange(t *testing.T) {
	policy := acceptedPolicy("safety", 3, 3, "https://tenant-a-nemo.kube-system.svc.cluster.local")
	recorder := events.NewFakeRecorder(10)
	r := &Reconciler{Client: fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(policy).Build(), Recorder: recorder}
	outside := []nemoCallout{{name: "safety", serviceName: "tenant-a-nemo", serviceNamespace: "kube-system"}}
	report := func(outside []nemoCallout) {
		r.reportNemoOutsideInfra(context.Background(), logr.Discard(), "tenant-a", "odh-ai-gateway-infra", outside)
	}
	expectEvents := func(want int) {
		t.Helper()
		if got := len(recorder.Events); got != want {
			t.Fatalf("events = %d, want %d", got, want)
		}
		for range want {
			if event := <-recorder.Events; !strings.Contains(event, reasonProviderOutsideInfra) || !strings.Contains(event, "kube-system/tenant-a-nemo") {
				t.Fatalf("event = %q", event)
			}
		}
	}

	report(outside)
	expectEvents(1)
	report(outside)
	expectEvents(0)
	report(nil)
	expectEvents(0)
	report(outside)
	expectEvents(1)
}

func TestCompileNemoExtprocRequiresRequestIDFilter(t *testing.T) {
	callouts := []nemoCallout{{name: "safety", endpoint: "http://tenant-a-nemo.odh-ai-gateway-infra.svc", checks: []nemoCheck{{configID: "pii-v1", request: true}}}}
	if _, err := compileNemoExtproc("filter_chains:\n  - name: ipp\n    filters: []\n", callouts); err == nil {
		t.Fatal("compiled checks into a chain with no request_id anchor")
	}
	if _, err := compileNemoExtproc("filter_chains: [", callouts); err == nil {
		t.Fatal("compiled checks into an unparsable config")
	}
}

func TestNemoCalloutChangedIgnoresConditionChurn(t *testing.T) {
	before := acceptedPolicy("safety", 3, 3, "http://tenant-a-nemo.odh-ai-gateway-infra.svc")
	after := before.DeepCopy()
	after.Status.Conditions[0].LastTransitionTime = metav1.Now()
	after.Status.Conditions = append(after.Status.Conditions, metav1.Condition{Type: guardrail.ConditionProviderReady, Status: metav1.ConditionTrue})
	after.Finalizers = []string{"x"}
	if nemoCalloutChanged().Update(event.UpdateEvent{ObjectOld: before, ObjectNew: after}) {
		t.Fatal("a condition timestamp or finalizer change requeued the tenant")
	}
	for name, mutate := range map[string]func(*aigatewayv1alpha1.AIGuardrail){
		"generation": func(p *aigatewayv1alpha1.AIGuardrail) { p.Generation++ },
		"accepted":   func(p *aigatewayv1alpha1.AIGuardrail) { p.Status.Conditions[0].Status = metav1.ConditionFalse },
		"revision":   func(p *aigatewayv1alpha1.AIGuardrail) { p.Status.BindingRevision = "rev2" },
		"endpoint":   func(p *aigatewayv1alpha1.AIGuardrail) { p.Status.ProviderEndpoint = "http://nemo.moved.svc" },
		"deleting":   func(p *aigatewayv1alpha1.AIGuardrail) { now := metav1.Now(); p.DeletionTimestamp = &now },
	} {
		changed := before.DeepCopy()
		mutate(changed)
		if !nemoCalloutChanged().Update(event.UpdateEvent{ObjectOld: before, ObjectNew: changed}) {
			t.Errorf("%s change did not requeue the tenant", name)
		}
	}
}

// TestNemoCalloutContract pins the post-auth extproc.yaml the controller
// compiles. Praxis builds a pipeline from the same file in its register
// tests (filters/testdata/ai-gateway-controller-extproc.yaml), so a field
// Praxis does not accept fails there. Regenerate with -update and copy the
// file to praxis-proxy/ai in the same change. It covers both provider shapes:
// plaintext, and kube-rbac-proxy with token_file and tls.ca_file.
func TestNemoCalloutContract(t *testing.T) {
	compiled, err := compileNemoExtproc(postAuthExtproc, []nemoCallout{{
		name:             "safety",
		endpoint:         "http://nemo.nemo-infra.svc.cluster.local",
		serviceNamespace: "nemo-infra",
		timeout:          5 * time.Second,
		checks: []nemoCheck{{
			configID: "pii-v1",
			header:   guardrail.CheckHeaderName("tenant-a", "safety", "pii"),
			request:  true,
			response: true,
		}},
	}, {
		name:             "secure",
		endpoint:         "https://tenant-a-nemo.odh-ai-gateway-infra.svc.cluster.local",
		serviceName:      "tenant-a-nemo",
		serviceNamespace: "odh-ai-gateway-infra",
		authenticated:    true,
		timeout:          5 * time.Second,
		checks: []nemoCheck{{
			configID: "jailbreak-v1",
			header:   guardrail.CheckHeaderName("tenant-a", "secure", "jailbreak"),
			request:  true,
			response: true,
		}},
	}})
	if err != nil {
		t.Fatalf("compileNemoExtproc: %v", err)
	}
	path := filepath.Join("..", "..", "test", "testdata", "nemo-callout-contract", "v1", "extproc.yaml")
	if *updateNemoContract {
		if err := os.WriteFile(path, []byte(compiled), 0o600); err != nil {
			t.Fatalf("write contract: %v", err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read contract: %v", err)
	}
	if string(want) != compiled {
		t.Fatalf("compiled extproc.yaml drifted from %s; rerun with -update and update the Praxis copy:\n%s", path, compiled)
	}
}

func TestAcceptedNemoCalloutsSkipsStalePolicy(t *testing.T) {
	current := acceptedPolicy("current", 3, 3, "http://tenant-a-nemo.odh-ai-gateway-infra.svc.cluster.local")
	refused := acceptedPolicy("refused", 3, 3, "http://tenant-a-nemo.odh-ai-gateway-infra.svc.cluster.local")
	refused.Status.Conditions[0].Status = metav1.ConditionFalse
	deleting := acceptedPolicy("deleting", 3, 3, "http://tenant-a-nemo.odh-ai-gateway-infra.svc.cluster.local")
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	deleting.Finalizers = []string{"aigateway.opendatahub.io/cleanup"}
	callouts, err := calloutsFor(t, current, refused, deleting)
	if err != nil {
		t.Fatalf("acceptedNemoCallouts: %v", err)
	}
	if len(callouts) != 3 || callouts[0].name != "current" || callouts[0].closed || len(callouts[0].checks) != 1 || !callouts[0].checks[0].request || callouts[0].checks[0].response {
		t.Fatalf("callouts = %#v, want the current policy first", callouts)
	}
	if callouts[0].serviceNamespace != "odh-ai-gateway-infra" || callouts[0].serviceName != "tenant-a-nemo" || callouts[0].authenticated {
		t.Fatalf("callout = %#v, want the auth-off tenant-a-nemo Service in odh-ai-gateway-infra", callouts[0])
	}
	if callouts[1].name != "deleting" || !callouts[1].closed || callouts[2].name != "refused" || !callouts[2].closed {
		t.Fatalf("callouts = %#v, want deleting and refused to reject their selection", callouts)
	}
	compiled, err := compileNemoExtproc(postAuthExtproc, callouts[2:])
	if err != nil || !strings.Contains(compiled, "filter: static_response") {
		t.Fatalf("refused config = %s, err = %v, want the selection rejected", compiled, err)
	}

	stale := acceptedPolicy("stale", 4, 3, "http://tenant-a-nemo.odh-ai-gateway-infra.svc.cluster.local")
	reader := attachedReader(current, stale)
	_, err = acceptedNemoCallouts(context.Background(), reader, "tenant-a", false)
	if !errors.Is(err, errStaleBinding) {
		t.Fatalf("acceptedNemoCallouts error = %v, want the stale policy to block publishing", err)
	}
	if has, err := hasNemoCallouts(context.Background(), reader, "tenant-a"); err != nil || !has {
		t.Fatalf("hasNemoCallouts = %v, %v; want the post-auth filter kept on while status catches up", has, err)
	}
}

// TestAcceptedNemoCalloutsUseCheckSelectionHeader pins that each check is
// keyed by the shared x-aigateway-guardrail-<id> header for its namespace,
// AIGuardrail name and check name, not its NeMo config ID.
func TestAcceptedNemoCalloutsUseCheckSelectionHeader(t *testing.T) {
	policy := acceptedPolicy("safety", 3, 3, "http://tenant-a-nemo.odh-ai-gateway-infra.svc")
	callouts, err := calloutsFor(t, policy)
	if err != nil {
		t.Fatalf("acceptedNemoCallouts: %v", err)
	}
	if len(callouts) != 1 || len(callouts[0].checks) != 1 {
		t.Fatalf("callouts = %#v, want one check", callouts)
	}
	if got, want := callouts[0].checks[0].header, guardrail.CheckHeaderName("tenant-a", "safety", "pii"); got != want {
		t.Fatalf("header = %q, want %q", got, want)
	}
	if has, err := hasNemoCallouts(context.Background(), attachedReader(policy), "tenant-a"); err != nil || !has {
		t.Fatalf("hasNemoCallouts = %v, %v; want true", has, err)
	}
}

// TestAcceptedNemoCalloutsIsolatesBrokenPolicy pins that one policy that
// cannot be compiled is left out instead of failing the whole tenant.
func TestAcceptedNemoCalloutsIsolatesBrokenPolicy(t *testing.T) {
	good := acceptedPolicy("good", 3, 3, "http://tenant-a-nemo.odh-ai-gateway-infra.svc")
	broken := map[string]string{
		"external-host":  "http://attacker.example",
		"other-service":  "http://other.odh-ai-gateway-infra.svc",
		"bad-namespace":  "http://nemo.in_fra.svc",
		"tenant-ns":      "http://nemo.tenant-a.svc",
		"system-ns":      "http://tenant-a-nemo.kube-system.svc",
		"bare-name":      "http://nemo.odh-ai-gateway-infra.svc",
		"wrong-port":     "http://tenant-a-nemo.odh-ai-gateway-infra.svc:8443",
		"pathed":         "http://tenant-a-nemo.odh-ai-gateway-infra.svc/proxy",
		"query":          "http://tenant-a-nemo.odh-ai-gateway-infra.svc/?x=1",
		"other-scheme":   "ftp://tenant-a-nemo.odh-ai-gateway-infra.svc",
		"https-port":     "https://tenant-a-nemo.odh-ai-gateway-infra.svc:8443",
		"https-other":    "https://other.odh-ai-gateway-infra.svc",
		"not-a-svc-name": "http://nemo.infra.pod",
	}
	broken["userinfo"] = "http://user:pw@nemo.odh-ai-gateway-infra.svc"
	policies := []*aigatewayv1alpha1.AIGuardrail{good}
	for name, endpoint := range broken {
		policies = append(policies, acceptedPolicy(name, 3, 3, endpoint))
	}
	noConfig := acceptedPolicy("no-config", 3, 3, "http://tenant-a-nemo.odh-ai-gateway-infra.svc")
	noConfig.Spec.Checks[0].ConfigID = ""
	policies = append(policies, noConfig)
	callouts, err := calloutsFor(t, policies...)
	if !errors.Is(err, errUncompilableCallout) {
		t.Fatalf("acceptedNemoCallouts error = %v, want %v", err, errUncompilableCallout)
	}
	var goodCallout *nemoCallout
	closed := map[string]bool{}
	for i := range callouts {
		if callouts[i].closed {
			closed[callouts[i].name] = true
			continue
		}
		if goodCallout != nil {
			t.Fatalf("callouts = %#v, want one compilable policy", callouts)
		}
		goodCallout = &callouts[i]
	}
	if goodCallout == nil || goodCallout.name != "good" {
		t.Fatalf("callouts = %#v, want the valid policy kept", callouts)
	}
	for _, policy := range policies {
		if policy.Name == "good" {
			continue
		}
		if !closed[policy.Name] {
			t.Fatalf("policy %s was omitted; a request that selects it would skip the check", policy.Name)
		}
	}
}

// TestAcceptedNemoCalloutsRejectsSubMillisecondTimeout pins the boundary
// Praxis can represent. 1ms is rendered as timeout_ms 1. 500us would become
// timeout_ms 0, which Praxis rejects, so that policy rejects its own selection
// and the sibling is still called.
func TestAcceptedNemoCalloutsRejectsSubMillisecondTimeout(t *testing.T) {
	ready := acceptedPolicy("ready", 3, 3, "http://tenant-a-nemo.odh-ai-gateway-infra.svc.cluster.local")
	ready.Spec.Provider.Timeout.Duration = time.Millisecond
	fine := acceptedPolicy("fine", 3, 3, "http://tenant-a-nemo.odh-ai-gateway-infra.svc.cluster.local")
	fine.Spec.Provider.Timeout.Duration = 500 * time.Microsecond
	callouts, err := calloutsFor(t, ready, fine)
	if !errors.Is(err, errUncompilableCallout) {
		t.Fatalf("acceptedNemoCallouts error = %v, want %v", err, errUncompilableCallout)
	}
	if len(callouts) != 2 || callouts[0].name != "fine" || !callouts[0].closed || callouts[1].name != "ready" || callouts[1].timeout != time.Millisecond {
		t.Fatalf("callouts = %#v, want the 1ms policy and a closed stand-in for the finer one", callouts)
	}
	entry := nemoFilterEntry(nemoFilter{endpoint: callouts[1].endpoint, configID: "pii-v1", header: "h", timeout: callouts[1].timeout, request: true})
	provider, ok := entry["provider"].(map[string]any)
	if !ok || provider["timeout_ms"] != int64(1) {
		t.Fatalf("provider = %v, want timeout_ms 1", entry["provider"])
	}
	omitted := nemoFilterEntry(nemoFilter{timeout: 500 * time.Microsecond, request: true})
	omittedProvider, ok := omitted["provider"].(map[string]any)
	if !ok {
		t.Fatalf("provider = %v, want a map", omitted["provider"])
	}
	if _, set := omittedProvider["timeout_ms"]; set {
		t.Fatal("a sub-millisecond timeout was rendered")
	}
}

// TestAcceptedNemoCalloutsClosesOutputChecksItCannotEnforce pins that, where the
// tenant cannot run response checks, an accepted policy with an Output check
// rejects its selection instead of compiling a response hook that would be
// skipped, and that Input-only policies still compile.
func TestTenantNemoCalloutsClosesOutputChecksItCannotEnforce(t *testing.T) {
	input := acceptedPolicy("input", 3, 3, "http://tenant-a-nemo.odh-ai-gateway-infra.svc.cluster.local")
	output := acceptedPolicy("output", 3, 3, "http://tenant-a-nemo.odh-ai-gateway-infra.svc.cluster.local")
	output.Spec.Checks[0].Phases = []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput, aigatewayv1alpha1.GuardrailPhaseOutput}
	reader := attachedReader(input, output)

	callouts, err := acceptedNemoCallouts(context.Background(), reader, "tenant-a", true)
	if !errors.Is(err, errUncompilableCallout) {
		t.Fatalf("error = %v, want %v", err, errUncompilableCallout)
	}
	if len(callouts) != 2 || callouts[0].name != "input" || callouts[0].closed || callouts[1].name != "output" || !callouts[1].closed {
		t.Fatalf("callouts = %#v, want the Input policy compiled and a closed stand-in for the Output one", callouts)
	}

	callouts, err = acceptedNemoCallouts(context.Background(), reader, "tenant-a", false)
	if err != nil || len(callouts) != 2 || callouts[1].closed || !callouts[1].checks[0].response {
		t.Fatalf("callouts = %#v, %v; want the Output check compiled where the tenant can enforce it", callouts, err)
	}
}

func TestOutputChecksUnsupportedFollowsAPITranslation(t *testing.T) {
	enabled := tenantConfigWithGuardrails(nil)
	enabled.Object["spec"] = map[string]any{"apiTranslation": map[string]any{"enabled": true}}
	disabled := tenantConfigWithGuardrails(nil)
	disabled.SetNamespace("tenant-b")
	reader := fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(enabled, disabled).Build()
	for namespace, want := range map[string]bool{"tenant-a": true, "tenant-b": false, "not-a-tenant": false} {
		why, err := OutputChecksUnsupported(context.Background(), reader, namespace)
		if err != nil || (why != "") != want {
			t.Errorf("OutputChecksUnsupported(%s) = %q, %v; want unsupported %v", namespace, why, err, want)
		}
	}
}

// TestAcceptedNemoCalloutsSkipsEmptyEndpoint pins that an accepted policy
// with no providerEndpoint cannot be called. It rejects its own selection.
// That is not a compile failure of the policies that do publish an endpoint.
func TestAcceptedNemoCalloutsSkipsEmptyEndpoint(t *testing.T) {
	ready := acceptedPolicy("ready", 3, 3, "http://tenant-a-nemo.odh-ai-gateway-infra.svc.cluster.local")
	empty := acceptedPolicy("empty", 3, 3, "")
	callouts, err := calloutsFor(t, ready, empty)
	if err != nil {
		t.Fatalf("acceptedNemoCallouts: %v", err)
	}
	if len(callouts) != 2 || callouts[0].name != "empty" || !callouts[0].closed || callouts[1].name != "ready" || callouts[1].endpoint != ready.Status.ProviderEndpoint {
		t.Fatalf("callouts = %#v, want the published policy and a closed stand-in for the empty endpoint", callouts)
	}
}

func parseCompiled(t *testing.T, config string) (map[string]any, []any) {
	t.Helper()
	var doc map[string]any
	if err := yaml.Unmarshal([]byte(config), &doc); err != nil {
		t.Fatalf("compiled extproc.yaml does not parse: %v\n%s", err, config)
	}
	chains, _ := doc["filter_chains"].([]any)
	chain, _ := chains[0].(map[string]any)
	filters, _ := chain["filters"].([]any)
	if len(filters) == 0 {
		t.Fatalf("compiled extproc.yaml has no filters:\n%s", config)
	}
	return doc, filters
}

// calloutsFor compiles the policies with each one attached, in name order,
// through the tenant config.
func calloutsFor(t *testing.T, policies ...*aigatewayv1alpha1.AIGuardrail) ([]nemoCallout, error) {
	t.Helper()
	callouts, err := acceptedNemoCallouts(context.Background(), attachedReader(policies...), "tenant-a", false)
	if err != nil && !softCalloutErr(err) {
		t.Fatalf("acceptedNemoCallouts: %v", err)
	}
	return callouts, err
}

// attachedReader serves the policies and a tenant config that attaches every
// one of them, in name order.
//
//nolint:ireturn // controller-runtime's fake client is only exposed as an interface.
func attachedReader(policies ...*aigatewayv1alpha1.AIGuardrail) client.Reader {
	names := make([]string, 0, len(policies))
	builder := fake.NewClientBuilder().WithScheme(mtcSchemeForTests())
	for _, policy := range policies {
		names = append(names, policy.Name)
		builder = builder.WithObjects(policy)
	}
	sort.Strings(names)
	entries := make([]any, 0, len(names))
	for _, name := range names {
		entries = append(entries, map[string]any{"ref": map[string]any{"name": name}})
	}
	return builder.WithObjects(tenantConfigWithGuardrails(entries)).Build()
}

func tenantConfigWithGuardrails(entries []any) *unstructured.Unstructured {
	mtc := NewMaasTenantConfig()
	mtc.SetName(MaasTenantConfigInstanceName)
	mtc.SetNamespace("tenant-a")
	spec := map[string]any{}
	if entries != nil {
		spec["guardrails"] = entries
	}
	mtc.Object["spec"] = spec
	return mtc
}

// TestCatalogCompilesOnlyAttachedChecks pins the AITenant attachment contract:
// a non-empty checks list selects only the named checks, and a policy applies
// only while something attaches it.
func TestCatalogCompilesOnlyAttachedChecks(t *testing.T) {
	policy := acceptedPolicy("safety", 3, 3, "http://tenant-a-nemo.odh-ai-gateway-infra.svc")
	policy.Spec.Checks = []aigatewayv1alpha1.AIGuardrailCheck{
		{Name: "pii", ConfigID: "pii-v1", Phases: []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput}},
		{Name: "jail", ConfigID: "jail-v1", Phases: []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput}},
	}
	unattached := acceptedPolicy("unattached", 3, 3, "http://tenant-a-nemo.odh-ai-gateway-infra.svc")
	refused := acceptedPolicy("refused", 3, 3, "http://tenant-a-nemo.odh-ai-gateway-infra.svc")
	refused.Status.Conditions[0].Status = metav1.ConditionFalse
	stale := acceptedPolicy("stale", 4, 3, "http://tenant-a-nemo.odh-ai-gateway-infra.svc")
	ait := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"guardrails": []any{
			map[string]any{"ref": map[string]any{"name": "safety"}, "checks": []any{"pii"}},
		}},
		"status": map[string]any{"tenantNamespace": "tenant-a"},
	}}
	ait.SetGroupVersionKind(AITenantGVK)
	ait.SetName("red")
	ait.SetNamespace("ai-tenants")
	mtc := tenantConfigWithGuardrails(nil)
	mtc.SetAnnotations(map[string]string{AnnotationAITenantName: "red", AnnotationAITenantNamespace: "ai-tenants"})

	reader := fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(policy, unattached, refused, stale, ait, mtc).Build()
	callouts, err := acceptedNemoCallouts(context.Background(), reader, "tenant-a", false)
	if err != nil {
		t.Fatalf("acceptedNemoCallouts: %v; an unattached stale policy must not block publishing", err)
	}
	if len(callouts) != 1 || callouts[0].name != "safety" || callouts[0].closed || len(callouts[0].checks) != 1 || callouts[0].checks[0].name != "pii" {
		t.Fatalf("callouts = %#v, want only safety/pii; jail is not selected and the others are not attached", callouts)
	}

	// The AITenant stays and drops its list: the attachment is what goes
	// away. An owner that is named but missing is a baseline that cannot be
	// read, and that attaches every policy rather than none.
	emptied := ait.DeepCopy()
	unstructured.RemoveNestedField(emptied.Object, "spec", "guardrails")
	detached := fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(policy, unattached, emptied, mtc).Build()
	callouts, err = acceptedNemoCallouts(context.Background(), detached, "tenant-a", false)
	if err != nil || len(callouts) != 0 {
		t.Fatalf("callouts = %#v, err = %v; want none once the last attachment is removed", callouts, err)
	}
	if has, err := HasPostAuthFilters(context.Background(), detached, "tenant-a"); err != nil || has {
		t.Fatalf("HasPostAuthFilters = %v, %v; want false with nothing attached", has, err)
	}
}

// TestCatalogFollowsGuardrailArrays checks that broader lists run before narrower
// ones, and that each list keeps the order it was written. Two subscriptions are
// ordered by name (aaa, then team). Two model refs are ordered by namespace
// (models/granite, then tenant-a/qwen), and the subscription's own model entry
// comes after those. A check named twice runs the first time. A scope that
// selects some of a policy's checks compiles only those, and a tenant policy
// nobody attaches is not compiled.
func TestCatalogFollowsGuardrailArrays(t *testing.T) {
	endpoint := "http://tenant-a-nemo.odh-ai-gateway-infra.svc"
	zebra := acceptedPolicy("zebra", 3, 3, endpoint)
	zebra.Spec.Checks = []aigatewayv1alpha1.AIGuardrailCheck{
		{Name: "pii", ConfigID: "pii-v1", Phases: []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput}},
		{Name: "jail", ConfigID: "jail-v1", Phases: []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput, aigatewayv1alpha1.GuardrailPhaseOutput}},
	}
	policies := []*aigatewayv1alpha1.AIGuardrail{
		zebra,
		acceptedPolicy("alpha", 3, 3, endpoint),
		acceptedPolicy("middle", 3, 3, endpoint),
		acceptedPolicy("late-name", 3, 3, endpoint),
		acceptedPolicy("early", 3, 3, endpoint),
		acceptedPolicy("model-policy", 3, 3, endpoint),
		acceptedPolicy("remote-policy", 3, 3, endpoint),
		acceptedPolicy("entry-policy", 3, 3, endpoint),
		acceptedPolicy("loose", 3, 3, endpoint),
		acceptedPolicy("unattached", 3, 3, endpoint),
	}
	subset := acceptedPolicy("subset", 3, 3, endpoint)
	subset.Spec.Checks = []aigatewayv1alpha1.AIGuardrailCheck{
		{Name: "pii", ConfigID: "pii-v1", Phases: []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput}},
		{Name: "jail", ConfigID: "jail-v1", Phases: []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput}},
	}
	policies = append(policies, subset)
	shared := acceptedPolicy("shared", 3, 3, endpoint)
	shared.Spec.Checks = []aigatewayv1alpha1.AIGuardrailCheck{
		{Name: "b", ConfigID: "b-v1", Phases: []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput}},
		{Name: "a", ConfigID: "a-v1", Phases: []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput}},
	}
	policies = append(policies, shared)

	aitenant := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"guardrails": []any{
			map[string]any{"ref": map[string]any{"name": "zebra"}},
			map[string]any{"ref": map[string]any{"name": "alpha"}},
			map[string]any{"ref": map[string]any{"name": "shared"}, "checks": []any{"a"}},
		}},
		"status": map[string]any{"tenantNamespace": "tenant-a"},
	}}
	aitenant.SetGroupVersionKind(AITenantGVK)
	aitenant.SetName("red")
	aitenant.SetNamespace("ai-tenants")

	mtc := NewMaasTenantConfig()
	mtc.SetName(MaasTenantConfigInstanceName)
	mtc.SetNamespace("tenant-a")
	mtc.SetAnnotations(map[string]string{
		AnnotationAITenantName:      "red",
		AnnotationAITenantNamespace: "ai-tenants",
	})
	_ = unstructured.SetNestedSlice(mtc.Object, []any{
		map[string]any{"ref": map[string]any{"name": "middle"}},
		map[string]any{"ref": map[string]any{"name": "zebra"}, "checks": []any{"pii", "jail"}},
	}, "spec", "guardrails")

	team := subscriptionWithGuardrails("team", []any{
		map[string]any{"ref": map[string]any{"name": "early"}},
		map[string]any{"ref": map[string]any{"name": "shared"}, "checks": []any{"b"}},
	}, []any{
		map[string]any{
			"name":      "granite",
			"namespace": "models",
			"guardrails": []any{
				map[string]any{"ref": map[string]any{"name": "entry-policy"}},
			},
		},
	})
	aaa := subscriptionWithGuardrails("aaa", []any{
		map[string]any{"ref": map[string]any{"name": "late-name"}},
		map[string]any{"ref": map[string]any{"name": "subset"}, "checks": []any{"pii"}},
	}, nil)
	model := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"guardrails": []any{
			map[string]any{"ref": map[string]any{"name": "model-policy"}},
		}},
	}}
	model.SetGroupVersionKind(MaaSModelRefGVK)
	model.SetName("qwen")
	model.SetNamespace("tenant-a")
	// granite lives next to the model. remote-policy is the tenant policy it names.
	// loose names a model policy, so it must not take the tenant policy of the same name.
	remote := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"guardrails": []any{
			map[string]any{"ref": map[string]any{"name": "remote-policy", "namespace": "tenant-a"}},
			map[string]any{"ref": map[string]any{"name": "loose", "namespace": "models"}},
		}},
	}}
	remote.SetGroupVersionKind(MaaSModelRefGVK)
	remote.SetName("granite")
	remote.SetNamespace("models")

	builder := fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(aitenant, mtc, team, aaa, model, remote)
	for _, policy := range policies {
		builder = builder.WithObjects(policy)
	}
	callouts, err := acceptedNemoCallouts(context.Background(), builder.Build(), "tenant-a", false)
	if err != nil {
		t.Fatalf("acceptedNemoCallouts: %v", err)
	}
	type placed struct{ policy, check string }
	var got []placed
	for _, callout := range callouts {
		for _, check := range callout.checks {
			got = append(got, placed{callout.name, check.name})
		}
	}
	want := []placed{
		{"zebra", "pii"}, {"zebra", "jail"},
		{"alpha", "pii"},
		{"shared", "a"},
		{"middle", "pii"},
		{"late-name", "pii"},
		{"subset", "pii"},
		{"early", "pii"}, {"shared", "b"},
		{"remote-policy", "pii"},
		{"model-policy", "pii"},
		{"entry-policy", "pii"},
	}
	if len(got) != len(want) {
		t.Fatalf("checks = %v, want %v", got, want)
	}
	for _, check := range got {
		switch {
		case check == placed{"subset", "jail"}:
			t.Fatalf("checks = %v; subset/jail compiled though its only attachment selects pii", got)
		case check.policy == "unattached" || check.policy == "loose":
			t.Fatalf("checks = %v; %s compiled though no tenant attachment references it", got, check.policy)
		}
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("check %d = %v, want %v\nfull = %v", i, got[i], want[i], got)
		}
	}
}

// TestMalformedAttachmentFailsClosed pins that a bad entry never drops a
// policy it may reference. Unreadable checks select the whole named policy;
// an entry with no readable policy attaches every tenant policy. Readable
// entries keep their order and selection.
func TestMalformedAttachmentFailsClosed(t *testing.T) {
	ctx := context.Background()
	twoChecks := func(name string) *aigatewayv1alpha1.AIGuardrail {
		policy := acceptedPolicy(name, 1, 1, "http://tenant-a-nemo.odh-ai-gateway-infra.svc")
		policy.Spec.Checks = []aigatewayv1alpha1.AIGuardrailCheck{
			{Name: "pii", ConfigID: "pii-v1", Phases: []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput}},
			{Name: "jail", ConfigID: "jail-v1", Phases: []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput}},
		}
		return policy
	}
	selected := func(callouts []nemoCallout) []string {
		var got []string
		for _, callout := range callouts {
			for _, check := range callout.checks {
				got = append(got, callout.name+"/"+check.name)
			}
		}
		return got
	}
	mtcWith := func(guardrails any) *unstructured.Unstructured {
		mtc := NewMaasTenantConfig()
		mtc.SetName(MaasTenantConfigInstanceName)
		mtc.SetNamespace("tenant-a")
		mtc.Object["spec"] = map[string]any{"guardrails": guardrails}
		return mtc
	}

	cases := []struct {
		name       string
		guardrails any
		want       []string
	}{
		{
			name: "unreadable checks select the whole named policy",
			guardrails: []any{
				map[string]any{"ref": map[string]any{"name": "named"}, "checks": "pii"},
			},
			want: []string{"named/pii", "named/jail"},
		},
		{
			name: "an entry with no policy attaches every tenant policy after the readable ones",
			guardrails: []any{
				map[string]any{"ref": map[string]any{"name": "named"}, "checks": []any{"pii"}},
				map[string]any{"ref": map[string]any{}},
			},
			want: []string{"named/pii", "named/jail", "other/pii", "other/jail"},
		},
		{
			name:       "a list that is not a list attaches every tenant policy",
			guardrails: "not-a-list",
			want:       []string{"named/pii", "named/jail", "other/pii", "other/jail"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).
				WithObjects(twoChecks("named"), twoChecks("other"), mtcWith(tc.guardrails)).Build()
			callouts, err := acceptedNemoCallouts(ctx, reader, "tenant-a", false)
			if !errors.Is(err, errMalformedAttachment) || !softCalloutErr(err) {
				t.Fatalf("acceptedNemoCallouts error = %v, want a soft malformed attachment", err)
			}
			if got := selected(callouts); !slices.Equal(got, tc.want) {
				t.Fatalf("checks = %v, want %v", got, tc.want)
			}
			if has, err := hasNemoCallouts(ctx, reader, "tenant-a"); err != nil || !has {
				t.Fatalf("hasNemoCallouts = %v, %v; want the post-auth filter on", has, err)
			}
		})
	}
}

func TestMalformedAttachmentDoesNotDropTheCatalog(t *testing.T) {
	policy := acceptedPolicy("ok", 1, 1, "http://tenant-a-nemo.odh-ai-gateway-infra.svc")
	mtc := NewMaasTenantConfig()
	mtc.SetName(MaasTenantConfigInstanceName)
	mtc.SetNamespace("tenant-a")
	mtc.Object["spec"] = map[string]any{"guardrails": []any{
		map[string]any{"ref": map[string]any{"name": "ok"}},
		map[string]any{"ref": map[string]any{"name": "ok"}, "checks": []any{true}},
	}}
	sub := subscriptionWithGuardrails("team", nil, []any{
		"not-an-object",
		map[string]any{"guardrails": "not-a-list"},
		map[string]any{"guardrails": []any{
			map[string]any{"ref": map[string]any{"name": "ok"}, "checks": []any{"pii"}},
		}},
	})
	client := fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(policy, mtc, sub).Build()
	callouts, err := acceptedNemoCallouts(context.Background(), client, "tenant-a", false)
	if !errors.Is(err, errMalformedAttachment) {
		t.Fatalf("acceptedNemoCallouts error = %v, want malformed attachment", err)
	}
	if len(callouts) != 1 || callouts[0].name != "ok" || len(callouts[0].checks) != 1 {
		t.Fatalf("callouts = %+v, want the readable ok/pii attachment", callouts)
	}
}

// TestBaselineSurvivesANarrowerAttachment pins that a narrower list cannot
// shrink the baseline. The subscription re-attaches the baseline policy
// asking for one of its checks; the baseline already took both, so the
// subscription's entry contributes nothing and jail still runs.
func TestBaselineSurvivesANarrowerAttachment(t *testing.T) {
	endpoint := "http://tenant-a-nemo.odh-ai-gateway-infra.svc"
	safety := policyWithChecks("safety", endpoint, "pii", "jail")
	aitenant, mtc := baselineTenant([]any{attachEntry("safety")}, nil)
	team := subscriptionWithGuardrails("team", []any{attachEntry("safety", "pii")}, nil)

	callouts := tenantCatalog(t, []client.Object{aitenant, mtc, team}, safety)
	assertCatalog(t, flattenChecks(callouts), []placedCheck{
		{"safety", "pii"}, {"safety", "jail"},
	})
}

// TestBaselineOrderIgnoresAttachmentOrder pins that a narrower list cannot
// reorder the baseline. The subscription asks for jail before pii, and for a
// policy of its own. The baseline took both checks in policy order, so that
// is the order they run in, and the subscription's own policy runs after
// them.
func TestBaselineOrderIgnoresAttachmentOrder(t *testing.T) {
	endpoint := "http://tenant-a-nemo.odh-ai-gateway-infra.svc"
	safety := policyWithChecks("safety", endpoint, "pii", "jail")
	extra := policyWithChecks("extra", endpoint, "toxicity")
	aitenant, mtc := baselineTenant([]any{attachEntry("safety")}, nil)
	team := subscriptionWithGuardrails("team", []any{
		attachEntry("safety", "jail", "pii"),
		attachEntry("extra"),
	}, nil)

	callouts := tenantCatalog(t, []client.Object{aitenant, mtc, team}, safety, extra)
	assertCatalog(t, flattenChecks(callouts), []placedCheck{
		{"safety", "pii"}, {"safety", "jail"}, {"extra", "toxicity"},
	})
}

// TestBaselineCheckIsNotReplacedByASameName pins that a tenant policy
// declaring a check the baseline already runs adds a second check rather
// than taking the baseline's place. A check is identified by its policy and
// its name, so the baseline keeps its own configuration and runs first.
func TestBaselineCheckIsNotReplacedByASameName(t *testing.T) {
	endpoint := "http://tenant-a-nemo.odh-ai-gateway-infra.svc"
	safety := policyWithChecks("safety", endpoint, "pii")
	lax := policyWithChecks("lax", endpoint, "pii")
	aitenant, mtc := baselineTenant([]any{attachEntry("safety")}, nil)
	team := subscriptionWithGuardrails("team", []any{attachEntry("lax")}, nil)

	callouts := tenantCatalog(t, []client.Object{aitenant, mtc, team}, safety, lax)
	assertCatalog(t, flattenChecks(callouts), []placedCheck{
		{"safety", "pii"}, {"lax", "pii"},
	})
	if got := callouts[0].checks[0].configID; got != "safety-pii" {
		t.Fatalf("baseline check runs configuration %q, want safety-pii", got)
	}
}

// TestBaselineDigestIgnoresAnOverrideAttempt compiles the same baseline with
// and without attachments that re-select it, and pins that the post-auth
// Deployment's config digest is unchanged. Dropping a check from the
// baseline moves it, so the first comparison is reading a digest that does
// track the catalog.
func TestBaselineDigestIgnoresAnOverrideAttempt(t *testing.T) {
	endpoint := "http://tenant-a-nemo.odh-ai-gateway-infra.svc"
	safety := policyWithChecks("safety", endpoint, "pii", "jail")

	aitenant, mtc := baselineTenant([]any{attachEntry("safety")}, nil)
	alone := nemoConfigDigest(t, tenantCatalog(t, []client.Object{aitenant, mtc}, safety))

	reselected, reselectedMTC := baselineTenant(
		[]any{attachEntry("safety")},
		[]any{attachEntry("safety", "pii")},
	)
	team := subscriptionWithGuardrails("team", []any{attachEntry("safety", "jail", "pii")}, nil)
	attempted := nemoConfigDigest(t, tenantCatalog(t, []client.Object{reselected, reselectedMTC, team}, safety))
	if attempted != alone {
		t.Fatalf("attachments that only re-select baseline checks changed the digest\nbaseline alone = %s\nwith attachments = %s", alone, attempted)
	}

	narrowed, narrowedMTC := baselineTenant([]any{attachEntry("safety", "pii")}, nil)
	dropped := nemoConfigDigest(t, tenantCatalog(t, []client.Object{narrowed, narrowedMTC}, safety))
	if dropped == alone {
		t.Fatalf("dropping jail from the baseline left the digest at %s", alone)
	}
}

// TestUnreadableBaselineAttachesEveryPolicy pins the baseline's fail-closed
// path. A config naming an owning AITenant this catalog cannot read has a
// baseline it cannot see, and an unseen baseline must not compile the same
// as an empty one. Every tenant policy runs instead, in policy name order.
func TestUnreadableBaselineAttachesEveryPolicy(t *testing.T) {
	endpoint := "http://tenant-a-nemo.odh-ai-gateway-infra.svc"
	foreign, _ := baselineTenant([]any{attachEntry("safety")}, nil)
	_ = unstructured.SetNestedField(foreign.Object, "other-tenant", "status", "tenantNamespace")

	for _, tc := range []struct {
		name  string
		owner []client.Object
	}{
		{"owning AITenant is gone", nil},
		{"owning AITenant binds another config", []client.Object{foreign}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, mtc := baselineTenant([]any{attachEntry("safety")}, nil)
			builder := fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).
				WithObjects(append([]client.Object{mtc}, tc.owner...)...).
				WithObjects(policyWithChecks("safety", endpoint, "pii"), policyWithChecks("other", endpoint, "toxicity"))
			callouts, err := acceptedNemoCallouts(context.Background(), builder.Build(), "tenant-a", false)
			if !errors.Is(err, errMalformedAttachment) {
				t.Fatalf("acceptedNemoCallouts error = %v, want malformed attachment", err)
			}
			assertCatalog(t, flattenChecks(callouts), []placedCheck{
				{"other", "toxicity"}, {"safety", "pii"},
			})
		})
	}
}

// placedCheck is one compiled check, named by the policy it came from.
type placedCheck struct{ policy, check string }

// flattenChecks reads a catalog as the checks it runs, in order.
func flattenChecks(callouts []nemoCallout) []placedCheck {
	var placed []placedCheck
	for _, callout := range callouts {
		for _, check := range callout.checks {
			placed = append(placed, placedCheck{callout.name, check.name})
		}
	}
	return placed
}

func assertCatalog(t *testing.T, got, want []placedCheck) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("checks = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("check %d = %v, want %v\nfull = %v", i, got[i], want[i], got)
		}
	}
}

// baselineTenant builds the AITenant carrying the platform baseline and the
// MaasTenantConfig it owns.
func baselineTenant(baseline, config []any) (aitenant, mtc *unstructured.Unstructured) {
	aitenant = &unstructured.Unstructured{Object: map[string]any{
		"spec":   map[string]any{"guardrails": baseline},
		"status": map[string]any{"tenantNamespace": "tenant-a"},
	}}
	aitenant.SetGroupVersionKind(AITenantGVK)
	aitenant.SetName("red")
	aitenant.SetNamespace("ai-tenants")

	mtc = NewMaasTenantConfig()
	mtc.SetName(MaasTenantConfigInstanceName)
	mtc.SetNamespace("tenant-a")
	mtc.SetAnnotations(map[string]string{
		AnnotationAITenantName:      "red",
		AnnotationAITenantNamespace: "ai-tenants",
	})
	if config != nil {
		_ = unstructured.SetNestedSlice(mtc.Object, config, "spec", "guardrails")
	}
	return aitenant, mtc
}

// attachEntry is one entry in a guardrails list. No checks selects the whole
// policy.
func attachEntry(name string, checks ...string) map[string]any {
	entry := map[string]any{"ref": map[string]any{"name": name}}
	if len(checks) > 0 {
		selected := make([]any, 0, len(checks))
		for _, check := range checks {
			selected = append(selected, check)
		}
		entry["checks"] = selected
	}
	return entry
}

// policyWithChecks is an accepted policy whose checks run on input, in the
// order given. Each check names its own configuration, so a test can tell
// two same-named checks apart.
func policyWithChecks(name, endpoint string, checks ...string) *aigatewayv1alpha1.AIGuardrail {
	policy := acceptedPolicy(name, 3, 3, endpoint)
	policy.Spec.Checks = nil
	for _, check := range checks {
		policy.Spec.Checks = append(policy.Spec.Checks, aigatewayv1alpha1.AIGuardrailCheck{
			Name:     check,
			ConfigID: name + "-" + check,
			Phases:   []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput},
		})
	}
	return policy
}

func tenantCatalog(t *testing.T, objects []client.Object, policies ...*aigatewayv1alpha1.AIGuardrail) []nemoCallout {
	t.Helper()
	builder := fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(objects...)
	for _, policy := range policies {
		builder = builder.WithObjects(policy)
	}
	callouts, err := acceptedNemoCallouts(context.Background(), builder.Build(), "tenant-a", false)
	if err != nil {
		t.Fatalf("acceptedNemoCallouts: %v", err)
	}
	return callouts
}

// nemoConfigDigest compiles the catalog into the post-auth config and
// returns the digest the Deployment carries.
func nemoConfigDigest(t *testing.T, callouts []nemoCallout) string {
	t.Helper()
	resources := nemoCalloutFixtures("red", "gw")
	resources, err := configureNemoCallouts(resources, "red", "gw", "red", callouts)
	if err != nil {
		t.Fatalf("configureNemoCallouts: %v", err)
	}
	deployment := findResource(resources, "Deployment", PayloadProcessingDeploymentName("red"), "gw")
	digest, found, err := unstructured.NestedString(deployment.Object, "spec", "template", "metadata", "annotations", nemoCalloutConfigAnnotation)
	if err != nil || !found {
		t.Fatalf("read %s: found=%v err=%v", nemoCalloutConfigAnnotation, found, err)
	}
	return digest
}

func subscriptionWithGuardrails(name string, guardrails, modelRefs []any) *unstructured.Unstructured {
	spec := map[string]any{}
	if guardrails != nil {
		spec["guardrails"] = guardrails
	}
	if modelRefs != nil {
		spec["modelRefs"] = modelRefs
	}
	u := &unstructured.Unstructured{Object: map[string]any{"spec": spec}}
	u.SetGroupVersionKind(MaaSSubscriptionGVK)
	u.SetName(name)
	u.SetNamespace("tenant-a")
	return u
}

// acceptedPolicy references NemoGuardrails "nemo" and has an Accepted
// condition observed at the given generation.
func acceptedPolicy(name string, generation, observed int64, endpoint string) *aigatewayv1alpha1.AIGuardrail {
	return &aigatewayv1alpha1.AIGuardrail{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tenant-a", Generation: generation},
		Spec: aigatewayv1alpha1.AIGuardrailSpec{
			Provider: aigatewayv1alpha1.AIGuardrailProvider{
				Nemo: aigatewayv1alpha1.AIGuardrailNemoProvider{Ref: aigatewayv1alpha1.AIGuardrailNamespacedReference{Name: "nemo"}},
			},
			Checks: []aigatewayv1alpha1.AIGuardrailCheck{{
				Name: "pii", ConfigID: "pii-v1", Phases: []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput},
			}},
		},
		Status: aigatewayv1alpha1.AIGuardrailStatus{
			BindingRevision:  "rev",
			ProviderEndpoint: endpoint,
			Conditions: []metav1.Condition{{
				Type:               guardrail.ConditionAccepted,
				Status:             metav1.ConditionTrue,
				ObservedGeneration: observed,
			}},
		},
	}
}

// nemoCalloutFixtures returns the post-auth Deployment, the pre-auth
// Deployment, the NetworkPolicy and the plugins ConfigMap, in that order.
func nemoCalloutFixtures(tenantID, namespace string) []unstructured.Unstructured {
	deployment := deploymentFixture(PayloadProcessingDeploymentName(tenantID), PayloadProcessingName, PayloadProcessingServiceAccountName(tenantID), PayloadProcessingPluginsConfigMapForTenant(tenantID))
	deployment.SetNamespace(namespace)
	pre := deploymentFixture(PayloadPreProcessingDeploymentName(tenantID), PayloadPreProcessingName, PayloadProcessingServiceAccountName(tenantID), PayloadProcessingPluginsConfigMapForTenant(tenantID))
	pre.SetNamespace(namespace)
	policy := unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "networking.k8s.io/v1",
		"kind":       "NetworkPolicy",
		"metadata":   map[string]any{"name": PayloadProcessingNetworkPolicyName(tenantID), "namespace": namespace},
		"spec": map[string]any{
			"egress": []any{map[string]any{"ports": []any{map[string]any{"protocol": "TCP", "port": int64(6443)}}}},
		},
	}}
	configMap := unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": PayloadProcessingPluginsConfigMapForTenant(tenantID), "namespace": namespace},
		"data":       map[string]any{"extproc.yaml": postAuthExtproc, "pre-extproc.yaml": "pre"},
	}}
	return []unstructured.Unstructured{deployment, pre, policy, configMap}
}

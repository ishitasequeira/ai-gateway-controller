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

package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
)

var (
	testScope = envelope.Scope{
		Network:   "prod",
		Gateway:   "my-gateway",
		Namespace: "serving",
		LocalSite: "us-east-1",
	}
	fixedT0 = time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	fixedT1 = time.Date(2026, 9, 3, 11, 0, 0, 0, time.UTC)
)

// routeSetOne returns a single model with two uniform-weight routes, the
// minimal published shape the golden vectors exercise.
func routeSetOne() *resolver.ResolvedRouteSet {
	return &resolver.ResolvedRouteSet{Models: []resolver.ModelRoutes{{
		ModelRef: "serving/model-a",
		Routes: []resolver.Route{
			{Model: "model-a", ClientName: "model-a", Namespace: "serving", Provider: "prov-1", ProviderType: "openai", //nolint:gosec // fixture: secret names/keys, not credential values
				Cluster: "provider-prov-1", Endpoint: "prov-1.example.com", TargetModel: "model-a",
				APIFormat: "openai-chat", Path: "/v1/chat/completions", Weight: 1, AuthType: "apikey",
				SecretName: "prov-1-creds",
				SecretKey:  "api-key"},
			{Model: "model-a", ClientName: "model-a", Namespace: "serving", Provider: "prov-2", ProviderType: "openai", //nolint:gosec // fixture: secret names/keys, not credential values
				Cluster: "provider-prov-2", Endpoint: "prov-2.example.com", TargetModel: "model-a",
				APIFormat: "openai-chat", Path: "/v1/chat/completions", Weight: 1, AuthType: "apikey",
				SecretName: "prov-2-creds",
				SecretKey:  "api-key"},
		},
	}}}
}

// routeSetTwo adds a second model — content change, digest change.
func routeSetTwo() *resolver.ResolvedRouteSet {
	set := routeSetOne()
	set.Models = append(set.Models, resolver.ModelRoutes{
		ModelRef: "serving/model-b",
		Routes: []resolver.Route{{ //nolint:gosec // fixture: secret names/keys, not credential values
			Model: "model-b", ClientName: "model-b", Namespace: "serving", Provider: "prov-3", ProviderType: "openai",
			Cluster: "provider-prov-3", Endpoint: "prov-3.example.com", TargetModel: "model-b",
			APIFormat: "openai-chat", Path: "/v1/chat/completions", Weight: 1, AuthType: "apikey",
			SecretName: "prov-3-creds",
			SecretKey:  "api-key",
		}},
	})
	return set
}

func testOpts() envelope.Options {
	return envelope.Options{
		KnownClusters:   []string{"provider-prov-1", "provider-prov-2", "provider-prov-3"},
		SourceUID:       "prod/serving",
		ProducerVersion: "0.1.0",
	}
}

func newTestPublisher(t *testing.T, objs ...client.Object) *Publisher {
	t.Helper()
	p, err := New(fake.NewClientBuilder().WithObjects(objs...).Build(), Config{Namespace: "praxis"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.now = func() time.Time { return fixedT0 }
	return p
}

func readCM(t *testing.T, p *Publisher) *corev1.ConfigMap {
	t.Helper()
	var cm corev1.ConfigMap
	if err := p.client.Get(context.Background(), client.ObjectKey{Namespace: "praxis", Name: DefaultName}, &cm); err != nil {
		t.Fatalf("get configmap: %v", err)
	}
	return &cm
}

func parseEnvelope(t *testing.T, data string) envelope.Envelope {
	t.Helper()
	var env envelope.Envelope
	if err := json.Unmarshal([]byte(data), &env); err != nil {
		t.Fatalf("published data is not a parseable envelope: %v", err)
	}
	return env
}

func TestNewValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{name: "defaults name and data key", cfg: Config{Namespace: "praxis"}},
		{name: "rejects empty namespace", cfg: Config{Name: "x"}, wantErr: true},
		{name: "explicit name and key", cfg: Config{Namespace: "praxis", Name: "custom", DataKey: "overlay.json"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New(fake.NewClientBuilder().Build(), tc.cfg)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			wantName, wantKey := tc.cfg.Name, tc.cfg.DataKey
			if wantName == "" {
				wantName = DefaultName
			}
			if wantKey == "" {
				wantKey = DefaultDataKey
			}
			if p.cfg.Name != wantName || p.cfg.DataKey != wantKey {
				t.Errorf("Config = %+v, want Name=%q DataKey=%q", p.cfg, wantName, wantKey)
			}
		})
	}
}

func TestPublishFirstCreatesEnvelope(t *testing.T) {
	p := newTestPublisher(t)
	res, err := p.Publish(context.Background(), routeSetOne(), testScope, testOpts())
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !res.Created || res.Updated || res.Unchanged {
		t.Fatalf("Result = %+v, want Created only", res)
	}

	cm := readCM(t, p)
	if _, ok := cm.Labels[labelManagedBy]; !ok {
		t.Errorf("missing %s label", labelManagedBy)
	}
	if got := cm.Annotations[AnnotationSourceGeneration]; got != "1" {
		t.Errorf("source-generation annotation = %q, want 1", got)
	}

	env := parseEnvelope(t, cm.Data[DefaultDataKey])
	if env.SchemaVersion != envelope.SchemaVersion {
		t.Errorf("schema_version = %q, want %q", env.SchemaVersion, envelope.SchemaVersion)
	}
	if env.Provenance.SourceGeneration != 1 {
		t.Errorf("source_generation = %d, want 1", env.Provenance.SourceGeneration)
	}
	if cm.Annotations[AnnotationContentDigest] != env.ContentDigest.Value {
		t.Errorf("annotation digest %q != envelope digest %q", cm.Annotations[AnnotationContentDigest], env.ContentDigest.Value)
	}
	// The published bytes must hash to their own declared revision — the
	// invariant the read-back path (and the next Publish) relies on.
	got, err := envelope.ComputeDigestFromWire([]byte(cm.Data[DefaultDataKey]))
	if err != nil {
		t.Fatalf("ComputeDigestFromWire: %v", err)
	}
	if got != env.Revision.Value {
		t.Errorf("wire digest %s != declared %s", got, env.Revision.Value)
	}
	if len(env.Overlay.Candidates) != 2 {
		t.Errorf("candidates = %d, want 2", len(env.Overlay.Candidates))
	}
	// Both supported bearer_token and CRD apikey routes render a reference.
	for i, candidate := range env.Overlay.Candidates {
		if candidate.Credential == nil {
			t.Errorf("candidate %d: expected credential reference, got nil", i)
		}
	}
}

func TestPublishSecondRunIsIdempotent(t *testing.T) {
	p := newTestPublisher(t)
	if _, err := p.Publish(context.Background(), routeSetOne(), testScope, testOpts()); err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	before := readCM(t, p)

	res, err := p.Publish(context.Background(), routeSetOne(), testScope, testOpts())
	if err != nil {
		t.Fatalf("second Publish: %v", err)
	}
	if !res.Unchanged || res.Created || res.Updated {
		t.Fatalf("Result = %+v, want Unchanged only", res)
	}
	after := readCM(t, p)
	if after.ResourceVersion != before.ResourceVersion {
		t.Errorf("resourceVersion changed %s -> %s on a no-op publish", before.ResourceVersion, after.ResourceVersion)
	}
}

func TestPublishContentChangeBumpsGeneration(t *testing.T) {
	p := newTestPublisher(t)
	if _, err := p.Publish(context.Background(), routeSetOne(), testScope, testOpts()); err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	first := parseEnvelope(t, readCM(t, p).Data[DefaultDataKey])

	p.now = func() time.Time { return fixedT1 }
	res, err := p.Publish(context.Background(), routeSetTwo(), testScope, testOpts())
	if err != nil {
		t.Fatalf("second Publish: %v", err)
	}
	if !res.Updated {
		t.Fatalf("Result = %+v, want Updated", res)
	}
	second := parseEnvelope(t, readCM(t, p).Data[DefaultDataKey])
	if second.Provenance.SourceGeneration != first.Provenance.SourceGeneration+1 {
		t.Errorf("source_generation = %d, want %d", second.Provenance.SourceGeneration, first.Provenance.SourceGeneration+1)
	}
	if second.Revision.Value == first.Revision.Value {
		t.Error("digest did not change on content change")
	}
	if second.Provenance.RenderedAt != fixedT1.UTC().Format(time.RFC3339) {
		t.Errorf("rendered_at = %q, want fresh stamp on content change", second.Provenance.RenderedAt)
	}
	if readCM(t, p).Annotations[AnnotationSourceGeneration] != "2" {
		t.Errorf("annotation not refreshed to 2")
	}

	// Converges on a third run: same set -> Unchanged, no further bump.
	res, err = p.Publish(context.Background(), routeSetTwo(), testScope, testOpts())
	if err != nil {
		t.Fatalf("third Publish: %v", err)
	}
	if !res.Unchanged {
		t.Fatalf("Result = %+v, want Unchanged on repeat of identical set", res)
	}
}

func TestPublishProvenanceChangeKeepsGenerationAndRenderedAt(t *testing.T) {
	p := newTestPublisher(t)
	if _, err := p.Publish(context.Background(), routeSetOne(), testScope, testOpts()); err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	first := parseEnvelope(t, readCM(t, p).Data[DefaultDataKey])
	if first.Provenance.RenderedAt != fixedT0.UTC().Format(time.RFC3339) {
		t.Fatalf("rendered_at = %q, want injected now()", first.Provenance.RenderedAt)
	}

	// Producer version changes: bytes change, digest does not. No
	// generation bump, and rendered_at is inherited (content did not
	// change, so re-stamping would churn the ConfigMap for nothing).
	p.now = func() time.Time { return fixedT1 }
	opts := testOpts()
	opts.ProducerVersion = "0.2.0"
	res, err := p.Publish(context.Background(), routeSetOne(), testScope, opts)
	if err != nil {
		t.Fatalf("second Publish: %v", err)
	}
	if !res.Updated {
		t.Fatalf("Result = %+v, want Updated (provenance bytes changed)", res)
	}
	second := parseEnvelope(t, readCM(t, p).Data[DefaultDataKey])
	if second.Revision.Value != first.Revision.Value {
		t.Error("digest changed on a provenance-only change")
	}
	if second.Provenance.SourceGeneration != first.Provenance.SourceGeneration {
		t.Errorf("source_generation = %d, want %d (no bump on digest-invariant change)",
			second.Provenance.SourceGeneration, first.Provenance.SourceGeneration)
	}
	if second.Provenance.RenderedAt != first.Provenance.RenderedAt {
		t.Errorf("rendered_at changed %q -> %q on a digest-invariant publish",
			first.Provenance.RenderedAt, second.Provenance.RenderedAt)
	}
}

func TestPublishTamperedDigestRefused(t *testing.T) {
	p := newTestPublisher(t)
	if _, err := p.Publish(context.Background(), routeSetOne(), testScope, testOpts()); err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	cm := readCM(t, p)

	// Out-of-band edit: rename a candidate. The bytes no longer hash to
	// the declared revision.
	env := parseEnvelope(t, cm.Data[DefaultDataKey])
	env.Overlay.Candidates[0].Name = "model-b"
	tampered, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		t.Fatalf("marshal tampered: %v", err)
	}
	tampered = append(tampered, '\n')
	cm.Data[DefaultDataKey] = string(tampered)
	if err := p.client.Update(context.Background(), cm); err != nil {
		t.Fatalf("simulate out-of-band edit: %v", err)
	}
	before := readCM(t, p)

	_, err = p.Publish(context.Background(), routeSetOne(), testScope, testOpts())
	if !errors.Is(err, ErrEnvelopeTampered) {
		t.Fatalf("Publish error = %v, want ErrEnvelopeTampered", err)
	}
	// The tampered baseline must not be chained from or overwritten.
	after := readCM(t, p)
	if after.ResourceVersion != before.ResourceVersion {
		t.Error("tampered ConfigMap was modified despite refusal")
	}
}

func TestPublishUnparsableDataRefused(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "praxis", Name: DefaultName},
		Data:       map[string]string{DefaultDataKey: "not-json"},
	}
	p := newTestPublisher(t, cm)
	_, err := p.Publish(context.Background(), routeSetOne(), testScope, testOpts())
	if !errors.Is(err, ErrEnvelopeTampered) {
		t.Fatalf("Publish error = %v, want ErrEnvelopeTampered", err)
	}
}

func TestPublishMissingKeyRefused(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "praxis", Name: DefaultName},
		Data:       map[string]string{"unrelated": "data"},
	}
	p := newTestPublisher(t, cm)
	_, err := p.Publish(context.Background(), routeSetOne(), testScope, testOpts())
	if !errors.Is(err, ErrNoEnvelope) {
		t.Fatalf("Publish error = %v, want ErrNoEnvelope", err)
	}
}

func TestPublishAnnotationDriftIsHarmless(t *testing.T) {
	p := newTestPublisher(t)
	if _, err := p.Publish(context.Background(), routeSetOne(), testScope, testOpts()); err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	cm := readCM(t, p)
	cm.Annotations[AnnotationSourceGeneration] = "999" // stale/wrong annotation
	if err := p.client.Update(context.Background(), cm); err != nil {
		t.Fatalf("corrupt annotation: %v", err)
	}

	// Data is the source of truth; a stale annotation must not block a
	// no-op publish (bytes identical) and must be refreshed on the next
	// real write.
	res, err := p.Publish(context.Background(), routeSetOne(), testScope, testOpts())
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !res.Unchanged {
		t.Fatalf("Result = %+v, want Unchanged (annotations are audit, not state)", res)
	}

	// A content change rewrites the annotations from the envelope.
	res, err = p.Publish(context.Background(), routeSetTwo(), testScope, testOpts())
	if err != nil {
		t.Fatalf("second Publish: %v", err)
	}
	if !res.Updated {
		t.Fatalf("Result = %+v, want Updated", res)
	}
	if got := readCM(t, p).Annotations[AnnotationSourceGeneration]; got != "2" {
		t.Errorf("annotation after rewrite = %q, want 2", got)
	}
}

func TestPublishUnequalWeightsEmitsWeightedRandom(t *testing.T) {
	set := routeSetOne()
	set.Models[0].Routes[1].Weight = 2 // non-uniform: 1 vs 2
	p := newTestPublisher(t)
	result, err := p.Publish(context.Background(), set, testScope, testOpts())
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	env := parseEnvelope(t, readCM(t, p).Data[DefaultDataKey])
	var policy struct {
		Mode string `json:"mode"`
	}
	if err := json.Unmarshal(env.Overlay.SelectionPolicy, &policy); err != nil || policy.Mode != "weightedRandom" {
		t.Fatalf("selection_policy = %s, want weightedRandom (err=%v)", env.Overlay.SelectionPolicy, err)
	}
	if result.Envelope.Revision.Value == "" || result.Envelope.Revision.Value != env.Revision.Value {
		t.Fatalf("publish result digest = %q, envelope = %q", result.Envelope.Revision.Value, env.Revision.Value)
	}
}

func TestPublishUnknownClusterRefused(t *testing.T) {
	p := newTestPublisher(t)
	_, err := p.Publish(context.Background(), routeSetTwo(), testScope, testOpts())
	if err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	// A new set referencing a cluster missing from the allowlist.
	set := routeSetTwo()
	set.Models[1].Routes[0].Cluster = "provider-gone"
	_, err = p.Publish(context.Background(), set, testScope, testOpts())
	if !errors.Is(err, envelope.ErrUnknownCluster) {
		t.Fatalf("Publish error = %v, want ErrUnknownCluster", err)
	}
}

func TestPublishCallerRenderedAtUsedVerbatim(t *testing.T) {
	p := newTestPublisher(t) // now() = fixedT0
	opts := testOpts()
	opts.RenderedAt = "2026-01-01T00:00:00Z"
	if _, err := p.Publish(context.Background(), routeSetOne(), testScope, opts); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	env := parseEnvelope(t, readCM(t, p).Data[DefaultDataKey])
	if env.Provenance.RenderedAt != "2026-01-01T00:00:00Z" {
		t.Errorf("rendered_at = %q, want the caller-supplied value verbatim", env.Provenance.RenderedAt)
	}
}

func TestPublishCustomNameAndDataKey(t *testing.T) {
	p, err := New(fake.NewClientBuilder().Build(), Config{Namespace: "praxis", Name: "custom", DataKey: "overlay.json"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.now = func() time.Time { return fixedT0 }
	if _, err := p.Publish(context.Background(), routeSetOne(), testScope, testOpts()); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	var cm corev1.ConfigMap
	if err := p.client.Get(context.Background(), client.ObjectKey{Namespace: "praxis", Name: "custom"}, &cm); err != nil {
		t.Fatalf("get custom configmap: %v", err)
	}
	if _, ok := cm.Data["overlay.json"]; !ok {
		t.Errorf("data key = %v, want overlay.json", keysOf(cm.Data))
	}
}

func TestPublishGenerationRegressionImpossible(t *testing.T) {
	// The read-back baseline comes from the ConfigMap itself, so a
	// regression can only be manufactured by editing the envelope's own
	// generation while keeping the declared digest honest is impossible —
	// but a hand-rolled baseline with a higher generation than the content
	// deserves must still chain forward, never backward.
	p := newTestPublisher(t)
	if _, err := p.Publish(context.Background(), routeSetOne(), testScope, testOpts()); err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	cm := readCM(t, p)
	env := parseEnvelope(t, cm.Data[DefaultDataKey])
	// Pretend some external producer advanced the chain to generation 41
	// for this exact content: re-digest must still pass (bytes honest),
	// and the next unchanged publish must NOT reset it to 1.
	env.Provenance.SourceGeneration = 41
	data, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	data = append(data, '\n')
	if _, err := envelope.ComputeDigestFromWire(data); err != nil {
		t.Fatalf("honest re-digest of generation edit should pass: %v", err)
	}
	cm.Data[DefaultDataKey] = string(data)
	if err := p.client.Update(context.Background(), cm); err != nil {
		t.Fatalf("simulate external chain: %v", err)
	}

	res, err := p.Publish(context.Background(), routeSetOne(), testScope, testOpts())
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !res.Unchanged {
		t.Fatalf("Result = %+v, want Unchanged", res)
	}
	after := parseEnvelope(t, readCM(t, p).Data[DefaultDataKey])
	if after.Provenance.SourceGeneration != 41 {
		t.Errorf("source_generation = %d, want 41 (inherited from the honest baseline)", after.Provenance.SourceGeneration)
	}
}

func keysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

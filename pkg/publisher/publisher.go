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

// Package publisher is the write path from the resolved route set to the
// praxis routing-overlay ConfigMap: it reads the previously distributed
// envelope, chains the content-addressed revision, renders the new one, and
// server-side-applies it. It is the in-cluster replacement for the
// test/overlay-e2e/render-overlay stand-in.
//
// Authority model (port plan §4.4, two planes): this controller is the sole
// writer of the ConfigMap; the praxis data plane reads it read-only through
// a projected volume. The mount must NOT use subPath — the praxis watcher
// keys on the projected volume's ..data symlink replacement.
//
// The ConfigMap's data is the source of truth for distribution state; the
// annotations are a cheap audit summary derived from it. Before chaining a
// new revision, the published bytes are re-digested
// (envelope.ComputeDigestFromWire) and checked against the envelope's
// declared revision: the ConfigMap is expected to be unmodified between our
// writes, and a mismatch means it was edited out-of-band — publishing over a
// tampered baseline would silently launder the edit into generation N+1, so
// the writer refuses loudly (ErrEnvelopeTampered) instead.
package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
)

// fieldOwner is the SSA field manager, matching render.FieldOwner so every
// resource this controller applies carries one consistent owner.
const fieldOwner = "ai-gateway-controller"

const (
	// DefaultName is the routing-overlay ConfigMap name (praxis mount
	// convention from the overlay e2e).
	DefaultName = "routing-overlay"
	// DefaultDataKey is the envelope key inside the ConfigMap.
	DefaultDataKey = "routing-overlay.json"

	// AnnotationSourceGeneration mirrors provenance.source_generation for
	// audit without parsing the envelope.
	AnnotationSourceGeneration = "inference.opendatahub.io/routing-overlay-source-generation"
	// AnnotationContentDigest mirrors content_digest.value for audit.
	AnnotationContentDigest = "inference.opendatahub.io/routing-overlay-content-digest"

	labelManagedBy = "app.kubernetes.io/managed-by"
)

// Frozen error surface (port plan §6): each maps to a Ready=False condition
// with a stable reason.
var (
	// ErrEnvelopeTampered: the ConfigMap's envelope bytes do not hash to
	// the revision they declare (or do not parse). Someone or something
	// modified it out-of-band; refuse to chain from an untrusted baseline.
	ErrEnvelopeTampered = errors.New("publisher: published envelope does not match its declared digest; refusing to chain from an untrusted baseline")
	// ErrNoEnvelope: the ConfigMap exists but carries no envelope data key
	// — a different owner's object or a half-removed publish.
	ErrNoEnvelope = errors.New("publisher: ConfigMap exists but has no envelope data key")
)

// Config locates the routing-overlay ConfigMap. Namespace is where praxis
// runs (where the pipeline mounts it) — NOT envelope.Scope.Namespace, which
// is the serving scope rendered into the envelope.
type Config struct {
	Namespace string
	// Name and DataKey default to DefaultName / DefaultDataKey.
	Name    string
	DataKey string
}

// Publisher renders and publishes routing-overlay envelopes to a ConfigMap.
// The k8s client is the only I/O; Render stays pure in pkg/envelope.
type Publisher struct {
	client client.Client
	cfg    Config
	now    func() time.Time // injectable for tests; defaults to time.Now
}

// New builds a Publisher. Namespace and Name must be non-empty.
func New(c client.Client, cfg Config) (*Publisher, error) {
	if cfg.Namespace == "" {
		return nil, errors.New("publisher: namespace is required")
	}
	if cfg.Name == "" {
		cfg.Name = DefaultName
	}
	if cfg.DataKey == "" {
		cfg.DataKey = DefaultDataKey
	}
	return &Publisher{client: c, cfg: cfg, now: time.Now}, nil
}

// Result reports what Publish did so the reconciler can emit events.
type Result struct {
	Envelope  envelope.Envelope
	Created   bool // the ConfigMap did not exist before
	Updated   bool // the ConfigMap existed and its bytes changed
	Unchanged bool // bytes identical; no write was performed
}

// Publish resolves nothing (the caller already has the resolved set): it
// reads the currently distributed envelope, renders the next one, and
// applies it if the bytes changed.
//
// opts.RenderedAt semantics: when empty, the writer stamps now() on a
// content change but INHERITS the existing rendered_at when the digest is
// unchanged — content that did not change is not "re-rendered", and
// re-stamping would churn the ConfigMap (and trigger a praxis reload) on
// every no-op reconcile. A caller-supplied RenderedAt is used verbatim.
func (p *Publisher) Publish(ctx context.Context, set *resolver.ResolvedRouteSet, scope envelope.Scope, opts envelope.Options) (Result, error) {
	key := client.ObjectKey{Namespace: p.cfg.Namespace, Name: p.cfg.Name}
	var cm corev1.ConfigMap
	exists := true
	if err := p.client.Get(ctx, key, &cm); err != nil {
		if !apierrors.IsNotFound(err) {
			return Result{}, fmt.Errorf("publisher: get configmap %s: %w", key, err)
		}
		exists = false
	}

	prev := envelope.Revision{}
	existingData := ""
	existingRenderedAt := ""
	if exists {
		raw, ok := cm.Data[p.cfg.DataKey]
		if !ok {
			return Result{}, fmt.Errorf("%w: %s has no %q key", ErrNoEnvelope, key, p.cfg.DataKey)
		}
		existingData = raw
		var env envelope.Envelope
		if err := json.Unmarshal([]byte(raw), &env); err != nil {
			return Result{}, fmt.Errorf("%w: %s: %w", ErrEnvelopeTampered, key, err)
		}
		got, err := envelope.ComputeDigestFromWire([]byte(raw))
		if err != nil {
			return Result{}, fmt.Errorf("%w: %s: %w", ErrEnvelopeTampered, key, err)
		}
		if got != env.Revision.Value {
			return Result{}, fmt.Errorf("%w: %s declares %s, content hashes to %s",
				ErrEnvelopeTampered, key, env.Revision.Value, got)
		}
		prev = envelope.Revision{Generation: env.Provenance.SourceGeneration, Digest: env.Revision.Value}
		existingRenderedAt = env.Provenance.RenderedAt
	}

	env, err := envelope.Render(set, scope, prev, opts)
	if err != nil {
		return Result{}, err
	}
	if opts.RenderedAt == "" {
		if exists && env.Revision.Value == prev.Digest {
			env.Provenance.RenderedAt = existingRenderedAt
		} else {
			env.Provenance.RenderedAt = p.now().UTC().Format(time.RFC3339)
		}
	}
	if err := envelope.CheckRevisionTransition(prev,
		envelope.Revision{Generation: env.Provenance.SourceGeneration, Digest: env.Revision.Value}); err != nil {
		return Result{}, err
	}

	data, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		return Result{}, fmt.Errorf("publisher: marshal envelope: %w", err)
	}
	data = append(data, '\n')

	if exists && string(data) == existingData {
		return Result{Envelope: env, Unchanged: true}, nil
	}

	// First-class SSA (controller-runtime >= v0.23): the typed
	// apply-configuration goes through client.Apply, same field owner and
	// force-ownership as pkg/render's installer path.
	apply := corev1ac.ConfigMap(p.cfg.Name, p.cfg.Namespace).
		WithLabels(map[string]string{
			labelManagedBy: "ai-gateway-controller",
		}).
		WithAnnotations(map[string]string{
			AnnotationSourceGeneration: strconv.FormatInt(env.Provenance.SourceGeneration, 10),
			AnnotationContentDigest:    env.Revision.Value,
		}).
		WithData(map[string]string{p.cfg.DataKey: string(data)})
	if err := p.client.Apply(ctx, apply, client.FieldOwner(fieldOwner), client.ForceOwnership); err != nil {
		return Result{}, fmt.Errorf("publisher: apply configmap %s: %w", key, err)
	}
	return Result{Envelope: env, Created: !exists, Updated: exists}, nil
}

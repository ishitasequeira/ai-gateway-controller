// Command render-overlay renders a routing-overlay.json envelope from a set
// of model:provider routes through the real pkg/resolver + pkg/envelope code
// paths, so the overlay-e2e harness exercises the same producer the
// reconciler will use. It stands in for the reconciler's publish step until
// the ConfigMap writer lands.
//
// Example:
//
//	go run ./test/overlay-e2e/render-overlay \
//	  --route echo-one:katan-a \
//	  --known-clusters provider-katan-a,provider-katan-b \
//	  --out routing-overlay.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
)

type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

func main() {
	var routes, knownClusters stringList
	flag.Var(&routes, "route", "model:provider pair (repeatable), provider is an ExternalProvider CR name")
	flag.Var(&knownClusters, "known-clusters", "load_balancer cluster allowlist (repeatable or comma-separated)")
	ns := flag.String("namespace", "overlay-e2e", "namespace for the synthetic CRs and overlay scope")
	network := flag.String("network", "e2e-net", "overlay scope: network")
	gateway := flag.String("gateway", "praxis", "overlay scope: gateway")
	localSite := flag.String("local-site", "kind-local", "overlay scope: local_site")
	sourceUID := flag.String("source-uid", "overlay-e2e", "provenance source_uid")
	prevGeneration := flag.Uint64("prev-generation", 0, "previous revision generation (0 = first revision)")
	prevDigest := flag.String("prev-digest", "", "previous revision digest (empty = first revision)")
	renderedAt := flag.String("rendered-at", "", "RFC3339 rendered_at stamp (default: now)")
	out := flag.String("out", "", "output path (default: stdout)")
	flag.Parse()

	if len(routes) == 0 {
		fmt.Fprintln(os.Stderr, "render-overlay: at least one --route model:provider is required")
		os.Exit(2)
	}

	var models []*v1alpha1.ExternalModel
	var providers []*v1alpha1.ExternalProvider
	seen := map[string]bool{}
	for _, r := range routes {
		model, provider, ok := strings.Cut(r, ":")
		if !ok || model == "" || provider == "" {
			fmt.Fprintf(os.Stderr, "render-overlay: bad --route %q, want model:provider\n", r)
			os.Exit(2)
		}
		models = append(models, &v1alpha1.ExternalModel{
			ObjectMeta: metav1.ObjectMeta{Name: model, Namespace: *ns},
			Spec: v1alpha1.ExternalModelSpec{
				ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{
					Ref:         v1alpha1.ExternalProviderReference{Name: provider},
					TargetModel: model,
					APIFormat:   "messages",
					Path:        "/v1/messages",
				}},
			},
		})
		if !seen[provider] {
			seen[provider] = true
			providers = append(providers, &v1alpha1.ExternalProvider{
				ObjectMeta: metav1.ObjectMeta{Name: provider, Namespace: *ns},
				Spec: v1alpha1.ExternalProviderSpec{
					Provider: "anthropic",
					Endpoint: provider + "." + *ns + ".svc.cluster.local:8000",
					Auth: v1alpha1.AuthConfig{
						Type:      "apikey",
						SecretRef: v1alpha1.NameReference{Name: provider + "-key"},
					},
				},
				// The e2e renders from an already-reconciled world; Resolve
				// skips refs whose provider is not Ready, so stamp it here.
				Status: v1alpha1.ExternalProviderStatus{Phase: resolver.PhaseReady},
			})
		}
	}

	set, err := resolver.Resolve(models, providers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "render-overlay: resolve: %v\n", err)
		os.Exit(1)
	}

	var known []string
	for _, c := range knownClusters {
		known = append(known, strings.Split(c, ",")...)
	}
	at := *renderedAt
	if at == "" {
		at = time.Now().UTC().Format(time.RFC3339)
	}
	scope := envelope.Scope{
		Network:   *network,
		Gateway:   *gateway,
		Namespace: *ns,
		LocalSite: *localSite,
	}
	prev := envelope.Revision{Generation: *prevGeneration, Digest: *prevDigest}
	env, err := envelope.Render(set, scope, prev, envelope.Options{
		KnownClusters: known,
		SourceUID:     *sourceUID,
		RenderedAt:    at,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "render-overlay: render: %v\n", err)
		os.Exit(1)
	}

	data, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "render-overlay: marshal: %v\n", err)
		os.Exit(1)
	}
	data = append(data, '\n')

	if *out == "" {
		os.Stdout.Write(data)
		return
	}
	if err := os.WriteFile(*out, data, 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "render-overlay: write: %v\n", err)
		os.Exit(1)
	}
	// Echo the revision so callers can chain generations without jq.
	fmt.Fprintf(os.Stderr, "rendered generation=%d digest=%s -> %s\n",
		env.Provenance.SourceGeneration, env.Revision.Value, *out)
}

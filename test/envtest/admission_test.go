//go:build envtest

package envtest_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aigatewayv1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/aigateway/v1alpha1"
)

// testGuardrailTimeoutAdmission checks that accepted timeouts remain readable by typed clients.
func testGuardrailTimeoutAdmission(t *testing.T, c client.Client) {
	t.Helper()
	const namespace = "guardrail-admission"
	createNamespace(t, c, namespace)
	for _, tc := range []struct {
		name, timeout string
		want          time.Duration
		wantErr       bool
		// message overrides the expected rejection; empty means the CEL rule message.
		message string
	}{
		{name: "seconds", timeout: "30s", want: 30 * time.Second},
		{name: "millisecond", timeout: "1ms", want: time.Millisecond},
		{name: "fractional", timeout: "250ms", want: 250 * time.Millisecond},
		{name: "sub-millisecond", timeout: "500us", wantErr: true, message: "timeout must be at least 1ms"},
		{name: "compound", timeout: "1h2m3s", want: time.Hour + 2*time.Minute + 3*time.Second},
		{name: "days", timeout: "1d", wantErr: true},
		{name: "long-units", timeout: "3hours", wantErr: true},
		{name: "spaces", timeout: "5 minutes", wantErr: true},
		{name: "zero", timeout: "0s", wantErr: true},
		{name: "negative", timeout: "-1s", wantErr: true},
		{name: "malformed", timeout: "not-a-duration", wantErr: true},
		{name: "empty", timeout: "", wantErr: true},
		// A valid Go duration, so only MaxLength can reject it.
		{name: "too-long", timeout: strings.Repeat("0", 64) + "1s", wantErr: true, message: "Too long"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Override only the timeout so malformed strings reach admission
			// without Go's Duration decoder rejecting them first.
			guardrail := &unstructured.Unstructured{}
			require.NoError(t, c.Scheme().Convert(aiGuardrail(tc.name, namespace), guardrail, nil))
			require.NoError(t, unstructured.SetNestedField(guardrail.Object, tc.timeout, "spec", "provider", "timeout"))
			err := c.Create(t.Context(), guardrail)
			if err == nil {
				defer func() { require.NoError(t, c.Delete(t.Context(), guardrail)) }()
			}
			if tc.wantErr {
				require.True(t, apierrors.IsInvalid(err), "timeout %q must be rejected, got %v", tc.timeout, err)
				assert.ErrorContains(t, err, "spec.provider.timeout")
				message := tc.message
				if message == "" {
					message = "timeout must be a valid positive duration"
				}
				assert.ErrorContains(t, err, message)
				return
			}
			require.NoError(t, err)
			var got aigatewayv1alpha1.AIGuardrail
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(guardrail), &got), "typed Get for %q", tc.timeout)
			assert.Equal(t, tc.want, got.Spec.Provider.Timeout.Duration)
			var list aigatewayv1alpha1.AIGuardrailList
			assert.NoError(t, c.List(t.Context(), &list), "timeout %q must not poison typed List", tc.timeout)
		})
	}
}

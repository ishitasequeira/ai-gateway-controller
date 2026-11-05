package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestSafeRoutingDiagnosticAcceptsOnlyBoundedHarnessIDs(t *testing.T) {
	t.Parallel()

	if got := safeRoutingDiagnostic("ipp-route-trace-mixed-23"); got != "ipp-route-trace-mixed-23" {
		t.Fatalf("safe diagnostic ID changed: %q", got)
	}
	for _, value := range []string{"", "contains space", "secret=value", string(make([]byte, 97))} {
		if got := safeRoutingDiagnostic(value); got != "" {
			t.Errorf("unsafe diagnostic ID was retained: %q", got)
		}
	}
}

func TestSafeRoutingCandidateAllowsOnlyProviderAwareFixtureCandidates(t *testing.T) {
	t.Parallel()

	for input, want := range map[string]string{
		"demo-model/provider-api-translation-openai":         "demo-model/provider-api-translation-openai",
		"demo-model/provider-api-translation-anthropic":      "demo-model/provider-api-translation-anthropic",
		"demo-model/provider-api-translation-bedrock-mantle": "demo-model/provider-api-translation-bedrock-mantle",
		"spoofed-candidate": "",
	} {
		if got := safeRoutingCandidate(input); got != want {
			t.Errorf("safeRoutingCandidate(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestProviderAwareRecorderSelectsBedrockCredentialByTargetModel(t *testing.T) {
	t.Parallel()

	config := recorderConfig{
		responseMode:       "provider-aware",
		openAICredential:   "openai-test-key",
		bedrockCredential:  "bedrock-test-key",
		credentialRequired: true,
	}
	request := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/chat/completions", nil)
	request.Header.Set("Authorization", "Bearer bedrock-test-key")

	if !config.validCredential(request, map[string]any{"model": "openai.gpt-oss-20b"}) {
		t.Fatal("Bedrock target model should validate against its bearer fixture key")
	}
	if config.validCredential(request, map[string]any{"model": "mock-openai-model"}) {
		t.Fatal("Bedrock key must not validate for ordinary OpenAI target model")
	}
	request.Header.Del("Authorization")
	request.Header.Set("X-Api-Key", "bedrock-test-key")
	if config.validCredential(request, map[string]any{"model": "openai.gpt-oss-20b"}) {
		t.Fatal("Bedrock Mantle fixture must require a Bearer credential")
	}
}

func TestSafeRoutingRevisionAndRequestID(t *testing.T) {
	t.Parallel()

	revision := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if got := safeRoutingRevision(revision); got != revision {
		t.Fatalf("safe revision changed: %q", got)
	}
	if got := safeRoutingRevision("not-a-revision"); got != "" {
		t.Errorf("invalid revision was retained: %q", got)
	}
	requestID := "01234567-89ab-cdef-0123-456789abcdef"
	if got := safeRoutingRequestID(requestID); got != requestID {
		t.Fatalf("safe routing request ID changed: %q", got)
	}
	if got := safeRoutingRequestID("not-a-request-id"); got != "" {
		t.Errorf("invalid request ID was retained: %q", got)
	}
}

func TestAnthropicOverlayReloadStreamUsesDedicatedDelay(t *testing.T) {
	t.Parallel()

	config := recorderConfig{
		streamEventDelay:   400 * time.Millisecond,
		overlayReloadDelay: 12 * time.Second,
	}
	reloadRequest := map[string]any{
		"messages": []any{map[string]any{
			"role": "user", "content": "externalmodel-overlay-reload-active-anthropic-stream",
		}},
	}
	ordinaryRequest := map[string]any{
		"messages": []any{map[string]any{"role": "user", "content": "regular stream"}},
	}

	if got := config.anthropicStreamDelay(reloadRequest); got != 12*time.Second {
		t.Errorf("overlay reload stream delay = %s, want 12s", got)
	}
	if got := config.anthropicStreamDelay(ordinaryRequest); got != 400*time.Millisecond {
		t.Errorf("ordinary stream delay = %s, want 400ms", got)
	}
	config.overlayReloadDelay = 0
	if got := config.anthropicStreamDelay(reloadRequest); got != 400*time.Millisecond {
		t.Errorf("overlay reload stream without dedicated delay = %s, want 400ms", got)
	}
}

func TestBedrockOverlayReloadStreamUsesDedicatedDelay(t *testing.T) {
	t.Setenv("BEDROCK_OVERLAY_RELOAD_STREAM_EVENT_DELAY", "40s")

	requestBody := map[string]any{
		"messages": []any{map[string]any{
			"role": "user", "content": "externalmodel-overlay-reload-active-bedrock-stream",
		}},
	}
	if got := bedrockOverlayReloadDelay(requestBody); got != 40*time.Second {
		t.Errorf("Bedrock overlay reload stream delay = %s, want 40s", got)
	}
}

func TestOpenAILargeJSONFixtureExceedsTranslationResponseLimit(t *testing.T) {
	t.Parallel()

	requestBody := map[string]any{
		"model": "mock-openai-model",
		"messages": []any{map[string]any{
			"role": "user", "content": "externalmodel-openai-large-json-response",
		}},
	}
	response := httptest.NewRecorder()
	writeOpenAIResponse(response, requestBody)

	var decoded openAIResponse
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("large OpenAI fixture response is invalid JSON: %v", err)
	}
	expectedContent := "openai large response:" + strings.Repeat("x", externalModelResponseLimitBytes+128) + openAILargeResponseTerminal
	if len(decoded.Choices) != 1 || decoded.Choices[0].Message.Content != expectedContent {
		t.Fatalf("large OpenAI response content was not generated: choices=%d", len(decoded.Choices))
	}
	if decoded.ID != "chatcmpl-kind" || decoded.Model != "mock-openai-model" ||
		decoded.Choices[0].FinishReason != "stop" || decoded.Usage.TotalTokens != 3 {
		t.Fatalf("large OpenAI response metadata changed: %#v", decoded)
	}
}

func TestOpenAIUnderLimitJSONFixtureRemainsLargeButBounded(t *testing.T) {
	t.Parallel()

	requestBody := map[string]any{
		"model": "mock-openai-model",
		"messages": []any{map[string]any{
			"role": "user", "content": "externalmodel-openai-under-limit-json-response",
		}},
	}
	response := httptest.NewRecorder()
	writeOpenAIResponse(response, requestBody)

	var decoded openAIResponse
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("under-limit OpenAI fixture response is invalid JSON: %v", err)
	}
	if got := len(response.Body.Bytes()); got <= 700*1024 || got >= externalModelResponseLimitBytes {
		t.Fatalf("under-limit OpenAI fixture response is %d bytes, want 700 KiB < size < %d", got, externalModelResponseLimitBytes)
	}
	if len(decoded.Choices) != 1 || len(decoded.Choices[0].Message.Content) <= 700*1024 {
		t.Fatalf("under-limit OpenAI response content was not generated: choices=%d", len(decoded.Choices))
	}
}

func TestInspectAnthropicRequestReportsOnlyStructuralCompatibilityData(t *testing.T) {
	t.Parallel()

	request := map[string]any{
		"model":      "provider-model",
		"max_tokens": float64(4096),
		"messages": []any{map[string]any{
			"role": "user",
			"content": []any{
				map[string]any{"type": "text", "text": "before"},
				map[string]any{"type": "image", "source": map[string]any{
					"type": "base64", "media_type": "image/png",
					"data": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADUlEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC",
				}},
				map[string]any{"type": "text", "text": "after"},
				map[string]any{"type": "image", "source": map[string]any{
					"type": "url", "url": "https://images.example.test/p.png",
				}},
			},
		}},
	}
	inspection := inspectAnthropicRequest(request)
	if !inspection.Valid || inspection.MaxTokens != 4096 || !inspection.LegacyOptionsAbsent {
		t.Fatalf("Anthropic request metadata = valid %t max_tokens %d legacy_absent %t", inspection.Valid, inspection.MaxTokens, inspection.LegacyOptionsAbsent)
	}
	if got, want := inspection.BlockTypes, []string{"text", "image", "text", "image"}; !equalStrings(got, want) {
		t.Fatalf("content block types = %v, want %v", got, want)
	}
	if got, want := inspection.ImageSources, []string{"base64", "url"}; !equalStrings(got, want) {
		t.Fatalf("image source types = %v, want %v", got, want)
	}
	for field, value := range map[string]any{
		"user":                "must-not-reach-anthropic",
		"n":                   float64(2),
		"seed":                float64(17),
		"parallel_tool_calls": true,
	} {
		requestWithLegacyOption := map[string]any{}
		for key, item := range request {
			requestWithLegacyOption[key] = item
		}
		requestWithLegacyOption[field] = value
		inspection = inspectAnthropicRequest(requestWithLegacyOption)
		if inspection.Valid || inspection.LegacyOptionsAbsent {
			t.Errorf("unexpected compatibility option %q accepted: valid=%t legacy_absent=%t", field, inspection.Valid, inspection.LegacyOptionsAbsent)
		}
	}
}

func TestObserveDoesNotPersistLargeRequestText(t *testing.T) {
	t.Parallel()

	prompt := strings.Repeat("x", 1536*1024)
	r := httptest.NewRequestWithContext(t.Context(), "POST", "/v1/messages", nil)
	body := map[string]any{"messages": []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": prompt}}}}}
	observation := observe(r, "provider-aware", body, "", true, true, true)
	if len(observation.MessageText) != 0 {
		t.Fatalf("large prompt text was persisted in observation: %d strings", len(observation.MessageText))
	}
}

func TestProviderAwareRecorderRejectsInvalidAnthropicPayload(t *testing.T) {
	t.Parallel()

	config := recorderConfig{
		provider:           "translation",
		responseMode:       "provider-aware",
		tlsRequired:        false,
		credentialRequired: false,
		requestIDPattern:   regexp.MustCompile(`^[A-Za-z0-9._-]{1,80}$`),
	}
	request := httptest.NewRequestWithContext(t.Context(), "POST", "http://provider.test/v1/messages", strings.NewReader(
		`{"model":"provider-model","max_tokens":8,"messages":[{"role":"user","content":[]}]} `))
	response := httptest.NewRecorder()
	config.handleInference(response, request)
	if response.Code != 400 {
		t.Fatalf("invalid Anthropic payload status = %d, want 400", response.Code)
	}
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

func TestRecorderRejectsOversizedJSONWithTrailingWhitespace(t *testing.T) {
	t.Parallel()
	body := `{}` + strings.Repeat(" ", (12<<20)-1)
	request := httptest.NewRequestWithContext(t.Context(), "POST", "/v1/messages", strings.NewReader(body))
	response := httptest.NewRecorder()
	recorderConfig{}.handleInference(response, request)
	if response.Code != 413 {
		t.Fatalf("oversized valid JSON status = %d, want 413", response.Code)
	}
}

func TestAnthropicStreamMustBeBoolean(t *testing.T) {
	t.Parallel()
	for _, value := range []any{"true", float64(1), nil} {
		if anthropicTopLevelFieldsAreValid(map[string]any{"stream": value}) {
			t.Errorf("accepted non-boolean stream: %T", value)
		}
	}
	for _, value := range []bool{true, false} {
		if !anthropicTopLevelFieldsAreValid(map[string]any{"stream": value}) {
			t.Errorf("rejected stream %t", value)
		}
	}
	if !anthropicTopLevelFieldsAreValid(map[string]any{}) {
		t.Fatal("stream may be omitted")
	}
}

package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
)

type response struct {
	Provider   string `json:"provider"`
	TLS        bool   `json:"tls"`
	SNI        bool   `json:"sni_ok"`
	Authority  bool   `json:"authority_ok"`
	Credential bool   `json:"credential_ok"`
}

type recordedRequest struct {
	At                   string   `json:"at"`
	RequestID            string   `json:"request_id,omitempty"`
	DiagnosticID         string   `json:"diagnostic_id,omitempty"`
	RoutingCandidate     string   `json:"routing_candidate,omitempty"`
	RoutingRevision      string   `json:"routing_revision,omitempty"`
	RoutingRequestID     string   `json:"routing_request_id,omitempty"`
	Provider             string   `json:"provider"`
	Method               string   `json:"method"`
	Path                 string   `json:"path"`
	Authority            string   `json:"authority"`
	TLS                  bool     `json:"tls"`
	SNI                  bool     `json:"sni_ok"`
	AuthorityOK          bool     `json:"authority_ok"`
	Model                string   `json:"model,omitempty"`
	Stream               bool     `json:"stream"`
	MessageRoles         []string `json:"message_roles,omitempty"`
	MessageText          []string `json:"message_text,omitempty"`
	ToolNames            []string `json:"tool_names,omitempty"`
	XAPIKeyPresent       bool     `json:"x_api_key_present"`
	AuthorizationPresent bool     `json:"authorization_present"`
	AuthorizationScheme  string   `json:"authorization_scheme,omitempty"`
	CredentialOK         bool     `json:"credential_ok"`
}

type openAIResponse struct {
	ID      string              `json:"id"`
	Object  string              `json:"object"`
	Model   string              `json:"model"`
	Choices []openAIChoice      `json:"choices"`
	Usage   openAIResponseUsage `json:"usage"`
}

type openAIChoice struct {
	Index        int                `json:"index"`
	Message      openAIMessage      `json:"message"`
	FinishReason string             `json:"finish_reason"`
	Delta        *openAIStreamDelta `json:"delta,omitempty"`
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIStreamDelta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

type openAIResponseUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type openAIStreamEvent struct {
	ID      string             `json:"id"`
	Object  string             `json:"object"`
	Model   string             `json:"model"`
	Choices []openAIStreamItem `json:"choices"`
}

type openAIStreamItem struct {
	Index        int               `json:"index"`
	Delta        openAIStreamDelta `json:"delta"`
	FinishReason *string           `json:"finish_reason"`
}

const observationPath = "/var/run/provider-observations/requests.jsonl"
const externalModelResponseLimitBytes = 1 << 20

var (
	diagnosticValuePattern  = regexp.MustCompile(`^[A-Za-z0-9._-]{1,80}$`)
	revisionValuePattern    = regexp.MustCompile(`^[0-9a-f]{64}$`)
	routingRequestIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
)

type recorderConfig struct {
	provider            string
	providerLabel       string
	responseMode        string
	tlsRequired         bool
	expectedHosts       []string
	credentialRequired  bool
	recordObservations  bool
	expectedCredential  string
	openAICredential    string
	anthropicCredential string
	requestIDPattern    *regexp.Regexp
	streamEventDelay    time.Duration
	overlayReloadDelay  time.Duration
}

type serverSentEvent struct {
	name  string
	value any
}

func required(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("required environment variable %s is empty", name)
	}
	return value
}

func main() {
	config := loadRecorderConfig()
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	if config.recordObservations {
		mux.HandleFunc("/__test/observations", handleObservations)
	}
	mux.HandleFunc("/v1/chat/completions", config.handleInference)
	mux.HandleFunc("/v1/messages", config.handleInference)

	listenAddress := os.Getenv("LISTEN_ADDR")
	if listenAddress == "" {
		listenAddress = ":8443"
	}
	server := &http.Server{Addr: listenAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Printf("provider recorder ready provider=%s", config.providerLabel)
	var serveErr error
	if config.tlsRequired {
		serveErr = server.ListenAndServeTLS("/etc/provider/tls/tls.crt", "/etc/provider/tls/tls.key")
	} else {
		serveErr = server.ListenAndServe()
	}
	if serveErr != nil && serveErr != http.ErrServerClosed {
		log.Fatal(serveErr)
	}
}

func loadRecorderConfig() recorderConfig {
	provider := required("PROVIDER_NAME")
	providerLabel, ok := map[string]string{
		"proof-a":        "proof-a",
		"proof-b":        "proof-b",
		"proof-c":        "proof-c",
		"translation":    "translation",
		"provider-aware": "provider-aware",
	}[provider]
	if !ok {
		log.Fatal("PROVIDER_NAME must be proof-a, proof-b, proof-c, translation, or provider-aware")
	}
	responseMode := os.Getenv("RESPONSE_MODE")
	if responseMode == "" {
		responseMode = "proof"
	}
	config := recorderConfig{
		provider:           provider,
		providerLabel:      providerLabel,
		responseMode:       responseMode,
		tlsRequired:        os.Getenv("TLS_MODE") != "http",
		credentialRequired: os.Getenv("CREDENTIAL_REQUIRED") != "false",
		recordObservations: os.Getenv("ENABLE_TEST_OBSERVATIONS") == "true",
		requestIDPattern:   regexp.MustCompile(`^[A-Za-z0-9._-]{1,80}$`),
		streamEventDelay:   durationFromEnv("STREAM_EVENT_DELAY"),
		overlayReloadDelay: durationFromEnv("OVERLAY_RELOAD_STREAM_EVENT_DELAY"),
	}
	expectedHosts := strings.FieldsFunc(os.Getenv("EXPECTED_HOSTS"), func(r rune) bool { return r == ',' })
	if singleHost := os.Getenv("EXPECTED_HOST"); singleHost != "" {
		expectedHosts = append(expectedHosts, singleHost)
	}
	config.expectedHosts = expectedHosts
	if len(config.expectedHosts) == 0 && config.tlsRequired {
		log.Fatal("EXPECTED_HOST or EXPECTED_HOSTS is required for the HTTPS recorder")
	}
	if config.credentialRequired {
		if config.responseMode == "provider-aware" {
			config.openAICredential = readCredential("OPENAI_CREDENTIAL_FILE")
			config.anthropicCredential = readCredential("ANTHROPIC_CREDENTIAL_FILE")
		} else {
			config.expectedCredential = readCredential("CREDENTIAL_FILE")
		}
	}
	return config
}

func handleObservations(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	data, err := os.ReadFile(observationPath)
	if err != nil && !os.IsNotExist(err) {
		http.Error(w, "could not read sanitized observations", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	_, _ = w.Write(data)
}

func (config recorderConfig) handleInference(w http.ResponseWriter, r *http.Request) {
	requestBytes, readErr := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if readErr != nil {
		http.Error(w, "could not read request body", http.StatusBadRequest)
		return
	}
	requestBody := map[string]any{}
	if err := json.Unmarshal(requestBytes, &requestBody); err != nil {
		http.Error(w, "request body is not JSON", http.StatusBadRequest)
		return
	}
	expectedHost := expectedHostFor(r.Host, config.expectedHosts)
	sniOK := !config.tlsRequired || (expectedHost != "" && r.TLS != nil && r.TLS.ServerName == expectedHost)
	authorityOK := len(config.expectedHosts) == 0 || expectedHost != "" && r.Host == expectedHost
	credentialOK := config.validCredential(r)
	status := http.StatusOK
	if !sniOK || !authorityOK {
		status = http.StatusMisdirectedRequest
	} else if !credentialOK {
		status = http.StatusUnauthorized
	}
	requestID := r.Header.Get("X-Translation-Request-Id")
	if requestID == "" {
		requestID = r.Header.Get("X-Issue28-Request-Id")
	}
	if !config.requestIDPattern.MatchString(requestID) {
		requestID = ""
	}
	sessionHash := ""
	if sessionID := r.Header.Get("X-Session-Id"); sessionID != "" {
		digest := sha256.Sum256([]byte(sessionID))
		sessionHash = hex.EncodeToString(digest[:])
	}
	// #nosec G706 -- providerLabel is a fixed enum; status and flags are derived from constants.
	log.Printf(
		"provider=%s status=%d tls=%t sni_ok=%t authority_ok=%t credential_ok=%t request_id=%s session_id_sha256=%s",
		config.providerLabel, status, r.TLS != nil, sniOK, authorityOK, credentialOK, requestID, sessionHash,
	)
	if config.recordObservations {
		observation := observe(r, config.providerLabel, requestBody, requestID, credentialOK, sniOK, authorityOK)
		if err := appendObservation(observation); err != nil {
			log.Printf("failed to save sanitized request observation: %v", err)
		}
	}
	if status != http.StatusOK {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		writeProofResponse(w, response{Provider: config.provider, TLS: r.TLS != nil, SNI: sniOK, Authority: authorityOK, Credential: credentialOK})
		return
	}
	if config.responseMode == "provider-aware" {
		config.writeProviderAwareResponse(w, r, requestBody)
		return
	}
	if config.responseMode == "openai" {
		writeOpenAIResponse(w, requestBody)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	writeProofResponse(w, response{Provider: config.provider, TLS: r.TLS != nil, SNI: sniOK, Authority: authorityOK, Credential: credentialOK})
}

func (config recorderConfig) validCredential(r *http.Request) bool {
	if !config.credentialRequired {
		return true
	}
	const bearerPrefix = "Bearer "
	authorization := r.Header.Get("Authorization")
	if config.responseMode == "provider-aware" && r.URL.Path == "/v1/messages" {
		return constantTimeEqual(r.Header.Get("X-Api-Key"), config.anthropicCredential)
	}
	expected := config.expectedCredential
	if config.responseMode == "provider-aware" {
		expected = config.openAICredential
	}
	credential := strings.TrimPrefix(authorization, bearerPrefix)
	return strings.HasPrefix(authorization, bearerPrefix) && constantTimeEqual(credential, expected)
}

func (config recorderConfig) writeProviderAwareResponse(w http.ResponseWriter, r *http.Request, requestBody map[string]any) {
	if hasMessageMarker(requestBody, "provider-error") {
		writeProviderError(w)
		return
	}
	if r.URL.Path == "/v1/messages" {
		writeAnthropicResponse(w, requestBody, config.anthropicStreamDelay(requestBody))
		return
	}
	writeOpenAIResponse(w, requestBody)
}

func (config recorderConfig) anthropicStreamDelay(requestBody map[string]any) time.Duration {
	if config.overlayReloadDelay > 0 && hasMessageMarker(requestBody,
		"externalmodel-overlay-reload-active-anthropic-stream") {
		return config.overlayReloadDelay
	}
	return config.streamEventDelay
}

func durationFromEnv(name string) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return 0
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < 0 {
		log.Fatalf("%s must be a non-negative Go duration", name)
	}
	return duration
}

func readCredential(envName string) string {
	credentialFile := required(envName)
	credential, err := os.ReadFile(credentialFile)
	if err != nil {
		log.Fatalf("read expected credential file configured by %s: %v", envName, err)
	}
	value := strings.TrimSpace(string(credential))
	if value == "" {
		log.Fatalf("credential file configured by %s is empty", envName)
	}
	return value
}

func expectedHostFor(authority string, expectedHosts []string) string {
	for _, host := range expectedHosts {
		if authority == strings.TrimSpace(host) {
			return strings.TrimSpace(host)
		}
	}
	return ""
}

func constantTimeEqual(actual, expected string) bool {
	return len(actual) == len(expected) && subtle.ConstantTimeCompare([]byte(actual), []byte(expected)) == 1
}

func hasMessageMarker(body map[string]any, marker string) bool {
	messages, _ := body["messages"].([]any)
	for _, value := range messages {
		message, _ := value.(map[string]any)
		if strings.Contains(fmt.Sprint(message["content"]), marker) {
			return true
		}
	}
	return false
}

func writeProviderError(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = io.WriteString(w, `{"type":"error","error":{"type":"rate_limit_error","message":"synthetic provider limit"}}`)
}

func writeAnthropicResponse(w http.ResponseWriter, requestBody map[string]any, streamEventDelay time.Duration) {
	if stream, _ := requestBody["stream"].(bool); stream {
		writeAnthropicStream(w, requestBody, streamEventDelay)
		return
	}
	model, _ := requestBody["model"].(string)
	content := []map[string]any{{"type": "text", "text": "anthropic translated reply"}}
	chunkMode := ""
	if hasMessageMarker(requestBody, "externalmodel-anthropic-split-json-response") {
		content[0]["text"] = "anthropic split response"
		chunkMode = "split"
	}
	if hasMessageMarker(requestBody, "externalmodel-anthropic-oversized-json-response") {
		content[0]["text"] = strings.Repeat("x", externalModelResponseLimitBytes+128)
		chunkMode = "oversized"
	}
	if hasMessageMarker(requestBody, "externalmodel-anthropic-json-response-trailer") {
		content[0]["text"] = "anthropic trailer response"
		chunkMode = "trailer"
	}
	if hasMessageMarker(requestBody, "externalmodel-anthropic-invalid-json-response") {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"type":`)
		return
	}
	stopReason := "end_turn"
	if name := firstToolName(requestBody); name != "" {
		content = append(content, map[string]any{"type": "tool_use", "id": "toolu-kind", "name": name, "input": map[string]any{"ok": true}})
		stopReason = "tool_use"
	}
	body, err := json.Marshal(map[string]any{
		"id": "msg-kind", "type": "message", "role": "assistant", "model": model,
		"content": content, "stop_reason": stopReason, "stop_sequence": nil,
		"usage": map[string]any{"input_tokens": 7, "output_tokens": 3},
	})
	if err != nil {
		log.Printf("failed to encode synthetic Anthropic JSON response: %v", err)
		return
	}
	if chunkMode != "" {
		writeAnthropicJSONFixture(w, body, chunkMode)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(append(body, '\n')); err != nil {
		log.Printf("failed to write synthetic Anthropic JSON response: %v", err)
	}
}

// writeAnthropicJSONFixture emits test responses as real HTTP chunks so the
// Gateway exercises the ExtProc streamed response-body path.
func writeAnthropicJSONFixture(w http.ResponseWriter, body []byte, mode string) {
	w.Header().Set("Content-Type", "application/json")
	if mode == "trailer" {
		w.Header().Add("Trailer", "X-Provider-Completion")
	}
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	writeChunk := func(chunk []byte) bool {
		if _, err := w.Write(chunk); err != nil {
			return false
		}
		if flusher != nil {
			flusher.Flush()
		}
		return true
	}
	switch mode {
	case "split":
		split := len(body) / 2
		if !writeChunk(body[:split]) {
			return
		}
		time.Sleep(80 * time.Millisecond)
		_ = writeChunk(body[split:])
	case "oversized":
		for offset := 0; offset < len(body); offset += 32 * 1024 {
			end := min(offset+32*1024, len(body))
			if !writeChunk(body[offset:end]) {
				return
			}
		}
	case "trailer":
		if writeChunk(body) {
			w.Header().Set("X-Provider-Completion", "complete")
		}
	}
}

func firstToolName(body map[string]any) string {
	tools, _ := body["tools"].([]any)
	if len(tools) == 0 {
		return ""
	}
	tool, _ := tools[0].(map[string]any)
	name, _ := tool["name"].(string)
	return name
}

func writeAnthropicStream(w http.ResponseWriter, requestBody map[string]any, streamEventDelay time.Duration) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	flusher, _ := w.(http.Flusher)
	toolName := firstToolName(requestBody)
	stopReason := "end_turn"
	events := []serverSentEvent{
		{
			name: "message_start",
			value: map[string]any{
				"type": "message_start",
				"message": map[string]any{
					"id": "msg-kind", "type": "message", "role": "assistant", "model": "mock-anthropic-model",
					"content": []any{}, "stop_reason": nil, "stop_sequence": nil,
					"usage": map[string]any{"input_tokens": 7, "output_tokens": 0},
				},
			},
		},
		{
			name: "content_block_start",
			value: map[string]any{
				"type": "content_block_start", "index": 0,
				"content_block": map[string]any{"type": "text", "text": ""},
			},
		},
		{
			name: "content_block_delta",
			value: map[string]any{
				"type": "content_block_delta", "index": 0,
				"delta": map[string]any{"type": "text_delta", "text": "anthropic translated stream"},
			},
		},
	}
	if toolName != "" {
		stopReason = "tool_use"
		events = append(events,
			serverSentEvent{
				name: "content_block_start",
				value: map[string]any{
					"type": "content_block_start", "index": 1,
					"content_block": map[string]any{
						"type": "tool_use", "id": "toolu-kind", "name": toolName, "input": map[string]any{},
					},
				},
			},
			serverSentEvent{
				name: "content_block_delta",
				value: map[string]any{
					"type": "content_block_delta", "index": 1,
					"delta": map[string]any{"type": "input_json_delta", "partial_json": `{"ok":true}`},
				},
			},
		)
	}
	events = append(events,
		serverSentEvent{
			name: "message_delta",
			value: map[string]any{
				"type":  "message_delta",
				"delta": map[string]any{"stop_reason": stopReason, "stop_sequence": nil},
				"usage": map[string]any{"output_tokens": 3},
			},
		},
		serverSentEvent{name: "message_stop", value: map[string]any{"type": "message_stop"}},
	)
	for index, event := range events {
		data, err := json.Marshal(event.value)
		if err != nil {
			return
		}
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.name, data); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		if streamEventDelay > 0 && index < len(events)-1 {
			time.Sleep(streamEventDelay)
		}
	}
}

func observe(r *http.Request, provider string, body map[string]any, requestID string, credentialOK, sniOK, authorityOK bool) recordedRequest {
	observation := recordedRequest{
		At:                   time.Now().UTC().Format(time.RFC3339Nano),
		RequestID:            requestID,
		DiagnosticID:         safeRoutingDiagnostic(r.Header.Get("X-Ipp-Diagnostic-Id")),
		RoutingCandidate:     safeRoutingCandidate(r.Header.Get("X-Ai-Routing-Candidate")),
		RoutingRevision:      safeRoutingRevision(r.Header.Get("X-Ai-Routing-Revision")),
		RoutingRequestID:     safeRoutingRequestID(r.Header.Get("X-Ai-Routing-Request-Id")),
		Provider:             provider,
		Method:               r.Method,
		Path:                 r.URL.Path,
		Authority:            r.Host,
		TLS:                  r.TLS != nil,
		SNI:                  sniOK,
		AuthorityOK:          authorityOK,
		XAPIKeyPresent:       r.Header.Get("X-Api-Key") != "",
		AuthorizationPresent: r.Header.Get("Authorization") != "",
		CredentialOK:         credentialOK,
	}
	if model, ok := body["model"].(string); ok {
		observation.Model = model
	}
	if stream, ok := body["stream"].(bool); ok {
		observation.Stream = stream
	}
	if authorization := r.Header.Get("Authorization"); authorization != "" {
		observation.AuthorizationScheme = strings.SplitN(authorization, " ", 2)[0]
	}
	messages, _ := body["messages"].([]any)
	for _, item := range messages {
		message, _ := item.(map[string]any)
		if role, ok := message["role"].(string); ok {
			observation.MessageRoles = append(observation.MessageRoles, role)
		}
		switch content := message["content"].(type) {
		case string:
			observation.MessageText = append(observation.MessageText, content)
		case []any:
			for _, part := range content {
				block, _ := part.(map[string]any)
				if text, ok := block["text"].(string); ok {
					observation.MessageText = append(observation.MessageText, text)
				}
			}
		}
	}
	tools, _ := body["tools"].([]any)
	for _, value := range tools {
		tool, _ := value.(map[string]any)
		if name, ok := tool["name"].(string); ok {
			observation.ToolNames = append(observation.ToolNames, name)
		}
	}
	return observation
}

// Keep only the bounded diagnostic forms created by the local E2E harness.
// These headers are not credentials, but the recorder must never persist
// arbitrary client-provided values as diagnostic evidence.
func safeRoutingDiagnostic(value string) string {
	if len(value) > 96 || !diagnosticValuePattern.MatchString(value) {
		return ""
	}
	return value
}

func safeRoutingCandidate(value string) string {
	switch value {
	case "demo-model/provider-api-translation-openai", "demo-model/provider-api-translation-anthropic":
		return value
	default:
		return ""
	}
}

func safeRoutingRevision(value string) string {
	if len(value) != 64 || !revisionValuePattern.MatchString(value) {
		return ""
	}
	return value
}

func safeRoutingRequestID(value string) string {
	if len(value) != 36 || !routingRequestIDPattern.MatchString(value) {
		return ""
	}
	return value
}

func appendObservation(observation recordedRequest) error {
	if err := os.MkdirAll("/var/run/provider-observations", 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(observationPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := json.NewEncoder(file).Encode(observation); err != nil {
		return err
	}
	return nil
}

func writeProofResponse(w http.ResponseWriter, value response) {
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("failed to encode proof response: %v", err)
	}
}

func writeOpenAIResponse(w http.ResponseWriter, requestBody map[string]any) {
	model, _ := requestBody["model"].(string)
	if model == "" {
		model = "test-model"
	}
	stream, _ := requestBody["stream"].(bool)
	if stream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		flusher, _ := w.(http.Flusher)
		streamEventDelay, _ := time.ParseDuration(os.Getenv("STREAM_EVENT_DELAY"))
		stop := "stop"
		events := []openAIStreamEvent{
			{ID: "chatcmpl-kind", Object: "chat.completion.chunk", Model: model,
				Choices: []openAIStreamItem{{Index: 0, Delta: openAIStreamDelta{Role: "assistant"}}}},
			{ID: "chatcmpl-kind", Object: "chat.completion.chunk", Model: model,
				Choices: []openAIStreamItem{{Index: 0, Delta: openAIStreamDelta{Content: "translated stream reply"}}}},
			{ID: "chatcmpl-kind", Object: "chat.completion.chunk", Model: model,
				Choices: []openAIStreamItem{{Index: 0, FinishReason: &stop}}},
		}
		for index, event := range events {
			encoded, err := json.Marshal(event)
			if err != nil {
				log.Printf("failed to encode OpenAI stream event: %v", err)
				return
			}
			_, _ = io.WriteString(w, "data: "+string(encoded)+"\n\n")
			if flusher != nil {
				flusher.Flush()
			}
			if streamEventDelay > 0 && index < len(events)-1 {
				time.Sleep(streamEventDelay)
			}
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	value := openAIResponse{
		ID: "chatcmpl-kind", Object: "chat.completion", Model: model,
		Choices: []openAIChoice{{
			Index: 0, Message: openAIMessage{Role: "assistant", Content: "translated reply"}, FinishReason: "stop",
		}},
		Usage: openAIResponseUsage{PromptTokens: 1, CompletionTokens: 2, TotalTokens: 3},
	}
	if hasMessageMarker(requestBody, "externalmodel-openai-large-json-response") {
		value.Choices[0].Message.Content = "openai large response:" + strings.Repeat("x", externalModelResponseLimitBytes+128)
	}
	if hasMessageMarker(requestBody, "externalmodel-openai-under-limit-json-response") {
		value.Choices[0].Message.Content = "openai large response:" + strings.Repeat("x", externalModelResponseLimitBytes*3/4)
	}
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("failed to encode OpenAI response: %v", err)
	}
}

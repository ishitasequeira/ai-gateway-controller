"""Pure helpers for runtime API-translation qualification contracts."""

import hashlib
import json
import re


def chat_request_with_exact_json_size(
    size,
    *,
    compatibility_fields=False,
    model="demo",
    compatibility_user_value="compatibility-test-user",
):
    """Build valid ASCII Chat Completions JSON with exactly ``size`` bytes."""
    body = {
        "model": model,
        "messages": [{"role": "user", "content": ""}],
    }
    if compatibility_fields:
        body.update({
            "user": compatibility_user_value,
            "n": 2,
            "seed": 17,
            "parallel_tool_calls": True,
        })
    empty = json.dumps(body, separators=(",", ":"), ensure_ascii=True).encode()
    content_size = size - len(empty)
    if content_size < 0:
        raise ValueError(f"requested JSON size {size} is smaller than the fixed request envelope {len(empty)}")
    body["messages"][0]["content"] = "x" * content_size
    encoded = json.dumps(body, separators=(",", ":"), ensure_ascii=True).encode()
    if len(encoded) != size:
        raise AssertionError(f"constructed JSON size {len(encoded)} does not match requested {size}")
    return body


def chat_request_json_with_exact_size(size, *, trailing_whitespace_bytes=0, **kwargs):
    """Build an exact-size valid request with ignorable trailing JSON whitespace."""
    if trailing_whitespace_bytes < 0 or trailing_whitespace_bytes >= size:
        raise ValueError("trailing whitespace must be non-negative and smaller than the requested size")
    body = chat_request_with_exact_json_size(size - trailing_whitespace_bytes, **kwargs)
    compact = json.dumps(body, separators=(",", ":"), ensure_ascii=True)
    payload = compact + (" " * trailing_whitespace_bytes)
    if len(payload.encode()) != size:
        raise AssertionError(f"constructed request size {len(payload.encode())} does not match requested {size}")
    return body, payload


def compact_translation_diagnostic_id(run_prefix, scenario):
    """Build a short diagnostic ID accepted by the provider recorder."""
    run_token = str(run_prefix).rstrip("-").rsplit("-", 1)[-1]
    safe_scenario = re.sub(r"[^A-Za-z0-9.-]+", "-", str(scenario)).strip("-") or "request"
    value = f"translation-{run_token}-{safe_scenario}"
    if len(value) > 80:
        value = value[:80]
    return value


def _indented_block(lines, start, parent_indent):
    block = []
    for line in lines[start + 1:]:
        if line.strip() and len(line) - len(line.lstrip()) <= parent_indent:
            break
        block.append(line)
    return block


def _integer_field(lines, key):
    pattern = re.compile(rf"^\s+{re.escape(key)}:\s*(\d+)\s*(?:#.*)?$")
    for line in lines:
        match = pattern.match(line)
        if match:
            return int(match.group(1))
    return None


def extproc_translation_limits(config_text):
    """Read only the relevant limits from the mounted ExtProc YAML."""
    lines = config_text.splitlines()
    server = None
    for index, line in enumerate(lines):
        if line == "server:":
            server = _indented_block(lines, index, 0)
            break

    translator = None
    filter_pattern = re.compile(r"^(\s*)- filter: openai_chat_completions_to_anthropic\s*$")
    for index, line in enumerate(lines):
        match = filter_pattern.match(line)
        if match:
            indent = len(match.group(1))
            translator = _indented_block(lines, index, indent)
            break

    return {
        "server_max_body_bytes": _integer_field(server or [], "max_body_bytes"),
        "translator_max_request_body_bytes": _integer_field(translator or [], "max_request_body_bytes"),
        "translator_max_response_body_bytes": _integer_field(translator or [], "max_response_body_bytes"),
        "translator_filter_present": translator is not None,
    }


def is_owned_translation_diagnostic_authpolicy(policy, namespace, name, route_name):
    """Require exact identity before removing an interrupted diagnostic route policy."""
    metadata = policy.get("metadata", {}) if isinstance(policy, dict) else {}
    target = policy.get("spec", {}).get("targetRef", {}) if isinstance(policy, dict) else {}
    return (
        metadata.get("namespace") == namespace
        and metadata.get("name") == name
        and metadata.get("labels", {}).get("app.kubernetes.io/managed-by")
        == "api-translation-kind-e2e-diagnostic"
        and target.get("group") == "gateway.networking.k8s.io"
        and target.get("kind") == "HTTPRoute"
        and target.get("name") == route_name
    )


def prometheus_counter_total(metrics_text, metric_name):
    """Sum a Prometheus counter across its label sets; return None if absent."""
    pattern = re.compile(rf"^{re.escape(metric_name)}(?:\{{[^}}]*\}})?\s+([0-9.eE+-]+)(?:\s|$)")
    values = []
    for line in metrics_text.splitlines():
        match = pattern.match(line.strip())
        if match:
            values.append(float(match.group(1)))
    return sum(values) if values else None


def prometheus_counter_total_or_zero(metrics_text, metric_name):
    """Read a lazily-exported counter, treating an omitted zero sample as 0."""
    total = prometheus_counter_total(metrics_text, metric_name)
    return 0 if total is None else total


def classify_body_rejection(status, extproc_counter_delta):
    """Attribute a 413 only when the ExtProc body-rejection counter proves contact.

    The counter covers ExtProc body-size guards, not a particular filter or
    translator. A 413 with no observed increment remains unattributed.
    """
    if str(status) != "413":
        return "not_a_413"
    if extproc_counter_delta == 1:
        return "extproc_body_size_rejection_observed"
    if extproc_counter_delta == 0:
        return "not_attributed_to_extproc_body_size_counter"
    return "unattributed_counter_unavailable_or_ambiguous"


def extproc_counter_delta(before, after):
    """Return a counter delta only when both samples came from the same pod."""
    if not isinstance(before, dict) or not isinstance(after, dict):
        return None
    if before.get("count") is None or after.get("count") is None:
        return None
    if not before.get("pod_uid") or before.get("pod_uid") != after.get("pod_uid"):
        return None
    for key in ("container_id_sha256", "container_restart_count"):
        if key in before and before.get(key) != after.get(key):
            return None
    if after["count"] < before["count"]:
        return None
    return after["count"] - before["count"]


def authorino_scopes_for_path(plugin_config, path_prefix):
    """Return AuthConfig scopes used by gateway action sets for a route prefix."""
    if isinstance(plugin_config, str):
        import json

        try:
            plugin_config = json.loads(plugin_config)
        except json.JSONDecodeError:
            return []
    if not isinstance(plugin_config, dict):
        return []

    scopes = set()
    for action_set in plugin_config.get("actionSets", []):
        predicates = action_set.get("routeRuleConditions", {}).get("predicates", [])
        if not any(path_prefix in predicate for predicate in predicates if isinstance(predicate, str)):
            continue
        for action in action_set.get("actions", []):
            scope = action.get("scope")
            if (action.get("service") == "auth-service" and isinstance(scope, str)
                    and re.fullmatch(r"[0-9a-f]{64}", scope)):
                scopes.add(scope)
    return sorted(scopes)


def authorino_expression_fingerprints(value):
    """Fingerprint policy expressions without retaining expression contents."""
    fingerprints = []

    def visit(item, path, blocked):
        if isinstance(item, dict):
            for key, child in item.items():
                key_text = str(key)
                next_blocked = blocked or any(token in key_text.lower() for token in (
                    "token", "secret", "credential", "password", "headers",
                ))
                visit(child, f"{path}.{key_text}", next_blocked)
        elif isinstance(item, list):
            for index, child in enumerate(item):
                visit(child, f"{path}[{index}]", blocked)
        elif (isinstance(item, str) and not blocked
              and path.rsplit(".", 1)[-1].lower() in {"expression", "selector", "predicate", "rego"}):
            lowered = item.lower()
            fingerprints.append({
                "field_path": path,
                "sha256": hashlib.sha256(item.encode()).hexdigest(),
                "length": len(item),
                "mentions_identity": bool(re.search(r"\bidentity\b", lowered)),
                "mentions_subscription_info": "subscription-info" in lowered,
                "mentions_selected_subscription": "selected_subscription" in lowered,
                "mentions_responsebodyjson": "responsebodyjson" in lowered,
            })

    visit(value, "spec", False)
    return fingerprints


def authorino_authconfig_readiness(authconfig, expected_route_annotation):
    """Summarize readiness for the route-owned AuthConfig without exposing policy data."""
    metadata = authconfig.get("metadata", {}) if isinstance(authconfig, dict) else {}
    status = authconfig.get("status", {}) if isinstance(authconfig, dict) else {}
    spec = authconfig.get("spec", {}) if isinstance(authconfig, dict) else {}
    annotations = metadata.get("annotations", {})
    route_identity = annotations.get("HTTPRouteRule.gateway.networking.k8s.io", "")
    conditions = status.get("conditions", [])
    generation = metadata.get("generation")
    condition_summary = []
    ready = {}
    current_generation = True
    for condition in conditions:
        condition_summary.append({key: condition.get(key) for key in (
            "type", "status", "reason", "lastTransitionTime", "observedGeneration",
        )})
        ready[condition.get("type")] = condition.get("status")
        observed = condition.get("observedGeneration")
        if observed is not None and generation is not None and observed != generation:
            current_generation = False
    route_matches = isinstance(route_identity, str) and route_identity.startswith(expected_route_annotation)
    return {
        "name": metadata.get("name"),
        "generation": metadata.get("generation"),
        "resource_version": metadata.get("resourceVersion"),
        "authentication_rule_names": sorted(spec.get("authentication", {})),
        "metadata_rule_names": sorted(spec.get("metadata", {})),
        "authorization_rule_names": sorted(spec.get("authorization", {})),
        "expression_fingerprints": authorino_expression_fingerprints(spec),
        "current_generation": current_generation,
        "route_identity": route_identity or None,
        "route_identity_matches": route_matches,
        "conditions": condition_summary,
        "ready": (route_matches and current_generation and ready.get("Ready") == "True"
                  and ready.get("Available") == "True"),
    }


def route_authconfigs_ready(evidence):
    """Validate the complete stable-readiness result returned by the waiter."""
    if not isinstance(evidence, dict) or evidence.get("result") != "PASS":
        return False
    scopes = evidence.get("authconfig_scopes_selected_from_live_action_sets")
    configs = evidence.get("authconfigs")
    if (not isinstance(scopes, list) or not scopes or not isinstance(configs, list)
            or len(configs) != len(scopes) or evidence.get("stable_ready_polls", 0) < 2):
        return False
    return all(
        isinstance(config, dict)
        and config.get("exists") is True
        and config.get("ready") is True
        and config.get("route_identity_matches") is True
        and config.get("current_generation") is True
        for config in configs
    )


def authorino_logged_header_facts(http):
    """Report only headers Authorino actually includes in its log object.

    Authorino may omit request headers from structured logs. An omitted field
    means "not logged", not that the header was absent from the authorization
    request.
    """
    headers = http.get("headers") if isinstance(http, dict) else None
    if not isinstance(headers, dict):
        return {
            "headers_logged": False,
            "authorization_present": None,
            "authorization_scheme": None,
            "authorization_token_family": "not_logged",
            "x_api_key_present": None,
            "client_identity_header_names": [],
        }
    normalized = {str(key).lower(): value for key, value in headers.items()}
    authorization = normalized.get("authorization", "")
    if isinstance(authorization, list):
        authorization = authorization[0] if authorization else ""
    authorization = authorization if isinstance(authorization, str) else ""
    candidate = authorization.split(" ", 1)[0] if " " in authorization else None
    schemes = {"bearer": "Bearer", "basic": "Basic", "apikey": "ApiKey"}
    scheme = schemes.get(candidate.lower(), "other") if candidate is not None else None
    identity_headers = sorted(name for name in normalized if name in {
        "x-maas-username", "x-maas-group", "x-maas-identity", "x-maas-subscription",
    })
    return {
        "headers_logged": True,
        "authorization_present": bool(authorization),
        "authorization_scheme": scheme,
        "authorization_token_family": "sk-oai" if "sk-oai-" in authorization else (
            "other" if authorization else "absent"),
        "x_api_key_present": "x-api-key" in normalized,
        "client_identity_header_names": identity_headers,
    }


def authorino_event_context(entry):
    """Keep evaluator identifiers from an Authorino log, never raw values."""
    if not isinstance(entry, dict):
        return {"structured_field_names": [], "evaluator_context": {}}

    safe_keys = {
        "action", "action_name", "authconfig", "auth_config", "authconfig_name",
        "auth_config_name", "code", "decision", "evaluator", "evaluator_name",
        "kind", "phase", "response_status", "rule", "rule_name", "scope",
        "service", "status", "type", "authorized",
    }
    forbidden_value_markers = (
        "bearer", "sk-oai", "api-key", "apikey", "token", "secret", "password",
        "cookie", "authorization", "credential",
    )
    context = {}
    for key, value in entry.items():
        normalized = str(key).lower().replace("-", "_")
        if normalized not in safe_keys:
            continue
        if isinstance(value, bool) or (isinstance(value, (int, float)) and not isinstance(value, bool)):
            context[normalized] = value
            continue
        if not isinstance(value, str) or not value or len(value) > 160:
            continue
        lowered = value.lower()
        if any(marker in lowered for marker in forbidden_value_markers):
            continue
        if re.fullmatch(r"[A-Za-z0-9._:/#-]+", value):
            context[normalized] = value

    return {
        "structured_field_names": sorted(str(key) for key in entry),
        "evaluator_context": context,
    }

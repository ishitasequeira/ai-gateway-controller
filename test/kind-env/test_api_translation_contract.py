import ast
import json
import pathlib
import sys
import unittest
from types import SimpleNamespace

from api_translation_contract import (
    authorino_expression_fingerprints,
    authorino_authconfig_readiness,
    authorino_event_context,
    authorino_logged_header_facts,
    authorino_scopes_for_path,
    classify_body_rejection,
    chat_request_json_with_exact_size,
    chat_request_with_exact_json_size,
    compact_translation_diagnostic_id,
    extproc_translation_limits,
    extproc_counter_delta,
    is_owned_translation_diagnostic_authpolicy,
    prometheus_counter_total,
    prometheus_counter_total_or_zero,
    route_authconfigs_ready,
)


class TranslationRuntimeContractTest(unittest.TestCase):
    def test_diagnostic_auth_policy_cleanup_matches_route_names(self):
        source = pathlib.Path(__file__).with_name("e2e-api-translation.py")
        tree = ast.parse(source.read_text())
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                        and node.name == "remove_interrupted_translation_auth_diagnostics")
        for owned, remains, expected_pass in ((True, False, True), (True, True, False), (False, True, False)):
            with self.subTest(owned=owned, remains=remains):
                policy = {"metadata": {"name": "api-translation-messages-auth", "namespace": "models-as-a-service"},
                          "spec": {"targetRef": {"group": "gateway.networking.k8s.io", "kind": "HTTPRoute",
                                                 "name": "api-translation-messages"}}}
                calls, records = [], []
                def kubectl(*args, **kwargs):
                    calls.append(args)
                    items = [policy] if len(calls) == 1 or remains else []
                    return SimpleNamespace(stdout=json.dumps({"items": items}), returncode=0)
                ticks = iter((0, 0, 100))
                namespace = {"kubectl": kubectl, "json": json, "NAMESPACE": "models-as-a-service",
                             "EVIDENCE": pathlib.Path("/unused"), "save_json": lambda *args: None,
                             "is_owned_translation_diagnostic_authpolicy": lambda *args: owned,
                             "time": SimpleNamespace(monotonic=lambda: next(ticks), sleep=lambda _: None),
                             "record": lambda name, passed, details: records.append(passed)}
                exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), "exec"), namespace)
                namespace["remove_interrupted_translation_auth_diagnostics"]()
                self.assertEqual(records, [expected_pass])
                self.assertEqual(any("delete" in args for args in calls), owned)

    def test_wrapper_returns_nonzero_when_cleanup_fails(self):
        source = pathlib.Path(__file__).with_name("e2e-api-translation.py")
        tree = ast.parse(source.read_text())
        wrapper = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "main_wrapper")
        for initial_status, cleanup_fails, expected_exit in [
            ("PASS", False, 0), ("PARTIAL", False, 3),
            ("PASS", True, 1), ("PARTIAL", True, 1),
        ]:
            with self.subTest(initial_status=initial_status, cleanup_fails=cleanup_fails):
                failures = []
                namespace = {
                    "main": lambda: None,
                    "cleanup": lambda: failures.append("restoration failed") if cleanup_fails else None,
                    "RESULT": {"status": initial_status}, "CLEANUP_FAILURES": failures,
                    "RESULTS": SimpleNamespace(exists=lambda: False),
                    "save_json": lambda *_args: None, "sys": sys,
                }
                exec(compile(ast.Module(body=[wrapper], type_ignores=[]), str(source), "exec"), namespace)
                if expected_exit:
                    with self.assertRaises(SystemExit) as caught:
                        namespace["main_wrapper"]()
                    self.assertEqual(caught.exception.code, expected_exit)
                else:
                    namespace["main_wrapper"]()

    def test_exact_size_chat_json_builder_preserves_valid_model_identity(self):
        body = chat_request_with_exact_json_size(512)
        encoded = json.dumps(body, separators=(",", ":"), ensure_ascii=True).encode()
        self.assertEqual(len(encoded), 512)
        self.assertEqual(json.loads(encoded)["model"], "demo")

    def test_trailing_json_whitespace_offsets_only_target_model_expansion(self):
        size = 512
        body, payload = chat_request_json_with_exact_size(
            size, trailing_whitespace_bytes=13
        )
        self.assertEqual(len(payload.encode()), size)
        self.assertEqual(json.loads(payload), body)
        self.assertEqual(body["model"], "demo")
        rewritten = {**body, "model": "mock-openai-model"}
        rewritten_payload = json.dumps(rewritten, separators=(",", ":"), ensure_ascii=True)
        self.assertEqual(len(rewritten_payload.encode()), size)

    def test_exact_size_chat_json_builder_rejects_invalid_padding(self):
        with self.assertRaises(ValueError):
            chat_request_json_with_exact_size(32, trailing_whitespace_bytes=32)

    def test_compact_diagnostic_ids_fit_recorder_character_and_length_contract(self):
        diagnostic_id = compact_translation_diagnostic_id(
            "ipp-route-trace-20261029T003533-31c04cac4690-",
            "anthropic-legacy-parallel_tool_calls",
        )
        self.assertRegex(diagnostic_id, r"^[A-Za-z0-9.-]{1,80}$")
        self.assertIn("parallel-tool-calls", diagnostic_id)
        self.assertNotIn("_", diagnostic_id)

    def test_route_authconfig_wait_result_requires_complete_stable_readiness(self):
        evidence = {
            "result": "PASS",
            "authconfig_scopes_selected_from_live_action_sets": ["scope-a", "scope-b"],
            "stable_ready_polls": 2,
            "authconfigs": [
                {"exists": True, "ready": True, "route_identity_matches": True,
                 "current_generation": True},
                {"exists": True, "ready": True, "route_identity_matches": True,
                 "current_generation": True},
            ],
        }
        self.assertTrue(route_authconfigs_ready(evidence))
        self.assertFalse(route_authconfigs_ready({**evidence, "result": "FAIL"}))
        self.assertFalse(route_authconfigs_ready({**evidence, "stable_ready_polls": 1}))
        self.assertFalse(route_authconfigs_ready({**evidence, "authconfigs": evidence["authconfigs"][:1]}))
        stale = {**evidence, "authconfigs": [
            evidence["authconfigs"][0], {**evidence["authconfigs"][1], "current_generation": False},
        ]}
        self.assertFalse(route_authconfigs_ready(stale))

    def test_authconfig_fingerprints_never_include_policy_expression_text(self):
        spec = {"metadata": {"apiKeyValidation": {"when": [{
            "predicate": 'auth.identity.matches("^Bearer sk-oai-.*")',
        }]}}, "response": {"success": {"headers": {"Authorization": {
            "plain": {"expression": "do not fingerprint secret-bearing header policy"},
        }}}}}
        fingerprints = authorino_expression_fingerprints(spec)
        self.assertEqual(len(fingerprints), 1)
        self.assertEqual(fingerprints[0]["field_path"], "spec.metadata.apiKeyValidation.when[0].predicate")
        self.assertTrue(fingerprints[0]["mentions_identity"])
        self.assertNotIn("expression", repr(fingerprints))
        self.assertNotIn("sk-oai", repr(fingerprints))

    def test_reads_server_and_filter_limits_without_confusing_them(self):
        config = """server:
  max_body_bytes: 10485760
filter_chains:
  - name: post-auth
    filters:
      - filter: intelligent_route
      - filter: openai_chat_completions_to_anthropic_stream
        max_partial_event_bytes: 1048576
      - filter: openai_chat_completions_to_anthropic
        max_request_body_bytes: 2097152
        max_response_body_bytes: 1048576
      - filter: credential_inject
"""
        self.assertEqual(extproc_translation_limits(config), {
            "server_max_body_bytes": 10485760,
            "translator_max_request_body_bytes": 2097152,
            "translator_max_response_body_bytes": 1048576,
            "translator_filter_present": True,
        })

    def test_missing_translator_has_no_translation_limits(self):
        limits = extproc_translation_limits("server:\n  max_body_bytes: 10485760\nfilter_chains: []\n")
        self.assertEqual(limits["server_max_body_bytes"], 10485760)
        self.assertIsNone(limits["translator_max_request_body_bytes"])
        self.assertIsNone(limits["translator_max_response_body_bytes"])
        self.assertFalse(limits["translator_filter_present"])

    def test_sums_body_rejection_counter_and_ignores_comments(self):
        metrics = """# TYPE praxis_extproc_body_size_rejections_total counter
praxis_extproc_body_size_rejections_total{processor="a"} 2
praxis_extproc_body_size_rejections_total{processor="b"} 3
other_metric 99
"""
        self.assertEqual(prometheus_counter_total(metrics, "praxis_extproc_body_size_rejections_total"), 5)
        self.assertIsNone(prometheus_counter_total("# no samples\n", "missing_metric"))

    def test_omitted_zero_counter_sample_is_read_as_zero(self):
        self.assertEqual(prometheus_counter_total_or_zero(
            "# TYPE lazy_counter_total counter\n", "lazy_counter_total"), 0)
        self.assertEqual(prometheus_counter_total_or_zero(
            "lazy_counter_total 4\n", "lazy_counter_total"), 4)

    def test_body_rejection_attribution_requires_counter_evidence(self):
        self.assertEqual(classify_body_rejection("200", 1), "not_a_413")
        self.assertEqual(classify_body_rejection("413", 1), "extproc_body_size_rejection_observed")
        self.assertEqual(classify_body_rejection("413", 0),
                         "not_attributed_to_extproc_body_size_counter")
        self.assertEqual(classify_body_rejection("413", None),
                         "unattributed_counter_unavailable_or_ambiguous")

    def test_extproc_counter_delta_requires_same_pod_identity(self):
        before = {"count": 2, "pod_uid": "pod-a", "container_id_sha256": "container-a",
                  "container_restart_count": 0}
        self.assertEqual(extproc_counter_delta(before, {**before, "count": 3}), 1)
        self.assertIsNone(extproc_counter_delta(before, {**before, "pod_uid": "pod-b", "count": 0}))
        self.assertIsNone(extproc_counter_delta(before, {**before, "container_id_sha256": "container-b"}))
        self.assertIsNone(extproc_counter_delta(before, {**before, "container_restart_count": 1}))
        self.assertIsNone(extproc_counter_delta(before, {**before, "count": None}))

    def test_recognizes_only_exact_owned_translation_route_policy(self):
        policy = {
            "metadata": {
                "namespace": "models-as-a-service",
                "name": "api-translation-messages-auth",
                "labels": {"app.kubernetes.io/managed-by": "api-translation-kind-e2e-diagnostic"},
            },
            "spec": {"targetRef": {
                "group": "gateway.networking.k8s.io",
                "kind": "HTTPRoute",
                "name": "api-translation-messages",
            }},
        }
        self.assertTrue(is_owned_translation_diagnostic_authpolicy(
            policy, "models-as-a-service", "api-translation-messages-auth", "api-translation-messages"))
        self.assertFalse(is_owned_translation_diagnostic_authpolicy(
            policy, "other-namespace", "api-translation-messages-auth", "api-translation-messages"))
        self.assertFalse(is_owned_translation_diagnostic_authpolicy(
            policy, "models-as-a-service", "api-translation-messages-auth", "another-route"))
        foreign = {**policy, "metadata": {**policy["metadata"], "labels": {}}}
        self.assertFalse(is_owned_translation_diagnostic_authpolicy(
            foreign, "models-as-a-service", "api-translation-messages-auth", "api-translation-messages"))

    def test_extracts_only_auth_service_scopes_for_matching_route_prefix(self):
        config = {
            "actionSets": [
                {"routeRuleConditions": {"predicates": [
                    "request.url_path.startsWith('/models-as-a-service/demo')",
                ]}, "actions": [
                    {"service": "auth-service", "scope": "a" * 64},
                    {"service": "ratelimit-check-service", "scope": "b" * 64},
                ]},
                {"routeRuleConditions": {"predicates": [
                    "request.url_path.startsWith('/v1/messages')",
                ]}, "actions": [{"service": "auth-service", "scope": "c" * 64}]},
                {"routeRuleConditions": {"predicates": [
                    "request.url_path.startsWith('/models-as-a-service/demo')",
                ]}, "actions": [{"service": "auth-service", "scope": "a" * 64}]},
            ],
        }
        self.assertEqual(authorino_scopes_for_path(config, "/models-as-a-service/demo"), ["a" * 64])

    def test_scope_extraction_accepts_json_and_rejects_malformed_or_unrelated_actions(self):
        valid = {
            "actionSets": [{
                "routeRuleConditions": {"predicates": ["path startsWith('/tenant/model')"]},
                "actions": [
                    {"service": "auth-service", "scope": "d" * 64},
                    {"service": "metadata-service", "scope": "e" * 64},
                    {"service": "auth-service", "scope": "not-a-scope"},
                ],
            }],
        }
        self.assertEqual(authorino_scopes_for_path(
            __import__("json").dumps(valid), "/tenant/model"), ["d" * 64])
        self.assertEqual(authorino_scopes_for_path("not-json", "/tenant/model"), [])
        self.assertEqual(authorino_scopes_for_path(valid, "/another-model"), [])

    def test_authconfig_readiness_requires_route_identity_and_both_conditions(self):
        config = {
            "metadata": {
                "name": "a" * 64,
                "generation": 3,
                "annotations": {
                    "HTTPRouteRule.gateway.networking.k8s.io":
                        "httproute.gateway.networking.k8s.io:models-as-a-service/external-model-demo-model#rule-3",
                },
            },
            "spec": {
                "authentication": {"api-keys": {}, "openshift-identities": {}},
                "metadata": {"apiKeyValidation": {}, "subscription-info": {}},
                "authorization": {"auth-valid": {}, "subscription-valid": {}},
            },
            "status": {"conditions": [
                {"type": "Available", "status": "True", "reason": "HostsLinked"},
                {"type": "Ready", "status": "True", "reason": "Reconciled"},
            ]},
        }
        expected = "httproute.gateway.networking.k8s.io:models-as-a-service/external-model-demo-model#"
        ready = authorino_authconfig_readiness(config, expected)
        self.assertTrue(ready["ready"])
        self.assertEqual(ready["authentication_rule_names"], ["api-keys", "openshift-identities"])
        self.assertEqual(ready["metadata_rule_names"], ["apiKeyValidation", "subscription-info"])
        self.assertEqual(ready["authorization_rule_names"], ["auth-valid", "subscription-valid"])
        not_ready = {**config, "status": {"conditions": [
            {"type": "Available", "status": "True"}, {"type": "Ready", "status": "False"},
        ]}}
        self.assertFalse(authorino_authconfig_readiness(not_ready, expected)["ready"])
        wrong_route = {**config, "metadata": {**config["metadata"], "annotations": {
            "HTTPRouteRule.gateway.networking.k8s.io": "httproute.gateway.networking.k8s.io:other/route#rule-0",
        }}}
        self.assertFalse(authorino_authconfig_readiness(wrong_route, expected)["ready"])
        stale_generation = {**config, "metadata": {**config["metadata"], "generation": 4},
                            "status": {"conditions": [
                                {"type": "Available", "status": "True", "observedGeneration": 3},
                                {"type": "Ready", "status": "True", "observedGeneration": 4},
                            ]}}
        stale = authorino_authconfig_readiness(stale_generation, expected)
        self.assertFalse(stale["current_generation"])
        self.assertFalse(stale["ready"])

    def test_missing_authorino_log_headers_are_unknown_not_absent(self):
        facts = authorino_logged_header_facts({"id": "request-id", "path": "/v1/messages"})
        self.assertFalse(facts["headers_logged"])
        self.assertIsNone(facts["authorization_present"])
        self.assertEqual(facts["authorization_token_family"], "not_logged")

    def test_authorino_log_header_summary_never_returns_credential_values(self):
        facts = authorino_logged_header_facts({"headers": {
            "Authorization": "Bearer sk-oai-never-persist-this",
            "x-api-key": "provider-key-never-persist-this",
            "x-maas-group": "system:authenticated",
        }})
        self.assertEqual(facts["authorization_token_family"], "sk-oai")
        self.assertTrue(facts["authorization_present"])
        self.assertTrue(facts["x_api_key_present"])
        self.assertEqual(facts["client_identity_header_names"], ["x-maas-group"])
        self.assertNotIn("sk-oai-never-persist-this", repr(facts))
        self.assertNotIn("provider-key-never-persist-this", repr(facts))

    def test_unknown_authorization_scheme_cannot_expose_a_credential(self):
        for value in ("sk-oai-never-persist-this extra", "sk-oai-never-persist-this"):
            facts = authorino_logged_header_facts({"headers": {"Authorization": value}})
            self.assertNotIn("sk-oai-never-persist-this", repr(facts))
            self.assertIn(facts["authorization_scheme"], ("other", None))

    def test_authorino_event_context_keeps_evaluator_identity_but_redacts_values(self):
        event = authorino_event_context({
            "request id": "request-123",
            "authconfig": "a" * 64,
            "evaluator": "subscription-valid",
            "rule_name": "provider-key-check",
            "status": 403,
            "authorization": "Bearer sk-oai-do-not-save",
            "error": "secret=never-save",
            "headers": {"Authorization": "Bearer sk-oai-do-not-save"},
        })
        self.assertEqual(event["evaluator_context"], {
            "authconfig": "a" * 64,
            "evaluator": "subscription-valid",
            "rule_name": "provider-key-check",
            "status": 403,
        })
        self.assertIn("authorization", event["structured_field_names"])
        self.assertNotIn("sk-oai-do-not-save", repr(event))
        self.assertNotIn("never-save", repr(event))


if __name__ == "__main__":
    unittest.main()

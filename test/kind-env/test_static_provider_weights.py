#!/usr/bin/env python3
"""Focused regression tests for the static-weight Kind harness gates."""

import importlib.util
import pathlib
from types import SimpleNamespace
import unittest
from unittest.mock import patch


SCRIPT = pathlib.Path(__file__).with_name("e2e-static-provider-weights.py")
SPEC = importlib.util.spec_from_file_location("static_provider_weights_e2e", SCRIPT)
E2E = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(E2E)


class RouteReadinessTests(unittest.TestCase):
    def test_uses_controller_published_route_name(self):
        model = {"status": {"httpRouteName": "external-model-demo-model"}}
        route = {"status": {"parents": [{"conditions": [
            {"type": "Accepted", "status": "True"},
            {"type": "ResolvedRefs", "status": "True"},
        ]}]}}
        with patch.object(E2E, "get_json", side_effect=[model, route]) as get_json:
            self.assertTrue(E2E.route_ready())
        self.assertEqual(get_json.call_args_list[0].args,
                         ("-n", E2E.NAMESPACE, "get", "externalmodel", E2E.MODEL))
        self.assertEqual(get_json.call_args_list[1].args,
                         ("-n", E2E.NAMESPACE, "get", "httproute", "external-model-demo-model"))

    def test_missing_published_route_name_is_not_ready(self):
        with patch.object(E2E, "get_json", return_value={"status": {}}) as get_json:
            self.assertFalse(E2E.route_ready())
        get_json.assert_called_once()

    def test_requires_accepted_and_resolved_refs(self):
        model = {"status": {"httpRouteName": "external-model-demo-model"}}
        route = {"status": {"parents": [{"conditions": [
            {"type": "Accepted", "status": "True"},
            {"type": "ResolvedRefs", "status": "False"},
        ]}]}}
        with patch.object(E2E, "get_json", side_effect=[model, route]):
            self.assertFalse(E2E.route_ready())

    def test_reads_requested_sibling_model(self):
        model = {"status": {"httpRouteName": "external-model-healthy-sibling"}}
        route = {"status": {"parents": [{"conditions": [
            {"type": "Accepted", "status": "True"},
            {"type": "ResolvedRefs", "status": "True"},
        ]}]}}
        with patch.object(E2E, "get_json", side_effect=[model, route]) as get_json:
            self.assertTrue(E2E.route_ready("healthy-sibling"))
        self.assertEqual(get_json.call_args_list[0].args,
                         ("-n", E2E.NAMESPACE, "get", "externalmodel", "healthy-sibling"))


class ProviderReferenceFixtureTests(unittest.TestCase):
    def test_can_target_a_distinct_backend_model(self):
        reference = E2E.make_ref("provider-a", 1, target_model="healthy-sibling")
        self.assertEqual(reference["targetModel"], "healthy-sibling")
        self.assertEqual(reference["weight"], 1)

    def test_receipt_validation_requires_the_requested_backend_model(self):
        request_id = "sibling-model-request"
        receipt = {"request_id": request_id, "provider": "proof-a", "method": "POST",
                   "path": "/v1/chat/completions", "model": "healthy-sibling", "tls": True,
                   "sni_ok": True, "authority_ok": True, "credential_ok": True}
        response = {"request_id": request_id, "status": 200, "provider": "proof-a", "checks": True}
        self.assertTrue(E2E.one_request_receipt_result(
            response, [receipt], request_id, "proof-a", expected_model="healthy-sibling")["pass"])
        wrong_model = {**receipt, "model": "demo"}
        self.assertFalse(E2E.one_request_receipt_result(
            response, [wrong_model], request_id, "proof-a", expected_model="healthy-sibling")["pass"])


class ExtProcTemplateHashTests(unittest.TestCase):
    def test_reads_hash_from_pod_template_metadata(self):
        expected = "a" * 64
        deployment = {"spec": {"template": {"metadata": {"annotations": {
            "ai-gateway-controller.opendatahub.io/extproc-config-sha256": expected,
        }}}}}
        self.assertEqual(E2E.deployment_extproc_config_hash(deployment), expected)

    def test_missing_template_hash_is_empty(self):
        self.assertEqual(E2E.deployment_extproc_config_hash({"spec": {"template": {}}}), "")

    def test_parses_revision_signal_with_terminal_color_codes(self):
        accepted = "a" * 64
        serving = "b" * 64
        logs = (f"\x1b[32mINFO\x1b[0m accepted_revision\x1b[0m=\x1b[0m\"{accepted}\" "
                f"serving_revision\x1b[0m=\x1b[0m\"{serving}\"")
        self.assertEqual(E2E.overlay_revisions_from_logs(logs), (accepted, serving))

    def test_parses_unquoted_reload_revision_and_prefers_latest_runtime_state(self):
        startup = f'accepted_revision="{"a" * 64}" serving_revision="{"a" * 64}"'
        reloaded = f'candidate_count=2 accepted_revision={"b" * 64} serving_revision={"b" * 64}'
        self.assertEqual(E2E.overlay_revisions_from_logs(f"{startup}\n{reloaded}"),
                         ("b" * 64, "b" * 64))


class BaselineConvergenceTests(unittest.TestCase):
    def test_requires_route_digest_runtime_and_provider_agreement(self):
        digest = "c" * 64
        state = {
            "model_ready": True,
            "route_ready": True,
            "declared_digest": digest,
            "recomputed_digest": digest,
            "mounted_digest": digest,
            "wire_revision": digest,
            "generation": 7,
            "wire_generation": 7,
            "candidates": [{"stable_id": "demo-model/provider-provider-a",
                            "cluster": "provider-provider-a"}],
            "runtime": {"accepted_revision": digest, "serving_revision": digest,
                        "deployment_generation": 3, "deployment_observed_generation": 3,
                        "deployment_replicas": 1, "deployment_ready_replicas": 1,
                        "deployment_template_sha256": "d" * 64,
                        "extproc_config_sha256": "d" * 64,
                        "mounted_extproc_sha256": "d" * 64, "restart_count": 0},
        }
        self.assertTrue(E2E.baseline_state_is_ready(state, ["provider-provider-a"]))
        state["runtime"]["serving_revision"] = "e" * 64
        self.assertFalse(E2E.baseline_state_is_ready(state, ["provider-provider-a"]))

    def test_partial_model_can_serve_with_a_failed_provider_ref(self):
        digest = "c" * 64
        config_hash = "d" * 64
        model = {"metadata": {"generation": 8}, "status": {
            "observedGeneration": 8, "phase": "Failed"}}
        state = {
            "route_ready": True, "declared_digest": digest, "recomputed_digest": digest,
            "mounted_digest": digest, "wire_revision": digest, "generation": 9,
            "wire_generation": 9,
            "candidates": [{"stable_id": "demo-model/provider-a", "cluster": "provider-provider-proof-a"},
                           {"stable_id": "demo-model/provider-c", "cluster": "provider-provider-proof-c"}],
            "runtime": {"accepted_revision": digest, "serving_revision": digest,
                        "runtime_clusters": ["provider-provider-proof-a", "provider-provider-proof-c"],
                        "deployment_generation": 2, "deployment_observed_generation": 2,
                        "deployment_replicas": 1, "deployment_ready_replicas": 1,
                        "deployment_template_sha256": config_hash,
                        "extproc_config_sha256": config_hash, "mounted_extproc_sha256": config_hash,
                        "restart_count": 0},
        }
        with patch.object(E2E, "get_json", return_value=model):
            self.assertTrue(E2E.partial_serving_state_is_ready(
                state, ["provider-provider-proof-a", "provider-provider-proof-c"]))
            self.assertFalse(E2E.partial_serving_state_is_ready(
                state, ["provider-provider-proof-a", "provider-provider-proof-b"]))
            state["runtime"]["serving_revision"] = "e" * 64
            self.assertFalse(E2E.partial_serving_state_is_ready(
                state, ["provider-provider-proof-a", "provider-provider-proof-c"]))

    def test_primary_restoration_accepts_exact_healthy_sibling_overlay(self):
        digest = "c" * 64
        config_hash = "d" * 64
        primary = {"metadata": {"generation": 4}, "status": {
            "phase": "Ready", "observedGeneration": 4}}
        sibling = {"metadata": {"generation": 2}, "status": {
            "phase": "Ready", "observedGeneration": 2}}
        candidates = [
            {"stable_id": f"{E2E.MODEL}/provider-{provider}", "cluster": f"provider-{provider}",
             "selection_group": 0, "traffic_weight": None}
            for provider in E2E.PROVIDER_RESOURCES
        ] + [{"stable_id": f"{E2E.SIBLING_MODEL}/provider-{E2E.PROVIDER_RESOURCES[0]}",
              "cluster": f"provider-{E2E.PROVIDER_RESOURCES[0]}",
              "selection_group": None, "traffic_weight": None}]
        state = {
            "declared_digest": digest, "recomputed_digest": digest,
            "mounted_digest": digest, "wire_revision": digest,
            "generation": 8, "wire_generation": 8,
            "candidates": candidates, "mounted_candidates": candidates,
            "runtime": {"accepted_revision": digest, "serving_revision": digest,
                        "runtime_clusters": sorted(f"provider-{p}" for p in E2E.PROVIDER_RESOURCES),
                        "deployment_generation": 5, "deployment_observed_generation": 5,
                        "deployment_replicas": 1, "deployment_ready_replicas": 1,
                        "deployment_template_sha256": config_hash,
                        "extproc_config_sha256": config_hash,
                        "mounted_extproc_sha256": config_hash, "restart_count": 0},
        }
        self.assertTrue(E2E.primary_and_sibling_serving_state_is_ready(
            state, primary, sibling, True, True))
        self.assertFalse(E2E.primary_and_sibling_serving_state_is_ready(
            state, primary, sibling, False, True))
        self.assertFalse(E2E.primary_and_sibling_serving_state_is_ready(
            {**state, "runtime": {**state["runtime"], "serving_revision": "e" * 64}},
            primary, sibling, True, True))
        self.assertFalse(E2E.primary_and_sibling_serving_state_is_ready(
            {**state, "candidates": candidates[:-1]}, primary, sibling, True, True))

    def test_weight_convergence_requires_extproc_to_serve_published_revision(self):
        digest = "c" * 64
        config_hash = "d" * 64
        candidates = [
            {"cluster": f"provider-{provider}", "selection_group": 0,
             "traffic_weight": None}
            for provider in E2E.PROVIDER_RESOURCES
        ]
        names = {f"{provider}-credentials" for provider in E2E.PROVIDER_RESOURCES}
        state = {
            "model_ready": True, "route_ready": True,
            "declared_digest": digest, "recomputed_digest": digest,
            "mounted_digest": digest, "wire_revision": digest,
            "generation": 10, "wire_generation": 10,
            "selection_policy": {"mode": "random"},
            "candidates": candidates, "mounted_candidates": candidates,
            "runtime": {
                "accepted_revision": digest, "serving_revision": digest,
                "runtime_clusters": sorted(f"provider-{provider}" for provider in E2E.PROVIDER_RESOURCES),
                "credential_entries": [{"name": name} for name in names],
                "projected_secrets": [{"name": name} for name in names],
                "configmap_extproc_sha256": config_hash,
                "extproc_config_sha256": config_hash,
                "mounted_extproc_sha256": config_hash,
                "deployment_template_sha256": config_hash,
                "deployment_generation": 6, "deployment_observed_generation": 6,
                "deployment_replicas": 1, "deployment_ready_replicas": 1,
                "restart_count": 0,
            },
        }
        self.assertTrue(E2E.state_is_ready(state, {"a": 1, "b": 1, "c": 1}))
        state["runtime"]["accepted_revision"] = "e" * 64
        state["runtime"]["serving_revision"] = "e" * 64
        self.assertFalse(E2E.state_is_ready(state, {"a": 1, "b": 1, "c": 1}))

    def test_pre_serving_window_requires_published_and_mounted_new_overlay(self):
        old_revision = "a" * 64
        new_revision = "b" * 64
        config_hash = "c" * 64
        model = {"metadata": {"generation": 11}, "status": {
            "phase": "Ready", "observedGeneration": 11}}
        state = {
            "model_ready": True, "route_ready": True,
            "declared_digest": new_revision, "recomputed_digest": new_revision,
            "mounted_digest": new_revision, "wire_revision": new_revision,
            "generation": 11, "wire_generation": 11,
            "selection_policy": None,
            "candidates": [{"cluster": "provider-provider-proof-b", "selection_group": None,
                            "traffic_weight": None}],
            "runtime": {"accepted_revision": old_revision, "serving_revision": old_revision,
                        "deployment_generation": 6, "deployment_observed_generation": 6,
                        "deployment_replicas": 1, "deployment_ready_replicas": 1,
                        "deployment_template_sha256": config_hash,
                        "extproc_config_sha256": config_hash,
                        "mounted_extproc_sha256": config_hash},
        }
        self.assertTrue(E2E.published_runtime_pending_state_is_ready(
            state, model, {"a": 0, "b": 100, "c": 0}, 11))
        state["mounted_digest"] = old_revision
        self.assertFalse(E2E.published_runtime_pending_state_is_ready(
            state, model, {"a": 0, "b": 100, "c": 0}, 11))
        state["mounted_digest"] = new_revision
        state["runtime"]["serving_revision"] = new_revision
        self.assertFalse(E2E.published_runtime_pending_state_is_ready(
            state, model, {"a": 0, "b": 100, "c": 0}, 11))


class WeightWireTests(unittest.TestCase):
    def test_expected_wire_uses_configured_weights_without_gcd_rewriting(self):
        state = {
            "selection_policy": {"mode": "weightedRandom"},
            "candidates": [
                {"cluster": "provider-provider-proof-a", "selection_group": 0, "traffic_weight": 50},
                {"cluster": "provider-provider-proof-b", "selection_group": 0, "traffic_weight": 25},
                {"cluster": "provider-provider-proof-c", "selection_group": 0, "traffic_weight": 25},
            ],
        }
        self.assertTrue(E2E.expected_wire_ok(state, {"a": 50, "b": 25, "c": 25}))
        self.assertFalse(E2E.expected_wire_ok(state, {"a": 2, "b": 1, "c": 1}))

    def test_relative_weight_vectors_have_same_ratio_signature(self):
        self.assertEqual(E2E.ratio_signature({"a": 50, "b": 25, "c": 25}),
                         E2E.ratio_signature({"a": 2, "b": 1, "c": 1}))
        self.assertNotEqual(E2E.ratio_signature({"a": 2, "b": 1, "c": 1}),
                            E2E.ratio_signature({"a": 3, "b": 1, "c": 1}))

    def test_sampling_plan_is_bounded_and_predeclared(self):
        self.assertEqual(E2E.SAMPLE_PLAN, {
            "equal-active-active-50-50": 500,
            "unequal-active-active-50-25-25": 500,
            "active-passive-100-0": 50,
            "administrative-switch-0-100": 50,
            "canary-95-5": 500,
        })
        self.assertEqual(E2E.SAMPLE_CONCURRENCY, 8)
        self.assertEqual(E2E.STATISTICAL_ALPHA, 0.001)
        self.assertEqual(E2E.CHI_SQUARE_CRITICAL, {1: 10.828, 2: 13.816})

    def test_generated_receipt_ids_fit_provider_recorder_allowlist(self):
        request_ids = [E2E.new_request_id() for _ in range(20)]
        self.assertEqual(len(set(request_ids)), len(request_ids))
        self.assertTrue(all(len(request_id) <= E2E.RECORDER_REQUEST_ID_MAX_LENGTH
                            and E2E.RECORDER_REQUEST_ID_RE.fullmatch(request_id)
                            for request_id in request_ids))

    def test_incomplete_withdrawal_sequence_cannot_report_pass(self):
        result = {"assertions": [{"result": "PASS"}],
                  "withdrawal_sequence_required": True,
                  "withdrawal_sequence_completed": False}
        self.assertFalse(E2E.results_are_complete_pass(result))
        result["withdrawal_sequence_completed"] = True
        self.assertTrue(E2E.results_are_complete_pass(result))
        result["assertions"].append({"result": "FAIL"})
        self.assertFalse(E2E.results_are_complete_pass(result))
        result["assertions"].pop()
        result["cleanup_failure"] = ["owned fixture cleanup failed"]
        self.assertFalse(E2E.results_are_complete_pass(result))

    def test_all_zero_withdrawal_requires_current_no_routes_and_resource_removal(self):
        model = {"metadata": {"generation": 12}, "status": {
            "observedGeneration": 12, "phase": "Failed", "conditions": [
                {"type": "Ready", "status": "False", "reason": "NoRoutes"},
            ]}}
        self.assertTrue(E2E.all_zero_withdrawal_ready(model, None, None))
        self.assertFalse(E2E.all_zero_withdrawal_ready(
            {**model, "status": {**model["status"], "observedGeneration": 11}}, None, None))
        self.assertFalse(E2E.all_zero_withdrawal_ready(model, {"metadata": {"name": "route"}}, None))
        self.assertFalse(E2E.all_zero_withdrawal_ready(model, None, {
            "overlay": {"candidates": [{"stable_id": "demo-model/provider-a"}]}}))
        self.assertFalse(E2E.all_zero_withdrawal_ready(model, None, {"invalid": True}))

    def test_all_zero_model_can_be_withdrawn_while_healthy_sibling_candidate_remains(self):
        model = {"metadata": {"generation": 12}, "status": {
            "observedGeneration": 12, "phase": "Failed", "conditions": [
                {"type": "Ready", "status": "False", "reason": "NoRoutes"},
            ]}}
        sibling_overlay = {"overlay": {"candidates": [
            {"stable_id": "healthy-sibling/provider-proof-a"},
        ]}}
        self.assertTrue(E2E.all_zero_withdrawal_ready(model, None, sibling_overlay))

    def test_withdrawal_request_requires_404_without_provider_receipt(self):
        request_id = "withdrawn-request"
        self.assertTrue(E2E.one_request_receipt_result(
            {"request_id": request_id, "status": 404, "provider": ""}, [], request_id, None)["pass"])
        self.assertFalse(E2E.one_request_receipt_result(
            {"request_id": request_id, "status": 200, "provider": "proof-a"}, [], request_id, None)["pass"])
        self.assertFalse(E2E.one_request_receipt_result(
            {"request_id": request_id, "status": 404, "provider": ""},
            [{"request_id": request_id}], request_id, None)["pass"])

    def test_reactivation_requires_one_verified_provider_a_receipt(self):
        request_id = "reactivation-request"
        receipt = {"request_id": request_id, "provider": "proof-a", "method": "POST",
                   "path": "/v1/chat/completions", "model": "demo", "tls": True,
                   "sni_ok": True, "authority_ok": True, "credential_ok": True}
        response = {"request_id": request_id, "status": 200, "provider": "proof-a", "checks": True}
        self.assertTrue(E2E.one_request_receipt_result(response, [receipt], request_id, "proof-a")["pass"])
        self.assertFalse(E2E.one_request_receipt_result(response, [receipt, receipt], request_id, "proof-a")["pass"])
        wrong_provider = {**receipt, "provider": "proof-b"}
        self.assertFalse(E2E.one_request_receipt_result(response, [wrong_provider], request_id, "proof-a")["pass"])

    def test_partial_baseline_requires_one_attributed_receipt_from_a_healthy_provider(self):
        request_id = "partial-baseline"
        receipt = {"request_id": request_id, "provider": "proof-c", "method": "POST",
                   "path": "/v1/chat/completions", "model": "demo", "tls": True,
                   "sni_ok": True, "authority_ok": True, "credential_ok": True}
        response = {"request_id": request_id, "status": 200, "provider": "proof-c", "checks": True}
        self.assertTrue(E2E.one_request_any_provider_result(
            response, [receipt], request_id, {"proof-a", "proof-c"})["pass"])
        self.assertFalse(E2E.one_request_any_provider_result(
            response, [receipt], request_id, {"proof-a"})["pass"])
        self.assertFalse(E2E.one_request_any_provider_result(
            response, [receipt, receipt], request_id, {"proof-a", "proof-c"})["pass"])


class WithdrawalSequenceTests(unittest.TestCase):
    def test_each_withdrawal_case_has_its_own_serving_baseline_and_restoration(self):
        source = SCRIPT.read_text()
        section = source.split("def qualify_all_zero_withdrawal(initial_uid):", 1)[1].split(
            "\ndef withdraw_all_zero_and_probe", 1)[0]
        required_order = [
            'prove_active_provider_set("all_zero_ready_provider_serving_baseline"',
            'withdraw_all_zero_and_probe(\n        "all_zero_weights_with_ready_providers"',
            'set_weights(all_enabled)',
            'remove_provider_credential(provider_b)',
            'prove_active_provider_set("all_zero_unready_provider_serving_baseline"',
            'withdraw_all_zero_and_probe(\n        "all_zero_weights_with_unready_provider"',
            'apply_provider_credential(provider_b)',
            'set_weights(all_enabled)',
            'kubectl("-n", NAMESPACE, "delete", "externalprovider", provider_b',
            'prove_active_provider_set("all_zero_missing_provider_serving_baseline"',
            'withdraw_all_zero_and_probe(\n        "all_zero_weights_with_missing_provider"',
            'recreate_provider(provider_b)',
            'set_weights(all_enabled)',
        ]
        indices = []
        cursor = 0
        for marker in required_order:
            cursor = section.find(marker, cursor)
            indices.append(cursor)
            if cursor >= 0:
                cursor += len(marker)
        self.assertTrue(all(index >= 0 for index in indices), indices)

    def test_provider_attribution_supports_recorder_and_katan_shapes(self):
        self.assertEqual(E2E.response_provider({"provider": "proof-a"}), "proof-a")
        self.assertEqual(E2E.response_provider({"choices": [{"message": {
            "content": "[echo] host=katan-a-7f9c:8000 time=fixture"}}]}), "provider-a")
        self.assertEqual(E2E.response_provider({"choices": []}), "")

    def test_sample_dispatch_attempts_each_request_once_and_keeps_http_failure(self):
        request_ids = [f"request-{index}" for index in range(20)]
        attempted = []
        diagnostic_options = []

        def one_attempt(request_id, **kwargs):
            attempted.append(request_id)
            diagnostic_options.append(kwargs.get("capture_diagnostics"))
            return {"request_id": request_id, "status": 503 if request_id == "request-7" else 200}

        with patch.object(E2E, "gateway_request", side_effect=one_attempt):
            responses = E2E.sample_gateway_requests(request_ids, capture_diagnostics=True)

        self.assertCountEqual(attempted, request_ids)
        self.assertEqual(len(attempted), len(set(attempted)))
        self.assertEqual(diagnostic_options, [True] * len(request_ids))
        self.assertEqual([item["request_id"] for item in responses], request_ids)
        self.assertEqual(responses[7]["status"], 503)

    def test_rejects_unexpected_active_provider(self):
        state = {"model_ready": False, "route_ready": False,
                 "candidates": [{"stable_id": "demo-model/provider-proof-b",
                                 "cluster": "provider-provider-proof-b"}], "runtime": {}}
        self.assertFalse(E2E.baseline_state_is_ready(state, ["provider-provider-a"]))


class ResetRequestOrderingTests(unittest.TestCase):
    def test_provider_request_waits_for_overlay_and_is_never_retried(self):
        events = []
        with (patch.object(E2E, "MODEL_WAS_MUTATED", True),
              patch.object(E2E, "MODEL_CHANGED", False),
              patch.object(E2E, "MODEL_BEFORE", {"spec": {"externalProviderRefs": [
                  {"ref": {"name": "provider-a"}},
              ]}}),
              patch.object(E2E, "wait_for_provider_a_baseline", side_effect=lambda: events.append(("wait", "a")) or True),
              patch.object(E2E, "gateway_request", side_effect=lambda request_id: events.append(("request", request_id)) or
                           {"status": 503, "provider": "provider-a", "checks": False}),
              patch.object(E2E, "record") as record):
            E2E.verify_provider_a_reset()
        self.assertEqual(events[0], ("wait", "a"))
        self.assertEqual(len([event for event in events if event[0] == "request"]), 1)
        record.assert_called_once()
        self.assertEqual(record.call_args.args[0:2], ("provider_a_baseline_reset", False))
        self.assertEqual(record.call_args.args[2]["http_status"], 503)

    def test_no_provider_request_when_overlay_did_not_converge(self):
        with (patch.object(E2E, "MODEL_WAS_MUTATED", True),
              patch.object(E2E, "MODEL_CHANGED", False),
              patch.object(E2E, "MODEL_BEFORE", {"spec": {"externalProviderRefs": [
                  {"ref": {"name": "provider-a"}},
              ]}}),
              patch.object(E2E, "wait_for_provider_a_baseline", return_value=False),
              patch.object(E2E, "gateway_request") as gateway_request):
            with self.assertRaisesRegex(RuntimeError, "did not converge"):
                E2E.verify_provider_a_reset()
        gateway_request.assert_not_called()


class BackendReceiptCollectionTests(unittest.TestCase):
    def test_filters_observations_and_discards_unneeded_fields(self):
        body = ("{\"request_id\":\"wanted\",\"provider\":\"proof-a\",\"tls\":true,"
                "\"credential_ok\":true,\"message_text\":[\"fixture text\"],"
                "\"authorization_scheme\":\"Bearer\"}\n"
                "{\"request_id\":\"other\",\"provider\":\"proof-a\"}\n")
        receipts = E2E.parse_observation_receipts(body, {"wanted"})
        self.assertEqual(len(receipts), 1)
        self.assertEqual(receipts[0]["request_id"], "wanted")
        self.assertNotIn("message_text", receipts[0])
        self.assertNotIn("authorization_scheme", receipts[0])

    def test_fetch_uses_verified_tls_and_exact_service_hostname(self):
        body = "{\"request_id\":\"wanted\"}\n200"
        with (patch.object(E2E, "PROVIDER_CA_PATH", pathlib.Path("/tmp/provider-ca-fixture.crt")),
              patch.dict(E2E.LOCAL_PORTS, {"provider-proof-a-ext": 59001}),
              patch.object(E2E, "run", return_value=SimpleNamespace(returncode=0, stdout=body, stderr="")) as run):
            self.assertIn("wanted", E2E.fetch_provider_observations("a"))
        command = run.call_args.args[0]
        self.assertIn("--cacert", command)
        self.assertIn("--resolve", command)
        self.assertIn("provider-proof-a-ext.maas-system.svc.cluster.local:59001:127.0.0.1", command)
        self.assertTrue(command[-1].startswith("https://provider-proof-a-ext.maas-system.svc.cluster.local:59001/"))

    def test_receipt_smoke_requires_one_matching_verified_backend_receipt(self):
        receipt = {"provider": "proof-a", "method": "POST", "path": "/v1/chat/completions",
                   "model": "demo", "tls": True, "sni_ok": True,
                   "authority_ok": True, "credential_ok": True}
        with (patch.object(E2E, "gateway_request", return_value={
                  "status": 200, "provider": "proof-a", "checks": True}),
              patch.object(E2E, "backend_receipts", side_effect=lambda ids, _label:
                           [{**receipt, "request_id": ids[0]}]),
              patch.object(E2E, "record") as record):
            E2E.receipt_storage_smoke()
        self.assertTrue(record.call_args.args[1])


if __name__ == "__main__":
    unittest.main()

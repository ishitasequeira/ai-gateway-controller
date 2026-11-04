#!/usr/bin/env python3
"""Qualify shared and ExternalModel API translation through Kind Gateway."""

import datetime
import argparse
import hashlib
import http.client
import io
import json
import os
import pathlib
import re
import secrets
import shutil
import socket
import subprocess
import tarfile
import threading
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
CLUSTER = os.environ.get("LOCAL_ENV_CLUSTER", "external-model-two-plane")
NAMESPACE = "models-as-a-service"
EVIDENCE = pathlib.Path(os.environ.get("LOCAL_ENV_TRANSLATION_EVIDENCE") or (
    ROOT / "evidence" /
    (datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-api-translation")
))
EVIDENCE.mkdir(parents=True, exist_ok=True)
RESULTS = EVIDENCE / "results.json"
IMAGE = os.environ.get("PROVIDER_RECORDER_IMAGE", "provider-recorder:issue28-kind")
ISTIOCTL = os.environ.get("ISTIOCTL", "/tmp/istio-1.27.3/bin/istioctl")
API_PORT = None
GATEWAY_PORT = None
GATEWAY_AUTHORITY = None
API_FORWARD = None
GATEWAY_FORWARD = None
RECORDER_FORWARD = None
RECORDER_PORT = None
PROVIDER_RECORDER_FORWARD = None
PROVIDER_RECORDER_PORT = None
KEY_ID = None
KEY_VALUE = None
PROVIDER_TEST_KEYS = []
PROVIDER_CA_FILE = None
CLEANUP_FAILURES = []
ROUTE_TRACE_FILTER = "ipp-api-translation-route-trace"
ROUTE_TRACE_PREFIX = os.environ.get("IPP_ROUTE_TRACE_PREFIX") or (
    "ipp-route-trace-"
    + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S")
    + "-"
    + secrets.token_hex(6)
    + "-"
)
LIVE_PROVIDER_RESOURCES = []
LIVE_PROVIDER_SELECTION = None
MTC_ORIGINAL = None
MTC_HAD_TRANSLATION = False
MTC_CAPTURED = False
EXTERNAL_MODEL_ORIGINAL_REFS = None
EXTERNAL_MODEL_ORIGINAL_OVERLAY = None
EXTERNAL_MODEL_CHANGED = False
ANTHROPIC_PROVIDER_CREATED = False
PROVIDER_AWARE_CREATED = False
DIAGNOSTIC_ROUTE_AUTH_CREATED = False
TEMP = pathlib.Path(tempfile.mkdtemp(prefix="ipp-api-translation-"))
RESULT = {"status": "RUNNING", "assertions": [], "requests": [], "upstream_observations": []}


def run(args, *, input_text=None, check=True, timeout=90, env=None):
    proc = subprocess.run(args, input=input_text, text=True, encoding="utf-8", errors="replace",
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, env=env)
    if check and proc.returncode:
        detail = proc.stderr[-1200:].replace(KEY_VALUE or "\0", "[REDACTED]")
        raise RuntimeError(f"command failed rc={proc.returncode}: {' '.join(args)}: {detail}")
    return proc


def kubectl(*args, **kwargs):
    kwargs.setdefault("timeout", 180)
    return run(["kubectl", "--context", f"kind-{CLUSTER}", *args], **kwargs)


def save_json(path, value):
    pathlib.Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def redact_text(value):
    redacted = value.replace(KEY_VALUE or "\0", "[REDACTED]")
    for key in PROVIDER_TEST_KEYS:
        redacted = redacted.replace(key, "[REDACTED]")
    return re.sub(r"(?i)(bearer\s+)[A-Za-z0-9._~-]{12,}", r"\1[REDACTED]", redacted)


def record(name, passed, details):
    item = {"name": name, "result": "PASS" if passed else "FAIL", "details": details}
    RESULT["assertions"].append(item)
    save_json(RESULTS, RESULT)
    print(f"{item['result']} {name}: {details}")
    if not passed:
        raise RuntimeError(f"assertion failed: {name}")


def openai_message_content(payload):
    choices = payload.get("choices") if isinstance(payload, dict) else None
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        return None
    message = choices[0].get("message")
    return message.get("content") if isinstance(message, dict) else None


def local_image_config_id(image):
    try:
        saved = subprocess.run(
            ["docker", "image", "save", "--platform", "linux/amd64", image],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=180,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if saved.returncode != 0:
        return None
    try:
        with tarfile.open(fileobj=io.BytesIO(saved.stdout), mode="r:") as archive:
            manifest = json.load(archive.extractfile("manifest.json"))
            config_path = manifest[0]["Config"]
            config_blob = archive.extractfile(config_path).read()
        config_name = pathlib.PurePosixPath(config_path).name
    except (AttributeError, KeyError, IndexError, OSError, TypeError, tarfile.TarError, json.JSONDecodeError):
        return None
    declared_digest = config_name[:-5] if config_name.endswith(".json") else config_name
    if not re.fullmatch(r"[0-9a-f]{64}", declared_digest):
        return None
    actual_digest = hashlib.sha256(config_blob).hexdigest()
    if actual_digest != declared_digest:
        return None
    return f"sha256:{actual_digest}"


def verify_controller_image():
    deployment = json.loads(kubectl(
        "-n", "opendatahub", "get", "deployment", "ai-gateway-controller", "-o", "json").stdout)
    managers = [container for container in deployment.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
                if container.get("name") == "manager"]
    image = managers[0].get("image", "") if len(managers) == 1 else ""
    local = run(["docker", "image", "inspect", image, "--format", "{{.Id}}"], check=False) if image else None
    local_image_id = local.stdout.strip() if local and local.returncode == 0 else ""
    local_config_id = local_image_config_id(image) if image else None
    cri_reference = image if "/" in image else f"docker.io/library/{image}"
    selector = deployment.get("spec", {}).get("selector", {}).get("matchLabels", {})
    selector_text = ",".join(f"{key}={value}" for key, value in sorted(selector.items()))
    pods = json.loads(kubectl(
        "-n", "opendatahub", "get", "pods", "-l", selector_text, "-o", "json").stdout) if selector_text else {"items": []}
    ready_pods = []
    for pod in pods.get("items", []):
        if pod.get("status", {}).get("phase") != "Running":
            continue
        for status in pod.get("status", {}).get("containerStatuses", []):
            if status.get("name") == "manager" and status.get("ready") is True:
                ready_pods.append({
                    "name": pod.get("metadata", {}).get("name"),
                    "node": pod.get("spec", {}).get("nodeName"),
                    "image": status.get("image"),
                    "pod_imageID": status.get("imageID"),
                })
    for pod in ready_pods:
        node = pod.get("node")
        inspected = run(["docker", "exec", node or "", "crictl", "inspecti", cri_reference], check=False) if node else None
        try:
            runtime = json.loads(inspected.stdout) if inspected and inspected.returncode == 0 else {}
        except json.JSONDecodeError:
            runtime = {}
        runtime_status = runtime.get("status", {})
        pod["runtime_imageID"] = runtime_status.get("id")
        pod["repoTags"] = runtime_status.get("repoTags", [])
        pod["requested_tag_present"] = cri_reference in pod["repoTags"]
    desired = deployment.get("spec", {}).get("replicas", 1)
    matched = (
        len(managers) == 1 and bool(local_image_id) and bool(selector_text)
        and bool(local_config_id)
        and len(ready_pods) == desired
        and all(pod.get("requested_tag_present") is True
                and str(pod.get("runtime_imageID", "")).removeprefix("containerd://")
                == str(pod.get("pod_imageID", "")).removeprefix("containerd://")
                == local_config_id
                for pod in ready_pods)
    )
    record("ai_gateway_controller_exact_image_loaded", matched, {
        "deployment_image": image,
        "local_docker_image_id": local_image_id or None,
        "local_image_config_id": local_config_id,
        "runtime_image_reference": cri_reference,
        "desired_replicas": desired,
        "ready_pods": ready_pods,
    })


def verify_externalmodel_extproc_image():
    image = os.environ.get("PRAXIS_EXTPROC_IMAGE", "")
    deployment = json.loads(kubectl(
        "-n", NAMESPACE, "get", "deployment", "payload-processing-external-model", "-o", "json").stdout)
    containers = deployment.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
    extprocs = [container for container in containers if container.get("name") == "payload-processing"]
    local = run(["docker", "image", "inspect", image, "--format", "{{.Id}}"], check=False) if image else None
    local_image_id = local.stdout.strip() if local and local.returncode == 0 else ""
    local_config_id = local_image_config_id(image) if image else None
    cri_reference = image if "/" in image else f"docker.io/library/{image}"
    selector = deployment.get("spec", {}).get("selector", {}).get("matchLabels", {})
    selector_text = ",".join(f"{key}={value}" for key, value in sorted(selector.items()))
    pods = json.loads(kubectl(
        "-n", NAMESPACE, "get", "pods", "-l", selector_text, "-o", "json").stdout) if selector_text else {"items": []}
    desired = deployment.get("spec", {}).get("replicas", 1)
    ready_pods = []
    for pod in pods.get("items", []):
        if pod.get("metadata", {}).get("deletionTimestamp") or pod.get("status", {}).get("phase") != "Running":
            continue
        status = next((item for item in pod.get("status", {}).get("containerStatuses", [])
                       if item.get("name") == "payload-processing"), {})
        if status.get("ready") is not True:
            continue
        identity = {
            "name": pod.get("metadata", {}).get("name"),
            "node": pod.get("spec", {}).get("nodeName"),
            "image": status.get("image"),
            "pod_imageID": status.get("imageID"),
        }
        node = identity["node"]
        inspected = run(["docker", "exec", node or "", "crictl", "inspecti", cri_reference],
                        check=False) if node else None
        try:
            runtime = json.loads(inspected.stdout) if inspected and inspected.returncode == 0 else {}
        except json.JSONDecodeError:
            runtime = {}
        runtime_status = runtime.get("status", {})
        identity["runtime_imageID"] = runtime_status.get("id")
        identity["repoTags"] = runtime_status.get("repoTags", [])
        identity["requested_tag_present"] = cri_reference in identity["repoTags"]
        ready_pods.append(identity)

    matched = (
        bool(image) and len(extprocs) == 1 and extprocs[0].get("image") == image
        and bool(local_image_id) and bool(local_config_id) and desired > 0 and len(ready_pods) == desired
        and all(pod.get("requested_tag_present") is True
                and str(pod.get("runtime_imageID", "")).removeprefix("containerd://")
                == str(pod.get("pod_imageID", "")).removeprefix("containerd://")
                == local_config_id
                for pod in ready_pods)
    )
    record("externalmodel_exact_extproc_image_loaded", matched, {
        "requested_image": image or None,
        "local_docker_image_id": local_image_id or None,
        "local_image_config_id": local_config_id,
        "desired_replicas": desired,
        "ready_serving_pods": ready_pods,
        "source_sha": os.environ.get("PRAXIS_EXTPROC_SOURCE_SHA"),
        "source_fingerprint": os.environ.get("PRAXIS_EXTPROC_SOURCE_FINGERPRINT"),
        "praxis_ai_sha": os.environ.get("PRAXIS_AI_SOURCE_SHA"),
        "praxis_ai_fingerprint": os.environ.get("PRAXIS_AI_SOURCE_FINGERPRINT"),
        "fips_features": os.environ.get("PRAXIS_EXTPROC_FIPS_FEATURES"),
    })


def free_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def start_port_forward(service, remote_port, namespace="maas-system"):
    log_path = EVIDENCE / f"port-forward-{namespace}-{service}.log"
    log_stream = log_path.open("w")
    proc = subprocess.Popen(
        ["kubectl", "--context", f"kind-{CLUSTER}", "-n", namespace, "port-forward",
         f"svc/{service}", f"0:{remote_port}"],
        stdout=log_stream,
        stderr=subprocess.STDOUT,
        text=True,
    )
    log_stream.close()
    port = None
    for _ in range(90):
        if proc.poll() is not None:
            raise RuntimeError(f"port-forward for {service} exited early; see {log_path.name}")
        match = re.search(r"Forwarding from 127\.0\.0\.1:(\d+) ->", log_path.read_text())
        if match:
            port = int(match.group(1))
            break
        time.sleep(0.1)
    if port is None:
        proc.terminate()
        raise RuntimeError(f"port-forward for {service} did not report a bound local port; see {log_path.name}")
    sock = socket.socket()
    try:
        sock.settimeout(0.25)
        sock.connect(("127.0.0.1", port))
    except OSError as exc:
        proc.terminate()
        raise RuntimeError(f"port-forward for {service} reported a port that is not listening; see {log_path.name}") from exc
    finally:
        sock.close()
    return proc, port


def refresh_gateway_port_forward(label):
    """Rebind the run-owned Gateway tunnel after a Gateway pod rollout."""
    global GATEWAY_FORWARD, GATEWAY_PORT
    if GATEWAY_FORWARD and GATEWAY_FORWARD.poll() is None:
        GATEWAY_FORWARD.terminate()
        try:
            GATEWAY_FORWARD.wait(timeout=5)
        except subprocess.TimeoutExpired:
            GATEWAY_FORWARD.kill()
            GATEWAY_FORWARD.wait(timeout=5)
    GATEWAY_FORWARD, GATEWAY_PORT = start_port_forward("maas-default-gateway-istio", 80)
    save_json(EVIDENCE / f"gateway-port-forward-{label}.json", {
        "service": "maas-system/maas-default-gateway-istio",
        "remote_port": 80,
        "local_port_allocated": True,
        "label": label,
    })
    return GATEWAY_PORT


def curl_request(name, url, body, *, auth_file=None, extra_headers=None, ca_file=None,
                 resolve_host=None, stream=False, correlation_id=None,
                 persist_response_payload=True):
    raw = TEMP / f"{name}.body"
    err = TEMP / f"{name}.stderr"
    response_headers = TEMP / f"{name}.headers"
    args = ["curl", "--noproxy", "*", "-sS", "--connect-timeout", "5", "--max-time", "40",
            "-D", str(response_headers), "-o", str(raw), "-w", "%{http_code}",
            "-H", "content-type: application/json"]
    if stream:
        args[args.index("-sS")] = "--no-buffer"
        args.insert(args.index("--connect-timeout"), "-sS")
    if auth_file:
        args.extend(["--config", str(auth_file)])
    if ca_file:
        args.extend(["--cacert", str(ca_file)])
    if resolve_host:
        args.extend(["--resolve", resolve_host])
    request_headers = list(extra_headers or [])
    parsed_url = urllib.parse.urlsplit(url)
    if (GATEWAY_AUTHORITY and parsed_url.hostname == "127.0.0.1"
            and parsed_url.port == GATEWAY_PORT
            and not any(header.split(":", 1)[0].strip().lower() == "host"
                        for header in request_headers)):
        request_headers.append(f"Host: {GATEWAY_AUTHORITY}")
    if correlation_id:
        request_headers.append(f"x-request-id: {correlation_id}")
    for header in request_headers:
        args.extend(["-H", header])
    payload = json.dumps(body, separators=(",", ":"))
    if len(payload) > 64 * 1024:
        payload_file = TEMP / f"{name}.request.json"
        payload_file.write_text(payload)
        args.extend(["--data-binary", f"@{payload_file}", url])
    else:
        args.extend(["--data-binary", payload, url])
    proc = subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=err.open("w"), timeout=50)
    status = proc.stdout.strip()[-3:] if proc.stdout.strip() else "000"
    response_body = raw.read_bytes() if raw.exists() else b""
    response_text = redact_text(response_body.decode(errors="replace"))
    try:
        response_record = json.loads(response_text)
    except json.JSONDecodeError:
        response_record = {"body_text": response_text[:4096]}
    if not persist_response_payload:
        if isinstance(response_record, dict):
            choices = response_record.get("choices", [])
            usage = response_record.get("usage", {})
            response_record = {
                "response_payload_saved": False,
                "json_fields": sorted(response_record),
                "model": response_record.get("model"),
                "choices_count": len(choices) if isinstance(choices, list) else None,
                "usage_fields": sorted(usage) if isinstance(usage, dict) else [],
            }
        else:
            response_record = {"response_payload_saved": False, "json_decoded": False}
    safe_response_headers = {}
    if response_headers.exists():
        for line in response_headers.read_text(errors="replace").splitlines():
            if ":" not in line:
                continue
            header_name, value = line.split(":", 1)
            header_name = header_name.strip().lower()
            if header_name.startswith("x-ipp-authdiag-") or header_name in {
                "content-type", "x-ext-auth-reason", "www-authenticate", "server",
                "x-ipp-authdiag-authorization", "x-ipp-authdiag-identity",
                "x-ipp-authdiag-api-key-validation",
            }:
                safe_response_headers[header_name] = redact_text(value.strip())[:512]
    save_json(EVIDENCE / f"{name}-response.json", {
        "http_status": status,
        "curl_exit_status": proc.returncode,
        "body": response_record,
        "safe_response_headers": safe_response_headers,
    })
    safe_headers = {}
    if response_headers.exists():
        for line in response_headers.read_text(errors="replace").splitlines():
            header, separator, value = line.partition(":")
            normalized_header = header.lower().strip()
            if separator and (
                normalized_header.startswith("x-ipp-authdiag-")
                or normalized_header in {
                    "content-type", "server", "www-authenticate", "x-request-id",
                    "x-envoy-response-code-details", "x-envoy-upstream-service-time",
                    "x-ext-auth-reason", "x-authz-reason",
                }
            ):
                safe_headers[normalized_header] = value.strip()
    save_json(EVIDENCE / f"{name}-response-headers.json", safe_headers)
    RESULT["requests"].append({"name": name, "http_status": status, "curl_exit_status": proc.returncode,
                               "path": url.split("?", 1)[0].rsplit("/", 1)[-1]})
    save_json(RESULTS, RESULT)
    return proc.returncode, status, response_body


def progressive_stream_request(name, url, body, *, extra_headers=None, record_result=True,
                               on_first_event=None, request_timeout=15,
                               persist_first_event_payload=True):
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != "http" or not parsed.hostname or not parsed.port:
        raise RuntimeError(f"progressive stream test requires a local HTTP URL: {url}")
    connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=request_timeout)
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {KEY_VALUE}"}
    if GATEWAY_AUTHORITY and parsed.hostname == "127.0.0.1" and parsed.port == GATEWAY_PORT:
        headers["Host"] = GATEWAY_AUTHORITY
    for header in extra_headers or []:
        key, separator, value = header.partition(":")
        if not separator:
            raise RuntimeError(f"invalid test header for {name}")
        headers[key.strip()] = value.strip()
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    started = time.monotonic()
    try:
        connection.request("POST", path, body=json.dumps(body, separators=(",", ":")), headers=headers)
        response = connection.getresponse()
        first_event = []
        while True:
            line = response.readline()
            if not line:
                break
            first_event.append(line)
            if line in (b"\n", b"\r\n"):
                break
        first_event_ms = (time.monotonic() - started) * 1000
        if on_first_event is not None:
            on_first_event()
        response_body = b"".join(first_event) + response.read()
        total_ms = (time.monotonic() - started) * 1000
        status = str(response.status)
        content_type = response.getheader("Content-Type", "")
    finally:
        connection.close()
    if record_result:
        RESULT["requests"].append({"name": name, "http_status": status, "curl_exit_status": 0,
                                   "path": path.split("?", 1)[0].rsplit("/", 1)[-1],
                                   "first_event_latency_ms": round(first_event_ms, 1),
                                   "total_response_ms": round(total_ms, 1)})
    save_json(EVIDENCE / f"{name}-progressive-response.json", {
        "http_status": status,
        "content_type": content_type,
        "first_event_latency_ms": round(first_event_ms, 1),
        "total_response_ms": round(total_ms, 1),
        "first_event": (redact_text(b"".join(first_event).decode(errors="replace"))
                        if persist_first_event_payload else None),
        "first_event_payload_saved": persist_first_event_payload,
        "first_event_bytes": len(b"".join(first_event)),
        "body_bytes": len(response_body),
    })
    if record_result:
        save_json(RESULTS, RESULT)
    return 0, status, response_body, first_event_ms, total_ms, content_type


def cancel_stream_request(url, body, *, extra_headers=None):
    parsed = urllib.parse.urlsplit(url)
    connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=15)
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {KEY_VALUE}"}
    if GATEWAY_AUTHORITY and parsed.hostname == "127.0.0.1" and parsed.port == GATEWAY_PORT:
        headers["Host"] = GATEWAY_AUTHORITY
    for header in extra_headers or []:
        key, separator, value = header.partition(":")
        if not separator:
            raise RuntimeError("invalid test header for canceled stream")
        headers[key.strip()] = value.strip()
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    connection.request("POST", path, body=json.dumps(body, separators=(",", ":")), headers=headers)
    response = connection.getresponse()
    first_event = []
    while True:
        line = response.readline()
        if not line:
            break
        first_event.append(line)
        if line in (b"\n", b"\r\n"):
            break
    status = str(response.status)
    connection.close()
    return status, b"".join(first_event)


def wait_for_cancelled_stream_drain():
    """Allow Envoy to finish draining the canceled upstream stream.

    The provider-aware fixture deliberately emits delayed SSE chunks. Closing
    the downstream connection after the first event does not synchronously
    complete Envoy's upstream cancellation. A subsequent request issued in
    that small window can be rejected by the buffered pre-auth processor with
    no body bytes available. This is a bounded synchronization gate, not a
    retry of the canceled or subsequent HTTP response.
    """
    drain_seconds = 3
    time.sleep(drain_seconds)
    return drain_seconds


def wait_for_externalmodel_route(stable_id, path, label):
    """Wait for Envoy to receive the selected ExternalModel route.

    ExtProc's serving revision and Envoy's route snapshot converge through
    separate control-plane updates.  A request is not safe to issue until the
    route containing the selected-provider match is present in the current
    Gateway proxy configuration as well.
    """
    last = ""
    last_pod = None

    def ready_gateway_pod():
        pods = json.loads(kubectl(
            "-n", "maas-system", "get", "pods", "-l",
            "gateway.networking.k8s.io/gateway-name=maas-default-gateway", "-o", "json").stdout)
        return next((pod["metadata"]["name"] for pod in pods.get("items", [])
                     if not pod.get("metadata", {}).get("deletionTimestamp")
                     and any(condition.get("type") == "Ready" and condition.get("status") == "True"
                             for condition in pod.get("status", {}).get("conditions", []))), None)

    for _ in range(90):
        last_pod = ready_gateway_pod()
        if last_pod:
            result = run([ISTIOCTL, "--context", f"kind-{CLUSTER}", "proxy-config", "routes",
                          last_pod, "-n", "maas-system", "-o", "json"],
                         check=False, timeout=90)
            last = result.stdout or result.stderr
            if result.returncode == 0:
                try:
                    route_config = json.loads(result.stdout)
                except json.JSONDecodeError:
                    route_config = None

                def selected_route(node):
                    if isinstance(node, dict):
                        match = node.get("match", {})
                        headers = match.get("headers", [])
                        candidate_match = any(
                            header.get("name", "").lower() == "x-ai-routing-candidate"
                            and (header.get("string_match", header.get("stringMatch", {}))
                                 .get("exact") == stable_id)
                            for header in headers
                        )
                        path_match = (
                            match.get("path") == path
                            or match.get("path_separated_prefix") == path
                            or match.get("pathSeparatedPrefix") == path
                        )
                        if candidate_match and path_match:
                            return True
                        return any(selected_route(value) for value in node.values())
                    if isinstance(node, list):
                        return any(selected_route(value) for value in node)
                    return False

                if selected_route(route_config):
                    save_json(EVIDENCE / f"envoy-externalmodel-route-ready-{label}.json", {
                        "gateway_pod": last_pod,
                        "selected_candidate": stable_id,
                        "path": path,
                        "route_present": True,
                    })
                    return
        time.sleep(1)

    (EVIDENCE / f"envoy-externalmodel-route-{label}-not-ready.json").write_text(last)
    save_json(EVIDENCE / f"envoy-externalmodel-route-ready-{label}.json", {
        "gateway_pod": last_pod,
        "selected_candidate": stable_id,
        "path": path,
        "route_present": False,
    })
    raise RuntimeError(f"Envoy did not receive selected ExternalModel route: {label}")


def wait_route(name):
    for _ in range(90):
        route = kubectl("-n", NAMESPACE, "get", "httproute", name, "-o", "json", check=False)
        if route.returncode == 0:
            obj = json.loads(route.stdout)
            save_json(EVIDENCE / f"httproute-{name}-last-observed.json", obj)
            conditions = [c for p in obj.get("status", {}).get("parents", [])
                          for c in p.get("conditions", [])]
            accepted = any(c.get("type") == "Accepted" and c.get("status") == "True" for c in conditions)
            resolved = any(c.get("type") == "ResolvedRefs" and c.get("status") == "True" for c in conditions)
            if accepted and resolved:
                return obj
        else:
            (EVIDENCE / f"httproute-{name}-last-not-found.txt").write_text(
                f"observed_at={datetime.datetime.now(datetime.timezone.utc).isoformat()}\n{route.stderr}")
        time.sleep(1)
    route = kubectl("-n", NAMESPACE, "get", "httproute", name, "-o", "json", check=False)
    if route.returncode == 0:
        obj = json.loads(route.stdout)
        save_json(EVIDENCE / f"httproute-{name}-not-ready.json", obj)
        conditions = [
            {"parent": parent.get("parentRef", {}).get("name"), "type": condition.get("type"),
             "status": condition.get("status"), "reason": condition.get("reason"),
             "message": condition.get("message")}
            for parent in obj.get("status", {}).get("parents", [])
            for condition in parent.get("conditions", [])
        ]
    else:
        conditions = []
        (EVIDENCE / f"httproute-{name}-get-error.txt").write_text(route.stderr[-2000:])
    gateway = kubectl("-n", "maas-system", "get", "gateway", "maas-default-gateway", "-o", "json", check=False)
    if gateway.returncode == 0:
        save_json(EVIDENCE / f"gateway-for-{name}-not-ready.json", json.loads(gateway.stdout))
    raise RuntimeError(f"HTTPRoute {name} did not become Accepted and ResolvedRefs; conditions={conditions}")


def wait_auth_policy(name, namespace=NAMESPACE):
    for _ in range(90):
        policy = kubectl("-n", namespace, "get", "authpolicy", name, "-o", "json", check=False)
        if policy.returncode == 0:
            obj = json.loads(policy.stdout)
            conditions = obj.get("status", {}).get("conditions", [])
            accepted = any(c.get("type") == "Accepted" and c.get("status") == "True"
                           for c in conditions)
            enforced = any(c.get("type") == "Enforced" and c.get("status") == "True"
                           for c in conditions)
            if accepted and enforced:
                return obj
        time.sleep(1)
    obj = json.loads(kubectl("-n", namespace, "get", "authpolicy", name, "-o", "json").stdout)
    save_json(EVIDENCE / f"authpolicy-{namespace}-{name}-not-enforced.json", obj)
    raise RuntimeError(f"AuthPolicy {namespace}/{name} did not report Accepted=True and Enforced=True")


def wait_maas_auth_policy_ready(name="kind-e2e-access", namespace=NAMESPACE, not_before=None):
    """Wait for current-generation Ready to remain stable after routes are installed.

    The MaaSAuthPolicy generation does not change when route reconciliation
    materializes the associated Authorino AuthConfigs. A fresh Ready
    transition is therefore not a valid proxy for those AuthConfig syncs.
    The caller separately waits for the Gateway AuthPolicy to be Accepted and
    Enforced. Require the controller's explicit non-conflict condition so a
    Ready=True policy cannot mask a route AuthPolicy that overrides it.
    """
    last = None
    stable_ready_polls = 0
    poll_history = []
    min_transition = not_before.timestamp() if not_before is not None else None
    for _ in range(180):
        policy = kubectl("-n", namespace, "get", "maasauthpolicy", name, "-o", "json", check=False)
        if policy.returncode == 0:
            last = json.loads(policy.stdout)
            conditions = last.get("status", {}).get("conditions", [])
            generation = last.get("metadata", {}).get("generation")
            ready_condition = next((condition for condition in conditions
                                    if condition.get("type") == "Ready"), None)
            conflict_condition = next((condition for condition in conditions
                                       if condition.get("type") == "ConflictingAuthPolicy"), None)
            transition_time = None
            if ready_condition and ready_condition.get("lastTransitionTime"):
                try:
                    transition_time = datetime.datetime.fromisoformat(
                        ready_condition["lastTransitionTime"].replace("Z", "+00:00")
                    ).timestamp()
                except ValueError:
                    transition_time = None
            current_generation = (ready_condition is not None
                                  and ready_condition.get("observedGeneration") in (None, generation))
            fresh_transition = min_transition is None or (
                transition_time is not None and transition_time >= min_transition
            )
            ready_now = (ready_condition is not None
                         and ready_condition.get("status") == "True"
                         and current_generation)
            no_conflict = (conflict_condition is not None
                           and conflict_condition.get("status") == "False"
                           and conflict_condition.get("observedGeneration") in (None, generation))
            ready_now = ready_now and no_conflict
            stable_ready_polls = stable_ready_polls + 1 if ready_now else 0
            poll_history.append({
                "observed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "generation": generation,
                "ready_status": ready_condition.get("status") if ready_condition else None,
                "ready_reason": ready_condition.get("reason") if ready_condition else None,
                "ready_message": ready_condition.get("message") if ready_condition else None,
                "ready_observed_generation": ready_condition.get("observedGeneration") if ready_condition else None,
                "ready_transition_time": ready_condition.get("lastTransitionTime") if ready_condition else None,
                "current_generation": current_generation,
                "fresh_transition": fresh_transition,
                "conflicting_auth_policy_status": conflict_condition.get("status") if conflict_condition else None,
                "conflicting_auth_policy_reason": conflict_condition.get("reason") if conflict_condition else None,
                "conflict_condition_current_generation": (
                    conflict_condition.get("observedGeneration") in (None, generation)
                    if conflict_condition else False),
                "no_conflicting_auth_policy": no_conflict,
                "stable_ready_polls": stable_ready_polls,
            })
            if stable_ready_polls >= 3:
                summary = [{key: condition.get(key) for key in
                            ("type", "status", "reason", "message", "observedGeneration")}
                           for condition in conditions]
                evidence = {
                    "generation": generation,
                    "not_before": not_before.isoformat() if not_before is not None else None,
                    "ready_condition_transition_time": ready_condition.get("lastTransitionTime"),
                    "ready_transition_after_route_reconcile": fresh_transition,
                    "conflicting_auth_policy_status": conflict_condition.get("status") if conflict_condition else None,
                    "no_conflicting_auth_policy": no_conflict,
                    "stable_ready_polls": stable_ready_polls,
                    "conditions": summary,
                }
                save_json(EVIDENCE / "maas-auth-policy-readiness-polls.json", poll_history)
                save_json(EVIDENCE / f"maas-auth-policy-{namespace}-{name}-ready.json", evidence)
                return evidence
        time.sleep(1)
    conditions = [{key: condition.get(key) for key in
                   ("type", "status", "reason", "message", "observedGeneration")}
                  for condition in (last or {}).get("status", {}).get("conditions", [])]
    save_json(EVIDENCE / f"maas-auth-policy-{namespace}-{name}-not-ready.json", {
        "not_before": not_before.isoformat() if not_before is not None else None,
        "conditions": conditions,
    })
    save_json(EVIDENCE / "maas-auth-policy-readiness-polls.json", poll_history)
    raise RuntimeError(f"MaaSAuthPolicy {namespace}/{name} did not report stable current-generation Ready=True; conditions={conditions}")


def route_target_auth_policies(route_name, route_namespace=NAMESPACE):
    """List route-targeted AuthPolicies without retaining policy expressions."""
    result = kubectl("get", "authpolicy", "-A", "-o", "json", check=False)
    snapshot = {"exit_status": result.returncode, "route": f"{route_namespace}/{route_name}",
                "policies": []}
    if result.returncode != 0:
        return snapshot
    for item in json.loads(result.stdout).get("items", []):
        target = item.get("spec", {}).get("targetRef", {})
        if (target.get("kind") != "HTTPRoute" or target.get("name") != route_name
                or item.get("metadata", {}).get("namespace") != route_namespace):
            continue
        conditions = item.get("status", {}).get("conditions", [])
        snapshot["policies"].append({
            "namespace": item.get("metadata", {}).get("namespace"),
            "name": item.get("metadata", {}).get("name"),
            "generation": item.get("metadata", {}).get("generation"),
            "target": {key: target.get(key) for key in ("group", "kind", "name")},
            "condition_statuses": [{key: condition.get(key) for key in
                                    ("type", "status", "reason", "observedGeneration")}
                                   for condition in conditions],
        })
    return snapshot


def wait_rate_limit_policy(name):
    for _ in range(90):
        policy = kubectl("-n", NAMESPACE, "get", "tokenratelimitpolicy", name, "-o", "json", check=False)
        if policy.returncode == 0:
            obj = json.loads(policy.stdout)
            conditions = obj.get("status", {}).get("conditions", [])
            accepted = any(c.get("type") == "Accepted" and c.get("status") == "True"
                           for c in conditions)
            enforced = any(c.get("type") == "Enforced" and c.get("status") == "True"
                           for c in conditions)
            if accepted and enforced:
                return obj
        time.sleep(1)
    obj = json.loads(kubectl("-n", NAMESPACE, "get", "tokenratelimitpolicy", name, "-o", "json").stdout)
    save_json(EVIDENCE / f"rate-limit-policy-{name}-not-enforced.json", obj)
    raise RuntimeError(f"TokenRateLimitPolicy {name} did not report Accepted=True and Enforced=True")


def observations():
    if RECORDER_PORT is None:
        return []
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{RECORDER_PORT}/__test/observations", timeout=5) as response:
            body = response.read().decode()
    except (OSError, urllib.error.URLError):
        return []
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def provider_aware_observations():
    if PROVIDER_RECORDER_PORT is None or PROVIDER_CA_FILE is None:
        return []
    host = "provider-translation-ext.maas-system.svc.cluster.local"
    resolve = f"{host}:{PROVIDER_RECORDER_PORT}:127.0.0.1"
    result = run([
        "curl", "--noproxy", "*", "-sS", "--connect-timeout", "5", "--max-time", "10",
        "--cacert", str(PROVIDER_CA_FILE), "--resolve", resolve,
        f"https://{host}:{PROVIDER_RECORDER_PORT}/__test/observations",
    ], check=False, timeout=12)
    if result.returncode:
        return []
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


def wait_provider_observation(predicate, label):
    for _ in range(80):
        for item in reversed(provider_aware_observations()):
            if predicate(item):
                RESULT["upstream_observations"].append(item)
                save_json(RESULTS, RESULT)
                return item
        time.sleep(0.25)
    raise RuntimeError(f"provider-aware mock did not observe {label}")


def create_test_secret(namespace, name, value):
    path = TEMP / f"{namespace}-{name}.secret"
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "w") as secret_file:
        secret_file.write(value)
    generated = kubectl("-n", namespace, "create", "secret", "generic", name,
                        f"--from-file=api-key={path}", "--dry-run=client", "-o", "yaml").stdout
    kubectl("-n", namespace, "apply", "-f", "-", input_text=generated)
    path.unlink(missing_ok=True)


def private_live_credential(provider):
    """Read one authorized live key without logging or saving the value."""
    key_file = pathlib.Path.home() / "rhoai" / "provider-keys.md"
    if not key_file.is_file():
        return None
    source = key_file.read_text(errors="replace")
    if provider == "anthropic":
        pattern = r"(?<![A-Za-z0-9])sk-ant-[A-Za-z0-9_-]{20,}"
    elif provider == "openai":
        pattern = r"(?<![A-Za-z0-9])sk-(?!ant-)[A-Za-z0-9_-]{20,}"
    elif provider == "bedrock":
        pattern = r"(?im)^\s*(?:[-*]\s*)?bedrock\s*[-:]\s*`?(ABSK[A-Za-z0-9+/=_-]{20,})`?\s*$"
    else:
        return None
    matches = re.findall(pattern, source)
    return matches[0] if len(matches) == 1 else None


def append_live_provider_result(provider, status, details):
    result = {"provider": provider, "status": status, "details": details}
    RESULT.setdefault("live_provider_results", []).append(result)
    save_json(RESULTS, RESULT)
    print(f"LIVE {status} {provider}: {details.get('reason', 'qualification completed')}")


def live_response_model_matches(provider, configured_model, response_model, client_model):
    """Check the provider-specific OpenAI response model contract."""
    if provider == "anthropic":
        # This translator deliberately exposes the client-visible model alias
        # in Chat Completions JSON/SSE, while the selected target model is sent
        # to Anthropic upstream.
        return response_model == client_model
    if provider == "bedrock":
        return response_model == configured_model
    if provider != "openai":
        return False
    if response_model == configured_model:
        return True
    if not isinstance(response_model, str):
        return False
    return re.fullmatch(re.escape(configured_model) + r"-\d{4}-\d{2}-\d{2}", response_model) is not None


def wait_live_provider_ready(name):
    last = None
    for _ in range(120):
        response = kubectl("-n", NAMESPACE, "get", "externalprovider", name, "-o", "json", check=False)
        if response.returncode == 0:
            last = json.loads(response.stdout)
            generation = last.get("metadata", {}).get("generation")
            ready_condition = next((condition for condition in last.get("status", {}).get("conditions", [])
                                    if condition.get("type") == "Ready"), {})
            if (last.get("status", {}).get("phase") == "Ready"
                    and last.get("status", {}).get("observedGeneration") == generation
                    and ready_condition.get("status") == "True"):
                return last
        time.sleep(1)
    save_json(EVIDENCE / f"live-externalprovider-{name}-not-ready.json", last or {})
    raise RuntimeError("live ExternalProvider did not become Ready")


def qualify_live_provider(provider, auth_file, gateway_auth_context=None):
    """Exercise a selected live provider only through the deployed Gateway."""
    global EXTERNAL_MODEL_CHANGED
    credential = private_live_credential(provider)
    if credential is None:
        append_live_provider_result(provider, "SKIPPED", {
            "reason": "the authorized key file did not contain exactly one recognized provider credential",
            "direct_provider_request": False,
        })
        return

    PROVIDER_TEST_KEYS.append(credential)
    if not PROVIDER_AWARE_CREATED:
        raise RuntimeError("live provider fixture requires the run-owned deterministic provider resources")
    provider_name = ("api-translation-bedrock-mantle" if provider == "bedrock"
                     else f"api-translation-{provider}")
    provider_type = "bedrock-openai" if provider == "bedrock" else provider
    live_secret_name = f"api-translation-live-{provider}-credentials"
    existing_secret = kubectl("-n", NAMESPACE, "get", "secret", live_secret_name, check=False)
    if existing_secret.returncode == 0:
        raise RuntimeError("run-owned live provider Secret already exists")
    existing_provider = kubectl("-n", NAMESPACE, "get", "externalprovider", provider_name, "-o", "json",
                                check=False)
    if existing_provider.returncode:
        raise RuntimeError("run-owned deterministic ExternalProvider fixture is missing")
    provider_before = json.loads(existing_provider.stdout)
    expected_mock_endpoint = {
        "anthropic": "provider-translation-ext.maas-system.svc.cluster.local",
        "openai": "provider-translation-openai-ext.maas-system.svc.cluster.local",
        "bedrock": "provider-translation-bedrock-ext.maas-system.svc.cluster.local",
    }[provider]
    if (provider_before.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/managed-by")
            != "api-translation-kind-e2e"
            or provider_before.get("spec", {}).get("provider") != provider_type
            or provider_before.get("spec", {}).get("endpoint") != expected_mock_endpoint):
        raise RuntimeError("refusing to replace an ExternalProvider outside this run's mock fixture")
    save_json(EVIDENCE / f"live-{provider}-provider-fixture-before.json", {
        "provider_name": provider_name,
        "provider": provider_before.get("spec", {}).get("provider"),
        "endpoint": provider_before.get("spec", {}).get("endpoint"),
        "credential_secret_reference": provider_before.get("spec", {}).get("auth", {}).get("secretRef", {}).get("name"),
        "fixture_label": provider_before.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/managed-by"),
        "config_fields": sorted(provider_before.get("spec", {}).get("config", {})),
    })
    LIVE_PROVIDER_RESOURCES.append(("secret", live_secret_name))
    create_test_secret(NAMESPACE, live_secret_name, credential)
    endpoint_and_target = {
        "anthropic": ("api.anthropic.com", "claude-haiku-4-5-20251001", "messages", "/v1/messages"),
        "openai": ("api.openai.com", "gpt-4o-mini", "openai-chat", "/v1/chat/completions"),
    }
    if provider == "bedrock":
        region = os.environ.get("BEDROCK_MANTLE_REGION", "us-east-1").strip()
        model = os.environ.get("BEDROCK_MANTLE_MODEL", "openai.gpt-oss-20b").strip()
        if not re.fullmatch(r"[a-z0-9-]+", region):
            raise RuntimeError("BEDROCK_MANTLE_REGION must contain only lowercase letters, digits, and hyphens")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", model):
            raise RuntimeError("BEDROCK_MANTLE_MODEL has an invalid model identifier")
        endpoint_and_target["bedrock"] = (
            f"bedrock-mantle.{region}.api.aws", model, "openai-chat", "/v1/chat/completions")
    endpoint, target_model, api_format, path = endpoint_and_target[provider]
    client_model = "demo"
    provider_object = {
        "apiVersion": "inference.opendatahub.io/v1alpha1",
        "kind": "ExternalProvider",
        "metadata": {"name": provider_name, "namespace": NAMESPACE,
                     "labels": {"app.kubernetes.io/managed-by": "api-translation-kind-e2e"}},
        "spec": {
            "provider": provider_type,
            "endpoint": endpoint,
            "auth": {"type": "apikey", "secretRef": {"name": live_secret_name}},
        },
    }
    kubectl("-n", NAMESPACE, "apply", "-f", "-", input_text=json.dumps(provider_object))
    updated_provider = wait_live_provider_ready(provider_name)
    updated_spec = updated_provider.get("spec", {})
    updated_secret = updated_spec.get("auth", {}).get("secretRef", {}).get("name")
    if updated_spec.get("endpoint") != endpoint or updated_secret != live_secret_name or updated_spec.get("config"):
        raise RuntimeError("live provider spec did not converge to the requested endpoint and credential reference")
    save_json(EVIDENCE / f"live-{provider}-provider-fixture-active.json", {
        "provider_name": provider_name,
        "provider": updated_spec.get("provider"),
        "endpoint": updated_spec.get("endpoint"),
        "credential_secret_reference": updated_secret,
        "config_fields": sorted(updated_spec.get("config", {})),
        "phase": updated_provider.get("status", {}).get("phase"),
        "observed_generation": updated_provider.get("status", {}).get("observedGeneration"),
        "generation": updated_provider.get("metadata", {}).get("generation"),
    })

    before = overlay_state(f"before-live-{provider}")
    ref = {
        "ref": {"name": provider_name}, "targetModel": target_model,
        "apiFormat": api_format, "path": path, "weight": 1,
    }
    EXTERNAL_MODEL_CHANGED = True
    kubectl("-n", NAMESPACE, "patch", "externalmodel", "demo-model", "--type=merge", "-p",
            json.dumps({"spec": {"externalProviderRefs": [ref]}}))
    runtime = wait_externalmodel_revision({f"provider-{provider_name}"}, f"live-{provider}",
                                           changed_from=before["content_digest"])
    profile = next((candidate for candidate in runtime["demo_model_candidates"]
                    if candidate.get("api_format") == api_format), {})
    profile_evidence = {
        "published_revision": runtime["published_digest"],
        "api_format": profile.get("api_format"),
        "stable_id": profile.get("stable_id"),
        "path": profile.get("path"),
        "target_model": profile.get("target_model"),
        "client_model": client_model,
        "provider_cluster": profile.get("cluster"),
        "endpoint": endpoint,
        "provider_auth_type": "apikey",
        "provider_secret_reference": live_secret_name,
        "request_path": path,
        "request_model": target_model,
        "serving_pods": [pod["name"] for pod in runtime["pods"] if pod["ready"] and not pod["deleting"]],
    }
    save_json(EVIDENCE / f"live-{provider}-serving-profile.json", profile_evidence)
    envoy_label = f"live-{provider}-selected-upstream"
    capture_envoy(envoy_label)
    envoy_route = selected_live_provider_route(envoy_label, profile, endpoint)
    gateway_pod = json.loads((EVIDENCE / "envoy-runtime-route-trace-ready.json").read_text())["gateway_pod"]
    live_authconfig_snapshot = safe_authconfig_snapshot(f"live-{provider}-before-auth-controls")
    if not all(envoy_route.get(field) is True for field in (
        "route_selected_candidate", "path_matches_profile", "authority_matches_endpoint",
        "tls_socket_enabled", "sni_matches_endpoint",
    )):
        append_live_provider_result(provider, "FAIL", {
            **profile_evidence,
            "reason": "effective Envoy provider route did not match the selected path, endpoint authority, TLS, and SNI before live traffic",
            "effective_selected_route": envoy_route,
            "provider_credential_value_saved": False,
            "live_requests_sent": False,
        })
        return

    externalmodel_url = f"http://127.0.0.1:{GATEWAY_PORT}/{NAMESPACE}/demo/v1/chat/completions"
    live_max_token_field = "max_completion_tokens" if provider == "bedrock" else "max_tokens"
    live_max_tokens = 256 if provider == "bedrock" else 16
    json_marker = f"ipp-live-{provider}-json-synthetic"
    json_request_id = f"{ROUTE_TRACE_PREFIX}live-{provider}-json"
    json_request = {
        "model": client_model,
        live_max_token_field: live_max_tokens,
        "messages": [{"role": "user", "content": f"Reply with the word blue. Marker: {json_marker}"}],
    }
    json_callbacks_before = callback_log_snapshot()
    json_rc, json_status, json_raw = curl_request(
        f"live-{provider}-json", externalmodel_url, json_request,
        auth_file=auth_file,
        extra_headers=[f"x-ipp-diagnostic-id: {json_request_id}"],
        correlation_id=json_request_id,
        persist_response_payload=False,
    )
    json_trace = capture_runtime_route_trace(json_request_id, f"live-{provider}-json", gateway_pod)
    json_auth_context = auth_context_for_route_trace(
        "/models-as-a-service/demo/v1/chat/completions", envoy_label,
        live_authconfig_snapshot, json_trace, gateway_auth_context)
    json_auth_evidence = gateway_auth_request_evidence(
        json_request_id, f"live-{provider}-json", gateway_pod, json_trace,
        json_callbacks_before, json_auth_context)
    json_maas_auth_ok = (
        maas_auth_request_decision_ok(json_auth_evidence, authorized=True, expected_status="200")
        and json_auth_evidence.get("matching_candidate_maas_authconfig_count", 0) > 0
    )
    try:
        json_response = json.loads(json_raw)
    except json.JSONDecodeError:
        json_response = {}
    json_choices = json_response.get("choices", [])
    json_choice_shapes = [
        {
            "finish_reason": choice.get("finish_reason"),
            "message_keys": sorted(choice.get("message", {}).keys())
            if isinstance(choice.get("message"), dict) else [],
            "message_content_type": type(choice.get("message", {}).get("content")).__name__
            if isinstance(choice.get("message"), dict) else "missing",
            "message_refusal_present": bool(choice.get("message", {}).get("refusal"))
            if isinstance(choice.get("message"), dict) else False,
            "tool_call_count": len(choice.get("message", {}).get("tool_calls", []))
            if isinstance(choice.get("message"), dict)
            and isinstance(choice.get("message", {}).get("tool_calls"), list) else 0,
        }
        for choice in json_choices if isinstance(choice, dict)
    ]
    json_ok = (json_rc == 0 and json_status == "200" and bool(json_choices)
               and isinstance(json_choices[0].get("message", {}).get("content"), str))

    stream_marker = f"ipp-live-{provider}-sse-synthetic"
    stream_request_id = f"{ROUTE_TRACE_PREFIX}live-{provider}-sse"
    stream_request = {
        "model": client_model,
        live_max_token_field: live_max_tokens,
        "stream": True,
        "messages": [{"role": "user", "content": f"Reply with the word blue. Marker: {stream_marker}"}],
    }
    stream_callbacks_before = callback_log_snapshot()
    stream_rc, stream_status, stream_raw, first_ms, total_ms, content_type = progressive_stream_request(
        f"live-{provider}-sse", externalmodel_url, stream_request,
        extra_headers=[f"x-ipp-diagnostic-id: {stream_request_id}", f"x-request-id: {stream_request_id}"],
        record_result=True,
        request_timeout=60,
        persist_first_event_payload=False,
    )
    stream_trace = capture_runtime_route_trace(stream_request_id, f"live-{provider}-sse", gateway_pod)
    stream_auth_context = auth_context_for_route_trace(
        "/models-as-a-service/demo/v1/chat/completions", envoy_label,
        live_authconfig_snapshot, stream_trace, gateway_auth_context)
    stream_auth_evidence = gateway_auth_request_evidence(
        stream_request_id, f"live-{provider}-sse", gateway_pod, stream_trace,
        stream_callbacks_before, stream_auth_context)
    stream_maas_auth_ok = (
        maas_auth_request_decision_ok(stream_auth_evidence, authorized=True, expected_status="200")
        and stream_auth_evidence.get("matching_candidate_maas_authconfig_count", 0) > 0
    )
    stream_events = sse_events(stream_raw)
    stream_data = [event["data"] for event in stream_events]
    stream_choice_shapes = [
        {
            "finish_reason": choice.get("finish_reason"),
            "delta_keys": sorted(choice.get("delta", {}).keys())
            if isinstance(choice.get("delta"), dict) else [],
            "delta_content_type": type(choice.get("delta", {}).get("content")).__name__
            if isinstance(choice.get("delta"), dict) else "missing",
        }
        for item in stream_data if isinstance(item, dict)
        for choice in item.get("choices", []) if isinstance(choice, dict)
    ]
    stream_terminal_choice = next((choice for item in reversed(stream_data) if isinstance(item, dict)
                                   for choice in reversed(item.get("choices", []))
                                   if isinstance(choice, dict)), {})
    stream_terminal_finish_reason = stream_terminal_choice.get("finish_reason")
    stream_done_sentinel = "[DONE]" in stream_data
    stream_finish_reasons = {"stop", "length", "content_filter", "tool_calls", "function_call"}
    stream_terminal_chunk_valid = (
        stream_terminal_finish_reason in stream_finish_reasons
        and stream_rc == 0
    )
    stream_termination_valid = stream_done_sentinel or (
        provider == "bedrock" and stream_terminal_chunk_valid
    )
    stream_termination_mode = (
        "done_sentinel" if stream_done_sentinel
        else "finish_reason_and_clean_eof" if provider == "bedrock" and stream_terminal_chunk_valid
        else "missing_completion_marker"
    )
    stream_ok = (stream_rc == 0 and stream_status == "200" and content_type.startswith("text/event-stream")
                 and any(isinstance(item, dict) and item.get("choices") for item in stream_data)
                 and stream_termination_valid and len(stream_events) > 1 and total_ms > first_ms)
    json_model = json_response.get("model")
    stream_models = {item.get("model") for item in stream_data
                     if isinstance(item, dict) and isinstance(item.get("model"), str)}
    json_route_ok = live_provider_trace_matches(json_trace, envoy_route, path)
    stream_route_ok = live_provider_trace_matches(stream_trace, envoy_route, path)
    json_model_ok = live_response_model_matches(provider, target_model, json_model, client_model)
    stream_model_ok = bool(stream_models) and all(
        live_response_model_matches(provider, target_model, model, client_model) for model in stream_models
    )
    shared_mock_cluster = "translation-openai-mock.models-as-a-service.svc.cluster.local"
    live_traces = {"json": json_trace, "sse": stream_trace}
    shared_mock_hits = {
        request_type: trace.get("upstream_cluster", "").rstrip(";")
        for request_type, trace in live_traces.items()
        if shared_mock_cluster in trace.get("upstream_cluster", "")
    }
    # The correlated Envoy route and upstream cluster are authoritative for
    # these two requests. The recorder's message_text is not guaranteed to
    # contain the synthetic marker as a standalone array element.
    shared_markers_absent = all(
        trace.get("captured") is True
        and shared_mock_cluster not in trace.get("upstream_cluster", "")
        for trace in live_traces.values()
    )
    details = {
        **profile_evidence,
        "client_gateway_path": "/models-as-a-service/demo/v1/chat/completions",
        "json": {"http_status": json_status, "curl_exit_status": json_rc,
                 "openai_choices": len(json_choices), "response_model": json_model,
                 "choice_shapes": json_choice_shapes,
                 "valid_chat_response": json_ok, "model_matches_selected_provider": json_model_ok,
                 "runtime_route_ok": json_route_ok, "runtime_route": json_trace,
                 "maas_gateway_auth_ok": json_maas_auth_ok,
                 "gateway_auth_evidence": json_auth_evidence},
        "sse": {"http_status": stream_status, "curl_exit_status": stream_rc,
                "content_type": content_type, "events": len(stream_events),
                "termination": stream_done_sentinel, "termination_mode": stream_termination_mode,
                "terminal_finish_reason": stream_terminal_finish_reason,
                "choice_shapes": stream_choice_shapes,
                "first_event_latency_ms": round(first_ms, 1), "total_response_ms": round(total_ms, 1),
                "valid_chat_stream": stream_ok, "response_models": sorted(stream_models),
                "model_matches_selected_provider": stream_model_ok,
                "runtime_route_ok": stream_route_ok, "runtime_route": stream_trace,
                "maas_gateway_auth_ok": stream_maas_auth_ok,
                "gateway_auth_evidence": stream_auth_evidence},
        "effective_selected_route": envoy_route,
        "response_model_policy": (
            "preserve the OpenAI client model alias" if provider == "anthropic"
            else "return the configured Bedrock Mantle model id" if provider == "bedrock"
            else "configured model or date-versioned provider snapshot"
        ),
        "shared_mock_received_neither_request": shared_markers_absent,
        "shared_mock_cluster_hits": shared_mock_hits,
        "requests_via_gateway": True,
        "upstream_credential_acceptance": "valid provider response returned through selected external route",
        "provider_credential_value_saved": False,
    }
    auth_qualification_ok = (provider != "bedrock" or (json_maas_auth_ok and stream_maas_auth_ok))
    if (json_ok and stream_ok and json_model_ok and stream_model_ok and json_route_ok
            and stream_route_ok and shared_markers_absent and auth_qualification_ok):
        append_live_provider_result(provider, "PASS", details)
        return

    statuses = {json_status, stream_status}
    if statuses & {"401", "403"}:
        status = "BLOCKED"
        details["reason"] = "provider rejected credentials or account authorization"
    elif statuses & {"404", "429"}:
        status = "BLOCKED"
        details["reason"] = "provider rejected the configured model/path or rate/account quota"
    elif statuses & {"000", "502", "503", "504"} or json_rc != 0 or stream_rc != 0:
        status = "BLOCKED"
        details["reason"] = "live provider network or upstream gateway path did not complete"
    else:
        status = "FAIL"
        details["reason"] = "live request completed but downstream JSON or SSE did not match the OpenAI Chat Completions contract"
    save_json(EVIDENCE / f"live-{provider}-failure-profile.json", profile_evidence)
    append_live_provider_result(provider, status, details)


def externalmodel_runtime_snapshot(label):
    overlay_cm = json.loads(kubectl("-n", NAMESPACE, "get", "configmap", "routing-overlay", "-o", "json").stdout)
    published_content = overlay_cm.get("data", {}).get("routing-overlay.json", "")
    wire = json.loads(published_content or "{}")
    published_digest = overlay_cm.get("metadata", {}).get("annotations", {}).get(
        "inference.opendatahub.io/routing-overlay-content-digest", "")
    model_candidates = [candidate for candidate in wire.get("overlay", {}).get("candidates", [])
                        if candidate.get("name") == "demo"]
    extproc_cm = json.loads(kubectl("-n", NAMESPACE, "get", "configmap",
        "payload-processing-external-model-plugins", "-o", "json").stdout)
    extproc_config = extproc_cm.get("data", {}).get("extproc.yaml", "")
    deployment = json.loads(kubectl("-n", NAMESPACE, "get", "deployment",
        "payload-processing-external-model", "-o", "json").stdout)
    selector = deployment.get("spec", {}).get("selector", {}).get("matchLabels", {})
    label_selector = ",".join(f"{key}={value}" for key, value in sorted(selector.items()))
    pod_list = json.loads(kubectl("-n", NAMESPACE, "get", "pods", "-l", label_selector, "-o", "json").stdout)
    pods = []
    for pod in pod_list.get("items", []):
        name = pod.get("metadata", {}).get("name", "")
        # A rollout can leave the old pod selected briefly while its
        # replacement is becoming Ready.  Do not exec into a terminating
        # or non-running pod: the API server may keep the connection open
        # until the pod disappears, turning a snapshot into a timeout.
        if (not name
                or pod.get("metadata", {}).get("deletionTimestamp")
                or pod.get("status", {}).get("phase") != "Running"):
            continue
        mounted = kubectl("-n", NAMESPACE, "exec", f"pod/{name}", "-c", "payload-processing",
                          "--", "cat", "/etc/praxis/routing/routing-overlay.json", check=False, timeout=20)
        mounted_overlay = mounted.stdout if mounted.returncode == 0 else ""
        logs = kubectl("-n", NAMESPACE, "logs", f"pod/{name}", "-c", "payload-processing",
                       "--tail=500", check=False, timeout=20).stdout
        # ExtProc's default tracing subscriber colors the structured log fields.
        # Remove terminal escapes before extracting the accepted and serving
        # overlay revisions so the convergence check works with real pod logs.
        logs = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", logs)
        reload_events = []
        for line in logs.splitlines():
            lowered = line.lower()
            if not any(marker in lowered for marker in (
                "overlay snapshot initialized", "overlay file watcher started",
                "overlay reloaded", "overlay reload failed", "failed to read overlay file",
                "overlay watcher error", "overlay change detected",
            )):
                continue
            # Keep only overlay lifecycle diagnostics and redact any credential-like
            # fields before retaining a runtime log excerpt with the attempt.
            safe_line = re.sub(
                r"(?i)(authorization|x-api-key|api-key|token|secret)(\s*[=:]\s*)[^, }\"]+",
                r"\1\2[REDACTED]", line,
            )
            reload_events.append(safe_line[:1200])
        revision = ""
        for line in reversed(logs.splitlines()):
            match = re.search(
                r'accepted_revision\s*=\s*"?([0-9a-f]{64})"?.*?'
                r'serving_revision\s*=\s*"?([0-9a-f]{64})"?',
                line,
            )
            if match:
                revision = line
                break
        match = re.search(
            r'accepted_revision\s*=\s*"?([0-9a-f]{64})"?.*?'
            r'serving_revision\s*=\s*"?([0-9a-f]{64})"?',
            revision,
        )
        ready = any(condition.get("type") == "Ready" and condition.get("status") == "True"
                    for condition in pod.get("status", {}).get("conditions", []))
        pods.append({
            "name": name,
            "uid": pod.get("metadata", {}).get("uid"),
            "pod_ip": pod.get("status", {}).get("podIP"),
            "ready": ready,
            "deleting": bool(pod.get("metadata", {}).get("deletionTimestamp")),
            "restarts": sum(container.get("restartCount", 0)
                             for container in pod.get("status", {}).get("containerStatuses", [])),
            "mounted_overlay_matches_published": mounted_overlay == published_content,
            "mounted_overlay_sha256": hashlib.sha256(mounted_overlay.encode()).hexdigest()
                if mounted_overlay else "",
            "accepted_revision": match.group(1) if match else "",
            "serving_revision": match.group(2) if match else "",
            "overlay_reload_log_excerpt": reload_events[-30:],
        })
    endpoint_slices = json.loads(kubectl("-n", NAMESPACE, "get", "endpointslices", "-l",
        "kubernetes.io/service-name=payload-processing-external-model", "-o", "json").stdout)
    endpoints = [{
        "target": endpoint.get("targetRef", {}).get("name"),
        "ready": endpoint.get("conditions", {}).get("ready") is True,
        "addresses": endpoint.get("addresses", []),
    } for endpoint_slice in endpoint_slices.get("items", []) for endpoint in endpoint_slice.get("endpoints", [])]
    snapshot = {
        "label": label,
        "published_digest": published_digest,
        "candidate_clusters": sorted({candidate.get("cluster") for candidate in wire.get("overlay", {}).get("candidates", [])}),
        "demo_model_candidate_clusters": sorted({candidate.get("cluster") for candidate in model_candidates}),
        "demo_model_candidates": model_candidates,
        "deployment_generation": deployment.get("metadata", {}).get("generation"),
        "deployment_observed_generation": deployment.get("status", {}).get("observedGeneration"),
        "deployment_replicas": deployment.get("status", {}).get("replicas"),
        "deployment_updated_replicas": deployment.get("status", {}).get("updatedReplicas"),
        "deployment_ready_replicas": deployment.get("status", {}).get("readyReplicas"),
        "deployment_available_replicas": deployment.get("status", {}).get("availableReplicas"),
        "extproc_config": extproc_config,
        "pods": pods,
        "endpoints": endpoints,
    }
    save_json(EVIDENCE / f"externalmodel-runtime-{label}.json", snapshot)
    return snapshot


def wait_externalmodel_revision(expected_clusters, label, *, changed_from=None, require_serving=True,
                                api_translation_enabled=True):
    for _ in range(180):
        snapshot = externalmodel_runtime_snapshot(label)
        replicas = snapshot.get("deployment_replicas", 0) or 0
        ready_pods = [pod for pod in snapshot["pods"] if pod["ready"] and not pod["deleting"]]
        ready_names = {pod["name"] for pod in ready_pods}
        ready_endpoints = {endpoint["target"] for endpoint in snapshot["endpoints"] if endpoint["ready"]}
        rollout_ready = (
            snapshot.get("deployment_observed_generation", 0) >= snapshot.get("deployment_generation", 0)
            and snapshot.get("deployment_updated_replicas", 0) == replicas
            and snapshot.get("deployment_ready_replicas", 0) == replicas
            and snapshot.get("deployment_available_replicas", 0) == replicas
        )
        mounted_pods_ready = bool(ready_pods) and all(
            pod["mounted_overlay_matches_published"] for pod in ready_pods
        )
        # Envoy may retain an ExtProc gRPC connection to a terminating pod
        # after Kubernetes has published the replacement endpoint. Do not
        # send attributed traffic until every selected pod is on this revision
        # and old processes have exited.
        all_pods_current = bool(snapshot["pods"]) and all(
            pod["ready"] and not pod["deleting"]
            and pod["mounted_overlay_matches_published"]
            and pod["accepted_revision"] == snapshot["published_digest"]
            and pod["serving_revision"] == snapshot["published_digest"]
            for pod in snapshot["pods"]
        )
        serving_pods_ready = mounted_pods_ready and all_pods_current
        pods_ready = serving_pods_ready if require_serving else mounted_pods_ready
        endpoint_targets = {endpoint["target"] for endpoint in snapshot["endpoints"]}
        endpoints_ready = (
            bool(ready_endpoints)
            and ready_endpoints == ready_names
            and endpoint_targets == ready_names
            and all(endpoint["ready"] for endpoint in snapshot["endpoints"])
        )
        selected_profile_filter_present = (
            "filter: openai_chat_completions_to_anthropic\n" in snapshot["extproc_config"]
        )
        translation_enabled_marker = "api_translation_enabled: true" in snapshot["extproc_config"]
        translation_stream_filter_present = (
            "openai_chat_completions_to_anthropic_stream" in snapshot["extproc_config"]
        )
        # Translation is an opt-in rendering decision. Flag-off removes both
        # the converter and its trusted profile input so targetModel is not
        # rewritten on OpenAI-compatible requests.
        translation_filter_state_matches = (
            selected_profile_filter_present and translation_stream_filter_present
            if api_translation_enabled else
            not selected_profile_filter_present and not translation_stream_filter_present
        )
        translation_setting_state_matches = (
            translation_enabled_marker if api_translation_enabled
            else "api_translation_enabled:" not in snapshot["extproc_config"]
        )
        provider_profile_fields = ("api_format", "path", "target_model")
        profile_candidates = snapshot["demo_model_candidates"]
        provider_profile_state_matches = (
            bool(profile_candidates) and all(
                all(candidate.get(field) for field in provider_profile_fields)
                for candidate in profile_candidates
            )
            if api_translation_enabled else
            all(not any(field in candidate for field in provider_profile_fields)
                for candidate in profile_candidates)
        )
        # The translator's limit is the selected-upstream request limit; it
        # matches the server limit so long-context requests are not rejected.
        body_limit_state_matches = "filter: stream_usage_inject" in snapshot["extproc_config"] and (
            "api_translation_enabled: true\n        max_body_bytes: 10485760" in snapshot["extproc_config"]
            and translation_stream_filter_present
            if api_translation_enabled else
            "\n  max_body_bytes:" not in snapshot["extproc_config"]
            and "allow_unbounded_body: true" in snapshot["extproc_config"]
            and not translation_stream_filter_present
        )
        if (set(snapshot["demo_model_candidate_clusters"]) == set(expected_clusters)
                and snapshot["published_digest"]
                and (changed_from is None or snapshot["published_digest"] != changed_from)
                and translation_filter_state_matches
                and translation_setting_state_matches
                and provider_profile_state_matches
                and body_limit_state_matches
                and rollout_ready and pods_ready and endpoints_ready):
            record(f"externalmodel_runtime_{'converged' if require_serving else 'published_mounted'}_{label}", True, {
                "published_revision": snapshot["published_digest"],
                "serving_pods": [pod["name"] for pod in ready_pods],
                "ready_endpoints": sorted(ready_endpoints),
                "endpoint_targets": sorted(endpoint_targets),
                "terminating_pods": [pod["name"] for pod in snapshot["pods"] if pod["deleting"]],
                "selected_candidate_clusters": sorted(expected_clusters),
                "provider_profiles_match_translation_setting": provider_profile_state_matches,
                "serving_revision_required": require_serving,
            })
            return snapshot
        time.sleep(0.5)
    raise RuntimeError(f"ExternalModel published, mounted, accepted, serving, and endpoint revisions did not converge: {label}")


def externalmodel_snapshot_serves_revision(snapshot, expected_clusters, expected_revision):
    replicas = snapshot.get("deployment_replicas", 0) or 0
    ready_pods = [pod for pod in snapshot.get("pods", []) if pod["ready"] and not pod["deleting"]]
    ready_names = {pod["name"] for pod in ready_pods}
    ready_endpoints = {endpoint["target"] for endpoint in snapshot.get("endpoints", []) if endpoint["ready"]}
    endpoint_targets = {endpoint["target"] for endpoint in snapshot.get("endpoints", [])}
    return (
        snapshot.get("published_digest") == expected_revision
        and set(snapshot.get("demo_model_candidate_clusters", [])) == set(expected_clusters)
        and snapshot.get("deployment_observed_generation", 0) >= snapshot.get("deployment_generation", 0)
        and snapshot.get("deployment_updated_replicas", 0) == replicas
        and snapshot.get("deployment_ready_replicas", 0) == replicas
        and snapshot.get("deployment_available_replicas", 0) == replicas
        and bool(ready_pods)
        and len(ready_pods) == replicas
        and all(pod["mounted_overlay_matches_published"]
                and pod["accepted_revision"] == expected_revision
                and pod["serving_revision"] == expected_revision for pod in ready_pods)
        and ready_endpoints == ready_names
        and endpoint_targets == ready_names
        and all(endpoint["ready"] for endpoint in snapshot.get("endpoints", []))
    )


def wait_observation(predicate, label):
    for _ in range(60):
        current = observations()
        for item in reversed(current):
            if predicate(item):
                RESULT["upstream_observations"].append(item)
                save_json(RESULTS, RESULT)
                return item
        time.sleep(0.25)
    raise RuntimeError(f"mock upstream did not observe {label}")


def sse_events(raw):
    events = []
    for block in re.split(r"\r?\n\r?\n", raw.decode(errors="replace")):
        event_name = ""
        data_lines = []
        for line in block.splitlines():
            if line.startswith("event:"):
                event_name = line[6:].strip()
            elif line.startswith("data:"):
                data_lines.append(line[5:].strip())
        if data_lines:
            data = "\n".join(data_lines)
            try:
                parsed = json.loads(data)
            except json.JSONDecodeError:
                parsed = data
            events.append({"event": event_name, "data": parsed})
    return events


def capture_shared_runtime(label):
    config = json.loads(kubectl("-n", "maas-system", "get", "configmap", "payload-processing-plugins",
                                "-o", "json").stdout)
    extproc = config.get("data", {}).get("extproc.yaml", "")
    deployment = json.loads(kubectl("-n", "maas-system", "get", "deployment", "payload-processing",
                                   "-o", "json").stdout)
    deployment_uid = deployment.get("metadata", {}).get("uid")
    replica_sets = json.loads(kubectl("-n", "maas-system", "get", "replicasets",
                                      "-l", "app=payload-processing", "-o", "json").stdout)
    owned_replica_sets = {
        rs.get("metadata", {}).get("name"): rs
        for rs in replica_sets.get("items", [])
        if any(owner.get("kind") == "Deployment"
               and owner.get("name") == "payload-processing"
               and owner.get("uid") == deployment_uid
               for owner in rs.get("metadata", {}).get("ownerReferences", []))
    }
    all_pods = json.loads(kubectl("-n", "maas-system", "get", "pods", "-l", "app=payload-processing",
                                  "-o", "json").stdout)
    target_pods = [pod for pod in all_pods.get("items", [])
                   if any(owner.get("kind") == "ReplicaSet" and owner.get("name") in owned_replica_sets
                          for owner in pod.get("metadata", {}).get("ownerReferences", []))]
    pod_snapshots = []
    for pod in target_pods:
        name = pod["metadata"]["name"]
        mounted = kubectl("-n", "maas-system", "exec", f"pod/{name}", "--",
                          "cat", "/etc/praxis/extproc.yaml", check=False)
        pod_snapshots.append({
            "name": name,
            "uid": pod.get("metadata", {}).get("uid"),
            "phase": pod.get("status", {}).get("phase"),
            "deletion_timestamp": pod.get("metadata", {}).get("deletionTimestamp"),
            "labels": pod.get("metadata", {}).get("labels", {}),
            "owner_references": pod.get("metadata", {}).get("ownerReferences", []),
            "owner_replica_set": next((owner.get("name") for owner in pod.get("metadata", {}).get("ownerReferences", [])
                                        if owner.get("kind") == "ReplicaSet"), None),
            "pod_ip": pod.get("status", {}).get("podIP"),
            "ready": any(c.get("type") == "Ready" and c.get("status") == "True"
                         for c in pod.get("status", {}).get("conditions", [])),
            "image_ids": [c.get("imageID") for c in pod.get("status", {}).get("containerStatuses", [])],
            "mounted_config": mounted.stdout if mounted.returncode == 0 else "",
        })
    endpoint_slices = json.loads(kubectl(
        "-n", "maas-system", "get", "endpointslices",
        "-l", "kubernetes.io/service-name=payload-processing", "-o", "json").stdout)
    endpoint_snapshots = []
    for endpoint_slice in endpoint_slices.get("items", []):
        endpoint_snapshots.append({
            "name": endpoint_slice.get("metadata", {}).get("name"),
            "endpoints": [{
                "target_ref": endpoint.get("targetRef", {}),
                "addresses": endpoint.get("addresses", []),
                "conditions": endpoint.get("conditions", {}),
            } for endpoint in endpoint_slice.get("endpoints", [])],
        })
    save_json(EVIDENCE / f"shared-runtime-{label}.json", {
        "configmap": config,
        "rendered_extproc": extproc,
        "deployment": deployment,
        "deployment_replica_sets": list(owned_replica_sets.values()),
        "pods": pod_snapshots,
        "service_endpoint_slices": endpoint_snapshots,
    })
    return extproc, pod_snapshots


def wait_shared_runtime_convergence(label):
    last_extproc = ""
    last_pods = []
    last_deployment = {}
    previous_signature = None
    stable_observations = 0
    for _ in range(120):
        last_extproc, last_pods = capture_shared_runtime(label)
        snapshot = json.loads((EVIDENCE / f"shared-runtime-{label}.json").read_text())
        last_deployment = snapshot.get("deployment", {})
        spec_replicas = last_deployment.get("spec", {}).get("replicas", 1)
        status = last_deployment.get("status", {})
        rollout_converged = (
            status.get("observedGeneration", 0) >= last_deployment.get("metadata", {}).get("generation", 0)
            and status.get("updatedReplicas", 0) == spec_replicas
            and status.get("readyReplicas", 0) == spec_replicas
            and status.get("availableReplicas", 0) == spec_replicas
            and (status.get("terminatingReplicas") or 0) == 0
        )
        deployment_revision = last_deployment.get("metadata", {}).get("annotations", {}).get(
            "deployment.kubernetes.io/revision")
        current_replica_sets = {
            replica_set.get("metadata", {}).get("name")
            for replica_set in snapshot.get("deployment_replica_sets", [])
            if replica_set.get("metadata", {}).get("annotations", {}).get(
                "deployment.kubernetes.io/revision") == deployment_revision
        }
        serving_pods = [pod for pod in last_pods
                        if pod.get("owner_replica_set") in current_replica_sets]
        pods_converged = (
            len(serving_pods) == spec_replicas
            and all(pod["ready"] and not pod["deletion_timestamp"]
                    and pod["mounted_config"] == last_extproc for pod in serving_pods)
        )
        pod_names = {pod["name"] for pod in serving_pods}
        all_endpoint_targets = {
            endpoint.get("target_ref", {}).get("name")
            for endpoint_slice in snapshot.get("service_endpoint_slices", [])
            for endpoint in endpoint_slice.get("endpoints", [])
        }
        ready_endpoint_targets = {
            endpoint.get("target_ref", {}).get("name")
            for endpoint_slice in snapshot.get("service_endpoint_slices", [])
            for endpoint in endpoint_slice.get("endpoints", [])
            if endpoint.get("conditions", {}).get("ready") is True
        }
        endpoints_converged = (
            bool(pod_names)
            and all_endpoint_targets == pod_names
            and ready_endpoint_targets == pod_names
            and all(
                endpoint.get("conditions", {}).get("ready") is True
                for endpoint_slice in snapshot.get("service_endpoint_slices", [])
                for endpoint in endpoint_slice.get("endpoints", [])
            )
        )
        signature = (
            last_extproc,
            tuple(sorted((pod["name"], pod["uid"]) for pod in serving_pods)),
            tuple(sorted(all_endpoint_targets)),
            last_deployment.get("metadata", {}).get("generation"),
        )
        if signature == previous_signature:
            stable_observations += 1
        else:
            stable_observations = 1
            previous_signature = signature
        if (rollout_converged and pods_converged and endpoints_converged
                and stable_observations >= 2):
            return last_extproc, last_pods
        time.sleep(1)
    raise RuntimeError(f"shared ExtProc rollout did not converge to its mounted and serving configuration: {label}")


def capture_envoy(label):
    pods = json.loads(kubectl("-n", "maas-system", "get", "pods", "-l",
                              "gateway.networking.k8s.io/gateway-name=maas-default-gateway", "-o", "json").stdout)
    pod = next((p["metadata"]["name"] for p in pods.get("items", [])
                if not p.get("metadata", {}).get("deletionTimestamp")
                and any(c.get("type") == "Ready" and c.get("status") == "True"
                        for c in p.get("status", {}).get("conditions", []))), None)
    if not pod:
        raise RuntimeError("no Ready maas-default-gateway Envoy pod")
    for resource in ("all", "listeners", "routes", "clusters", "endpoints"):
        result = run([ISTIOCTL, "--context", f"kind-{CLUSTER}", "proxy-config", resource, pod,
                      "-n", "maas-system", "-o", "json"],
                     check=False, timeout=90)
        if result.returncode != 0:
            (EVIDENCE / f"envoy-{label}-{resource}.json").write_text(result.stdout or result.stderr)
            raise RuntimeError(f"istioctl proxy-config {resource} failed; see Envoy evidence")
        if resource == "endpoints":
            try:
                endpoint_rows = json.loads(result.stdout)
                endpoint_rows = [row for row in endpoint_rows
                                 if "payload-processing" in row.get("name", "")]
                (EVIDENCE / f"envoy-{label}-{resource}.json").write_text(
                    json.dumps(endpoint_rows, indent=2, sort_keys=True) + "\n"
                )
            except json.JSONDecodeError:
                (EVIDENCE / f"envoy-{label}-{resource}.json").write_text(result.stdout)
                raise RuntimeError("Envoy ExtProc endpoint snapshot was not valid JSON")
        else:
            (EVIDENCE / f"envoy-{label}-{resource}.json").write_text(result.stdout or result.stderr)


def wait_gateway_ready_stable(label):
    """Wait for one non-terminating Gateway pod to be Ready without restarts."""
    previous = None
    stable_samples = 0
    history = []
    for _ in range(90):
        pods = json.loads(kubectl(
            "-n", "maas-system", "get", "pods", "-l",
            "gateway.networking.k8s.io/gateway-name=maas-default-gateway", "-o", "json").stdout)
        ready = []
        for pod in pods.get("items", []):
            if pod.get("metadata", {}).get("deletionTimestamp"):
                continue
            conditions = pod.get("status", {}).get("conditions", [])
            if not any(condition.get("type") == "Ready" and condition.get("status") == "True"
                       for condition in conditions):
                continue
            restarts = sum(status.get("restartCount", 0)
                           for status in pod.get("status", {}).get("containerStatuses", []))
            ready.append({
                "name": pod.get("metadata", {}).get("name"),
                "uid": pod.get("metadata", {}).get("uid"),
                "restarts": restarts,
            })
        sample = ready[0] if len(ready) == 1 else None
        history.append({"ready_pods": ready, "selected": sample})
        current = (sample or {}).get("uid"), (sample or {}).get("restarts")
        if sample and current == previous:
            stable_samples += 1
        else:
            stable_samples = 1 if sample else 0
        previous = current if sample else None
        if stable_samples >= 2:
            save_json(EVIDENCE / f"gateway-serving-readiness-{label}.json", {
                "stable_samples": stable_samples,
                "selected_pod": sample,
                "history": history[-10:],
            })
            return sample["name"]
        time.sleep(1)
    save_json(EVIDENCE / f"gateway-serving-readiness-{label}-failure.json", {
        "stable_samples": stable_samples,
        "history": history[-20:],
    })
    raise RuntimeError(f"Gateway did not remain Ready with stable restarts: {label}")


def externalmodel_send_body_before_header_flags(label):
    """Read the effective dedicated ExtProc setting from Envoy's listener config."""
    config_dump = json.loads((EVIDENCE / f"envoy-{label}-all.json").read_text())
    flags = []

    def visit(value):
        if isinstance(value, dict):
            if value.get("name") == "envoy.filters.http.ext_proc.external-model":
                typed = value.get("typed_config", value.get("typedConfig", {}))
                flags.append(typed.get("send_body_without_waiting_for_header_response",
                                      typed.get("sendBodyWithoutWaitingForHeaderResponse")))
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(config_dump)
    return flags


def auth_policy_input_filter_order(listener_output):
    """Prove the sanitized probe runs after pre-auth ExtProc and before Kuadrant auth."""
    result = {"verified": False, "listener_name": None, "http_filter_names": []}
    try:
        listeners = json.loads(listener_output)
    except json.JSONDecodeError:
        result["parse_error"] = True
        return result
    auth_filter = "extensions.istio.io/wasmplugin/maas-system.kuadrant-maas-default-gateway"
    for listener in listeners if isinstance(listeners, list) else []:
        for chain in listener.get("filterChains", []):
            for network_filter in chain.get("filters", []):
                manager = network_filter.get("typedConfig", {})
                names = [item.get("name") for item in manager.get("httpFilters", [])]
                if auth_filter not in names:
                    continue
                pre_index = names.index("envoy.filters.http.ext_proc.ipp-pre") if "envoy.filters.http.ext_proc.ipp-pre" in names else -1
                auth_index = names.index(auth_filter)
                probe_indices = [i for i, name in enumerate(names) if name == "envoy.filters.http.lua"]
                probe_index = next((i for i in probe_indices if pre_index < i < auth_index), -1)
                result = {
                    "verified": pre_index >= 0 and probe_index > pre_index and auth_index > probe_index,
                    "listener_name": listener.get("name"),
                    "http_filter_names": names,
                    "pre_auth_extproc_index": pre_index,
                    "auth_input_probe_index": probe_index,
                    "kuadrant_auth_index": auth_index,
                }
                if result["verified"]:
                    return result
    return result


def install_runtime_route_trace():
    """Add a run-owned, request-ID-filtered Envoy access log for routing diagnosis."""
    auth_presence_lua = r'''function envoy_on_request(handle)
  local headers = handle:headers()
  local diagnostic = headers:get("x-ipp-diagnostic-id") or ""
  if diagnostic == "" then return end
  local path = headers:get(":path") or ""
  path = string.match(path, "^[^?]*") or path
  if not (string.match(path, "/v1/messages$") or string.match(path, "/v1/chat/completions$")) then return end
  local authorization = headers:get("authorization")
  local authorization_present = authorization ~= nil and "true" or "false"
  local authorization_scheme = "absent"
  local credential_family = "absent"
  if authorization ~= nil then
    authorization_scheme = string.match(authorization, "^(%S+)") or "other"
    if string.match(authorization, "^Bearer%s+sk%-oai%-") then
      credential_family = "sk-oai"
    else
      credential_family = "other"
    end
  end
  local identity_headers = {}
  for _, name in ipairs({"x-maas-username", "x-maas-group", "x-maas-identity", "x-maas-subscription"}) do
    if headers:get(name) ~= nil then table.insert(identity_headers, name) end
  end
  local fields = {
    diagnostic,
    headers:get(":authority") or "",
    path,
    headers:get("x-gateway-model-name") or "",
    authorization_present,
    authorization_scheme,
    credential_family,
    headers:get("x-api-key") ~= nil and "true" or "false",
    table.concat(identity_headers, ","),
  }
  -- Warning is intentional: the run-owned proxy uses warning-level log output by default.
  handle:logWarn("IPP-AUTH-INPUT-TRACE|" .. table.concat(fields, "|"))
end'''
    access_log = {
        "name": "envoy.access_loggers.file",
        "filter": {
            "extension_filter": {
                "name": "envoy.access_loggers.extension_filters.cel",
                "typed_config": {
                    "@type": "type.googleapis.com/envoy.extensions.access_loggers.filters.cel.v3.ExpressionFilter",
                    "expression": (
                        "request.method == 'POST' && "
                        "(request.path.endsWith('/v1/chat/completions') || "
                        "request.path.endsWith('/v1/messages'))"
                    ),
                },
            },
        },
        "typed_config": {
            "@type": "type.googleapis.com/envoy.extensions.access_loggers.file.v3.FileAccessLog",
            "path": "/dev/stdout",
            "log_format": {
                "text_format_source": {
                    "inline_string": (
                        "%REQ(X-IPP-DIAGNOSTIC-ID)%\t%REQ(X-REQUEST-ID)%\t%REQ(:METHOD)%\t"
                        "%REQ(:AUTHORITY)%\t%REQ(:PATH)%\t%REQ(X-GATEWAY-MODEL-NAME)%\t"
                        "%REQ(X-AI-ROUTING-CANDIDATE)%\t%REQ(X-AI-ROUTING-REVISION)%\t"
                        "%REQ(X-AI-ROUTING-REQUEST-ID)%\t"
                        "%ROUTE_NAME%\t%UPSTREAM_CLUSTER%\t%RESPONSE_FLAGS%\t"
                        "%RESPONSE_CODE%\t%RESPONSE_CODE_DETAILS%\t%UPSTREAM_HOST%\t%DURATION%\n"
                    ),
                },
            },
        },
    }
    envoy_filter = {
        "apiVersion": "networking.istio.io/v1alpha3",
        "kind": "EnvoyFilter",
        "metadata": {
            "name": ROUTE_TRACE_FILTER,
            "namespace": "maas-system",
            "labels": {"app.kubernetes.io/managed-by": "api-translation-kind-e2e"},
        },
        "spec": {
            "workloadSelector": {
                "labels": {"gateway.networking.k8s.io/gateway-name": "maas-default-gateway"},
            },
            "priority": 100,
            "configPatches": [{
                "applyTo": "HTTP_FILTER",
                "match": {
                    "context": "GATEWAY",
                    "listener": {
                        "portNumber": 80,
                        "filterChain": {
                            "filter": {
                                "name": "envoy.filters.network.http_connection_manager",
                                "subFilter": {
                                    "name": "extensions.istio.io/wasmplugin/maas-system.kuadrant-maas-default-gateway",
                                },
                            },
                        },
                    },
                },
                "patch": {
                    "operation": "INSERT_BEFORE",
                    "value": {
                        "name": "envoy.filters.http.lua",
                        "typed_config": {
                            "@type": "type.googleapis.com/envoy.extensions.filters.http.lua.v3.Lua",
                            "inlineCode": auth_presence_lua,
                        },
                    },
                },
            }, {
                "applyTo": "NETWORK_FILTER",
                "match": {
                    "context": "GATEWAY",
                    "listener": {
                        "portNumber": 80,
                        "filterChain": {
                            "filter": {"name": "envoy.filters.network.http_connection_manager"},
                        },
                    },
                },
                "patch": {
                    "operation": "MERGE",
                    "value": {
                        "typed_config": {
                            "@type": "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager",
                            "access_log": [access_log],
                        },
                    },
                },
            }],
        },
    }
    save_json(EVIDENCE / "envoy-runtime-route-trace-filter.json", envoy_filter)
    kubectl("-n", "maas-system", "apply", "-f", "-", input_text=json.dumps(envoy_filter))

    def ready_gateway_pod():
        gateway_pods = json.loads(kubectl(
            "-n", "maas-system", "get", "pods", "-l",
            "gateway.networking.k8s.io/gateway-name=maas-default-gateway", "-o", "json").stdout)
        return next((pod["metadata"]["name"] for pod in gateway_pods.get("items", [])
                     if not pod.get("metadata", {}).get("deletionTimestamp")
                     and any(condition.get("type") == "Ready" and condition.get("status") == "True"
                             for condition in pod.get("status", {}).get("conditions", []))), None)

    gateway_pod = ready_gateway_pod()
    if not gateway_pod:
        raise RuntimeError("no Ready, non-terminating maas-default-gateway Envoy pod for runtime route trace")

    last = ""
    for _ in range(60):
        gateway_pod = ready_gateway_pod()
        if not gateway_pod:
            time.sleep(1)
            continue
        result = run([ISTIOCTL, "--context", f"kind-{CLUSTER}", "proxy-config", "listeners", gateway_pod,
                      "-n", "maas-system", "-o", "json"],
                     check=False, timeout=90)
        last = result.stdout or result.stderr
        input_filter_order = auth_policy_input_filter_order(last)
        if (result.returncode == 0 and all(
                field in last for field in ("%ROUTE_NAME%", "%UPSTREAM_CLUSTER%", "%RESPONSE_FLAGS%",
                                            "%REQ(X-AI-ROUTING-REVISION)%", "%RESPONSE_CODE_DETAILS%",
                                            "IPP-AUTH-INPUT-TRACE"))
                and input_filter_order["verified"]):
            (EVIDENCE / "envoy-runtime-route-trace-listener.json").write_text(last)
            save_json(EVIDENCE / "envoy-runtime-route-trace-ready.json", {
                "gateway_pod": gateway_pod,
                "request_id_prefix": ROUTE_TRACE_PREFIX,
                "auth_presence_trace": "request-header-presence-only via run-owned Lua after pre-auth ExtProc and immediately before Kuadrant auth",
                "auth_input_filter_order": input_filter_order,
                "access_log_fields": ["diagnostic_id", "envoy_request_id", "authority", "request_path", "model_header",
                                      "selected_candidate_header", "selected_overlay_revision_header",
                                      "routing_request_id", "route_name", "upstream_cluster",
                                      "response_flags", "response_code", "response_code_details", "upstream_host"],
                "listener_config_contains_trace_logger": True,
            })
            return gateway_pod
        time.sleep(1)
    (EVIDENCE / "envoy-runtime-route-trace-listener.json").write_text(last)
    save_json(EVIDENCE / "envoy-runtime-route-trace-auth-input-order-failure.json", {
        "istioctl_exit_status": result.returncode if "result" in locals() else None,
        "auth_input_filter_order": input_filter_order if "input_filter_order" in locals() else None,
        "listener_config_saved": True,
    })
    control_plane_logs = kubectl("-n", "istio-system", "logs", "deploy/istiod", "--since=5m", check=False)
    (EVIDENCE / "envoy-runtime-route-trace-control-plane-on-failure.log").write_text(
        redact_text(control_plane_logs.stdout + control_plane_logs.stderr))
    raise RuntimeError("Envoy did not accept the runtime route trace access logger; inspect listener and proxy diagnostics")


def capture_auth_policy_input_trace(request_id, label, gateway_pod):
    """Capture sanitized headers immediately before the Kuadrant auth filter."""
    trace = {"request_id": request_id, "captured": False, "raw_log_saved": False}
    marker = f"IPP-AUTH-INPUT-TRACE|{request_id}|"
    for _ in range(20):
        result = kubectl("-n", "maas-system", "logs", f"pod/{gateway_pod}", "-c", "istio-proxy",
                         "--since=20m", check=False, timeout=30)
        for line in result.stdout.splitlines():
            start = line.find(marker)
            if start < 0:
                continue
            # Envoy appends its worker suffix after the Lua log payload. Keep
            # that logger metadata out of the final client-header field.
            payload = line[start + len("IPP-AUTH-INPUT-TRACE|"):]
            payload = re.sub(r"\tthread=\d+\s*$", "", payload)
            fields = payload.strip().split("|")
            if len(fields) >= 9 and fields[0] == request_id:
                identity_headers = {
                    value.strip() for value in fields[8].split(",")
                    if value.strip() in {
                        "x-maas-username", "x-maas-group", "x-maas-identity", "x-maas-subscription",
                    }
                }
                trace.update({
                    "captured": True,
                    "authority": fields[1],
                    "path": fields[2],
                    "gateway_model_header_present": bool(fields[3]),
                    "gateway_model": fields[3] or None,
                    "authorization_present": fields[4] == "true",
                    "authorization_scheme": fields[5],
                    "credential_family": fields[6],
                    "x_api_key_present": fields[7] == "true",
                    "client_identity_header_names": sorted(identity_headers),
                })
                break
        if trace["captured"]:
            break
        time.sleep(0.25)
    save_json(EVIDENCE / f"envoy-auth-policy-input-trace-{label}.json", trace)
    RESULT.setdefault("diagnostics", []).append({"name": f"auth_policy_input_trace_{label}", **trace})
    save_json(RESULTS, RESULT)
    return trace


def capture_runtime_route_trace(request_id, label, gateway_pod):
    """Read the one Envoy access-log row keyed by the synthetic test request ID."""
    trace = {"request_id": request_id, "captured": False}
    for _ in range(20):
        result = kubectl("-n", "maas-system", "logs", f"pod/{gateway_pod}", "-c", "istio-proxy",
                         "--since=20m", check=False, timeout=30)
        lines = [line for line in result.stdout.splitlines()
                 if line.split("\t", 1)[0] == request_id]
        if lines:
            fields = lines[-1].split("\t")
            if len(fields) >= 16:
                trace.update({
                    "captured": True,
                    "diagnostic_id": fields[0],
                    "envoy_request_id": fields[1],
                    "method": fields[2],
                    "authority": fields[3],
                    "path_at_access_log": fields[4],
                    "model_header": fields[5],
                    "selected_candidate_header": fields[6],
                    "selected_overlay_revision_header": fields[7],
                    "routing_request_id": fields[8],
                    "route_name": fields[9],
                    "upstream_cluster": fields[10],
                    "response_flags": fields[11],
                    "response_code": fields[12],
                    "response_code_details": fields[13],
                    "upstream_host": fields[14],
                    "duration_ms": fields[15],
                })
            else:
                trace.update({"captured": True, "parse_error": "unexpected access-log field count"})
            break
        time.sleep(0.25)
    save_json(EVIDENCE / f"envoy-runtime-route-trace-{label}.json", trace)
    RESULT.setdefault("runtime_route_traces", []).append(trace)
    save_json(RESULTS, RESULT)
    return trace


def selected_live_provider_route(label, profile, endpoint):
    """Resolve the selected candidate's effective Envoy route and TLS cluster."""
    routes = json.loads((EVIDENCE / f"envoy-{label}-routes.json").read_text())
    clusters = json.loads((EVIDENCE / f"envoy-{label}-clusters.json").read_text())
    stable_id = profile.get("stable_id", "")
    path = profile.get("path", "")
    matches = []
    for route_config in routes:
        for virtual_host in route_config.get("virtualHosts", []):
            for route in virtual_host.get("routes", []):
                candidate_header = next((header for header in route.get("match", {}).get("headers", [])
                                         if header.get("name", "").lower() == "x-ai-routing-candidate"), {})
                if candidate_header.get("stringMatch", {}).get("exact") == stable_id:
                    matches.append({"route_config": route_config.get("name"),
                                    "virtual_host": virtual_host.get("name"), **route})
    selected = next((route for route in matches
                     if route.get("match", {}).get("path") == path), {})
    action = selected.get("route", {})
    cluster_name = action.get("cluster", "")
    cluster = next((item for item in clusters if item.get("name") == cluster_name), {})
    socket = cluster.get("transportSocket", {})
    tls_config = socket.get("typedConfig", {})
    common_tls = tls_config.get("commonTlsContext", {})
    route_details = {
        "stable_candidate_id": stable_id,
        "candidate_route_count": len(matches),
        "route_name": selected.get("name"),
        "route_match_path": selected.get("match", {}).get("path"),
        "expected_provider_path": path,
        "upstream_cluster": cluster_name,
        "upstream_authority": action.get("hostRewriteLiteral") or action.get("hostRewrite"),
        "expected_endpoint_authority": endpoint,
        "cluster_transport_socket": socket.get("name"),
        "cluster_sni": tls_config.get("sni") or common_tls.get("sni"),
        "expected_sni": endpoint,
        "route_selected_candidate": bool(selected and stable_id),
        "path_matches_profile": selected.get("match", {}).get("path") == path,
        "authority_matches_endpoint": (action.get("hostRewriteLiteral") or action.get("hostRewrite")) == endpoint,
        "tls_socket_enabled": socket.get("name") == "envoy.transport_sockets.tls",
        "sni_matches_endpoint": (tls_config.get("sni") or common_tls.get("sni")) == endpoint,
    }
    save_json(EVIDENCE / f"live-{label}-effective-route.json", route_details)
    return route_details


def live_provider_trace_matches(trace, route, path):
    expected_cluster = route.get("upstream_cluster", "").rstrip(";")
    actual_cluster = trace.get("upstream_cluster", "").rstrip(";")
    return (
        trace.get("captured") is True
        and trace.get("route_name") == route.get("route_name")
        and actual_cluster == expected_cluster
        and trace.get("path_at_access_log") == path
        and trace.get("authority") == route.get("expected_endpoint_authority")
        and trace.get("response_code") == "200"
        and trace.get("response_flags") == "-"
        and bool(trace.get("upstream_host"))
    )


def callback_log_snapshot():
    """Return safe access-log facts for MaaS metadata callback routes only."""
    result = kubectl("-n", "maas-system", "logs", "deploy/maas-api", "--since=20m", check=False)
    events = []
    callback_paths = {
        "/internal/v1/api-keys/validate",
        "/internal/v1/subscriptions/select",
    }
    for line in result.stdout.splitlines():
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        message = entry.get("body", entry.get("msg", entry.get("message", "")))
        if message != "request completed" or entry.get("path") not in callback_paths:
            continue
        auth_headers = entry.get("auth_headers")
        if isinstance(auth_headers, dict):
            header_names = {str(key).lower() for key in auth_headers}
        elif isinstance(auth_headers, list):
            header_names = {str(value).split(":", 1)[0].strip().lower()
                            for value in auth_headers if isinstance(value, str) and ":" in value}
        else:
            header_names = set()
        event = {key: entry.get(key) for key in (
            "timestamp", "request_id", "method", "path", "status", "latency_ms",
            "tenant_name", "tenant_namespace", "gateway_name",
        ) if key in entry}
        event["authorization_header_present"] = "authorization" in header_names
        event["x_api_key_header_present"] = "x-api-key" in header_names
        events.append(event)
    return {"exit_status": result.returncode, "events": events}


def auth_scope_for_route_path(path):
    """Resolve the auth-service scope selected by the captured Envoy Wasm config."""
    config_path = EVIDENCE / "envoy-feature-off-all.json"
    if not config_path.exists():
        return {"found": False, "reason": "Envoy config dump is absent"}
    try:
        config_dump = json.loads(config_path.read_text())
    except json.JSONDecodeError:
        return {"found": False, "reason": "Envoy config dump is invalid JSON"}

    candidates = []

    def find_action_sets(value):
        if isinstance(value, dict):
            for child in value.values():
                find_action_sets(child)
        elif isinstance(value, list):
            for child in value:
                find_action_sets(child)
        elif isinstance(value, str) and "actionSets" in value:
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return
            for action_set in parsed.get("actionSets", []):
                predicates = action_set.get("routeRuleConditions", {}).get("predicates", [])
                exact_path = f"request.url_path == '{path}'"
                if exact_path not in predicates or any("x-ai-routing-candidate" in p for p in predicates):
                    continue
                auth_action = next((action for action in action_set.get("actions", [])
                                    if action.get("service") == "auth-service"), None)
                if auth_action:
                    candidates.append({
                        "action_set_name": action_set.get("name"),
                        "scope": auth_action.get("scope"),
                        "route_predicates": predicates,
                    })

    find_action_sets(config_dump)
    if len(candidates) != 1:
        return {"found": False, "candidate_count": len(candidates), "candidates": candidates}
    return {"found": True, **candidates[0]}


def auth_scopes_for_route_path(path, config_label, authconfig_snapshot,
                               selected_candidate_header=None):
    """Map the request's matching Envoy auth scopes to sanitized AuthConfigs."""
    config_path = EVIDENCE / f"envoy-{config_label}-all.json"
    if not config_path.exists():
        return {"found": False, "reason": "Envoy config dump is absent", "candidates": []}
    try:
        config_dump = json.loads(config_path.read_text())
    except json.JSONDecodeError:
        return {"found": False, "reason": "Envoy config dump is invalid JSON", "candidates": []}

    authconfigs = {item.get("name"): item for item in authconfig_snapshot.get("items", [])}
    candidates = []

    def path_predicate_matches(predicate):
        exact = re.search(r"request\.url_path\s*==\s*'([^']+)'", predicate)
        if exact:
            return path == exact.group(1)
        prefix = re.search(r"request\.url_path\.startsWith\('([^']+)'\)", predicate)
        return bool(prefix and path.startswith(prefix.group(1)))

    def find_action_sets(value):
        if isinstance(value, dict):
            for child in value.values():
                find_action_sets(child)
        elif isinstance(value, list):
            for child in value:
                find_action_sets(child)
        elif isinstance(value, str) and "actionSets" in value:
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return
            for action_set in parsed.get("actionSets", []):
                predicates = action_set.get("routeRuleConditions", {}).get("predicates", [])
                candidate_predicates = [predicate for predicate in predicates
                                        if "x-ai-routing-candidate" in predicate.lower()]
                if not any(path_predicate_matches(predicate) for predicate in predicates):
                    continue
                if candidate_predicates and (
                        not selected_candidate_header
                        or not all(selected_candidate_header in predicate
                                   for predicate in candidate_predicates)):
                    continue
                for action in action_set.get("actions", []):
                    if action.get("service") != "auth-service":
                        continue
                    scope = action.get("scope")
                    config = authconfigs.get(scope, {})
                    candidates.append({
                        "action_set_name": action_set.get("name"),
                        "scope": scope,
                        "candidate_specific": bool(candidate_predicates),
                        "selected_candidate_header": selected_candidate_header,
                        "authconfig_present": bool(config),
                        "authconfig_generation": config.get("generation"),
                        "authconfig_authentication_types": config.get("authentication_types", []),
                        "authconfig_metadata_types": config.get("metadata_types", []),
                        "authconfig_authorization_types": config.get("authorization_types", []),
                        "authconfig_status_conditions": config.get("status_conditions", []),
                        "maas_api_key_and_subscription_contract": (
                            "api-keys" in config.get("authentication_types", [])
                            and {"apiKeyValidation", "subscription-info"}.issubset(
                                set(config.get("metadata_types", [])))
                            and "subscription-valid" in config.get("authorization_types", [])),
                        "route_predicates": predicates,
                    })

    find_action_sets(config_dump)
    # Envoy config dumps can repeat the same action set under its vhost and
    # route entries; keep one evidence row per AuthConfig scope.
    unique = {(candidate.get("scope"), candidate.get("candidate_specific")): candidate
              for candidate in candidates}
    result = sorted(unique.values(), key=lambda candidate: (
        str(candidate.get("scope")), bool(candidate.get("candidate_specific"))))
    maas_contracts = [candidate for candidate in result
                      if candidate.get("maas_api_key_and_subscription_contract")]
    return {"found": bool(result), "candidate_count": len(result), "candidates": result,
            "maas_authconfig_count": len(maas_contracts),
            "candidate_specific_maas_authconfig_count": sum(
                candidate.get("candidate_specific") is True for candidate in maas_contracts)}


def auth_context_for_route_trace(path, config_label, authconfig_snapshot, route_trace,
                                 base_context=None):
    scopes = auth_scopes_for_route_path(
        path, config_label, authconfig_snapshot,
        selected_candidate_header=route_trace.get("selected_candidate_header"),
    )
    candidates = scopes.get("candidates", [])
    context = dict(base_context or {})
    context.update({
        "auth_scopes": [candidate.get("scope") for candidate in candidates if candidate.get("scope")],
        "candidate_specific_auth_scopes": [candidate.get("scope") for candidate in candidates
                                            if candidate.get("candidate_specific") and candidate.get("scope")],
        "matching_maas_authconfig_count": scopes.get("maas_authconfig_count", 0),
        "matching_candidate_maas_authconfig_count": scopes.get(
            "candidate_specific_maas_authconfig_count", 0),
        "authconfig_match_evidence": scopes,
    })
    return context


def authorino_request_trace(envoy_request_id):
    """Summarize one Authorino decision without retaining credential-bearing fields."""
    result = kubectl("-n", "kuadrant-system", "logs", "deploy/authorino", "--since=20m", check=False)
    trace = {"request_id": envoy_request_id, "exit_status": result.returncode,
             "matched_events": [], "raw_messages_saved": False}
    for line in result.stdout.splitlines():
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if str(entry.get("request id", "")) != envoy_request_id:
            continue
        event = {"level": str(entry.get("level", "unknown")).lower(),
                 "message_class": "other"}
        message = str(entry.get("msg", "")).lower()
        if message == "incoming authorization request":
            event["message_class"] = "incoming_request"
            obj = entry.get("object", {})
            request = obj.get("request", {}) if isinstance(obj, dict) else {}
            http = request.get("http", {}) if isinstance(request, dict) else {}
            headers = http.get("headers")
            if not isinstance(headers, dict):
                headers = {}
            header_names = {str(key).lower() for key in headers}
            authorization = headers.get("authorization", headers.get("Authorization", ""))
            if isinstance(authorization, list):
                authorization = authorization[0] if authorization else ""
            authorization = authorization if isinstance(authorization, str) else ""
            event["request"] = {
                **{key: http.get(key) for key in ("id", "method", "path", "host", "scheme") if key in http},
                "http_fields": sorted(http.keys()),
                "headers_field_present": isinstance(http.get("headers"), dict),
                "header_names": sorted(header_names),
                "authorization_present": "authorization" in header_names,
                "authorization_scheme": authorization.split(" ", 1)[0] if " " in authorization else None,
                "authorization_token_family": "sk-oai" if "sk-oai-" in authorization else (
                    "other" if authorization else "absent"),
                "x_api_key_present": "x-api-key" in header_names,
                "client_identity_header_names": sorted(header_names & {
                    "x-maas-username", "x-maas-group", "x-maas-identity", "x-maas-subscription",
                }),
                "header_values_saved": False,
            }
        elif message == "outgoing authorization response":
            event["message_class"] = "authorization_response"
            event["authorized"] = entry.get("authorized") if isinstance(entry.get("authorized"), bool) else None
            obj = entry.get("object", {})
            if isinstance(obj, dict):
                event["response_status"] = obj.get("status") if isinstance(obj.get("status"), int) else None
                event["response_code"] = obj.get("code") if isinstance(obj.get("code"), int) else None
                event["response_message_present"] = bool(obj.get("message"))
        elif "failed to evaluate cel expression" in message:
            event["message_class"] = "cel_evaluation_failure"
            error = str(entry.get("error", "")).lower()
            missing = re.search(r"no such key[: ]+['\"]?([a-z0-9_.-]+)", error)
            undeclared = re.search(r"undeclared reference(?: to)?[: ]+['\"]?([a-z0-9_.-]+)", error)
            if missing:
                event["error_class"] = "missing_key"
                event["missing_key"] = missing.group(1)
            elif undeclared:
                event["error_class"] = "undeclared_reference"
                event["undeclared_reference"] = undeclared.group(1)
            else:
                event["error_class"] = "other"
            expression = entry.get("expression")
            if isinstance(expression, str):
                event["expression_length"] = len(expression)
                event["expression_sha256"] = hashlib.sha256(expression.encode()).hexdigest()
        elif "failed to parse cel expression" in message:
            event["message_class"] = "cel_parse_failure"
            error = str(entry.get("error", "")).lower()
            undeclared = re.search(r"undeclared reference(?: to)?[: ]+['\"]?([a-z0-9_.-]+)", error)
            if undeclared:
                event["undeclared_reference"] = undeclared.group(1)
            expression = entry.get("expression")
            if isinstance(expression, str):
                event["expression_length"] = len(expression)
                event["expression_sha256"] = hashlib.sha256(expression.encode()).hexdigest()
        elif entry.get("level") == "error" or entry.get("error"):
            event["message_class"] = "other_authorino_error"
            error = str(entry.get("error", "")).lower()
            event["error_length"] = len(error)
            event["error_sha256"] = hashlib.sha256(error.encode()).hexdigest() if error else None
            event["error_flags"] = {
                "timeout": bool(re.search(r"timeout|deadline exceeded", error)),
                "tls": bool(re.search(r"x509|certificate|tls handshake", error)),
                "connection": bool(re.search(r"connection refused|connection reset|unreachable", error)),
                "http_status": bool(re.search(r"\b[45][0-9]{2}\b", error)),
            }
            event["message_flags"] = {
                "http": "http" in message,
                "metadata": "metadata" in message,
                "callback": "callback" in message,
                "webhook": "webhook" in message,
            }
        elif message:
            event["message_class"] = "other_authorino_event"
            event["message_length"] = len(message)
            event["message_sha256"] = hashlib.sha256(message.encode()).hexdigest()
            event["message_flags"] = {
                "http": "http" in message,
                "metadata": "metadata" in message,
                "callback": "callback" in message,
                "webhook": "webhook" in message,
            }
        else:
            continue
        trace["matched_events"].append(event)
    return trace


def gateway_auth_request_evidence(request_id, label, gateway_pod, route_trace,
                                  callbacks_before, auth_context):
    """Correlate sanitized client-header, Authorino, and MaaS callback evidence."""
    auth_input = capture_auth_policy_input_trace(request_id, label, gateway_pod)
    envoy_request_id = route_trace.get("envoy_request_id") or request_id
    authorino = authorino_request_trace(envoy_request_id)
    callbacks_after = callback_log_snapshot()
    before_events = callbacks_before.get("events", [])
    before_signatures = {
        (event.get("request_id"), event.get("timestamp"), event.get("path"), event.get("status"))
        for event in before_events
    }
    callback_delta = [
        event for event in callbacks_after.get("events", [])
        if (event.get("request_id"), event.get("timestamp"), event.get("path"), event.get("status"))
        not in before_signatures
    ]
    incoming = next((event for event in authorino.get("matched_events", [])
                     if event.get("message_class") == "incoming_request"), {})
    decision = next((event for event in reversed(authorino.get("matched_events", []))
                     if event.get("message_class") == "authorization_response"), {})
    evidence = {
        "request_id": request_id,
        "envoy_request_id": envoy_request_id,
        "route_name": route_trace.get("route_name"),
        "upstream_cluster": route_trace.get("upstream_cluster"),
        "response_flags": route_trace.get("response_flags"),
        "response_code": route_trace.get("response_code"),
        "gateway_policy": "maas-system/maas-gateway-auth",
        "selected_authconfig_scopes": auth_context.get("auth_scopes", []),
        "selected_candidate_authconfig_scopes": auth_context.get("candidate_specific_auth_scopes", []),
        "matching_maas_authconfig_count": auth_context.get("matching_maas_authconfig_count", 0),
        "matching_candidate_maas_authconfig_count": auth_context.get(
            "matching_candidate_maas_authconfig_count", 0),
        "client_header_trace": auth_input,
        "authorino": authorino,
        "authorino_incoming_authorization_present": incoming.get("request", {}).get("authorization_present"),
        "authorino_incoming_x_api_key_present": incoming.get("request", {}).get("x_api_key_present"),
        "authorino_authorized": decision.get("authorized"),
        "authorino_response_status": decision.get("response_status"),
        "maas_callback_event_count": len(callback_delta),
        "maas_callback_events": callback_delta,
        "credential_values_saved": False,
    }
    save_json(EVIDENCE / f"gateway-auth-decision-{label}.json", evidence)
    return evidence


def maas_auth_request_decision_ok(evidence, *, authorized, expected_status):
    """Require a request to be decided by the MaaS AuthPolicy and its AuthConfig."""
    header_trace = evidence.get("client_header_trace", {})
    authorino = evidence.get("authorino", {})
    status = str(evidence.get("response_code", ""))
    expected_header_present = expected_status == "200"
    return (
        status == expected_status
        and header_trace.get("captured") is True
        and header_trace.get("authorization_present") is expected_header_present
        and header_trace.get("x_api_key_present") is False
        and evidence.get("authorino_authorized") is authorized
        and (evidence.get("authorino_response_status") in (None, int(expected_status)))
        and bool(evidence.get("selected_authconfig_scopes"))
        and evidence.get("matching_maas_authconfig_count", 0) > 0
    )


def wait_for_auth_policy_diagnostic_release():
    """Pause after route/policy readiness so an external harness can add temporary response markers."""
    ready_name = os.environ.get("IPP_AUTH_POLICY_DIAGNOSTIC_READY_FILE")
    release_name = os.environ.get("IPP_AUTH_POLICY_DIAGNOSTIC_RELEASE_FILE")
    if not ready_name and not release_name:
        return
    if not ready_name or not release_name:
        raise RuntimeError("both AuthPolicy diagnostic barrier files must be configured")
    pathlib.Path(ready_name).write_text("routes-and-policies-ready\n")
    release_path = pathlib.Path(release_name)
    deadline = time.monotonic() + 180
    while not release_path.is_file():
        if time.monotonic() >= deadline:
            raise RuntimeError("timed out waiting for AuthPolicy diagnostic barrier release")
        time.sleep(0.2)


def safe_authconfig_snapshot(label, expression_hashes=None, created_after=None):
    """Capture Authorino config identity/readiness and shape without storing secrets or expressions."""
    result = kubectl("-n", "kuadrant-system", "get", "authconfigs.authorino.kuadrant.io", "-o", "json",
                     check=False)
    snapshot = {"label": label, "exit_status": result.returncode, "items": []}
    if result.returncode == 0:
        listing = json.loads(result.stdout)
        for item in listing.get("items", []):
            metadata = item.get("metadata", {})
            spec = item.get("spec", {})
            snapshot["items"].append({
                "name": metadata.get("name"),
                "generation": metadata.get("generation"),
                "created_at": metadata.get("creationTimestamp"),
                "hosts": spec.get("hosts", []),
                "authentication_types": sorted(spec.get("authentication", {}).keys()),
                "metadata_types": sorted(spec.get("metadata", {}).keys()),
                "authorization_types": sorted(spec.get("authorization", {}).keys()),
                "response_types": sorted(spec.get("response", {}).keys()),
                "status_conditions": [{key: condition.get(key) for key in
                                       ("type", "status", "reason", "observedGeneration")}
                                      for condition in item.get("status", {}).get("conditions", [])],
            })
            if created_after is not None:
                try:
                    created_at = datetime.datetime.fromisoformat(
                        str(metadata.get("creationTimestamp", "")).replace("Z", "+00:00")
                    ).timestamp()
                except ValueError:
                    created_at = None
                if created_at is not None and created_at >= created_after.timestamp():
                    expression_index = []

                    def index_expressions(value, path):
                        if isinstance(value, dict):
                            for key, child in value.items():
                                if any(token in str(key).lower() for token in (
                                    "token", "secret", "credential", "password", "headers",
                                )):
                                    continue
                                index_expressions(child, f"{path}.{key}")
                        elif isinstance(value, list):
                            for index, child in enumerate(value):
                                index_expressions(child, f"{path}[{index}]")
                        elif isinstance(value, str) and path.rsplit(".", 1)[-1].lower() in (
                            "expression", "selector", "predicate", "rego",
                        ):
                            expression_index.append({
                                "field_path": path,
                                "expression_length": len(value),
                                "expression_sha256": hashlib.sha256(value.encode()).hexdigest(),
                                "mentions_responsebodyjson": "responsebodyjson" in value.lower(),
                                "mentions_identity": bool(re.search(r"\bidentity\b", value.lower())),
                                "mentions_selected_subscription": "selected_subscription" in value.lower(),
                                "mentions_subscription_info": "subscription_info" in value.lower(),
                            })

                    index_expressions(spec, str(metadata.get("name", "unknown")))
                    snapshot["items"][-1]["created_after_route_setup"] = True
                    snapshot["items"][-1]["expression_index"] = expression_index
    policy = kubectl("-n", NAMESPACE, "get", "maasauthpolicy", "kind-e2e-access", "-o", "json",
                     check=False)
    pending = []
    if policy.returncode == 0:
        obj = json.loads(policy.stdout)
        messages = " ".join(str(condition.get("message", ""))
                            for condition in obj.get("status", {}).get("conditions", []))
        pending = re.findall(r"AuthConfig \(([0-9a-f]{64})\)", messages)
    names = {item.get("name"): item for item in snapshot["items"]}
    snapshot["referenced_pending_authconfigs"] = [
        {"name": name, "present": name in names,
         "status_conditions": names.get(name, {}).get("status_conditions", [])}
        for name in pending
    ]
    wanted_hashes = set(expression_hashes or [])
    matches = []

    def find_expression_matches(value, path):
        if isinstance(value, dict):
            for key, child in value.items():
                if any(token in str(key).lower() for token in (
                    "token", "secret", "credential", "password", "headers",
                )):
                    continue
                find_expression_matches(child, f"{path}.{key}")
        elif isinstance(value, list):
            for index, child in enumerate(value):
                find_expression_matches(child, f"{path}[{index}]")
        elif isinstance(value, str):
            digest = hashlib.sha256(value.encode()).hexdigest()
            if digest in wanted_hashes:
                matches.append({
                    "authconfig": path.split(".", 1)[0],
                    "field_path": path,
                    "expression_length": len(value),
                    "expression_sha256": digest,
                    "mentions_responsebodyjson": "responsebodyjson" in value.lower(),
                    "mentions_identity": bool(re.search(r"\bidentity\b", value.lower())),
                    "mentions_selected_subscription": "selected_subscription" in value.lower(),
                    "mentions_subscription_info": "subscription_info" in value.lower(),
                })

    for item in listing.get("items", []) if result.returncode == 0 else []:
        find_expression_matches(item.get("spec", {}), str(item.get("metadata", {}).get("name", "unknown")))
    snapshot["matched_request_expression_hashes"] = matches
    save_json(EVIDENCE / f"authorino-authconfig-snapshot-{label}.json", snapshot)
    return snapshot


def safe_authpolicy_config_snapshot(label):
    """Record the installed auth contract without retaining selectors or header values."""
    policy = kubectl("-n", "maas-system", "get", "authpolicy", "maas-gateway-auth", "-o", "json",
                     check=False)
    snapshot = {"label": label, "exit_status": policy.returncode}
    if policy.returncode == 0:
        obj = json.loads(policy.stdout)
        rules = obj.get("spec", {}).get("defaults", {}).get("rules", {})
        authentication = {}
        for name, config in rules.get("authentication", {}).items():
            plain = config.get("plain", {}) if isinstance(config, dict) else {}
            selector = plain.get("selector") if isinstance(plain, dict) else None
            selector_text = selector if isinstance(selector, str) else ""
            authentication[name] = {
                "config_keys": sorted(config.keys()) if isinstance(config, dict) else [],
                "plain_selector_present": bool(selector_text),
                "plain_selector_sha256": hashlib.sha256(selector_text.encode()).hexdigest() if selector_text else None,
                "selector_uses_authorization_header": "authorization" in selector_text.lower(),
                "selector_uses_x_api_key_header": "x-api-key" in selector_text.lower(),
                "when_rule_count": len(config.get("when", [])) if isinstance(config, dict) else 0,
            }
        metadata = {}
        for name, config in rules.get("metadata", {}).items():
            http = config.get("http", {}) if isinstance(config, dict) else {}
            parsed_url = urllib.parse.urlsplit(http.get("url", ""))
            body = http.get("body", {}) if isinstance(http, dict) else {}
            expression = body.get("expression", "") if isinstance(body, dict) else ""
            expression = expression if isinstance(expression, str) else ""
            when_predicates = []
            for condition in config.get("when", []) if isinstance(config, dict) else []:
                predicate = condition.get("predicate") if isinstance(condition, dict) else None
                if isinstance(predicate, str):
                    predicate_lower = predicate.lower()
                    when_predicates.append({
                        "length": len(predicate),
                        "sha256": hashlib.sha256(predicate.encode()).hexdigest(),
                        "mentions_auth_identity": "auth.identity" in predicate_lower,
                        "mentions_request_path": "request.path" in predicate_lower,
                        "mentions_request_method": "request.method" in predicate_lower,
                        "mentions_authorization": "authorization" in predicate_lower,
                        "mentions_x_api_key": "x-api-key" in predicate_lower,
                        "mentions_model": "model" in predicate_lower,
                    })
            metadata[name] = {
                "config_keys": sorted(config.keys()) if isinstance(config, dict) else [],
                "http_present": bool(http),
                "url_scheme": parsed_url.scheme,
                "url_host": parsed_url.hostname,
                "url_path": parsed_url.path,
                "method": http.get("method"),
                "body_expression_sha256": hashlib.sha256(expression.encode()).hexdigest() if expression else None,
                "body_expression_length": len(expression) if expression else 0,
                "body_uses_auth_identity": "auth.identity" in expression,
                "body_uses_request_authorization": "authorization" in expression.lower(),
                "when_predicates": when_predicates,
            }
        snapshot.update({
            "generation": obj.get("metadata", {}).get("generation"),
            "conditions": [{key: condition.get(key) for key in
                            ("type", "status", "reason", "observedGeneration")}
                           for condition in obj.get("status", {}).get("conditions", [])],
            "authentication": authentication,
            "metadata": metadata,
            "authorization_rule_names": sorted(rules.get("authorization", {}).keys()),
            "response_rule_names": sorted(rules.get("response", {}).keys()),
            "raw_selectors_expressions_or_credentials_saved": False,
        })
    save_json(EVIDENCE / f"gateway-authpolicy-config-{label}.json", snapshot)
    return snapshot


def auth_policy_status_snapshot():
    status = {}
    policy = kubectl("-n", "maas-system", "get", "authpolicy", "maas-gateway-auth", "-o", "json", check=False)
    status["gateway_auth_policy_exit_status"] = policy.returncode
    if policy.returncode == 0:
        obj = json.loads(policy.stdout)
        status["gateway_auth_policy"] = [
            {key: condition.get(key) for key in ("type", "status", "reason", "message", "observedGeneration")}
            for condition in obj.get("status", {}).get("conditions", [])
        ]
    maas_policy = kubectl("-n", NAMESPACE, "get", "maasauthpolicy", "kind-e2e-access", "-o", "json", check=False)
    status["maas_auth_policy_exit_status"] = maas_policy.returncode
    if maas_policy.returncode == 0:
        obj = json.loads(maas_policy.stdout)
        status["maas_auth_policy"] = [
            {key: condition.get(key) for key in ("type", "status", "reason", "message", "observedGeneration")}
            for condition in obj.get("status", {}).get("conditions", [])
        ]
    return status


def capture_provider_cluster_endpoints(label, cluster_names):
    """Record Kubernetes Service endpoints and Envoy's resolved EDS health for provider clusters."""
    gateway_pods = json.loads(kubectl(
        "-n", "maas-system", "get", "pods", "-l",
        "gateway.networking.k8s.io/gateway-name=maas-default-gateway", "-o", "json").stdout)
    gateway_pod = next((pod["metadata"]["name"] for pod in gateway_pods.get("items", [])
                        if any(condition.get("type") == "Ready" and condition.get("status") == "True"
                               for condition in pod.get("status", {}).get("conditions", []))), None)
    snapshot = {"label": label, "captured_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "provider_clusters": {}, "gateway_pod": gateway_pod}
    for cluster in sorted(set(cluster_names)):
        host = cluster.split("||")[-1]
        host_parts = host.split(".")
        service = host_parts[0]
        namespace = host_parts[1] if len(host_parts) > 1 else "maas-system"
        item = {"cluster": cluster, "service": service}
        for resource, filename in (("service", "service"), ("endpoints", "endpoints"),
                                   ("endpointslices", "endpointslices")):
            if resource == "endpointslices":
                args = ("-n", namespace, "get", resource, "-l", f"kubernetes.io/service-name={service}", "-o", "json")
            else:
                args = ("-n", namespace, "get", resource, service, "-o", "json")
            result = kubectl(*args, check=False)
            if result.returncode == 0:
                try:
                    item[filename] = json.loads(result.stdout)
                except json.JSONDecodeError:
                    item[filename] = {"parse_error": True}
            else:
                item[filename] = {"available": False, "error": result.stderr.strip()[:500]}
        snapshot["provider_clusters"][cluster] = item

    if gateway_pod:
        result = run([ISTIOCTL, "proxy-config", "endpoints", gateway_pod, "-n", "maas-system", "-o", "json"],
                     check=False, timeout=90)
        snapshot["istioctl_proxy_config_endpoints_exit"] = result.returncode
        try:
            rows = json.loads(result.stdout)
            if isinstance(rows, list):
                rows = [row for row in rows if row.get("name") in cluster_names
                        or row.get("clusterName") in cluster_names
                        or row.get("cluster_name") in cluster_names]
            snapshot["envoy_resolved_endpoints"] = rows
        except json.JSONDecodeError:
            snapshot["envoy_resolved_endpoints_text"] = (result.stdout or result.stderr)[:20000]
    save_json(EVIDENCE / f"provider-cluster-endpoints-{label}.json", snapshot)
    return snapshot


def selected_provider_route_precedence(label, profiles):
    """Check selected model routes and translated target-path precedence."""
    route_configs = json.loads((EVIDENCE / f"envoy-{label}-routes.json").read_text())
    rows = []
    for route_config in route_configs:
        for virtual_host in route_config.get("virtualHosts", []):
            for index, route in enumerate(virtual_host.get("routes", [])):
                match = route.get("match", {})
                route_action = route.get("route", {})
                per_filter = route.get("typedPerFilterConfig", {})
                extproc = per_filter.get(
                    "envoy.filters.http.ext_proc.external-model", {}
                ).get("overrides", {}).get("processingMode", {})
                pre_extproc = per_filter.get(
                    "envoy.filters.http.ext_proc.external-model-pre", {}
                ).get("overrides", {}).get("processingMode", {})
                rows.append({
                    "route_config": route_config.get("name"),
                    "virtual_host": virtual_host.get("name"),
                    "index": index,
                    "name": route.get("name"),
                    "path": match.get("path"),
                    "path_separated_prefix": match.get("pathSeparatedPrefix"),
                    "prefix": match.get("prefix"),
                    "cluster": route_action.get("cluster"),
                    "headers": match.get("headers", []),
                    "external_model_processing_mode": extproc,
                    # Envoy omits protobuf enum defaults from its admin JSON.
                    # BodySendMode.NONE is enum value zero, so an absent field
                    # means NONE; record both the wire representation and its
                    # effective mode for the provider contract check.
                    "external_model_effective_response_body_mode": extproc.get("responseBodyMode", "NONE"),
                    "external_model_pre_request_body_mode": pre_extproc.get("requestBodyMode"),
                })

    # The initial model-path request has no trusted selected-candidate header,
    # so Envoy first matches a fail-closed sink route. Selected-provider
    # translation rewrites the complete request body and therefore requires
    # BUFFERED request processing; responses remain streamed for SSE.
    fallback_routes = [row for row in rows
                       if (row.get("name") or "").startswith("models-as-a-service.external-model-demo-model.")
                       and "provider-selection-required-demo-model" in (row.get("cluster") or "")]
    fallback_routes_stream_responses = (
        len(fallback_routes) == 2
        and all(row.get("external_model_processing_mode", {}).get("requestBodyMode") == "BUFFERED"
                and row.get("external_model_effective_response_body_mode") == "STREAMED"
                and row.get("external_model_pre_request_body_mode") == "BUFFERED"
                for row in fallback_routes)
    )

    checks = []
    model_path_prefix = f"/{NAMESPACE}/demo"
    model_request_path = f"{model_path_prefix}/v1/chat/completions"
    expected = (
        ("openai-chat", "/v1/chat/completions", "provider-translation-openai-ext.maas-system.svc.cluster.local", "STREAMED"),
        ("messages", "/v1/messages", "provider-translation-ext.maas-system.svc.cluster.local", "STREAMED"),
    )
    for api_format, path, backend, response_body_mode in expected:
        # OpenAI's exact target path is an alternative match on the existing
        # body-routing rule when translation is enabled. It must precede the
        # shared exact-path route without adding another Gateway rule.
        # Envoy expands the model-header and exact-path alternatives into
        # separate route entries. Both formats therefore expose three
        # selected-provider entries after translation is enabled.
        expected_candidate_route_count = 3
        candidate = profiles.get(api_format, {})
        stable_id = candidate.get("stable_id", "")
        exact_rows = [row for row in rows if row["path"] == path]
        selected_model_path = [row for row in rows
                               if row["path_separated_prefix"] == model_path_prefix and any(
                                   header.get("name", "").lower() == "x-ai-routing-candidate"
                                   and header.get("stringMatch", {}).get("exact") == stable_id
                                   for header in row["headers"]
                               )]
        shared = [row for row in exact_rows if "translation-openai-mock.models-as-a-service.svc.cluster.local"
                  in (row["cluster"] or "")
                  and not any(header.get("name", "").lower() == "x-ai-routing-candidate"
                              for header in row["headers"])]
        selected_target_path = [row for row in exact_rows if backend in (row["cluster"] or "") and any(
            header.get("name", "").lower() == "x-ai-routing-candidate"
            and header.get("stringMatch", {}).get("exact") == stable_id
            for header in row["headers"]
        )]
        candidate_routes = [row for row in rows if backend in (row["cluster"] or "") and any(
            header.get("name", "").lower() == "x-ai-routing-candidate"
            and header.get("stringMatch", {}).get("exact") == stable_id
            for header in row["headers"]
        )]
        chosen = selected_model_path[0] if selected_model_path else {}
        public = shared[0] if shared else {}
        if api_format == "openai-chat":
            target_path_contract_valid = (
                len(selected_target_path) == 1 and bool(public)
                and selected_target_path[0]["index"] < public["index"]
                and backend in (selected_target_path[0].get("cluster") or "")
            )
        else:
            target_path_contract_valid = (
                len(selected_target_path) == 1 and bool(public)
                and selected_target_path[0]["index"] < public["index"]
                and backend in (selected_target_path[0].get("cluster") or "")
            )
        checks.append({
            "api_format": api_format,
            "path": path,
            "expected_candidate_route_count": expected_candidate_route_count,
            "candidate_route_count_matches_contract": len(candidate_routes) == expected_candidate_route_count,
            "model_request_path": model_request_path,
            "stable_id_present": bool(stable_id),
            "selected_provider_route": chosen,
            "selected_model_path_routes": selected_model_path,
            "selected_target_path_routes": selected_target_path,
            "selected_provider_routes": candidate_routes,
            "selection_fallback_routes": fallback_routes,
            "fallback_routes_stream_response_bodies": fallback_routes_stream_responses,
            "shared_exact_route": public,
            "selected_route_points_to_expected_backend": backend in (chosen.get("cluster") or ""),
            "target_path_match_contract_valid": target_path_contract_valid,
            "selected_route_contract_valid": (
                backend in (chosen.get("cluster") or "")
                and chosen.get("path_separated_prefix") == model_path_prefix
                and target_path_contract_valid
            ),
            "body_modes_match_provider_contract": (
                len(candidate_routes) == expected_candidate_route_count
                and all(row.get("external_model_processing_mode", {}).get("requestBodyMode") == "BUFFERED"
                        and row.get("external_model_effective_response_body_mode") == response_body_mode
                        and row.get("external_model_pre_request_body_mode") == "BUFFERED"
                        for row in candidate_routes)
            ),
            "pre_auth_model_body_buffered": (
                len(candidate_routes) == expected_candidate_route_count
                and all(row.get("external_model_pre_request_body_mode") == "BUFFERED"
                        for row in candidate_routes)
            ),
            "selected_route_and_shared_route_use_same_virtual_host": bool(
                chosen and public and chosen["route_config"] == public["route_config"]
                and chosen["virtual_host"] == public["virtual_host"]
            ),
        })
    save_json(EVIDENCE / f"route-collision-{label}.json", {"checks": checks})
    return checks


def overlay_state(label):
    cm = json.loads(kubectl("-n", NAMESPACE, "get", "configmap", "routing-overlay", "-o", "json").stdout)
    content = json.loads(cm.get("data", {}).get("routing-overlay.json", "{}"))
    state = {
        "content_digest": cm.get("metadata", {}).get("annotations", {}).get(
            "inference.opendatahub.io/routing-overlay-content-digest", ""),
        "generation": cm.get("metadata", {}).get("annotations", {}).get(
            "inference.opendatahub.io/routing-overlay-source-generation", ""),
        "candidate_clusters": sorted({c.get("cluster") for c in content.get("overlay", {}).get("candidates", [])}),
        "content": content,
    }
    save_json(EVIDENCE / f"externalmodel-overlay-{label}.json", state)
    return state


def wait_model_phase(expected):
    for _ in range(90):
        result = kubectl("-n", NAMESPACE, "get", "externalmodel", "demo-model", "-o", "json", check=False)
        if result.returncode == 0:
            obj = json.loads(result.stdout)
            if obj.get("status", {}).get("phase") == expected:
                return obj
        time.sleep(1)
    raise RuntimeError(f"ExternalModel demo-model did not reach phase {expected}")


def test_externalmodel_provider_aware_translation(auth_file, gateway_auth_context):
    global EXTERNAL_MODEL_ORIGINAL_REFS, EXTERNAL_MODEL_ORIGINAL_OVERLAY, EXTERNAL_MODEL_CHANGED, PROVIDER_CA_FILE
    global PROVIDER_RECORDER_FORWARD, PROVIDER_RECORDER_PORT, PROVIDER_AWARE_CREATED
    original = json.loads(kubectl("-n", NAMESPACE, "get", "externalmodel", "demo-model", "-o", "json").stdout)
    EXTERNAL_MODEL_ORIGINAL_REFS = original.get("spec", {}).get("externalProviderRefs", [])
    save_json(EVIDENCE / "externalmodel-before-provider-aware-test.json", original)
    EXTERNAL_MODEL_ORIGINAL_OVERLAY = overlay_state("before-provider-aware-test")
    # Mark the fixture as cleanup-owned before creating any resources. If
    # setup fails before the first ExternalModel patch, cleanup can still
    # verify the unchanged overlay and remove the partially created fixture.
    EXTERNAL_MODEL_CHANGED = True
    externalmodel_url = f"http://127.0.0.1:{GATEWAY_PORT}/{NAMESPACE}/demo/v1/chat/completions"
    names = {
        "openai_provider": "api-translation-openai",
        "anthropic_provider": "api-translation-anthropic",
        "bedrock_provider": "api-translation-bedrock-mantle",
        "openai_secret": "api-translation-openai-credentials",
        "anthropic_secret": "api-translation-anthropic-credentials",
        "bedrock_secret": "api-translation-bedrock-credentials",
        "openai_backend_secret": "api-translation-openai-backend-credential",
        "anthropic_backend_secret": "api-translation-anthropic-backend-credential",
        "bedrock_backend_secret": "api-translation-bedrock-backend-credential",
    }
    for kind, namespace, name in (
        ("externalprovider", NAMESPACE, names["openai_provider"]),
        ("externalprovider", NAMESPACE, names["anthropic_provider"]),
        ("externalprovider", NAMESPACE, names["bedrock_provider"]),
        ("secret", NAMESPACE, names["openai_secret"]), ("secret", NAMESPACE, names["anthropic_secret"]),
        ("secret", NAMESPACE, names["bedrock_secret"]),
        ("secret", "maas-system", names["openai_backend_secret"]),
        ("secret", "maas-system", names["anthropic_backend_secret"]),
        ("secret", "maas-system", names["bedrock_backend_secret"]),
        ("deployment", "maas-system", "api-translation-provider-aware-mock"),
        ("service", "maas-system", "provider-translation-ext"),
        ("service", "maas-system", "provider-translation-openai-ext"),
        ("service", "maas-system", "provider-translation-bedrock-ext"),
    ):
        resource = f"{kind}/{name}"
        if kubectl("-n", namespace, "get", resource, check=False).returncode == 0:
            raise RuntimeError(f"run-owned provider-aware fixture resource already exists: {namespace}/{resource}")

    PROVIDER_AWARE_CREATED = True
    openai_key = secrets.token_urlsafe(32)
    anthropic_key = secrets.token_urlsafe(32)
    bedrock_key = secrets.token_urlsafe(32)
    PROVIDER_TEST_KEYS.extend([openai_key, anthropic_key, bedrock_key])
    create_test_secret(NAMESPACE, names["openai_secret"], openai_key)
    create_test_secret(NAMESPACE, names["anthropic_secret"], anthropic_key)
    create_test_secret(NAMESPACE, names["bedrock_secret"], bedrock_key)
    create_test_secret("maas-system", names["openai_backend_secret"], openai_key)
    create_test_secret("maas-system", names["anthropic_backend_secret"], anthropic_key)
    create_test_secret("maas-system", names["bedrock_backend_secret"], bedrock_key)

    provider_ca = kubectl("-n", "maas-system", "get", "configmap", "external-model-e2e-provider-ca",
                          "-o", r"jsonpath={.data.ca\.crt}").stdout
    PROVIDER_CA_FILE = TEMP / "provider-ca.crt"
    PROVIDER_CA_FILE.write_text(provider_ca)
    PROVIDER_CA_FILE.chmod(0o600)
    backend = (ROOT / "test/kind-env/manifests/api-translation-provider-aware-backend.yaml").read_text()
    backend = backend.replace("provider-recorder:issue28-kind", IMAGE)
    run(["kubectl", "--context", f"kind-{CLUSTER}", "apply", "-f", "-"], input_text=backend)
    kubectl("-n", "maas-system", "rollout", "status", "deployment/api-translation-provider-aware-mock", "--timeout=120s")
    PROVIDER_RECORDER_FORWARD, PROVIDER_RECORDER_PORT = start_port_forward("provider-translation-ext", 443)

    providers = [
        {
            "apiVersion": "inference.opendatahub.io/v1alpha1",
            "kind": "ExternalProvider",
            "metadata": {"name": names["openai_provider"], "namespace": NAMESPACE,
                         "labels": {"app.kubernetes.io/managed-by": "api-translation-kind-e2e"}},
            "spec": {
                "provider": "openai",
                "endpoint": "provider-translation-openai-ext.maas-system.svc.cluster.local",
                "auth": {"type": "apikey", "secretRef": {"name": names["openai_secret"]}},
                "config": {"tls.caCertificates": "/etc/external-model-e2e/provider-ca/ca.crt"},
            },
        },
        {
            "apiVersion": "inference.opendatahub.io/v1alpha1",
            "kind": "ExternalProvider",
            "metadata": {"name": names["anthropic_provider"], "namespace": NAMESPACE,
                         "labels": {"app.kubernetes.io/managed-by": "api-translation-kind-e2e"}},
            "spec": {
                "provider": "anthropic",
                "endpoint": "provider-translation-ext.maas-system.svc.cluster.local",
                "auth": {"type": "apikey", "secretRef": {"name": names["anthropic_secret"]}},
                "config": {"tls.caCertificates": "/etc/external-model-e2e/provider-ca/ca.crt"},
            },
        },
        {
            "apiVersion": "inference.opendatahub.io/v1alpha1",
            "kind": "ExternalProvider",
            "metadata": {"name": names["bedrock_provider"], "namespace": NAMESPACE,
                         "labels": {"app.kubernetes.io/managed-by": "api-translation-kind-e2e"}},
            "spec": {
                "provider": "bedrock-openai",
                "endpoint": "provider-translation-bedrock-ext.maas-system.svc.cluster.local",
                "auth": {"type": "apikey", "secretRef": {"name": names["bedrock_secret"]}},
                "config": {"tls.caCertificates": "/etc/external-model-e2e/provider-ca/ca.crt"},
            },
        },
    ]
    for provider in providers:
        kubectl("-n", NAMESPACE, "apply", "-f", "-", input_text=json.dumps(provider))
    for name in (names["openai_provider"], names["anthropic_provider"], names["bedrock_provider"]):
        ready_provider = None
        for _ in range(120):
            result = kubectl("-n", NAMESPACE, "get", "externalprovider", name, "-o", "json", check=False)
            if result.returncode == 0:
                ready_provider = json.loads(result.stdout)
                if ready_provider.get("status", {}).get("phase") == "Ready" and any(
                    condition.get("type") == "Ready" and condition.get("status") == "True"
                    for condition in ready_provider.get("status", {}).get("conditions", [])):
                    break
            time.sleep(1)
        if not ready_provider or ready_provider.get("status", {}).get("phase") != "Ready":
            save_json(EVIDENCE / f"externalprovider-{name}-not-ready.json", ready_provider or {})
            raise RuntimeError(f"ExternalProvider {name} did not become Ready")

    refs = [
        {"ref": {"name": names["openai_provider"]}, "targetModel": "mock-openai-model",
         "apiFormat": "openai-chat", "path": "/v1/chat/completions", "weight": 1},
        {"ref": {"name": names["anthropic_provider"]}, "targetModel": "mock-anthropic-model",
         "apiFormat": "messages", "path": "/v1/messages", "weight": 1},
    ]
    EXTERNAL_MODEL_CHANGED = True
    kubectl("-n", NAMESPACE, "patch", "externalmodel", "demo-model", "--type=merge", "-p",
            json.dumps({"spec": {"externalProviderRefs": refs}}))
    both = wait_externalmodel_revision({"provider-api-translation-openai", "provider-api-translation-anthropic"},
                                       "mixed-equal-weight")
    verify_externalmodel_extproc_image()
    profiles = {candidate.get("api_format"): candidate for candidate in both["demo_model_candidates"]}
    record("externalmodel_publishes_trusted_provider_profiles",
           set(profiles) == {"openai-chat", "messages"}
           and all(candidate.get("stable_id") and candidate.get("path") and candidate.get("target_model")
                   for candidate in profiles.values()),
           {"api_formats": sorted(profiles), "candidate_clusters": both["demo_model_candidate_clusters"],
            "profiles_include_stable_id_path_model": True})
    capture_envoy("externalmodel-pre-traffic")
    authconfig_snapshot = safe_authconfig_snapshot("externalmodel-before-auth-controls")
    route_auth_context = dict(gateway_auth_context)
    initial_auth_scopes = auth_scopes_for_route_path(
        "/models-as-a-service/demo/v1/chat/completions",
        "externalmodel-pre-traffic", authconfig_snapshot,
    )
    route_auth_context.update({
        "auth_scopes": [candidate.get("scope") for candidate in initial_auth_scopes.get("candidates", [])
                        if candidate.get("scope")],
        "matching_maas_authconfig_count": initial_auth_scopes.get("maas_authconfig_count", 0),
        "authconfig_match_evidence": initial_auth_scopes,
    })
    save_json(EVIDENCE / "externalmodel-selected-authconfig-before-traffic.json", {
        "gateway_policy": gateway_auth_context.get("gateway_policy_identity"),
        "maas_auth_policy": gateway_auth_context.get("maas_auth_policy"),
        "route_auth_policies": gateway_auth_context.get("route_auth_policies"),
        "route_scopes": initial_auth_scopes,
        "authconfig_snapshot": authconfig_snapshot,
    })
    authconfig_contract_ready = (
        gateway_auth_context.get("gateway_policy_accepted") is True
        and gateway_auth_context.get("gateway_policy_enforced") is True
        and gateway_auth_context.get("maas_auth_policy_ready") is True
        and gateway_auth_context.get("maas_auth_policy_no_conflict") is True
        and gateway_auth_context.get("route_auth_policies_absent") is True
        and initial_auth_scopes.get("maas_authconfig_count", 0) > 0
        and all(candidate.get("authconfig_present")
                and candidate.get("maas_api_key_and_subscription_contract")
                and any(condition.get("type") == "Ready" and condition.get("status") == "True"
                        for condition in candidate.get("authconfig_status_conditions", []))
                for candidate in initial_auth_scopes.get("candidates", []))
    )
    record("externalmodel_authconfig_is_from_maas_gateway_policy",
           authconfig_contract_ready,
           {"gateway_policy": gateway_auth_context.get("gateway_policy_identity"),
            "maas_auth_policy_ready": gateway_auth_context.get("maas_auth_policy_ready"),
            "maas_auth_policy_no_conflict": gateway_auth_context.get("maas_auth_policy_no_conflict"),
            "route_auth_policies": gateway_auth_context.get("route_auth_policies"),
            "authconfig_scopes": initial_auth_scopes})
    route_precedence = selected_provider_route_precedence("externalmodel-pre-traffic", profiles)
    send_body_before_header = externalmodel_send_body_before_header_flags("externalmodel-pre-traffic")
    translation_body_contract = (
        len(route_precedence) == 2
        and all(check["body_modes_match_provider_contract"] for check in route_precedence)
        and all(check["fallback_routes_stream_response_bodies"] for check in route_precedence)
        and send_body_before_header
        and all(flag is not True for flag in send_body_before_header)
    )
    record("externalmodel_pre_auth_model_extraction_and_post_auth_body_modes",
           translation_body_contract,
           {"route_request_modes": [
               {"route": check.get("selected_provider_route", {}).get("name"),
                "pre_request_body_mode": check.get("selected_provider_route", {}).get("external_model_pre_request_body_mode"),
                "post_request_body_mode": check.get("selected_provider_route", {}).get("external_model_processing_mode", {}).get("requestBodyMode"),
                "post_response_body_mode_wire_value": check.get("selected_provider_route", {}).get("external_model_processing_mode", {}).get("responseBodyMode"),
                "post_response_body_mode_effective": check.get("selected_provider_route", {}).get("external_model_effective_response_body_mode")}
               for check in route_precedence],
            "pre_auth_body_is_buffered_for_model_extraction": (
                len(route_precedence) == 2 and all(check["pre_auth_model_body_buffered"] for check in route_precedence)),
            "post_auth_bodies_buffered_for_selected_provider_translation": (
                len(route_precedence) == 2
                and all(check["body_modes_match_provider_contract"]
                        and check["fallback_routes_stream_response_bodies"] for check in route_precedence)),
            "send_body_without_waiting_for_header_response": send_body_before_header,
            "dedicated_extproc_accumulation_limit_bytes": 10 * 1024 * 1024,
            "configured_translation_filter_request_limit_bytes": 1024 * 1024,
            "Praxis_request_limit_enforcement_proven": False,
            "scope": "controller-owned ExternalModel routes"})
    record("externalmodel_selected_routes_respect_shared_route_contract",
           len(route_precedence) == 2 and all(
               check["selected_route_points_to_expected_backend"]
               and check["body_modes_match_provider_contract"]
               and check["fallback_routes_stream_response_bodies"]
               and check["selected_route_contract_valid"]
               and check["target_path_match_contract_valid"]
               and check["selected_route_and_shared_route_use_same_virtual_host"]
               for check in route_precedence),
           {"route_checks": route_precedence,
            "request_path_before_provider_rewrite": "/models-as-a-service/demo/v1/chat/completions",
            "selected_paths_after_translation": {"messages": "/v1/messages", "openai-chat": "/v1/chat/completions"}})
    route_trace_gateway_pod = install_runtime_route_trace()
    route_trace_gateway_pod = wait_gateway_ready_stable("externalmodel-before-traffic")
    refresh_gateway_port_forward("externalmodel-before-traffic")
    externalmodel_url = f"http://127.0.0.1:{GATEWAY_PORT}/{NAMESPACE}/demo/v1/chat/completions"
    no_key_marker = "externalmodel-no-maas-client-key-bedrock-route"
    no_key_request_id = f"{ROUTE_TRACE_PREFIX}no-maas-client-key"
    no_key_callbacks_before = callback_log_snapshot()
    no_key_rc, no_key_status, _ = curl_request(
        "externalmodel-no-maas-client-key", externalmodel_url,
        {"model": "demo", "messages": [{"role": "user", "content": no_key_marker}]},
        extra_headers=[f"x-ipp-diagnostic-id: {no_key_request_id}"],
    )
    no_key_route_trace = capture_runtime_route_trace(
        no_key_request_id, "externalmodel-no-maas-client-key", route_trace_gateway_pod)
    no_key_auth_evidence = gateway_auth_request_evidence(
        no_key_request_id, "externalmodel-no-maas-client-key", route_trace_gateway_pod,
        no_key_route_trace, no_key_callbacks_before, route_auth_context)
    no_key_marker_observed = any(
        no_key_marker in item.get("message_text", []) for item in provider_aware_observations())
    no_key_auth_ok = (
        no_key_rc == 0 and no_key_status == "401" and not no_key_marker_observed
        and maas_auth_request_decision_ok(
            no_key_auth_evidence, authorized=False, expected_status="401")
    )
    record("externalmodel_no_maas_client_key_gets_401_before_provider_traffic",
           no_key_auth_ok,
           {"http_status": no_key_status, "curl_exit_status": no_key_rc,
            "provider_marker_observed": no_key_marker_observed,
            "gateway_auth_evidence": no_key_auth_evidence,
            "rejected_by": "maas-system/maas-gateway-auth"})
    provider_route_clusters = [
        check["selected_provider_route"].get("cluster")
        for check in route_precedence if check.get("selected_provider_route", {}).get("cluster")
    ]
    route_configs = json.loads((EVIDENCE / "envoy-externalmodel-pre-traffic-routes.json").read_text())
    fallback_clusters = [
        route.get("route", {}).get("cluster", "")
        for route_config in route_configs
        for virtual_host in route_config.get("virtualHosts", [])
        for route in virtual_host.get("routes", [])
        if (route.get("name") or "").startswith("models-as-a-service.external-model-demo-model.")
        and "provider-selection-required-demo-model" in route.get("route", {}).get("cluster", "")
    ]
    routing_diagnostic_clusters = sorted(set(provider_route_clusters + fallback_clusters))
    capture_provider_cluster_endpoints("before-mixed-provider-samples", routing_diagnostic_clusters)
    route_trace_gateway_pod = wait_gateway_ready_stable("externalmodel-before-valid-request")
    refresh_gateway_port_forward("externalmodel-before-valid-request")
    externalmodel_url = f"http://127.0.0.1:{GATEWAY_PORT}/{NAMESPACE}/demo/v1/chat/completions"
    base_pod_uids = sorted(pod["uid"] for pod in both["pods"] if pod["ready"] and not pod["deleting"])

    mixed_counts = {"openai-chat": 0, "messages": 0}
    mixed_samples = []
    for index in range(24):
        marker = f"externalmodel-mixed-{index}"
        request_id = f"{ROUTE_TRACE_PREFIX}mixed-{index}"
        # Anthropic Messages requires max_tokens even when the next selected
        # candidate is OpenAI-compatible. Keep the mixed request valid for
        # both provider dialects so selection, rather than input validation,
        # determines the upstream path.
        body = {"model": "demo", "max_tokens": 32,
                "messages": [{"role": "user", "content": marker}]}
        spoof = "x-provider-format: messages" if index % 2 == 0 else "x-provider-format: openai-chat"
        callbacks_before = callback_log_snapshot() if index == 0 else None
        rc, status, raw = curl_request(f"externalmodel-mixed-{index}", externalmodel_url, body,
                                       auth_file=auth_file,
                                       extra_headers=[spoof, f"x-ipp-diagnostic-id: {request_id}"])
        route_trace = capture_runtime_route_trace(request_id, f"mixed-{index}", route_trace_gateway_pod)
        if index == 0:
            valid_auth_context = auth_context_for_route_trace(
                "/models-as-a-service/demo/v1/chat/completions", "externalmodel-pre-traffic",
                authconfig_snapshot, route_trace, route_auth_context)
            valid_auth_evidence = gateway_auth_request_evidence(
                request_id, "externalmodel-first-valid-maas-key", route_trace_gateway_pod,
                route_trace, callbacks_before, valid_auth_context)
            callback_paths = {event.get("path") for event in
                              valid_auth_evidence.get("maas_callback_events", [])
                              if str(event.get("status", "")).startswith("2")}
            valid_auth_ok = (
                status == "200"
                and maas_auth_request_decision_ok(
                    valid_auth_evidence, authorized=True, expected_status="200")
                and valid_auth_evidence.get("matching_candidate_maas_authconfig_count", 0) > 0
                and {"/internal/v1/api-keys/validate", "/internal/v1/subscriptions/select"}.issubset(
                    callback_paths)
            )
            record("externalmodel_valid_maas_key_allowed_by_selected_maas_authconfig",
                   valid_auth_ok,
                   {"http_status": status,
                    "gateway_policy": "maas-system/maas-gateway-auth",
                    "gateway_auth_evidence": valid_auth_evidence,
                    "successful_maas_callback_paths": sorted(callback_paths)})
        if status != "200":
            mixed_samples.append({"index": index, "status": status,
                                  "runtime_route_name": route_trace.get("route_name"),
                                  "runtime_upstream_cluster": route_trace.get("upstream_cluster"),
                                  "runtime_response_flags": route_trace.get("response_flags")})
            break
        route_trace_ok = bool(route_trace.get("captured") and route_trace.get("route_name")
                              and route_trace.get("upstream_cluster") and route_trace.get("response_flags") is not None)
        if status != "200" or not route_trace_ok:
            capture_provider_cluster_endpoints(f"after-mixed-{index}", routing_diagnostic_clusters)
        try:
            downstream = json.loads(raw)
        except json.JSONDecodeError:
            downstream = {}
        try:
            observation = wait_provider_observation(lambda item: marker in item.get("message_text", []), marker)
        except Exception:
            capture_provider_cluster_endpoints(f"after-mixed-{index}-no-recorder-observation", routing_diagnostic_clusters)
            raise
        api_format = "messages" if observation.get("path") == "/v1/messages" else "openai-chat"
        mixed_counts[api_format] += 1
        expected_cluster = (
            "provider-translation-ext.maas-system.svc.cluster.local"
            if api_format == "messages" else
            "provider-translation-openai-ext.maas-system.svc.cluster.local"
        )
        route_provider_ok = expected_cluster in route_trace.get("upstream_cluster", "")
        response_ok = isinstance(downstream.get("choices"), list) and bool(downstream["choices"])
        transport_ok = (observation.get("tls") is True and observation.get("sni_ok") is True
                        and observation.get("authority_ok") is True and observation.get("credential_ok") is True)
        format_auth_ok = (
            observation.get("path") == "/v1/messages"
            and observation.get("model") == "mock-anthropic-model"
            and observation.get("x_api_key_present") is True
            and observation.get("authorization_present") is False
        ) if api_format == "messages" else (
            observation.get("path") == "/v1/chat/completions"
            and observation.get("model") == "mock-openai-model"
            and observation.get("authorization_present") is True
            and observation.get("authorization_scheme") == "Bearer"
        )
        mixed_samples.append({"index": index, "format": api_format, "status": status,
                              "upstream_path": observation.get("path"), "upstream_model": observation.get("model"),
                              "upstream_authority": observation.get("authority"),
                              "upstream_diagnostic_id": observation.get("diagnostic_id"),
                              "upstream_candidate": observation.get("routing_candidate"),
                              "upstream_revision": observation.get("routing_revision"),
                              "upstream_routing_request_id": observation.get("routing_request_id"),
                              "diagnostic_id_correlates": observation.get("diagnostic_id") == request_id,
                              "routing_request_id_correlates": (
                                  bool(route_trace.get("routing_request_id"))
                                  and route_trace.get("routing_request_id") == observation.get("routing_request_id")
                              ),
                              "runtime_path_authority_match": (
                                  route_trace.get("path_at_access_log") == observation.get("path")
                                  and route_trace.get("authority") == observation.get("authority")
                              ),
                              "transport_auth_ok": transport_ok, "format_auth_ok": format_auth_ok,
                              "response_ok": response_ok, "runtime_route_trace_ok": route_trace_ok,
                              "runtime_provider_route_ok": route_provider_ok,
                              "runtime_route_name": route_trace.get("route_name"),
                              "runtime_upstream_cluster": route_trace.get("upstream_cluster"),
                              "runtime_response_flags": route_trace.get("response_flags")})
        if rc != 0 or status != "200" or not response_ok or not transport_ok or not format_auth_ok:
            break
    save_json(EVIDENCE / "externalmodel-mixed-provider-samples.json", mixed_samples)
    record("externalmodel_mixed_providers_route_and_translate_per_selection",
           len(mixed_samples) == 24 and all(sample["status"] == "200" and sample["response_ok"]
               and sample["transport_auth_ok"] and sample["format_auth_ok"]
               and sample["runtime_route_trace_ok"] and sample["runtime_provider_route_ok"]
               and sample["diagnostic_id_correlates"] and sample["runtime_path_authority_match"]
               for sample in mixed_samples)
           and all(count > 0 for count in mixed_counts.values()),
           {"requests": len(mixed_samples), "provider_counts": mixed_counts,
            "exact_split_required": False, "client_provider_format_header_trusted": False})

    openai_active = [dict(refs[0], weight=1), dict(refs[1], weight=0)]
    anthro_active = [dict(refs[0], weight=0), dict(refs[1], weight=1)]
    kubectl("-n", NAMESPACE, "patch", "externalmodel", "demo-model", "--type=merge", "-p",
            json.dumps({"spec": {"externalProviderRefs": openai_active}}))
    openai_runtime = wait_externalmodel_revision({"provider-api-translation-openai"}, "forced-openai",
                                                 changed_from=both["published_digest"])
    forced_openai_marker = "externalmodel-forced-openai-first"
    rc, forced_openai_status, forced_openai_raw = curl_request(
        "externalmodel-forced-openai-first", externalmodel_url,
        {"model": "demo", "messages": [{"role": "user", "content": forced_openai_marker}]},
        auth_file=auth_file,
        extra_headers=[
            "x-provider-format: messages",
            f"X-AI-Routing-Candidate: {profiles['messages']['stable_id']}",
            "x-tenant-id: spoofed-other-tenant",
            "x-ai-tenant-id: spoofed-other-tenant",
            f"x-ipp-diagnostic-id: {ROUTE_TRACE_PREFIX}forced-openai",
        ],
    )
    forced_openai_route = capture_runtime_route_trace(
        f"{ROUTE_TRACE_PREFIX}forced-openai", "forced-openai", route_trace_gateway_pod)
    try:
        forced_openai_response = json.loads(forced_openai_raw)
    except json.JSONDecodeError:
        forced_openai_response = {}
    forced_openai_observation = wait_provider_observation(
        lambda item: forced_openai_marker in item.get("message_text", []), "forced OpenAI selection")
    openai_served = wait_externalmodel_revision({"provider-api-translation-openai"},
        "forced-openai-after-request", changed_from=both["published_digest"])
    openai_route_ok = (
        forced_openai_route.get("captured") is True
        and "external-model-demo-model" in forced_openai_route.get("route_name", "")
        and "provider-translation-openai-ext.maas-system.svc.cluster.local"
            in forced_openai_route.get("upstream_cluster", "")
        and forced_openai_route.get("response_flags") == "-"
        and bool(forced_openai_route.get("upstream_host"))
    )
    openai_forced_ok = (
        rc == 0 and forced_openai_status == "200"
        and bool(forced_openai_response.get("choices"))
        and forced_openai_observation.get("path") == "/v1/chat/completions"
        and forced_openai_observation.get("model") == "mock-openai-model"
        and forced_openai_observation.get("credential_ok") is True
        and forced_openai_observation.get("tls") is True
        and forced_openai_observation.get("sni_ok") is True
        and forced_openai_observation.get("authority_ok") is True
        and openai_route_ok
        and all(pod["accepted_revision"] == openai_runtime["published_digest"]
                and pod["serving_revision"] == openai_runtime["published_digest"]
                for pod in openai_served["pods"] if pod["ready"] and not pod["deleting"])
    )

    previous_digest = openai_runtime["published_digest"]
    kubectl("-n", NAMESPACE, "patch", "externalmodel", "demo-model", "--type=merge", "-p",
            json.dumps({"spec": {"externalProviderRefs": anthro_active}}))
    anthro_runtime = wait_externalmodel_revision({"provider-api-translation-anthropic"},
        "forced-anthropic", changed_from=previous_digest)
    forced_anthropic_marker = "externalmodel-forced-anthropic-second"
    rc, forced_anthropic_status, forced_anthropic_raw = curl_request(
        "externalmodel-forced-anthropic-second", externalmodel_url,
        {"model": "demo", "max_tokens": 32,
         "messages": [{"role": "user", "content": forced_anthropic_marker}]},
        auth_file=auth_file,
        extra_headers=[
            "x-provider-format: openai-chat",
            f"X-AI-Routing-Candidate: {profiles['openai-chat']['stable_id']}",
            "x-tenant-id: spoofed-other-tenant",
            "x-ai-tenant-id: spoofed-other-tenant",
            f"x-ipp-diagnostic-id: {ROUTE_TRACE_PREFIX}forced-anthropic",
        ],
    )
    forced_anthropic_route = capture_runtime_route_trace(
        f"{ROUTE_TRACE_PREFIX}forced-anthropic", "forced-anthropic", route_trace_gateway_pod)
    try:
        forced_anthropic_response = json.loads(forced_anthropic_raw)
    except json.JSONDecodeError:
        forced_anthropic_response = {}
    forced_anthropic_observation = wait_provider_observation(
        lambda item: forced_anthropic_marker in item.get("message_text", []), "forced Anthropic selection")
    anthro_served = wait_externalmodel_revision({"provider-api-translation-anthropic"},
        "forced-anthropic-after-request", changed_from=previous_digest)
    anthro_route_ok = (
        forced_anthropic_route.get("captured") is True
        and "external-model-demo-model" in forced_anthropic_route.get("route_name", "")
        and "provider-translation-ext.maas-system.svc.cluster.local"
            in forced_anthropic_route.get("upstream_cluster", "")
        and forced_anthropic_route.get("response_flags") == "-"
        and bool(forced_anthropic_route.get("upstream_host"))
    )
    anthro_forced_ok = (
        rc == 0 and forced_anthropic_status == "200"
        and bool(forced_anthropic_response.get("choices"))
        and forced_anthropic_observation.get("path") == "/v1/messages"
        and forced_anthropic_observation.get("model") == "mock-anthropic-model"
        and forced_anthropic_observation.get("credential_ok") is True
        and forced_anthropic_observation.get("tls") is True
        and forced_anthropic_observation.get("sni_ok") is True
        and forced_anthropic_observation.get("authority_ok") is True
        and anthro_route_ok
        and all(pod["accepted_revision"] == anthro_runtime["published_digest"]
                and pod["serving_revision"] == anthro_runtime["published_digest"]
                for pod in anthro_served["pods"] if pod["ready"] and not pod["deleting"])
    )
    record("externalmodel_opposite_provider_selections_on_consecutive_requests",
           openai_forced_ok and anthro_forced_ok,
           {"request_order": ["OpenAI-compatible", "Anthropic Messages"],
            "openai": {"http_status": forced_openai_status,
                       "upstream_path": forced_openai_observation.get("path"),
                       "upstream_model": forced_openai_observation.get("model"),
                       "credential_tls_sni_authority_ok": openai_forced_ok,
                       "runtime_route_name": forced_openai_route.get("route_name"),
                       "runtime_upstream_cluster": forced_openai_route.get("upstream_cluster"),
                       "serving_revision": openai_runtime["published_digest"]},
            "anthropic": {"http_status": forced_anthropic_status,
                          "upstream_path": forced_anthropic_observation.get("path"),
                          "upstream_model": forced_anthropic_observation.get("model"),
                          "credential_tls_sni_authority_ok": anthro_forced_ok,
                          "runtime_route_name": forced_anthropic_route.get("route_name"),
                          "runtime_upstream_cluster": forced_anthropic_route.get("upstream_cluster"),
                          "serving_revision": anthro_runtime["published_digest"]},
            "client_provider_and_tenant_headers_ignored": openai_forced_ok and anthro_forced_ok})
    record("externalmodel_selection_changes_without_pod_restart",
           sorted(pod["uid"] for pod in openai_served["pods"] if pod["ready"] and not pod["deleting"]) == base_pod_uids
           and sorted(pod["uid"] for pod in anthro_served["pods"] if pod["ready"] and not pod["deleting"]) == base_pod_uids,
           {"before_pod_uids": base_pod_uids,
            "after_openai_pod_uids": sorted(pod["uid"] for pod in openai_served["pods"] if pod["ready"] and not pod["deleting"]),
            "after_anthropic_pod_uids": sorted(pod["uid"] for pod in anthro_served["pods"] if pod["ready"] and not pod["deleting"]),
            "previous_candidates": both["demo_model_candidate_clusters"],
            "openai_candidates": openai_runtime["demo_model_candidate_clusters"],
            "anthropic_candidates": anthro_runtime["demo_model_candidate_clusters"]})

    kubectl("-n", NAMESPACE, "patch", "externalmodel", "demo-model", "--type=merge", "-p",
            json.dumps({"spec": {"externalProviderRefs": openai_active}}))
    openai_regression_runtime = wait_externalmodel_revision(
        {"provider-api-translation-openai"}, "openai-cross-path-regressions",
        changed_from=anthro_runtime["published_digest"])

    # The OpenAI-compatible profile uses the same bounded selected-upstream
    # request phase, but provider errors must pass through in their original
    # OpenAI shape. Client format headers do not change the selected profile.
    openai_error_marker = "externalmodel-openai-provider-error"
    openai_error_request_id = f"{ROUTE_TRACE_PREFIX}openai-error"
    rc, openai_error_status, openai_error_raw = curl_request(
        "externalmodel-openai-provider-error", externalmodel_url,
        {"model": "demo", "messages": [{"role": "user", "content": openai_error_marker}]},
        auth_file=auth_file,
        extra_headers=["x-provider-format: messages", f"x-ipp-diagnostic-id: {openai_error_request_id}"],
    )
    try:
        openai_error_response = json.loads(openai_error_raw)
    except json.JSONDecodeError:
        openai_error_response = {}
    openai_error_observation = wait_provider_observation(
        lambda item: openai_error_marker in item.get("message_text", []), "OpenAI-compatible provider error")
    openai_error_route = capture_runtime_route_trace(
        openai_error_request_id, "openai-provider-error", route_trace_gateway_pod)
    record("externalmodel_openai_provider_error_passes_through",
           rc == 0 and openai_error_status == "429"
           and openai_error_response.get("error", {}).get("type") == "rate_limit_error"
           and openai_error_observation.get("path") == "/v1/chat/completions"
           and openai_error_observation.get("model") == "mock-openai-model"
           and openai_error_observation.get("credential_ok") is True
           and "provider-translation-openai-ext.maas-system.svc.cluster.local"
               in openai_error_route.get("upstream_cluster", "")
           and openai_error_route.get("response_flags") == "-",
           {"http_status": openai_error_status,
            "downstream_error_type": openai_error_response.get("error", {}).get("type"),
            "upstream_path": openai_error_observation.get("path"),
            "upstream_model": openai_error_observation.get("model"),
            "provider_credential_ok": openai_error_observation.get("credential_ok"),
            "runtime_route_name": openai_error_route.get("route_name"),
            "runtime_upstream_cluster": openai_error_route.get("upstream_cluster"),
            "runtime_response_flags": openai_error_route.get("response_flags")})

    # A large-but-under-limit OpenAI JSON response must pass through the
    # selected provider unchanged. The separate over-limit probe below records
    # the MaaS Envoy usage JSON parser's existing response buffer ceiling.
    openai_under_limit_marker = "externalmodel-openai-under-limit-json-response"
    openai_under_limit_request_id = f"{ROUTE_TRACE_PREFIX}openai-under-limit-response"
    shared_observations_before_under_limit = len(observations())
    rc, openai_under_limit_status, openai_under_limit_raw = curl_request(
        "externalmodel-openai-under-limit-json-response", externalmodel_url,
        {"model": "demo", "messages": [{"role": "user", "content": openai_under_limit_marker}]},
        auth_file=auth_file,
        extra_headers=[f"x-ipp-diagnostic-id: {openai_under_limit_request_id}"],
        persist_response_payload=False,
    )
    try:
        openai_under_limit_response = json.loads(openai_under_limit_raw)
    except json.JSONDecodeError:
        openai_under_limit_response = {}
    if not isinstance(openai_under_limit_response, dict):
        openai_under_limit_response = {}
    openai_under_limit_content = openai_message_content(openai_under_limit_response) or ""
    openai_under_limit_observation = wait_provider_observation(
        lambda item: openai_under_limit_marker in item.get("message_text", []),
        "under-limit large OpenAI-compatible JSON response")
    openai_under_limit_route = capture_runtime_route_trace(
        openai_under_limit_request_id, "openai-under-limit-response", route_trace_gateway_pod)
    time.sleep(0.5)
    shared_observations_after_under_limit = len(observations())
    openai_under_limit_route_ok = (
        openai_under_limit_route.get("captured") is True
        and "external-model-demo-model" in openai_under_limit_route.get("route_name", "")
        and "provider-translation-openai-ext.maas-system.svc.cluster.local"
            in openai_under_limit_route.get("upstream_cluster", "")
        and openai_under_limit_route.get("response_flags") == "-"
        and bool(openai_under_limit_route.get("upstream_host"))
    )
    openai_under_limit_provider_ok = (
        openai_under_limit_observation.get("provider") == "provider-aware"
        and openai_under_limit_observation.get("path") == "/v1/chat/completions"
        and openai_under_limit_observation.get("model") == "mock-openai-model"
        and openai_under_limit_observation.get("credential_ok") is True
        and openai_under_limit_observation.get("authorization_present") is True
        and openai_under_limit_observation.get("x_api_key_present") is False
        and openai_under_limit_observation.get("tls") is True
        and openai_under_limit_observation.get("sni_ok") is True
        and openai_under_limit_observation.get("authority_ok") is True
    )
    record("externalmodel_large_openai_json_response_below_gateway_limit_passes_through",
           rc == 0 and openai_under_limit_status == "200"
           and (700 * 1024) < len(openai_under_limit_raw) < (1 << 20)
           and (700 * 1024) < len(openai_under_limit_content.encode()) < (1 << 20)
           and openai_under_limit_content.startswith("openai large response:")
           and openai_under_limit_provider_ok and openai_under_limit_route_ok
           and shared_observations_after_under_limit == shared_observations_before_under_limit,
           {"http_status": openai_under_limit_status,
            "downstream_json_bytes": len(openai_under_limit_raw),
            "downstream_content_bytes": len(openai_under_limit_content.encode()),
            "response_payload_saved": False,
            "provider_recorder_label": openai_under_limit_observation.get("provider"),
            "upstream_path": openai_under_limit_observation.get("path"),
            "upstream_model": openai_under_limit_observation.get("model"),
            "credential_ok": openai_under_limit_observation.get("credential_ok"),
            "authorization_present": openai_under_limit_observation.get("authorization_present"),
            "x_api_key_present": openai_under_limit_observation.get("x_api_key_present"),
            "tls": openai_under_limit_observation.get("tls"),
            "sni_ok": openai_under_limit_observation.get("sni_ok"),
            "authority_ok": openai_under_limit_observation.get("authority_ok"),
            "runtime_route_name": openai_under_limit_route.get("route_name"),
            "runtime_upstream_cluster": openai_under_limit_route.get("upstream_cluster"),
            "runtime_response_flags": openai_under_limit_route.get("response_flags"),
            "shared_recorder_request_delta": (
                shared_observations_after_under_limit - shared_observations_before_under_limit)})

    # Observe the larger OpenAI passthrough response without assuming Envoy
    # rejects it. Either a complete upstream 200 or an explicit Envoy size
    # rejection is possible; neither establishes the translator's limit.
    openai_large_response_marker = "externalmodel-openai-large-json-response"
    openai_large_response_request_id = f"{ROUTE_TRACE_PREFIX}openai-large-response"
    shared_observations_before_large_response = len(observations())
    rc, openai_large_response_status, openai_large_response_raw = curl_request(
        "externalmodel-openai-large-json-response", externalmodel_url,
        {"model": "demo", "messages": [{"role": "user", "content": openai_large_response_marker}]},
        auth_file=auth_file,
        extra_headers=[f"x-ipp-diagnostic-id: {openai_large_response_request_id}"],
        persist_response_payload=False,
    )
    try:
        openai_large_response = json.loads(openai_large_response_raw)
    except json.JSONDecodeError:
        openai_large_response = {}
    if not isinstance(openai_large_response, dict):
        openai_large_response = {}
    openai_large_response_content = ""
    choices = openai_large_response.get("choices", [])
    if choices and isinstance(choices[0], dict):
        message = choices[0].get("message", {})
        if isinstance(message, dict) and isinstance(message.get("content"), str):
            openai_large_response_content = message["content"]
    openai_large_response_observation = wait_provider_observation(
        lambda item: openai_large_response_marker in item.get("message_text", []),
        "large OpenAI-compatible JSON response")
    openai_large_response_route = capture_runtime_route_trace(
        openai_large_response_request_id, "openai-large-response", route_trace_gateway_pod)
    time.sleep(0.5)
    shared_observations_after_large_response = len(observations())
    openai_large_response_route_ok = (
        openai_large_response_route.get("captured") is True
        and "external-model-demo-model" in openai_large_response_route.get("route_name", "")
        and "provider-translation-openai-ext.maas-system.svc.cluster.local"
            in openai_large_response_route.get("upstream_cluster", "")
        and openai_large_response_route.get("response_flags") == "-"
        and bool(openai_large_response_route.get("upstream_host"))
    )
    openai_large_response_provider_ok = (
        openai_large_response_observation.get("provider") == "provider-aware"
        and openai_large_response_observation.get("path") == "/v1/chat/completions"
        and openai_large_response_observation.get("model") == "mock-openai-model"
        and openai_large_response_observation.get("credential_ok") is True
        and openai_large_response_observation.get("authorization_present") is True
        and openai_large_response_observation.get("x_api_key_present") is False
        and openai_large_response_observation.get("tls") is True
        and openai_large_response_observation.get("sni_ok") is True
        and openai_large_response_observation.get("authority_ok") is True
    )
    complete_openai_passthrough = (
        openai_large_response_status == "200"
        and openai_large_response_route.get("response_code_details") == "via_upstream"
        and len(openai_large_response_content.encode())
            == len("openai large response:") + 1048576 + 128
        and openai_large_response.get("choices", [{}])[0].get("finish_reason") == "stop"
    )
    explicit_envoy_size_rejection = (
        openai_large_response_status == "500"
        and openai_large_response_route.get("response_code_details") == "response_payload_too_large"
    )
    record("externalmodel_large_openai_passthrough_has_complete_or_explicit_outcome",
           rc == 0 and (complete_openai_passthrough or explicit_envoy_size_rejection)
           and openai_large_response_provider_ok and openai_large_response_route_ok
           and shared_observations_after_large_response == shared_observations_before_large_response,
           {"http_status": openai_large_response_status,
            "downstream_json_bytes": len(openai_large_response_raw),
            "downstream_content_bytes": len(openai_large_response_content.encode()),
            "response_payload_saved": False,
            "response_code_details": openai_large_response_route.get("response_code_details"),
            "complete_openai_passthrough": complete_openai_passthrough,
            "explicit_envoy_size_rejection": explicit_envoy_size_rejection,
            "provider_recorder_label": openai_large_response_observation.get("provider"),
            "upstream_path": openai_large_response_observation.get("path"),
            "upstream_model": openai_large_response_observation.get("model"),
            "credential_ok": openai_large_response_observation.get("credential_ok"),
            "authorization_present": openai_large_response_observation.get("authorization_present"),
            "x_api_key_present": openai_large_response_observation.get("x_api_key_present"),
            "tls": openai_large_response_observation.get("tls"),
            "sni_ok": openai_large_response_observation.get("sni_ok"),
            "authority_ok": openai_large_response_observation.get("authority_ok"),
            "runtime_route_name": openai_large_response_route.get("route_name"),
            "runtime_upstream_cluster": openai_large_response_route.get("upstream_cluster"),
            "runtime_response_flags": openai_large_response_route.get("response_flags"),
            "shared_recorder_request_delta": (
                shared_observations_after_large_response - shared_observations_before_large_response)})

    # Existing Envoy preselection control: this proves the request was rejected
    # before provider contact, not that the Praxis selected-provider filter
    # enforced its own 1 MiB request limit. Keep the synthetic body out of
    # retained evidence.
    openai_large_marker = "externalmodel-openai-body-limit"
    openai_large_request_id = f"{ROUTE_TRACE_PREFIX}openai-large-request"
    openai_upstream_count = len(provider_aware_observations())
    large_content = openai_large_marker + ("x" * (1024 * 1024 + 128))
    rc, openai_large_status, openai_large_raw = curl_request(
        "externalmodel-openai-request-body-limit", externalmodel_url,
        {"model": "demo", "messages": [{"role": "user", "content": large_content}]},
        auth_file=auth_file,
        extra_headers=[f"x-ipp-diagnostic-id: {openai_large_request_id}"],
        persist_response_payload=False,
    )
    openai_large_route = capture_runtime_route_trace(
        openai_large_request_id, "openai-large-request", route_trace_gateway_pod)
    time.sleep(0.5)
    openai_upstream_after_limit = len(provider_aware_observations())
    openai_envoy_preselection_limit = (
        openai_large_route.get("response_code_details") == "request_payload_too_large"
        and openai_large_route.get("model_header") in (None, "-")
        and openai_large_route.get("selected_candidate_header") in (None, "-")
        and "provider-selection-required-demo-model" in openai_large_route.get("upstream_cluster", "")
        and openai_large_route.get("upstream_host") in (None, "-")
    )
    openai_large_route_captured = openai_large_route.get("captured") is True
    record("externalmodel_openai_request_rejected_by_envoy_before_provider_selection",
           rc == 0 and openai_large_status == "413"
           and openai_envoy_preselection_limit
           and len(openai_large_raw) < 4096
           and openai_upstream_after_limit == openai_upstream_count,
           {"http_status": openai_large_status,
            "request_bytes_exceed_configured_limit": True,
            "downstream_response_bytes": len(openai_large_raw),
            "praxis_selected_provider_filter_reached": False,
            "envoy_preselection_limit_observed": openai_envoy_preselection_limit,
            "rejection_layer": "Envoy buffered pre-auth ExternalModel processor",
            "route_trace_captured": openai_large_route_captured,
            "dedicated_extproc_accumulation_limit_bytes": 10 * 1024 * 1024,
            "runtime_route_name": openai_large_route.get("route_name"),
            "runtime_response_code_details": openai_large_route.get("response_code_details"),
            "runtime_upstream_cluster": openai_large_route.get("upstream_cluster"),
            "runtime_response_flags": openai_large_route.get("response_flags"),
            "runtime_upstream_host": openai_large_route.get("upstream_host"),
            "runtime_model_header": openai_large_route.get("model_header"),
            "runtime_selected_candidate_header": openai_large_route.get("selected_candidate_header"),
            "upstream_contact_count_before": openai_upstream_count,
            "upstream_contact_count_after": openai_upstream_after_limit})

    # Disabling the permission leaves OpenAI-compatible traffic available and
    # restores main-compatible rendering: provider profiles and the selected-
    # provider translator are absent, so the client's model value is preserved.
    kubectl("-n", NAMESPACE, "patch", "maastenantconfig", "default-tenant", "--type=merge",
            "-p", '{"spec":{"apiTranslation":{"enabled":false}}}')
    openai_off_runtime = wait_externalmodel_revision(
        {"provider-api-translation-openai"}, "openai-translation-permission-off",
        api_translation_enabled=False)
    openai_off_marker = "externalmodel-openai-translation-permission-off"
    rc, openai_off_status, openai_off_raw = curl_request(
        "externalmodel-openai-permission-off", externalmodel_url,
        {"model": "demo", "messages": [{"role": "user", "content": openai_off_marker}]},
        auth_file=auth_file,
        extra_headers=["x-provider-format: messages", f"x-ipp-diagnostic-id:{ROUTE_TRACE_PREFIX}openai-off"],
    )
    try:
        openai_off_response = json.loads(openai_off_raw)
    except json.JSONDecodeError:
        openai_off_response = {}
    openai_off_observation = wait_provider_observation(
        lambda item: openai_off_marker in item.get("message_text", []), "OpenAI with translation permission off")
    record("externalmodel_openai_pass_through_with_translation_permission_off",
           rc == 0 and openai_off_status == "200" and bool(openai_off_response.get("choices"))
           and openai_off_observation.get("path") == "/v1/chat/completions"
           and openai_off_observation.get("model") == "demo"
           and openai_off_observation.get("credential_ok") is True
           and "api_translation_enabled:" not in openai_off_runtime["extproc_config"]
           and "filter: openai_chat_completions_to_anthropic\n" not in openai_off_runtime["extproc_config"]
           and "openai_chat_completions_to_anthropic_stream" not in openai_off_runtime["extproc_config"]
           and "max_body_bytes" not in openai_off_runtime["extproc_config"]
           and "allow_unbounded_body: true" in openai_off_runtime["extproc_config"]
           and all(not any(field in candidate for field in ("api_format", "path", "target_model"))
                   for candidate in openai_off_runtime["demo_model_candidates"]),
           {"http_status": openai_off_status, "provider_path": openai_off_observation.get("path"),
            "provider_model": openai_off_observation.get("model"),
            "provider_credential_ok": openai_off_observation.get("credential_ok"),
            "client_model_preserved": openai_off_observation.get("model") == "demo",
            "provider_profile_fields_absent": all(
                not any(field in candidate for field in ("api_format", "path", "target_model"))
                for candidate in openai_off_runtime["demo_model_candidates"]),
            "translation_permission": False,
            "selected_provider_clusters": openai_off_runtime["demo_model_candidate_clusters"]})

    kubectl("-n", NAMESPACE, "patch", "maastenantconfig", "default-tenant", "--type=merge",
            "-p", '{"spec":{"apiTranslation":{"enabled":true}}}')
    wait_externalmodel_revision({"provider-api-translation-openai"}, "openai-translation-permission-on")

    kubectl("-n", NAMESPACE, "patch", "externalmodel", "demo-model", "--type=merge", "-p",
            json.dumps({"spec": {"externalProviderRefs": anthro_active}}))
    wait_externalmodel_revision({"provider-api-translation-anthropic"}, "anthropic-after-openai-cross-path-regressions",
                                changed_from=openai_regression_runtime["published_digest"])

    tool_request = {
        "model": "demo", "max_tokens": 32,
        "messages": [{"role": "user", "content": "externalmodel-anthropic-tools"}],
        "tools": [{"type": "function", "function": {"name": "lookup", "description": "lookup data",
                    "parameters": {"type": "object", "properties": {"ok": {"type": "boolean"}}}}}],
    }
    rc, status, raw = curl_request("externalmodel-anthropic-json-tools", externalmodel_url, tool_request,
        auth_file=auth_file, extra_headers=["x-provider-format: openai-chat", "x-api-key: client-spoof"])
    try:
        anthro_json = json.loads(raw)
    except json.JSONDecodeError:
        anthro_json = {}
    tool_observation = wait_provider_observation(
        lambda item: "externalmodel-anthropic-tools" in item.get("message_text", []), "Anthropic tools JSON")
    choice = anthro_json.get("choices", [{}])[0]
    tool_call = choice.get("message", {}).get("tool_calls", [{}])[0]
    record("externalmodel_anthropic_json_tools_usage_and_auth",
           rc == 0 and status == "200" and choice.get("finish_reason") == "tool_calls"
           and tool_call.get("function", {}).get("name") == "lookup"
           and anthro_json.get("usage", {}).get("total_tokens") == 10
           and tool_observation.get("path") == "/v1/messages"
           and tool_observation.get("model") == "mock-anthropic-model"
           and tool_observation.get("tool_names") == ["lookup"]
           and tool_observation.get("x_api_key_present") is True
           and tool_observation.get("authorization_present") is False
           and tool_observation.get("credential_ok") is True
           and tool_observation.get("sni_ok") is True and tool_observation.get("authority_ok") is True,
           {"http_status": status, "response_finish_reason": choice.get("finish_reason"),
            "tool_name": tool_call.get("function", {}).get("name"), "usage": anthro_json.get("usage"),
            "upstream_path": tool_observation.get("path"), "upstream_model": tool_observation.get("model"),
            "credential_ok": tool_observation.get("credential_ok"), "sni_ok": tool_observation.get("sni_ok"),
            "authority_ok": tool_observation.get("authority_ok"), "spoofed_format_header_ignored": True})

    split_marker = "externalmodel-anthropic-split-json-response"
    split_request_id = f"{ROUTE_TRACE_PREFIX}split-json-response"
    rc, split_status, split_raw = curl_request(
        "externalmodel-anthropic-split-json-response", externalmodel_url,
        {"model": "demo", "max_tokens": 32, "messages": [{"role": "user", "content": split_marker}]},
        auth_file=auth_file, extra_headers=[f"x-ipp-diagnostic-id: {split_request_id}"],
    )
    try:
        split_response = json.loads(split_raw)
    except json.JSONDecodeError:
        split_response = {}
    split_observation = wait_provider_observation(
        lambda item: split_marker in item.get("message_text", []), "split Anthropic JSON response")
    split_route = capture_runtime_route_trace(split_request_id, "split-anthropic-json-response", route_trace_gateway_pod)
    split_route_ok = (
        split_route.get("captured") is True
        and "external-model-demo-model" in split_route.get("route_name", "")
        and "provider-translation-ext.maas-system.svc.cluster.local" in split_route.get("upstream_cluster", "")
        and split_route.get("response_flags") == "-"
    )
    split_content = openai_message_content(split_response)
    record("externalmodel_anthropic_split_json_response_is_translated",
           rc == 0 and split_status == "200"
           and split_response.get("model") == "demo"
           and split_content == "anthropic split response"
           and split_observation.get("path") == "/v1/messages"
           and split_observation.get("model") == "mock-anthropic-model"
           and split_observation.get("credential_ok") is True
           and split_observation.get("tls") is True and split_observation.get("sni_ok") is True
           and split_observation.get("authority_ok") is True and split_route_ok,
           {"http_status": split_status, "downstream_model": split_response.get("model"),
            "downstream_content": split_content,
            "upstream_path": split_observation.get("path"), "upstream_model": split_observation.get("model"),
            "credential_ok": split_observation.get("credential_ok"), "tls": split_observation.get("tls"),
            "sni_ok": split_observation.get("sni_ok"), "authority_ok": split_observation.get("authority_ok"),
            "fixture_flushes_json_in_two_chunks": True,
            "runtime_route_name": split_route.get("route_name"),
            "runtime_upstream_cluster": split_route.get("upstream_cluster"),
            "runtime_response_flags": split_route.get("response_flags")})

    trailer_marker = "externalmodel-anthropic-json-response-trailer"
    rc, trailer_status, trailer_raw = curl_request(
        "externalmodel-anthropic-json-response-trailer", externalmodel_url,
        {"model": "demo", "max_tokens": 32, "messages": [{"role": "user", "content": trailer_marker}]},
        auth_file=auth_file,
    )
    try:
        trailer_response = json.loads(trailer_raw)
    except json.JSONDecodeError:
        trailer_response = {}
    trailer_observation = wait_provider_observation(
        lambda item: trailer_marker in item.get("message_text", []), "Anthropic JSON response with trailer")
    trailer_content = openai_message_content(trailer_response)
    record("externalmodel_anthropic_json_body_completes_with_skipped_upstream_trailers",
           rc == 0 and trailer_status == "200"
           and trailer_content == "anthropic trailer response"
           and trailer_observation.get("path") == "/v1/messages"
           and trailer_observation.get("credential_ok") is True
           and trailer_observation.get("tls") is True and trailer_observation.get("sni_ok") is True
           and trailer_observation.get("authority_ok") is True,
           {"http_status": trailer_status,
            "downstream_content": trailer_content,
            "upstream_path": trailer_observation.get("path"),
            "credential_ok": trailer_observation.get("credential_ok"),
            "tls": trailer_observation.get("tls"), "sni_ok": trailer_observation.get("sni_ok"),
            "authority_ok": trailer_observation.get("authority_ok"),
            "externalmodel_response_trailer_mode": "SKIP"})

    oversized_marker = "externalmodel-anthropic-oversized-json-response"
    oversized_request_id = f"{ROUTE_TRACE_PREFIX}oversized-json-response"
    rc, oversized_status, oversized_raw = curl_request(
        "externalmodel-anthropic-oversized-json-response", externalmodel_url,
        {"model": "demo", "max_tokens": 32, "messages": [{"role": "user", "content": oversized_marker}]},
        auth_file=auth_file,
        extra_headers=[f"x-ipp-diagnostic-id: {oversized_request_id}"],
        persist_response_payload=False,
    )
    try:
        oversized_response = json.loads(oversized_raw)
    except json.JSONDecodeError:
        oversized_response = {}
    oversized_observation = wait_provider_observation(
        lambda item: oversized_marker in item.get("message_text", []), "oversized Anthropic JSON response")
    oversized_route = capture_runtime_route_trace(
        oversized_request_id, "oversized-anthropic-json-response", route_trace_gateway_pod)
    oversized_filter_rejection = (
        oversized_response.get("error", {}).get("code") == "provider_response_too_large"
        and "Anthropic provider response exceeds a size limit set by the platform administrator."
        in oversized_response.get("error", {}).get("message", "")
    )
    record("externalmodel_anthropic_response_limit_is_enforced_by_praxis_filter",
           rc == 0 and oversized_status == "502" and oversized_filter_rejection
           and 0 < len(oversized_raw) < 4096 and b"xxxxxxxx" not in oversized_raw
           and oversized_observation.get("path") == "/v1/messages"
           and oversized_observation.get("model") == "mock-anthropic-model"
           and oversized_observation.get("credential_ok") is True
           and oversized_observation.get("tls") is True and oversized_observation.get("sni_ok") is True
           and oversized_observation.get("authority_ok") is True
           and "provider-translation-ext.maas-system.svc.cluster.local"
               in oversized_route.get("upstream_cluster", ""),
           {"http_status": oversized_status,
            "downstream_filter_error_code": oversized_response.get("error", {}).get("code"),
            "downstream_filter_error_message": oversized_response.get("error", {}).get("message"),
            "downstream_response_bytes": len(oversized_raw),
            "upstream_response_exceeds_configured_bytes": (1 << 20) + 128,
            "upstream_path": oversized_observation.get("path"),
            "upstream_model": oversized_observation.get("model"),
            "credential_ok": oversized_observation.get("credential_ok"),
            "tls": oversized_observation.get("tls"), "sni_ok": oversized_observation.get("sni_ok"),
            "authority_ok": oversized_observation.get("authority_ok"),
            "rejection_layer": "Praxis AI OpenAI-to-Anthropic response filter",
            "runtime_route_name": oversized_route.get("route_name"),
            "runtime_upstream_cluster": oversized_route.get("upstream_cluster"),
            "runtime_response_flags": oversized_route.get("response_flags")})

    invalid_response_marker = "externalmodel-anthropic-invalid-json-response"
    rc, invalid_response_status, invalid_response_raw = curl_request(
        "externalmodel-anthropic-invalid-json-response", externalmodel_url,
        {"model": "demo", "max_tokens": 32, "messages": [{"role": "user", "content": invalid_response_marker}]},
        auth_file=auth_file,
    )
    try:
        invalid_response = json.loads(invalid_response_raw)
    except json.JSONDecodeError:
        invalid_response = {}
    invalid_response_observation = wait_provider_observation(
        lambda item: invalid_response_marker in item.get("message_text", []), "invalid Anthropic JSON response")
    record("externalmodel_invalid_anthropic_json_returns_gateway_error",
           rc == 0 and invalid_response_status == "502"
           and invalid_response.get("error", {}).get("type") == "server_error"
           and invalid_response_observation.get("path") == "/v1/messages"
           and invalid_response_observation.get("credential_ok") is True,
           {"http_status": invalid_response_status,
            "downstream_error_type": invalid_response.get("error", {}).get("type"),
            "upstream_path": invalid_response_observation.get("path"),
            "credential_ok": invalid_response_observation.get("credential_ok")})

    before_error = len(provider_aware_observations())
    rc, error_status, error_raw = curl_request("externalmodel-anthropic-provider-error", externalmodel_url,
        {"model": "demo", "max_tokens": 32,
         "messages": [{"role": "user", "content": "provider-error"}]},
        auth_file=auth_file)
    try:
        translated_error = json.loads(error_raw)
    except json.JSONDecodeError:
        translated_error = {}
    error_observation = wait_provider_observation(
        lambda item: "provider-error" in item.get("message_text", []), "Anthropic provider error")
    record("externalmodel_anthropic_provider_error_is_openai_json",
           rc == 0 and error_status == "429" and translated_error.get("error", {}).get("type") == "rate_limit_error"
           and translated_error.get("error", {}).get("code") == 429 and error_observation.get("credential_ok") is True,
           {"http_status": error_status, "error": translated_error.get("error"),
            "upstream_contacted": len(provider_aware_observations()) >= before_error,
            "credential_ok": error_observation.get("credential_ok")})

    upstream_count = len(provider_aware_observations())
    rc, invalid_status, _ = curl_request("externalmodel-anthropic-invalid-fields", externalmodel_url,
        {"model": "demo", "max_tokens": 32,
         "messages": [{"role": "user", "content": "invalid-field"}], "logprobs": True},
        auth_file=auth_file)
    time.sleep(0.5)
    record("externalmodel_unsupported_field_rejected_before_upstream",
           rc == 0 and invalid_status == "400" and len(provider_aware_observations()) == upstream_count,
           {"http_status": invalid_status, "upstream_contact_count_before": upstream_count,
            "upstream_contact_count_after": len(provider_aware_observations())})

    # This large request is an Envoy preselection control, not a Praxis limit
    # test: the pre-auth processor rejects it before the selected-provider
    # filter receives a body.
    large_content = "x" * (1024 * 1024 + 128)
    upstream_count = len(provider_aware_observations())
    anthropic_large_request_id = f"{ROUTE_TRACE_PREFIX}anthropic-large-request"
    rc, large_status, large_raw = curl_request("externalmodel-anthropic-body-limit", externalmodel_url,
        {"model": "demo", "max_tokens": 32,
         "messages": [{"role": "user", "content": large_content}]}, auth_file=auth_file,
        extra_headers=[f"x-ipp-diagnostic-id: {anthropic_large_request_id}"],
        persist_response_payload=False)
    anthropic_large_route = capture_runtime_route_trace(
        anthropic_large_request_id, "anthropic-large-request", route_trace_gateway_pod)
    time.sleep(0.5)
    anthropic_upstream_after_limit = len(provider_aware_observations())
    anthropic_envoy_preselection_limit = (
        anthropic_large_route.get("response_code_details") == "request_payload_too_large"
        and anthropic_large_route.get("model_header") in (None, "-")
        and anthropic_large_route.get("selected_candidate_header") in (None, "-")
        and "provider-selection-required-demo-model" in anthropic_large_route.get("upstream_cluster", "")
        and anthropic_large_route.get("upstream_host") in (None, "-")
    )
    record("externalmodel_anthropic_request_rejected_by_envoy_before_provider_selection",
           rc == 0 and large_status == "413"
           and anthropic_envoy_preselection_limit
           and len(large_raw) < 4096 and anthropic_upstream_after_limit == upstream_count,
           {"http_status": large_status, "request_bytes_exceed_configured_limit": True,
            "downstream_response_bytes": len(large_raw),
            "praxis_selected_provider_filter_reached": False,
            "envoy_preselection_limit_observed": anthropic_envoy_preselection_limit,
            "rejection_layer": "Envoy buffered pre-auth ExternalModel processor",
            "route_trace_captured": anthropic_large_route.get("captured") is True,
            "dedicated_extproc_accumulation_limit_bytes": 10 * 1024 * 1024,
            "upstream_contact_count_before": upstream_count,
            "upstream_contact_count_after": anthropic_upstream_after_limit,
            "runtime_route_name": anthropic_large_route.get("route_name"),
            "runtime_response_code_details": anthropic_large_route.get("response_code_details"),
            "runtime_upstream_cluster": anthropic_large_route.get("upstream_cluster"),
            "runtime_response_flags": anthropic_large_route.get("response_flags"),
            "runtime_upstream_host": anthropic_large_route.get("upstream_host"),
            "runtime_model_header": anthropic_large_route.get("model_header"),
            "runtime_selected_candidate_header": anthropic_large_route.get("selected_candidate_header")})

    stream_request = {
        "model": "demo", "max_tokens": 32, "stream": True,
        "stream_options": {"include_usage": True},
        "messages": [{"role": "user", "content": "externalmodel-anthropic-stream-tools"}],
        "tools": [{"type": "function", "function": {"name": "lookup", "parameters": {"type": "object"}}}],
    }
    rc, stream_status, stream_raw, first_ms, total_ms, content_type = progressive_stream_request(
        "externalmodel-anthropic-sse", externalmodel_url, stream_request,
        extra_headers=["x-provider-format: openai-chat", "x-api-key: client-spoof"])
    stream_observation = wait_provider_observation(
        lambda item: "externalmodel-anthropic-stream-tools" in item.get("message_text", []), "Anthropic SSE")
    events = sse_events(stream_raw)
    stream_data = [event["data"] for event in events]
    serialized = json.dumps(stream_data)
    record("externalmodel_anthropic_sse_tools_usage_progressive_and_terminated",
           rc == 0 and stream_status == "200" and content_type.startswith("text/event-stream")
           and total_ms - first_ms >= 500
           and any(isinstance(data, dict) and data.get("choices") for data in stream_data)
           and "anthropic translated stream" in serialized and "lookup" in serialized
           and any(isinstance(data, dict) and data.get("usage", {}).get("total_tokens") == 10 for data in stream_data)
           and "[DONE]" in stream_data and stream_observation.get("credential_ok") is True
           and stream_observation.get("path") == "/v1/messages",
           {"http_status": stream_status, "content_type": content_type, "events": len(events),
            "progressive_delivery_ms": round(total_ms - first_ms, 1), "termination": "[DONE]" in stream_data,
            "upstream_path": stream_observation.get("path"), "credential_ok": stream_observation.get("credential_ok")})

    # The active stream keeps the Anthropic profile while a later request is
    # routed through a reloaded OpenAI-only overlay in the same serving pod.
    reload_marker = "externalmodel-overlay-reload-active-anthropic-stream"
    reload_body = {"model": "demo", "max_tokens": 32, "stream": True,
                   "messages": [{"role": "user", "content": reload_marker}],
                   "tools": [{"type": "function", "function": {
                       "name": "lookup", "parameters": {"type": "object"}}}]}
    reload_result = {}
    reload_first_event = threading.Event()
    def run_reload_stream():
        try:
            # The recorder pauses 18 seconds between upstream events. Some
            # events do not produce a downstream SSE chunk, so the socket may
            # be quiet across several pauses while the stream remains healthy.
            reload_result["result"] = progressive_stream_request(
                "externalmodel-overlay-reload-stream", externalmodel_url, reload_body,
                extra_headers=["x-provider-format: openai-chat"], record_result=False,
                on_first_event=reload_first_event.set, request_timeout=90)
        except Exception as exc:
            reload_result["error"] = repr(exc)

    stream_thread = threading.Thread(target=run_reload_stream)
    anthro_revision = anthro_runtime["published_digest"]
    stream_thread.start()
    reload_observation = wait_provider_observation(
        lambda item: reload_marker in item.get("message_text", []), "Anthropic SSE before overlay reload")
    first_event_before_reload = reload_first_event.wait(timeout=10)
    stream_open_before_reload = stream_thread.is_alive()
    record("externalmodel_overlay_reload_starts_after_anthropic_sse_event",
           first_event_before_reload and stream_open_before_reload
           and reload_observation.get("path") == "/v1/messages"
           and reload_observation.get("stream") is True
           and reload_observation.get("tool_names") == ["lookup"],
           {"first_downstream_event_received": first_event_before_reload,
            "stream_open_when_reload_started": stream_open_before_reload,
            "upstream_path": reload_observation.get("path"),
            "upstream_stream": reload_observation.get("stream"),
            "upstream_tool_names": reload_observation.get("tool_names")})
    openai_active = [dict(refs[0], weight=1), dict(refs[1], weight=0)]
    kubectl("-n", NAMESPACE, "patch", "externalmodel", "demo-model", "--type=merge", "-p",
            json.dumps({"spec": {"externalProviderRefs": openai_active}}))
    reloaded = wait_externalmodel_revision({"provider-api-translation-openai"}, "reload-to-openai",
                                           changed_from=anthro_revision)
    stream_open_when_new_revision_served = stream_thread.is_alive()
    new_revision_serving = (
        reloaded["published_digest"] != anthro_revision
        and bool(reloaded["pods"])
        and all(pod["ready"] and not pod["deleting"]
                and pod["mounted_overlay_matches_published"]
                and pod["accepted_revision"] == reloaded["published_digest"]
                and pod["serving_revision"] == reloaded["published_digest"]
                for pod in reloaded["pods"])
        and {endpoint["target"] for endpoint in reloaded["endpoints"] if endpoint["ready"]}
            <= {pod["name"] for pod in reloaded["pods"] if pod["ready"] and not pod["deleting"]}
    )
    record("externalmodel_openai_overlay_revision_serves_after_reload",
           new_revision_serving
           and reloaded["demo_model_candidate_clusters"] == ["provider-api-translation-openai"],
           {"old_revision": anthro_revision,
            "published_revision": reloaded["published_digest"],
            "mounted_accepted_and_serving_revision_converged": new_revision_serving,
            "stream_open_when_new_revision_served": stream_open_when_new_revision_served,
            "candidate_clusters": reloaded["demo_model_candidate_clusters"],
            "pods": [{"name": pod["name"], "uid": pod["uid"], "ready": pod["ready"],
                      "mounted_overlay_matches_published": pod["mounted_overlay_matches_published"],
                      "accepted_revision": pod["accepted_revision"],
                      "serving_revision": pod["serving_revision"]}
                     for pod in reloaded["pods"]],
            "endpoints": reloaded["endpoints"]})
    stream_thread.join(timeout=180)
    if stream_thread.is_alive():
        raise RuntimeError("Anthropic SSE request did not complete after overlay reload")
    if "result" not in reload_result:
        raise RuntimeError(f"Anthropic SSE request failed after overlay reload: {reload_result.get('error')}")
    reload_rc, reload_status, reload_raw, reload_first_ms, reload_total_ms, reload_type = reload_result["result"]
    reload_events = sse_events(reload_raw)
    reload_data = [event["data"] for event in reload_events]
    active_provider_profile_ok = (
        reload_observation.get("provider") == "provider-aware"
        and reload_observation.get("path") == "/v1/messages"
        and reload_observation.get("model") == "mock-anthropic-model"
        and reload_observation.get("stream") is True
        and reload_observation.get("tls") is True
        and reload_observation.get("sni_ok") is True
        and reload_observation.get("authority_ok") is True
        and reload_observation.get("credential_ok") is True
        and reload_observation.get("message_text") == [reload_marker]
    )
    parsed_reload_events = [{"event": event["event"], "data": event["data"]}
                            for event in reload_events]
    save_json(EVIDENCE / "externalmodel-overlay-reload-stream-parsed.json", {
        "http_status": reload_status,
        "content_type": reload_type,
        "first_event_latency_ms": round(reload_first_ms, 1),
        "total_response_ms": round(reload_total_ms, 1),
        "events": parsed_reload_events,
        "upstream_profile": {key: reload_observation.get(key) for key in
            ("provider", "path", "model", "stream", "tls", "sni_ok", "authority",
             "authority_ok", "credential_ok", "message_text")},
    })
    record("externalmodel_inflight_sse_retains_anthropic_profile_across_overlay_reload",
           reload_rc == 0 and reload_status == "200" and reload_type.startswith("text/event-stream")
           and "anthropic translated stream" in json.dumps(reload_data)
           and "lookup" in json.dumps(reload_data)
           and "[DONE]" in reload_data and len(reload_events) >= 4
           and reload_total_ms > reload_first_ms
           and active_provider_profile_ok and first_event_before_reload,
           {"active_stream_status": reload_status,
            "active_stream_content_type": reload_type,
            "active_stream_event_count": len(reload_events),
            "active_stream_termination": "[DONE]" in reload_data,
            "active_stream_translated_output": "anthropic translated stream" in json.dumps(reload_data),
            "active_stream_tool_output": "lookup" in json.dumps(reload_data),
            "first_downstream_event_received_before_reload": first_event_before_reload,
            "upstream_profile": {key: reload_observation.get(key) for key in
                ("provider", "path", "model", "stream", "tls", "sni_ok", "authority",
                 "authority_ok", "credential_ok", "message_text")},
            "new_published_and_serving_revision": reloaded["published_digest"],
            "next_candidate_clusters": reloaded["demo_model_candidate_clusters"],
            "initial_serving_pod_uids": base_pod_uids,
            "serving_pod_uids_after_reload": sorted(
                pod["uid"] for pod in reloaded["pods"] if pod["ready"] and not pod["deleting"]),
            "serving_pods_replaced_during_reload": sorted(
                pod["uid"] for pod in reloaded["pods"] if pod["ready"] and not pod["deleting"])
                != base_pod_uids})
    record("externalmodel_anthropic_sse_remains_open_until_new_revision_serves",
           stream_open_when_new_revision_served,
           {"stream_open_when_new_revision_served": stream_open_when_new_revision_served,
            "new_serving_revision": reloaded["published_digest"],
            "new_candidate_clusters": reloaded["demo_model_candidate_clusters"],
            "stream_completion_ms": round(reload_total_ms, 1),
            "serving_revision_state_checked_separately": True})
    RESULT["requests"].append({"name": "externalmodel-overlay-reload-stream", "http_status": reload_status,
                               "path": "chat/completions", "first_event_latency_ms": round(reload_first_ms, 1),
                               "total_response_ms": round(reload_total_ms, 1)})
    save_json(RESULTS, RESULT)

    next_openai_marker = "externalmodel-next-request-uses-openai-after-reload"
    rc, next_status, next_raw = curl_request("externalmodel-next-openai-after-reload", externalmodel_url,
        {"model": "demo", "messages": [{"role": "user", "content": next_openai_marker}]},
        auth_file=auth_file, extra_headers=["x-provider-format: messages"])
    try:
        next_json = json.loads(next_raw)
    except json.JSONDecodeError:
        next_json = {}
    next_observation = wait_provider_observation(
        lambda item: next_openai_marker in item.get("message_text", []), "OpenAI after overlay reload")
    reloaded_served = wait_externalmodel_revision({"provider-api-translation-openai"},
        "reload-to-openai-after-request", changed_from=anthro_revision)
    record("externalmodel_next_request_uses_reloaded_openai_profile",
           rc == 0 and next_status == "200" and bool(next_json.get("choices"))
           and next_observation.get("path") == "/v1/chat/completions"
           and next_observation.get("model") == "mock-openai-model"
           and next_observation.get("credential_ok") is True
           and all(pod["accepted_revision"] == reloaded["published_digest"]
                   and pod["serving_revision"] == reloaded["published_digest"]
                   for pod in reloaded_served["pods"] if pod["ready"] and not pod["deleting"]),
           {"http_status": next_status, "upstream_path": next_observation.get("path"),
            "upstream_model": next_observation.get("model"), "credential_ok": next_observation.get("credential_ok"),
            "published_revision": reloaded["published_digest"],
            "accepted_serving_revision": reloaded["published_digest"]})

    openai_stream_marker = "externalmodel-openai-stream-passthrough"
    rc, openai_stream_status, openai_stream_raw, openai_first_ms, openai_total_ms, openai_content_type = progressive_stream_request(
        "externalmodel-openai-sse", externalmodel_url,
        {"model": "demo", "stream": True,
         "messages": [{"role": "user", "content": openai_stream_marker}]},
        extra_headers=["x-provider-format: messages"],
    )
    openai_stream_observation = wait_provider_observation(
        lambda item: openai_stream_marker in item.get("message_text", []), "OpenAI-compatible SSE")
    openai_stream_events = sse_events(openai_stream_raw)
    openai_stream_data = [event["data"] for event in openai_stream_events]
    record("externalmodel_openai_candidate_sse_passthrough",
           rc == 0 and openai_stream_status == "200"
           and openai_content_type.startswith("text/event-stream")
           and openai_total_ms - openai_first_ms >= 500
           and any(isinstance(item, dict) and item.get("choices") for item in openai_stream_data)
           and "[DONE]" in openai_stream_data
           and openai_stream_observation.get("path") == "/v1/chat/completions"
           and openai_stream_observation.get("model") == "mock-openai-model"
           and openai_stream_observation.get("stream") is True
           and openai_stream_observation.get("credential_ok") is True,
           {"http_status": openai_stream_status, "upstream_path": openai_stream_observation.get("path"),
            "upstream_model": openai_stream_observation.get("model"), "content_type": openai_content_type,
            "events": len(openai_stream_events), "termination": "[DONE]" in openai_stream_data,
            "progressive_delivery_ms": round(openai_total_ms - openai_first_ms, 1),
            "credential_ok": openai_stream_observation.get("credential_ok")})

    # Cancel a separate active stream, then prove the next request gets a new
    # request-local profile and response state.
    kubectl("-n", NAMESPACE, "patch", "externalmodel", "demo-model", "--type=merge", "-p",
            json.dumps({"spec": {"externalProviderRefs": anthro_active}}))
    anthro_again = wait_externalmodel_revision({"provider-api-translation-anthropic"}, "cancel-stream-anthropic",
        changed_from=reloaded["published_digest"])
    cancel_marker = "externalmodel-cancel-anthropic-stream"
    cancel_status, first_event = cancel_stream_request(externalmodel_url,
        {"model": "demo", "max_tokens": 32, "stream": True,
         "messages": [{"role": "user", "content": cancel_marker}]},
        extra_headers=["x-provider-format: openai-chat"])
    cancel_drain_seconds = wait_for_cancelled_stream_drain()
    cancel_observation = wait_provider_observation(
        lambda item: cancel_marker in item.get("message_text", []), "canceled Anthropic SSE")
    anthro_again_served = wait_externalmodel_revision({"provider-api-translation-anthropic"},
        "cancel-stream-anthropic-after-request", changed_from=reloaded["published_digest"])
    kubectl("-n", NAMESPACE, "patch", "externalmodel", "demo-model", "--type=merge", "-p",
            json.dumps({"spec": {"externalProviderRefs": openai_active}}))
    after_cancel = wait_externalmodel_revision({"provider-api-translation-openai"}, "after-cancel-openai",
        changed_from=anthro_again["published_digest"])
    wait_for_externalmodel_route("demo-model/provider-api-translation-openai", "/v1/chat/completions",
                                "after-cancel-openai")
    record("externalmodel_cancellation_is_request_scoped",
           cancel_status == "200" and b"data:" in first_event and cancel_observation.get("path") == "/v1/messages"
           and after_cancel["published_digest"] != anthro_again["published_digest"]
           and all(pod["accepted_revision"] == anthro_again["published_digest"]
                   and pod["serving_revision"] == anthro_again["published_digest"]
                   for pod in anthro_again_served["pods"] if pod["ready"] and not pod["deleting"]),
           {"canceled_stream_status": cancel_status, "first_event_received": bool(first_event),
            "canceled_stream_provider_path": cancel_observation.get("path"),
            "canceled_stream_drain_wait_seconds": cancel_drain_seconds,
            "canceled_stream_revision": anthro_again["published_digest"],
            "next_published_revision": after_cancel["published_digest"]})
    next_marker = "externalmodel-after-cancel-openai"
    rc, after_cancel_status, after_cancel_raw = curl_request("externalmodel-after-cancel", externalmodel_url,
        {"model": "demo", "messages": [{"role": "user", "content": next_marker}]}, auth_file=auth_file)
    after_cancel_observation = wait_provider_observation(
        lambda item: next_marker in item.get("message_text", []), "OpenAI after canceled stream")
    after_cancel_served = wait_externalmodel_revision({"provider-api-translation-openai"},
        "after-cancel-openai-after-request", changed_from=anthro_again["published_digest"])
    record("externalmodel_request_after_cancel_uses_openai_profile",
           rc == 0 and after_cancel_status == "200" and after_cancel_observation.get("path") == "/v1/chat/completions"
           and after_cancel_observation.get("credential_ok") is True
           and all(pod["accepted_revision"] == after_cancel["published_digest"]
                   and pod["serving_revision"] == after_cancel["published_digest"]
                   for pod in after_cancel_served["pods"] if pod["ready"] and not pod["deleting"]),
           {"http_status": after_cancel_status, "upstream_path": after_cancel_observation.get("path"),
            "credential_ok": after_cancel_observation.get("credential_ok"),
            "response_bytes": len(after_cancel_raw),
            "published_revision": after_cancel["published_digest"],
            "accepted_serving_revision": after_cancel["published_digest"]})

    bedrock_host = "provider-translation-bedrock-ext.maas-system.svc.cluster.local"
    bedrock_ref = {"ref": {"name": names["bedrock_provider"]},
                   "targetModel": "openai.gpt-oss-20b", "apiFormat": "openai-chat",
                   "path": "/v1/chat/completions", "weight": 1}
    openai_bedrock_refs = [dict(refs[0], weight=1), bedrock_ref]
    kubectl("-n", NAMESPACE, "patch", "externalmodel", "demo-model", "--type=merge", "-p",
            json.dumps({"spec": {"externalProviderRefs": openai_bedrock_refs}}))
    bedrock_mixed = wait_externalmodel_revision(
        {"provider-api-translation-openai", "provider-api-translation-bedrock-mantle"},
        "bedrock-mantle-mixed-openai-chat")
    bedrock_profile = next((candidate for candidate in bedrock_mixed["demo_model_candidates"]
                            if candidate.get("stable_id") == "demo-model/provider-api-translation-bedrock-mantle"), {})
    record("bedrock_mantle_overlay_uses_openai_chat_profile",
           bedrock_profile.get("api_format") == "openai-chat"
           and bedrock_profile.get("path") == "/v1/chat/completions"
           and bedrock_profile.get("target_model") == "openai.gpt-oss-20b",
           {"stable_id": bedrock_profile.get("stable_id"), "api_format": bedrock_profile.get("api_format"),
            "path": bedrock_profile.get("path"), "target_model": bedrock_profile.get("target_model"),
            "published_revision": bedrock_mixed["published_digest"]})
    verify_externalmodel_extproc_image()
    bedrock_route_trace_pod = install_runtime_route_trace()

    # A direct no-key request proves the fixture rejects absent provider
    # credentials. The request body and response contain no credential value.
    bedrock_forward, bedrock_port = start_port_forward("provider-translation-bedrock-ext", 443)
    try:
        no_key_marker = "bedrock-mantle-no-provider-key"
        no_key_result = run([
            "curl", "--noproxy", "*", "-sS", "--connect-timeout", "5", "--max-time", "10",
            "--cacert", str(PROVIDER_CA_FILE), "--connect-to",
            f"{bedrock_host}:443:127.0.0.1:{bedrock_port}",
            "-H", "content-type: application/json", "-H", f"X-Translation-Request-Id: {no_key_marker}",
            "-o", os.devnull, "-w", "%{http_code}", "--data-binary",
            json.dumps({"model": "openai.gpt-oss-20b", "messages": [{"role": "user", "content": no_key_marker}]}),
            f"https://{bedrock_host}/v1/chat/completions",
        ], check=False, timeout=12)
        no_key_observation = wait_provider_observation(
            lambda item: item.get("request_id") == no_key_marker, "Bedrock no-key control")
    finally:
        if bedrock_forward.poll() is None:
            bedrock_forward.terminate()
        try:
            bedrock_forward.wait(timeout=5)
        except subprocess.TimeoutExpired:
            bedrock_forward.kill()
            bedrock_forward.wait(timeout=5)
    record("bedrock_mantle_recorder_rejects_missing_bearer_credential",
           no_key_result.returncode == 0 and no_key_result.stdout.strip() == "401"
           and no_key_observation.get("credential_ok") is False,
           {"http_status": no_key_result.stdout.strip() or "unknown",
            "curl_exit_status": no_key_result.returncode,
            "credential_ok": no_key_observation.get("credential_ok"),
            "credential_value_saved": False})

    bedrock_counts = {"openai": 0, "bedrock-mantle": 0, "unexpected": 0}
    bedrock_samples = []
    bedrock_cluster = "provider-translation-bedrock-ext.maas-system.svc.cluster.local"
    openai_cluster = "provider-translation-openai-ext.maas-system.svc.cluster.local"
    openai_authority = openai_cluster
    for index in range(24):
        marker = f"bedrock-mantle-mixed-{index}"
        diagnostic_id = f"{ROUTE_TRACE_PREFIX}bedrock-mixed-{index}"
        rc, status, raw = curl_request(
            f"bedrock-mantle-mixed-{index}", externalmodel_url,
            {"model": "demo", "messages": [{"role": "user", "content": marker}]},
            auth_file=auth_file, extra_headers=[f"x-ipp-diagnostic-id: {diagnostic_id}"])
        trace = capture_runtime_route_trace(diagnostic_id, f"bedrock-mixed-{index}", bedrock_route_trace_pod)
        try:
            downstream = json.loads(raw)
        except json.JSONDecodeError:
            downstream = {}
        observation = wait_provider_observation(lambda item: marker in item.get("message_text", []), marker)
        upstream_cluster = trace.get("upstream_cluster", "")
        if bedrock_cluster in upstream_cluster:
            selected = "bedrock-mantle"
            expected_host = bedrock_host
            expected_model = "openai.gpt-oss-20b"
            candidate = "demo-model/provider-api-translation-bedrock-mantle"
            expected_cluster = bedrock_cluster
        elif openai_cluster in upstream_cluster:
            selected = "openai"
            expected_host = openai_authority
            expected_model = "mock-openai-model"
            candidate = "demo-model/provider-api-translation-openai"
            expected_cluster = openai_cluster
        else:
            selected = "unexpected"
            expected_host = ""
            expected_model = ""
            candidate = ""
            expected_cluster = ""
        bedrock_counts[selected] += 1
        route_ok = (trace.get("captured") is True and bool(expected_host)
                    and expected_cluster in upstream_cluster
                    and observation.get("authority") == expected_host
                    and trace.get("route_name", "").startswith(
                        "models-as-a-service.external-model-demo-model.")
                    and trace.get("response_code") == status
                    and trace.get("model_header") == "demo")
        format_ok = (observation.get("path") == "/v1/chat/completions"
                     and observation.get("model") == expected_model
                     and observation.get("authorization_present") is True
                     and observation.get("authorization_scheme") == "Bearer"
                     and observation.get("x_api_key_present") is False
                     and observation.get("credential_ok") is True
                     and observation.get("tls") is True and observation.get("sni_ok") is True
                     and observation.get("authority_ok") is True)
        response_ok = (isinstance(downstream.get("choices"), list) and bool(downstream["choices"])
                       and downstream.get("model") == expected_model)
        bedrock_samples.append({"index": index, "provider": selected, "status": status,
                                "candidate": candidate,
                                "revision": bedrock_mixed["published_digest"],
                                "upstream_model": observation.get("model"),
                                "upstream_path": observation.get("path"), "route_ok": route_ok,
                                "transport_and_credential_ok": format_ok, "response_ok": response_ok,
                                "upstream_cluster": upstream_cluster,
                                "backend_authority": observation.get("authority"),
                                "x_api_key_present": observation.get("x_api_key_present"),
                                "response_flags": trace.get("response_flags"),
                                "credential_value_saved": False})
        if rc != 0 or status != "200" or not route_ok or not format_ok or not response_ok:
            break
    save_json(EVIDENCE / "externalmodel-bedrock-mantle-mixed-samples.json", bedrock_samples)
    bedrock_after_samples = externalmodel_runtime_snapshot("bedrock-mantle-mixed-after-samples")
    bedrock_revision_stable = externalmodel_snapshot_serves_revision(
        bedrock_after_samples,
        {"provider-api-translation-openai", "provider-api-translation-bedrock-mantle"},
        bedrock_mixed["published_digest"],
    )
    record("bedrock_mantle_and_openai_mixed_json_route_credential_tls",
           len(bedrock_samples) == 24 and all(item["status"] == "200" and item["route_ok"]
               and item["transport_and_credential_ok"] and item["response_ok"] for item in bedrock_samples)
           and bedrock_counts["openai"] > 0 and bedrock_counts["bedrock-mantle"] > 0
           and bedrock_counts["unexpected"] == 0 and bedrock_revision_stable,
           {"requests": len(bedrock_samples), "provider_counts": bedrock_counts,
            "exact_split_required": False,
            "serving_revision": bedrock_mixed["published_digest"],
            "revision_stable_before_and_after_samples": bedrock_revision_stable,
            "serving_pods_after_samples": [{"name": pod["name"], "uid": pod["uid"],
                "serving_revision": pod["serving_revision"]} for pod in bedrock_after_samples["pods"]],
            "unexpected_or_unattributed_responses": sum(
                1 for item in bedrock_samples if item["status"] != "200" or not item["route_ok"]),
            "credential_values_saved": False})

    bedrock_active = [dict(refs[0], weight=0), dict(bedrock_ref, weight=1)]
    kubectl("-n", NAMESPACE, "patch", "externalmodel", "demo-model", "--type=merge", "-p",
            json.dumps({"spec": {"externalProviderRefs": bedrock_active}}))
    bedrock_only = wait_externalmodel_revision({"provider-api-translation-bedrock-mantle"},
                                               "bedrock-mantle-forced", changed_from=bedrock_mixed["published_digest"])
    stream_marker = "bedrock-mantle-progressive-sse"
    stream_trace_id = f"{ROUTE_TRACE_PREFIX}bedrock-progressive-sse"
    _, stream_status, stream_raw, stream_first_ms, stream_total_ms, stream_type = progressive_stream_request(
        "bedrock-mantle-progressive-sse", externalmodel_url,
        {"model": "demo", "stream": True,
         "messages": [{"role": "user", "content": stream_marker}]},
        extra_headers=[f"x-ipp-diagnostic-id: {stream_trace_id}"])
    stream_trace = capture_runtime_route_trace(stream_trace_id, "bedrock-progressive-sse", bedrock_route_trace_pod)
    stream_observation = wait_provider_observation(
        lambda item: stream_marker in item.get("message_text", []), "Bedrock progressive SSE")
    stream_events = sse_events(stream_raw)
    stream_data = [event["data"] for event in stream_events]
    bedrock_after_sse = externalmodel_runtime_snapshot("bedrock-mantle-after-progressive-sse")
    stream_revision_stable = externalmodel_snapshot_serves_revision(
        bedrock_after_sse,
        {"provider-api-translation-bedrock-mantle"},
        bedrock_only["published_digest"],
    )
    stream_attribution_ok = (
        bedrock_cluster in stream_trace.get("upstream_cluster", "")
        and stream_trace.get("response_code") == stream_status
        and stream_trace.get("route_name", "").startswith(
            "models-as-a-service.external-model-demo-model.")
        and stream_trace.get("model_header") == "demo"
        and stream_observation.get("authority") == bedrock_host
        and stream_observation.get("model") == "openai.gpt-oss-20b"
        and stream_observation.get("credential_ok") is True
        and stream_observation.get("tls") is True
        and stream_observation.get("sni_ok") is True
        and stream_observation.get("authority_ok") is True
    )
    record("bedrock_mantle_openai_compatible_sse_is_progressive",
           stream_status == "200" and stream_type.startswith("text/event-stream")
           and stream_total_ms - stream_first_ms >= 500
           and any(isinstance(item, dict) and "choices" in item for item in stream_data)
           and "[DONE]" in stream_data
           and stream_revision_stable and stream_attribution_ok,
           {"http_status": stream_status, "content_type": stream_type,
            "event_count": len(stream_events), "done_event": "[DONE]" in stream_data,
            "first_event_latency_ms": round(stream_first_ms, 1),
            "total_response_ms": round(stream_total_ms, 1),
            "progressive_delivery_gap_ms": round(stream_total_ms - stream_first_ms, 1),
            "serving_revision": bedrock_only["published_digest"],
            "serving_revision_after_stream": bedrock_after_sse["published_digest"],
            "upstream_cluster": stream_trace.get("upstream_cluster"),
            "backend_authority": stream_observation.get("authority"),
            "candidate_attribution_source": "Envoy upstream cluster plus provider recorder authority and model",
            "credential_value_saved": False})

    reload_marker = "externalmodel-overlay-reload-active-bedrock-stream"
    reload_trace_id = f"{ROUTE_TRACE_PREFIX}bedrock-overlay-reload-sse"
    reload_result = {}
    reload_first_event = threading.Event()
    def run_bedrock_reload_stream():
        try:
            reload_result["result"] = progressive_stream_request(
                "bedrock-mantle-overlay-reload-sse", externalmodel_url,
                {"model": "demo", "stream": True,
                 "messages": [{"role": "user", "content": reload_marker}]},
                extra_headers=[f"x-ipp-diagnostic-id: {reload_trace_id}"],
                record_result=False, on_first_event=reload_first_event.set, request_timeout=90)
        except Exception as exc:
            reload_result["error"] = repr(exc)

    stream_thread = threading.Thread(target=run_bedrock_reload_stream, daemon=True)
    stream_thread.start()
    try:
        if not reload_first_event.wait(timeout=10):
            raise RuntimeError(f"Bedrock SSE first event did not arrive: {reload_result.get('error')}")
        stream_open_before_reload = stream_thread.is_alive()
        reload_observation = wait_provider_observation(
            lambda item: reload_marker in item.get("message_text", []), "Bedrock in-flight overlay reload")
        updated = [dict(refs[0], weight=0), dict(bedrock_ref, targetModel="openai.gpt-oss-20b-reloaded")]
        kubectl("-n", NAMESPACE, "patch", "externalmodel", "demo-model", "--type=merge", "-p",
                json.dumps({"spec": {"externalProviderRefs": updated}}))
        reloaded = wait_externalmodel_revision({"provider-api-translation-bedrock-mantle"},
            "bedrock-mantle-inflight-overlay-reload", changed_from=bedrock_only["published_digest"])
        stream_open_when_new_revision_served = stream_thread.is_alive()
        RESULT.setdefault("bedrock_overlay_reload", {}).update({
            "original_revision": bedrock_only["published_digest"],
            "new_revision": reloaded["published_digest"],
            "serving_revision": reloaded["published_digest"],
            "new_target_model": "openai.gpt-oss-20b-reloaded",
            "serving_pods": [{"name": pod["name"], "uid": pod["uid"],
                "serving_revision": pod["serving_revision"]} for pod in reloaded["pods"]],
            "first_downstream_event_received": reload_first_event.is_set(),
            "stream_open_when_reload_started": stream_open_before_reload,
            "stream_open_after_first_event_when_new_revision_serves": stream_open_when_new_revision_served,
        })
    finally:
        stream_thread.join(timeout=100)
    if stream_thread.is_alive():
        raise RuntimeError("Bedrock SSE request did not complete after overlay reload")
    if "result" not in reload_result:
        raise RuntimeError(f"Bedrock SSE request failed after overlay reload: {reload_result.get('error')}")
    _, reload_status, reload_raw, reload_first_ms, reload_total_ms, reload_type = reload_result["result"]
    RESULT["requests"].append({"name": "bedrock-mantle-overlay-reload-sse",
                               "http_status": reload_status, "curl_exit_status": 0,
                               "path": externalmodel_url.split("?", 1)[0].rsplit("/", 1)[-1],
                               "first_event_latency_ms": round(reload_first_ms, 1),
                               "total_response_ms": round(reload_total_ms, 1)})
    save_json(RESULTS, RESULT)
    reload_trace = capture_runtime_route_trace(reload_trace_id, "bedrock-overlay-reload-sse", bedrock_route_trace_pod)
    reload_events = sse_events(reload_raw)
    reload_data = [event["data"] for event in reload_events]
    new_revision = RESULT.get("bedrock_overlay_reload", {}).get("new_revision")
    record("bedrock_mantle_inflight_stream_keeps_selected_profile_across_overlay_reload",
           reload_status == "200" and reload_type.startswith("text/event-stream")
           and any(isinstance(item, dict) and "choices" in item for item in reload_data)
           and "[DONE]" in reload_data and bool(new_revision)
           and new_revision != bedrock_only["published_digest"]
           and RESULT.get("bedrock_overlay_reload", {}).get("stream_open_when_reload_started") is True
           and RESULT.get("bedrock_overlay_reload", {}).get(
               "stream_open_after_first_event_when_new_revision_serves") is True
           and bedrock_cluster in reload_trace.get("upstream_cluster", "")
           and reload_trace.get("response_code") == reload_status
           and reload_observation.get("authority") == bedrock_host
           and reload_observation.get("model") == "openai.gpt-oss-20b"
           and reload_observation.get("credential_ok") is True
           and reload_observation.get("tls") is True
           and reload_observation.get("sni_ok") is True
           and reload_observation.get("authority_ok") is True,
           {"http_status": reload_status, "content_type": reload_type,
            "event_count": len(reload_events), "done_event": "[DONE]" in reload_data,
            "old_request_serving_revision": bedrock_only["published_digest"],
            "new_published_serving_revision": new_revision,
            "first_downstream_event_received": reload_first_event.is_set(),
            "stream_open_when_reload_started": stream_open_before_reload,
            "stream_open_when_new_revision_served": stream_open_when_new_revision_served,
            "upstream_cluster": reload_trace.get("upstream_cluster"),
            "backend_authority": reload_observation.get("authority"),
            "candidate_attribution_source": "Envoy upstream cluster plus provider recorder authority and model",
            "inflight_upstream_model": reload_observation.get("model"),
            "first_event_latency_ms": round(reload_first_ms, 1),
            "total_response_ms": round(reload_total_ms, 1), "credential_value_saved": False})
    next_marker = "bedrock-mantle-after-overlay-reload"
    next_trace_id = f"{ROUTE_TRACE_PREFIX}bedrock-after-overlay-reload"
    rc, next_status, next_raw = curl_request("bedrock-mantle-after-overlay-reload", externalmodel_url,
        {"model": "demo", "messages": [{"role": "user", "content": next_marker}]}, auth_file=auth_file,
        extra_headers=[f"x-ipp-diagnostic-id: {next_trace_id}"])
    next_trace = capture_runtime_route_trace(next_trace_id, "bedrock-after-overlay-reload", bedrock_route_trace_pod)
    next_observation = wait_provider_observation(
        lambda item: next_marker in item.get("message_text", []), "Bedrock request after overlay reload")
    next_response = json.loads(next_raw) if next_raw else {}
    bedrock_after_reload_request = externalmodel_runtime_snapshot(
        "bedrock-mantle-after-overlay-reload-request")
    next_revision_served = externalmodel_snapshot_serves_revision(
        bedrock_after_reload_request,
        {"provider-api-translation-bedrock-mantle"},
        new_revision,
    )
    record("bedrock_mantle_next_request_uses_reloaded_profile",
           rc == 0 and next_status == "200" and next_response.get("model") == "openai.gpt-oss-20b-reloaded"
           and next_observation.get("model") == "openai.gpt-oss-20b-reloaded"
           and next_revision_served
           and bedrock_cluster in next_trace.get("upstream_cluster", "")
           and next_trace.get("response_code") == next_status
           and next_observation.get("authority") == bedrock_host
           and next_observation.get("credential_ok") is True
           and next_observation.get("tls") is True and next_observation.get("sni_ok") is True
           and next_observation.get("authority_ok") is True,
           {"http_status": next_status, "upstream_model": next_observation.get("model"),
            "upstream_cluster": next_trace.get("upstream_cluster"),
            "backend_authority": next_observation.get("authority"),
            "candidate_attribution_source": "Envoy upstream cluster plus provider recorder authority and model",
            "serving_revision": new_revision,
            "serving_revision_after_request": bedrock_after_reload_request["published_digest"],
            "response_bytes": len(next_raw),
            "credential_value_saved": False})


def sanitize_scan():
    patterns = [r"(?i)authorization:\s*bearer\s+[A-Za-z0-9._~-]{12,}",
                r"(?i)(?:api[_-]?key|token)\s*[=:]\s*[A-Za-z0-9._~-]{16,}"]
    failures = []
    for path in EVIDENCE.rglob("*"):
        if path.is_file() and path.name not in ("credential-scan.txt",):
            try:
                text = path.read_text(errors="ignore")
            except OSError:
                continue
            if any(re.search(pattern, text) for pattern in patterns):
                failures.append(str(path.relative_to(EVIDENCE)))
            if any(key and key in text for key in PROVIDER_TEST_KEYS):
                failures.append(str(path.relative_to(EVIDENCE)))
    (EVIDENCE / "credential-scan.txt").write_text(
        "clean=true\n" if not failures else "clean=false\nfiles=" + ",".join(failures) + "\n")
    record("credential_scan", not failures, {"files": failures})


def capture_auth_diagnostics_on_failure():
    """Save policy state and aggregate log counts without retaining request values."""
    diagnostics = {}
    policy = kubectl("-n", "maas-system", "get", "authpolicy", "maas-gateway-auth", "-o", "json", check=False)
    diagnostics["authpolicy_get_exit_status"] = policy.returncode
    if policy.returncode == 0:
        policy_obj = json.loads(policy.stdout)
        diagnostics["authpolicy_conditions"] = [
            {key: condition.get(key) for key in ("type", "status", "reason")}
            for condition in policy_obj.get("status", {}).get("conditions", [])
        ]

    authorino = kubectl("-n", "kuadrant-system", "logs", "deploy/authorino", "--since=5m", check=False)
    authorino_levels = {}
    authorino_categories = {}
    authorino_lines = 0
    for line in authorino.stdout.splitlines():
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        authorino_lines += 1
        level = str(entry.get("level", "unknown")).lower()
        authorino_levels[level] = authorino_levels.get(level, 0) + 1
        message = str(entry.get("msg", entry.get("message", ""))).lower()
        for category, needle in (
            ("cel_evaluation", "failed to evaluate cel expression"),
            ("cel_parse", "failed to parse cel expression"),
            ("callback", "callback"),
            ("authorization", "authorization"),
            ("identity", "identity"),
        ):
            if needle in message:
                authorino_categories[category] = authorino_categories.get(category, 0) + 1
                break
    diagnostics["authorino_logs"] = {
        "exit_status": authorino.returncode,
        "parsed_lines": authorino_lines,
        "levels": authorino_levels,
        "message_categories": authorino_categories,
        "raw_messages_saved": False,
    }

    maas_api = kubectl("-n", "maas-system", "logs", "deploy/maas-api", "--since=5m", check=False)
    api_logs = maas_api.stdout.lower()
    diagnostics["maas_api_log_path_counts"] = {
        "exit_status": maas_api.returncode,
        "api_key_validation": api_logs.count("/internal/v1/api-keys/validate"),
        "subscription_selection": api_logs.count("/internal/v1/subscriptions/select"),
        "raw_log_lines_saved": False,
    }
    save_json(EVIDENCE / "auth-diagnostics-on-failure.json", diagnostics)


def cleanup():
    global KEY_ID, MTC_ORIGINAL, MTC_CAPTURED, EXTERNAL_MODEL_CHANGED, PROVIDER_AWARE_CREATED
    global DIAGNOSTIC_ROUTE_AUTH_CREATED
    if KEY_ID and API_PORT and (TEMP / "admin.cfg").exists():
        ca_file = TEMP / "maas-api-ca.crt"
        host = f"maas-api.maas-system.svc.cluster.local:{API_PORT}:127.0.0.1"
        revoked = run(["curl", "--noproxy", "*", "-sS", "--connect-timeout", "5", "--max-time", "20",
             "--cacert", str(ca_file), "--resolve", host, "--config", str(TEMP / "admin.cfg"),
             "-X", "DELETE", "-o", os.devnull, "-w", "%{http_code}",
             f"https://maas-api.maas-system.svc.cluster.local:{API_PORT}/v1/api-keys/{KEY_ID}"],
            check=False, timeout=25)
        (EVIDENCE / "api-key-revocation.txt").write_text(
            f"http_status={revoked.stdout.strip() or 'unknown'} key_value_saved=false\n")
    if EXTERNAL_MODEL_CHANGED and EXTERNAL_MODEL_ORIGINAL_REFS is not None:
        kubectl("-n", NAMESPACE, "patch", "externalmodel", "demo-model", "--type=merge", "-p",
                json.dumps({"spec": {"externalProviderRefs": EXTERNAL_MODEL_ORIGINAL_REFS}}), check=False)
    if MTC_CAPTURED:
        if not MTC_HAD_TRANSLATION:
            kubectl("-n", NAMESPACE, "patch", "maastenantconfig", "default-tenant", "--type=json",
                    "-p", '[{"op":"remove","path":"/spec/apiTranslation"}]', check=False)
        else:
            kubectl("-n", NAMESPACE, "patch", "maastenantconfig", "default-tenant", "--type=merge",
                    "-p", json.dumps({"spec": {"apiTranslation": MTC_ORIGINAL}}), check=False)
    restored = not EXTERNAL_MODEL_CHANGED
    if EXTERNAL_MODEL_CHANGED and EXTERNAL_MODEL_ORIGINAL_OVERLAY is not None:
        for _ in range(120):
            current_model = kubectl("-n", NAMESPACE, "get", "externalmodel", "demo-model", "-o", "json", check=False)
            current_overlay = kubectl("-n", NAMESPACE, "get", "configmap", "routing-overlay", "-o", "json", check=False)
            if current_model.returncode == 0 and current_overlay.returncode == 0:
                model_obj = json.loads(current_model.stdout)
                overlay_obj = json.loads(current_overlay.stdout)
                phase = model_obj.get("status", {}).get("phase")
                digest = overlay_obj.get("metadata", {}).get("annotations", {}).get(
                    "inference.opendatahub.io/routing-overlay-content-digest", "")
                if phase == "Ready" and digest == EXTERNAL_MODEL_ORIGINAL_OVERLAY.get("content_digest"):
                    restored = True
                    break
            time.sleep(1)
    if restored:
        EXTERNAL_MODEL_CHANGED = False
        if PROVIDER_AWARE_CREATED:
            kubectl("-n", NAMESPACE, "delete", "externalprovider", "api-translation-openai",
                    "api-translation-anthropic", "api-translation-bedrock-mantle",
                    "--ignore-not-found", "--wait=true", check=False)
            kubectl("-n", "maas-system", "delete", "deployment", "api-translation-provider-aware-mock",
                    "--ignore-not-found", "--wait=true", check=False)
            kubectl("-n", "maas-system", "delete", "service", "provider-translation-ext",
                    "provider-translation-openai-ext", "provider-translation-bedrock-ext",
                    "--ignore-not-found", "--wait=true", check=False)
            kubectl("-n", NAMESPACE, "delete", "secret", "api-translation-openai-credentials",
                    "api-translation-anthropic-credentials", "api-translation-bedrock-credentials",
                    "--ignore-not-found", "--wait=true", check=False)
            kubectl("-n", "maas-system", "delete", "secret", "api-translation-openai-backend-credential",
                    "api-translation-anthropic-backend-credential", "api-translation-bedrock-backend-credential",
                    "--ignore-not-found", "--wait=true", check=False)
            PROVIDER_AWARE_CREATED = False
        for kind, name in reversed(LIVE_PROVIDER_RESOURCES):
            kubectl("-n", NAMESPACE, "delete", kind, name, "--ignore-not-found", "--wait=true", check=False)
        LIVE_PROVIDER_RESOURCES.clear()
        (EVIDENCE / "externalmodel-fixture-cleanup.txt").write_text(
            "Original ExternalModel refs and overlay revision restored before removing test provider resources.\n")
    elif EXTERNAL_MODEL_CHANGED:
        CLEANUP_FAILURES.append("ExternalModel serving overlay did not return to its original revision; provider fixture retained")
        (EVIDENCE / "externalmodel-fixture-cleanup.txt").write_text(
            "restore failed; provider fixture resources and credentials retained for diagnosis\n")
    else:
        (EVIDENCE / "externalmodel-fixture-cleanup.txt").write_text(
            "ExternalModel fixture was not changed; no provider fixture resources were created.\n")
    if DIAGNOSTIC_ROUTE_AUTH_CREATED:
        removed = kubectl(
            "-n", NAMESPACE, "delete", "authpolicy", "api-translation-messages-auth",
            "api-translation-openai-auth", "--ignore-not-found", "--wait=true", check=False)
        (EVIDENCE / "diagnostic-route-auth-cleanup.txt").write_text(
            f"delete_exit_status={removed.returncode} resources=2\n")
        DIAGNOSTIC_ROUTE_AUTH_CREATED = False
    kubectl("-n", NAMESPACE, "delete", "tokenratelimitpolicy", "api-translation-messages-rate-limit",
            "api-translation-openai-rate-limit", "--ignore-not-found", check=False)
    kubectl("-n", NAMESPACE, "delete", "httproute", "api-translation-messages", "api-translation-openai",
            "--ignore-not-found", check=False)
    kubectl("-n", NAMESPACE, "delete", "deployment", "api-translation-openai-mock", "--ignore-not-found",
            check=False)
    kubectl("-n", NAMESPACE, "delete", "service", "translation-openai-mock", "--ignore-not-found", check=False)
    kubectl("-n", "maas-system", "delete", "envoyfilter", ROUTE_TRACE_FILTER,
            "--ignore-not-found", check=False)
    trace_filter = kubectl("-n", "maas-system", "get", "envoyfilter", ROUTE_TRACE_FILTER,
                           "-o", "name", check=False)
    save_json(EVIDENCE / "envoy-runtime-route-trace-cleanup.json", {
        "filter": f"maas-system/{ROUTE_TRACE_FILTER}",
        "present_after_cleanup": trace_filter.returncode == 0,
        "filter_get_exit_status": trace_filter.returncode,
    })
    for proc in (API_FORWARD, GATEWAY_FORWARD, RECORDER_FORWARD, PROVIDER_RECORDER_FORWARD):
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
    shutil.rmtree(TEMP, ignore_errors=True)


def main():
    global LIVE_PROVIDER_SELECTION
    global DIAGNOSTIC_ROUTE_AUTH_CREATED
    global API_PORT, GATEWAY_PORT, GATEWAY_AUTHORITY, API_FORWARD, GATEWAY_FORWARD
    global RECORDER_FORWARD, RECORDER_PORT, KEY_ID, KEY_VALUE
    global MTC_ORIGINAL, MTC_HAD_TRANSLATION, MTC_CAPTURED
    parser = argparse.ArgumentParser(description="Run deterministic translation E2E; live providers are opt-in.")
    parser.add_argument("--scope", choices=("all", "shared", "externalmodel"), default="all",
                        help="qualify both pipelines (all), only the shared route, or only ExternalModel")
    parser.add_argument("--live-provider", choices=("anthropic", "openai", "bedrock", "all"),
                        help="optionally qualify one live provider or all through the deployed Gateway; Bedrock reads the local Bedrock key entry and accepts BEDROCK_MANTLE_REGION/MODEL overrides")
    parser.add_argument("--diagnostic-route-auth", action="store_true",
                        help="use run-owned route AuthPolicies that validate the MaaS key directly; diagnostic only, not final Gateway auth qualification")
    args = parser.parse_args()
    LIVE_PROVIDER_SELECTION = args.live_provider
    if LIVE_PROVIDER_SELECTION and args.scope == "shared":
        parser.error("--live-provider requires --scope externalmodel or --scope all")
    RESULT["scope"] = args.scope
    RESULT["authentication_mode"] = (
        "diagnostic_route_targeted_maas_key_validation" if args.diagnostic_route_auth else
        "installed_maas_gateway_authpolicy"
    )
    if LIVE_PROVIDER_SELECTION:
        RESULT["live_provider_selection"] = LIVE_PROVIDER_SELECTION
    save_json(RESULTS, RESULT)
    verify_controller_image()
    mtc_text = kubectl("-n", NAMESPACE, "get", "maastenantconfig", "default-tenant", "-o", "json").stdout
    mtc = json.loads(mtc_text)
    MTC_CAPTURED = True
    MTC_HAD_TRANSLATION = "apiTranslation" in mtc.get("spec", {})
    MTC_ORIGINAL = mtc.get("spec", {}).get("apiTranslation", None)
    save_json(EVIDENCE / "maastenantconfig-before.json", mtc)
    installed_schema = kubectl("get", "crd", "maastenantconfigs.maas.opendatahub.io", "-o", "json").stdout
    schema = json.loads(installed_schema)
    schema_field = schema["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]["properties"].get("apiTranslation")
    save_json(EVIDENCE / "installed-api-translation-schema.json", schema_field or {})
    record("installed_schema", isinstance(schema_field, dict) and "enabled" in schema_field.get("properties", {}),
           {"field_present": isinstance(schema_field, dict), "enabled_schema": (schema_field or {}).get("properties", {}).get("enabled")})

    provider_manifest = (ROOT / "test/kind-env/manifests/shared-translation-backend.yaml").read_text()
    provider_manifest = provider_manifest.replace("provider-recorder:issue28-kind", IMAGE)
    provider_apply = run(["kubectl", "--context", f"kind-{CLUSTER}", "apply", "-f", "-"], input_text=provider_manifest)
    (EVIDENCE / "shared-backend-apply.txt").write_text(provider_apply.stdout)
    route_manifest = (ROOT / "test/kind-env/manifests/shared-translation-routes.yaml").read_text()
    route_documents = [document for document in re.split(r"(?m)^---\s*$", route_manifest) if document.strip()]
    route_document_kinds = [
        re.findall(r"(?m)^kind:\s*([A-Za-z0-9]+)\s*$", document)
        for document in route_documents
    ]
    expected_route_document_kinds = [
        ["HTTPRoute"], ["HTTPRoute"], ["TokenRateLimitPolicy"], ["TokenRateLimitPolicy"],
    ]
    if route_document_kinds != expected_route_document_kinds:
        raise RuntimeError(
            "shared translation route manifest must contain two HTTPRoutes followed by two "
            f"TokenRateLimitPolicies; found document kinds {route_document_kinds}"
        )
    save_json(EVIDENCE / "shared-translation-routes-manifest-validation.json", {
        "status": "PASS", "document_count": len(route_documents), "document_kinds": route_document_kinds,
    })
    record("shared_translation_routes_manifest_structure", True, {
        "document_count": len(route_documents), "document_kinds": route_document_kinds,
    })
    (EVIDENCE / "shared-translation-routes-applied.yaml").write_text(route_manifest)
    route_reconcile_started_at = datetime.datetime.now(datetime.timezone.utc)
    route_apply = run(["kubectl", "--context", f"kind-{CLUSTER}", "apply", "-f", "-"], input_text=route_manifest)
    (EVIDENCE / "shared-routes-apply.txt").write_text(route_apply.stdout)
    for route_name in ("api-translation-messages", "api-translation-openai"):
        route_snapshot = kubectl("-n", NAMESPACE, "get", "httproute", route_name, "-o", "json", check=False)
        if route_snapshot.returncode == 0:
            save_json(EVIDENCE / f"httproute-{route_name}-immediately-after-apply.json",
                      json.loads(route_snapshot.stdout))
        else:
            (EVIDENCE / f"httproute-{route_name}-missing-after-apply.txt").write_text(route_snapshot.stderr)
    if args.diagnostic_route_auth:
        diagnostic_policy_names = ("api-translation-messages-auth", "api-translation-openai-auth")
        existing_policies = [name for name in diagnostic_policy_names
                             if kubectl("-n", NAMESPACE, "get", "authpolicy", name,
                                        check=False).returncode == 0]
        if existing_policies:
            raise RuntimeError(
                "diagnostic AuthPolicy fixture already exists; refusing to overwrite: "
                + ", ".join(existing_policies))
        DIAGNOSTIC_ROUTE_AUTH_CREATED = True
        diagnostic_policy_manifest = (
            ROOT / "test/kind-env/manifests/shared-translation-auth-diagnostic.yaml").read_text()
        (EVIDENCE / "shared-translation-auth-diagnostic-applied.yaml").write_text(
            diagnostic_policy_manifest)
        diagnostic_policy_apply = run(
            ["kubectl", "--context", f"kind-{CLUSTER}", "apply", "-f", "-"],
            input_text=diagnostic_policy_manifest)
        (EVIDENCE / "shared-translation-auth-diagnostic-apply.txt").write_text(
            diagnostic_policy_apply.stdout)
        for policy_name in diagnostic_policy_names:
            wait_auth_policy(policy_name, namespace=NAMESPACE)
        record("diagnostic_route_auth_fixture_ready", True, {
            "policies": [f"{NAMESPACE}/{name}" for name in diagnostic_policy_names],
            "authentication": "temporary Bearer MaaS key validated through MaaS API",
            "production_policy_changed": False,
            "final_gateway_auth_qualification": False,
        })
    rollout = kubectl("-n", NAMESPACE, "rollout", "status", "deployment/api-translation-openai-mock", "--timeout=120s")
    recorder_deployment = json.loads(kubectl(
        "-n", NAMESPACE, "get", "deployment", "api-translation-openai-mock", "-o", "json").stdout)
    recorder_pods = json.loads(kubectl(
        "-n", NAMESPACE, "get", "pods", "-l", "app=api-translation-openai-mock", "-o", "json").stdout)
    recorder_images = []
    for pod in recorder_pods.get("items", []):
        statuses = {status.get("name"): status for status in pod.get("status", {}).get("containerStatuses", [])}
        containers = []
        for container in pod.get("spec", {}).get("containers", []):
            status = statuses.get(container.get("name"), {})
            containers.append({
                "name": container.get("name"),
                "requestedImage": container.get("image"),
                "image": status.get("image"),
                "pod_imageID": status.get("imageID"),
                "ready": status.get("ready"),
            })
        recorder_images.append({
            "name": pod.get("metadata", {}).get("name"),
            "node": pod.get("spec", {}).get("nodeName"),
            "phase": pod.get("status", {}).get("phase"),
            "containers": containers,
        })
    for pod in recorder_images:
        for container in pod.get("containers", []):
            requested_image = container.get("requestedImage", "")
            cri_reference = requested_image if "/" in requested_image else f"docker.io/library/{requested_image}"
            inspected = run(["docker", "exec", pod.get("node", ""), "crictl", "inspecti", cri_reference],
                            check=False) if pod.get("node") and requested_image else None
            try:
                runtime = json.loads(inspected.stdout) if inspected and inspected.returncode == 0 else {}
            except json.JSONDecodeError:
                runtime = {}
            runtime_status = runtime.get("status", {})
            container["runtime_image_reference"] = cri_reference
            container["runtime_imageID"] = runtime_status.get("id")
            container["repoTags"] = runtime_status.get("repoTags", [])
            container["requested_tag_present"] = cri_reference in container["repoTags"]
    recorder_identity = {
        "requested_image": IMAGE,
        "local_docker_image_id": (run(["docker", "image", "inspect", IMAGE, "--format", "{{.Id}}"],
                                      check=False).stdout.strip() or None),
        "local_image_config_id": local_image_config_id(IMAGE),
        "deployment_template_images": [c.get("image") for c in recorder_deployment.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])],
        "pods": recorder_images,
    }
    save_json(EVIDENCE / "provider-recorder-image-identities.json", recorder_identity)
    recorder_identity_matches = (
        recorder_identity["deployment_template_images"] == [IMAGE]
        and len(recorder_images) == 1
        and recorder_images[0].get("phase") == "Running"
        and len(recorder_images[0].get("containers", [])) == 1
        and all(container.get("requestedImage") == IMAGE
                and container.get("requested_tag_present") is True
                and str(container.get("runtime_imageID", "")).removeprefix("containerd://")
                == str(recorder_identity.get("local_image_config_id", "")).removeprefix("containerd://")
                and str(container.get("runtime_imageID", "")).removeprefix("containerd://")
                == str(container.get("pod_imageID", "")).removeprefix("containerd://")
                and container.get("ready") is True
                for pod in recorder_images for container in pod.get("containers", []))
    )
    record("provider_recorder_exact_image_loaded", recorder_identity_matches, recorder_identity)
    RECORDER_FORWARD, RECORDER_PORT = start_port_forward("translation-openai-mock", 8080, NAMESPACE)
    route_messages = wait_route("api-translation-messages")
    route_openai = wait_route("api-translation-openai")
    externalmodel_auth_policy = None
    gateway_auth_context = {}
    gateway_auth_policy = wait_auth_policy("maas-gateway-auth", namespace="maas-system")
    maas_auth_policy_ready = wait_maas_auth_policy_ready(not_before=route_reconcile_started_at)
    if args.scope != "shared":
        # ExternalModel routes inherit MaaS's controller-owned Gateway
        # AuthPolicy. A fixture route policy would shadow this contract.
        externalmodel_auth_policy = route_target_auth_policies("external-model-demo-model")
    rate_limit_messages = wait_rate_limit_policy("api-translation-messages-rate-limit")
    rate_limit_openai = wait_rate_limit_policy("api-translation-openai-rate-limit")
    gateway_policy_conditions = gateway_auth_policy.get("status", {}).get("conditions", [])
    gateway_policy_accepted = any(condition.get("type") == "Accepted" and condition.get("status") == "True"
                                  for condition in gateway_policy_conditions)
    gateway_policy_enforced = any(condition.get("type") == "Enforced" and condition.get("status") == "True"
                                  for condition in gateway_policy_conditions)
    route_auth_policies_absent = (args.scope == "shared"
                                  or (externalmodel_auth_policy.get("exit_status") == 0
                                      and not externalmodel_auth_policy.get("policies")))
    gateway_policy_config = safe_authpolicy_config_snapshot("before-externalmodel-traffic")
    save_json(EVIDENCE / "route-and-policy-state.json", {
        "rollout": rollout.stdout,
        "message_route": {"name": route_messages.get("metadata", {}).get("name"),
                          "conditions": route_messages.get("status", {}).get("parents", [])},
        "openai_route": {"name": route_openai.get("metadata", {}).get("name"),
                         "conditions": route_openai.get("status", {}).get("parents", [])},
        "gateway_auth_policy": gateway_policy_config,
        "externalmodel_auth_policy": externalmodel_auth_policy,
        "maas_auth_policy_ready_conditions": maas_auth_policy_ready,
        "message_rate_limit_policy": rate_limit_messages,
        "openai_rate_limit_policy": rate_limit_openai,
    })
    gateway_auth_context = {
        "gateway_policy_identity": {
            "namespace": gateway_auth_policy.get("metadata", {}).get("namespace"),
            "name": gateway_auth_policy.get("metadata", {}).get("name"),
            "generation": gateway_auth_policy.get("metadata", {}).get("generation"),
        },
        "gateway_policy_accepted": gateway_policy_accepted,
        "gateway_policy_enforced": gateway_policy_enforced,
        "maas_auth_policy_ready": bool(maas_auth_policy_ready),
        "maas_auth_policy_no_conflict": maas_auth_policy_ready.get("no_conflicting_auth_policy") is True,
        "route_auth_policies": externalmodel_auth_policy,
        "route_auth_policies_absent": route_auth_policies_absent,
    }
    record("routes_and_auth_policies",
           gateway_policy_accepted and gateway_policy_enforced
           and (args.scope == "shared" or (
               gateway_auth_context["maas_auth_policy_ready"]
               and gateway_auth_context["maas_auth_policy_no_conflict"]
               and route_auth_policies_absent)), {
        "routes_accepted": ["api-translation-messages", "api-translation-openai"],
        "inherited_gateway_auth_policy": "maas-system/maas-gateway-auth",
        "gateway_auth_policy_accepted": gateway_policy_accepted,
        "gateway_auth_policy_enforced": gateway_policy_enforced,
        "maas_auth_policy_ready": gateway_auth_context["maas_auth_policy_ready"],
        "maas_auth_policy_no_conflict": gateway_auth_context["maas_auth_policy_no_conflict"],
        "route_auth_policies": externalmodel_auth_policy,
        "gateway_authentication_methods": sorted(
            gateway_auth_policy.get("spec", {}).get("defaults", {}).get("rules", {}).get("authentication", {}).keys()),
        "externalmodel_route_authentication_methods": [],
        "rate_limit_policies": ["100 requests/minute per MaaS identity on each test route"],
        "authorization_backend": "real maas-api internal api-key validation endpoint",
    })

    API_FORWARD, API_PORT = start_port_forward("maas-api", 8443)
    GATEWAY_FORWARD, GATEWAY_PORT = start_port_forward("maas-default-gateway-istio", 80)
    gateway = json.loads(kubectl(
        "-n", "maas-system", "get", "gateway", "maas-default-gateway", "-o", "json").stdout)
    GATEWAY_AUTHORITY = next((address.get("value") for address in
                              gateway.get("status", {}).get("addresses", [])
                              if address.get("type") == "Hostname" and address.get("value")), None)
    if not GATEWAY_AUTHORITY:
        raise RuntimeError("maas-default-gateway has no advertised Hostname address")
    record("gateway_authority_discovered", True, {
        "gateway": "maas-system/maas-default-gateway",
        "authority": GATEWAY_AUTHORITY,
        "port_forward_authority_overridden": True,
    })
    ca = kubectl("-n", "kuadrant-system", "get", "configmap", "authorino-maas-api-ca",
                 "-o", r"jsonpath={.data.ca\.crt}").stdout
    (TEMP / "maas-api-ca.crt").write_text(ca)
    (TEMP / "admin.cfg").write_text(
        'header = "X-MaaS-Username: kind-user"\n'
        'header = "X-MaaS-Group: [\\"system:authenticated\\"]"\n'
    )
    (TEMP / "admin.cfg").chmod(0o600)
    api_host = f"maas-api.maas-system.svc.cluster.local:{API_PORT}:127.0.0.1"
    key_body = TEMP / "key-response.json"
    key_request = '{"name":"ipp-phase1-api-translation","ephemeral":true,"subscription":"kind-e2e-subscription"}'
    key_args = ["curl", "--noproxy", "*", "-sS", "--connect-timeout", "5", "--max-time", "20",
                "--cacert", str(TEMP / "maas-api-ca.crt"), "--resolve", api_host,
                "--config", str(TEMP / "admin.cfg"), "-H", "content-type: application/json",
                "-d", key_request, "-o", str(key_body), "-w", "%{http_code}",
                f"https://maas-api.maas-system.svc.cluster.local:{API_PORT}/v1/api-keys"]
    key_result = run(key_args, check=False, timeout=25)
    key_status = key_result.stdout.strip()
    created = json.loads(key_body.read_text()) if key_body.exists() else {}
    KEY_ID = created.get("id") or created.get("keyId") or created.get("metadata", {}).get("id")
    KEY_VALUE = created.get("key") or created.get("apiKey") or created.get("token")
    if key_status != "201" or not KEY_ID or not KEY_VALUE:
        diagnostic = {
            "http_status": key_status or "unknown",
            "curl_exit_status": key_result.returncode,
            "stderr": key_result.stderr[-1200:],
            "api_key_saved": False,
        }
        save_json(EVIDENCE / "api-key-create-diagnostic.json", diagnostic)
        raise RuntimeError(f"MaaS API key setup failed with HTTP {key_status or 'unknown'}; see api-key-create-diagnostic.json")
    auth_cfg = TEMP / "auth.cfg"
    auth_cfg.write_text(f'header = "Authorization: Bearer {KEY_VALUE}"\n')
    auth_cfg.chmod(0o600)
    (EVIDENCE / "api-key-created.txt").write_text("http_status=201 key_id_present=true key_value_saved=false\n")
    record("maas_api_key_created", True, {"status": 201, "key_value_saved": False})

    # Check the exact metadata callback contract independently before sending
    # the key through Authorino. Keep the key only in the run-private temp
    # directory and save the validation result without saving its response.
    validation_input = TEMP / "api-key-validation-request.json"
    validation_input.write_text(json.dumps({"key": KEY_VALUE}))
    validation_input.chmod(0o600)
    validation_output = TEMP / "api-key-validation-response.json"
    validation = run(["curl", "--noproxy", "*", "-sS", "--connect-timeout", "5", "--max-time", "20",
         "--cacert", str(TEMP / "maas-api-ca.crt"), "--resolve", api_host,
         "-H", "content-type: application/json", "--data-binary", f"@{validation_input}",
         "-o", str(validation_output), "-w", "%{http_code}",
         f"https://maas-api.maas-system.svc.cluster.local:{API_PORT}/internal/v1/api-keys/validate"],
        check=False, timeout=25)
    validation_status = validation.stdout.strip()
    try:
        validation_body = json.loads(validation_output.read_text()) if validation_output.exists() else {}
    except json.JSONDecodeError:
        validation_body = {}
    validation_valid = validation_body.get("valid")
    validation_reason = validation_body.get("reason")
    if validation_reason not in {
        "invalid key format", "key not found", "key revoked or expired", "key has no subscription bound",
    }:
        validation_reason = "other" if validation_reason else None
    (EVIDENCE / "api-key-direct-validation.txt").write_text(
        f"http_status={validation_status or 'unknown'} valid={str(validation_valid).lower()} "
        f"reason={validation_reason or 'none'} "
        f"response_fields={','.join(sorted(validation_body.keys()))} key_value_saved=false\n")
    record("maas_api_key_direct_validation",
           validation.returncode == 0 and validation_status == "200" and validation_valid is True,
           {"http_status": validation_status or "unknown", "valid": validation_valid,
            "reason": validation_reason, "response_fields": sorted(validation_body.keys()),
            "key_value_saved": False})

    # Mirror the non-secret inputs used by the Gateway AuthPolicy's
    # subscription-info callback. This distinguishes a provider/auth policy
    # denial from a missing subscription or model binding without saving the
    # API key, identity payload, or full callback response.
    selection_request = {
        "groups": validation_body.get("groups", []),
        "username": validation_body.get("username", ""),
        "requestedSubscription": validation_body.get("subscription", ""),
        "requestedModel": "demo",
    }
    selection_input = TEMP / "subscription-selection-request.json"
    selection_input.write_text(json.dumps(selection_request))
    selection_input.chmod(0o600)
    selection_output = TEMP / "subscription-selection-response.json"
    selection_probe = run([
        "curl", "--noproxy", "*", "-sS", "--connect-timeout", "5", "--max-time", "20",
        "--cacert", str(TEMP / "maas-api-ca.crt"), "--resolve", api_host,
        "-H", "content-type: application/json", "--data-binary", f"@{selection_input}",
        "-o", str(selection_output), "-w", "%{http_code}",
        f"https://maas-api.maas-system.svc.cluster.local:{API_PORT}/internal/v1/subscriptions/select",
    ], check=False, timeout=25)
    try:
        selection_body = json.loads(selection_output.read_text()) if selection_output.exists() else {}
    except json.JSONDecodeError:
        selection_body = {}
    selection_diagnostic = {
        "http_status": selection_probe.stdout.strip() or "unknown",
        "curl_exit_status": selection_probe.returncode,
        "requested_model": "demo",
        "requested_subscription_matches_key": bool(selection_request["requestedSubscription"]),
        "group_count": len(selection_request["groups"] or []),
        "access_allowed": selection_body.get("accessAllowed"),
        "error": selection_body.get("error"),
        "phase": selection_body.get("phase"),
        "subscription_present": bool(selection_body.get("name")),
        "resolved_model": selection_body.get("resolvedModel"),
        "response_fields": sorted(selection_body.keys()),
        "identity_and_response_saved": False,
    }
    save_json(EVIDENCE / "direct-subscription-selection-diagnostic.json", selection_diagnostic)
    record("maas_subscription_selection_probe_completed",
           selection_probe.returncode == 0 and selection_diagnostic["http_status"] == "200",
           selection_diagnostic)

    path_selection_request = {**selection_request, "requestedModel": f"{NAMESPACE}/demo"}
    path_selection_input = TEMP / "path-subscription-selection-request.json"
    path_selection_input.write_text(json.dumps(path_selection_request))
    path_selection_input.chmod(0o600)
    path_selection_output = TEMP / "path-subscription-selection-response.json"
    path_selection_probe = run([
        "curl", "--noproxy", "*", "-sS", "--connect-timeout", "5", "--max-time", "20",
        "--cacert", str(TEMP / "maas-api-ca.crt"), "--resolve", api_host,
        "-H", "content-type: application/json", "--data-binary", f"@{path_selection_input}",
        "-o", str(path_selection_output), "-w", "%{http_code}",
        f"https://maas-api.maas-system.svc.cluster.local:{API_PORT}/internal/v1/subscriptions/select",
    ], check=False, timeout=25)
    try:
        path_selection_body = json.loads(path_selection_output.read_text()) if path_selection_output.exists() else {}
    except json.JSONDecodeError:
        path_selection_body = {}
    path_selection_diagnostic = {
        "http_status": path_selection_probe.stdout.strip() or "unknown",
        "curl_exit_status": path_selection_probe.returncode,
        "requested_model": path_selection_request["requestedModel"],
        "access_allowed": path_selection_body.get("accessAllowed"),
        "error": path_selection_body.get("error"),
        "phase": path_selection_body.get("phase"),
        "subscription_present": bool(path_selection_body.get("name")),
        "resolved_model": path_selection_body.get("resolvedModel"),
        "response_fields": sorted(path_selection_body.keys()),
        "identity_and_response_saved": False,
    }
    save_json(EVIDENCE / "direct-path-subscription-selection-diagnostic.json", path_selection_diagnostic)
    RESULT.setdefault("diagnostics", []).append({"name": "maas_subscription_selection_path_model",
                                                  **path_selection_diagnostic})
    save_json(RESULTS, RESULT)

    if args.scope == "externalmodel":
        kubectl("-n", NAMESPACE, "patch", "maastenantconfig", "default-tenant", "--type=merge",
                "-p", '{"spec":{"apiTranslation":{"enabled":true}}}')
        external_config_ready = False
        for _ in range(90):
            external_config = kubectl("-n", NAMESPACE, "get", "configmap",
                "payload-processing-external-model-plugins", "-o", "json", check=False)
            if external_config.returncode == 0:
                config_text = json.loads(external_config.stdout).get("data", {}).get("extproc.yaml", "")
                if "openai_chat_completions_to_anthropic" in config_text and "api_translation_enabled: true" in config_text:
                    external_config_ready = True
                    break
            time.sleep(1)
        record("externalmodel_translation_permission_projected", external_config_ready,
               {"authoritative_setting": "MaasTenantConfig.spec.apiTranslation.enabled",
                "enabled": external_config_ready})
        test_externalmodel_provider_aware_translation(auth_cfg, gateway_auth_context)
        if LIVE_PROVIDER_SELECTION:
            selected = (("anthropic", "openai", "bedrock") if LIVE_PROVIDER_SELECTION == "all"
                        else (LIVE_PROVIDER_SELECTION,))
            for provider in selected:
                try:
                    qualify_live_provider(provider, auth_cfg, gateway_auth_context)
                except Exception as exc:
                    append_live_provider_result(provider, "BLOCKED", {
                        "reason": redact_text(str(exc)),
                        "exception_type": type(exc).__name__,
                        "direct_provider_request": False,
                        "credential_value_saved": False,
                    })
        save_json(EVIDENCE / "provider-aware-upstream-observations.json", provider_aware_observations())
        current_mtc = json.loads(kubectl("-n", NAMESPACE, "get", "maastenantconfig", "default-tenant", "-o", "json").stdout)
        save_json(EVIDENCE / "maastenantconfig-feature-on.json", current_mtc)
        save_json(EVIDENCE / "request-outcomes.json", RESULT["requests"])
        sanitize_scan()
        live_statuses = [item["status"] for item in RESULT.get("live_provider_results", [])]
        live_complete = bool(live_statuses) and all(status == "PASS" for status in live_statuses)
        RESULT["deterministic_status"] = "PASS"
        RESULT["live_provider_status"] = (
            "PASS" if live_complete else "PARTIAL" if LIVE_PROVIDER_SELECTION else "NOT_RUN"
        )
        RESULT["status"] = (
            "PARTIAL" if LIVE_PROVIDER_SELECTION and not live_complete
            else "PASS"
        )
        RESULT["other_scope_status"] = "NOT_RUN"
        RESULT["authentication_mode"] = (
            "diagnostic route-targeted MaaS-key validation"
            if args.diagnostic_route_auth else "production AuthPolicy"
        )
        save_json(RESULTS, RESULT)
        return

    # Check the same MaaS API key on the Gateway's model-list route, which
    # exercises authentication without the body-model subscription condition.
    auth_control_headers = TEMP / "model-list-auth-control.headers"
    auth_control_body = TEMP / "model-list-auth-control.body"
    auth_control = run([
        "curl", "--noproxy", "*", "-sS", "--connect-timeout", "5", "--max-time", "20",
        "--config", str(auth_cfg), "-D", str(auth_control_headers), "-o", str(auth_control_body),
        "-H", f"Host: {GATEWAY_AUTHORITY}",
        "-w", "%{http_code}", f"http://127.0.0.1:{GATEWAY_PORT}/v1/models",
    ], check=False, timeout=25)
    auth_control_text = auth_control_headers.read_text(errors="replace") if auth_control_headers.exists() else ""
    auth_control_reason = next((line.split(":", 1)[1].strip() for line in auth_control_text.splitlines()
                                if line.lower().startswith("x-ext-auth-reason:")), None)
    auth_control_diagnostic = {
        "http_status": auth_control.stdout.strip() or "unknown",
        "curl_exit_status": auth_control.returncode,
        "response_bytes": auth_control_body.stat().st_size if auth_control_body.exists() else 0,
        "ext_auth_reason": auth_control_reason,
        "authorization_value_saved": False,
    }
    save_json(EVIDENCE / "gateway-model-list-auth-control.json", auth_control_diagnostic)
    RESULT.setdefault("diagnostics", []).append({"name": "gateway_model_list_auth_control",
                                                  **auth_control_diagnostic})
    save_json(RESULTS, RESULT)

    extproc_off, pods_off = capture_shared_runtime("feature-off")
    record("feature_off_config", all(name not in extproc_off for name in (
        "anthropic_messages_request", "anthropic_messages_to_chat_completions", "path_rewrite")),
        {"translator_filters_absent": True, "ready_shared_pods": [p["name"] for p in pods_off if p["ready"]]})
    capture_envoy("feature-off")
    shared_route_trace_gateway_pod = install_runtime_route_trace()
    wait_for_auth_policy_diagnostic_release()

    unauth_body = {"model": "demo", "max_tokens": 32,
                   "messages": [{"role": "user", "content": "phase1-auth-denied-marker"}]}
    before_unauth = observations()
    rc, status, _ = curl_request("unauthenticated", f"http://127.0.0.1:{GATEWAY_PORT}/v1/messages",
                                 unauth_body, extra_headers=["x-api-key: client-marker"])
    after_unauth = observations()
    record("maas_auth_denies_unauthenticated", rc == 0 and status == "401" and len(after_unauth) == len(before_unauth),
           {"http_status": status, "upstream_contact": len(after_unauth) != len(before_unauth)})

    off_marker = "phase1-feature-off-marker"
    anthropic_off = {"model": "demo", "max_tokens": 32,
                     "messages": [{"role": "user", "content": [{"type": "text", "text": off_marker}]}]}
    no_client_key_marker = "phase1-feature-off-no-client-key-diagnostic"
    no_client_key_body = {"model": "demo", "max_tokens": 16,
                          "messages": [{"role": "user", "content": no_client_key_marker}]}
    no_client_key_id = f"{ROUTE_TRACE_PREFIX}shared-feature-off-no-client-key"
    auth_probe_id = os.environ.get("IPP_AUTH_PROBE_ID") or (
        "ipp-auth-" + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-" + secrets.token_hex(4)
    )
    auth_probe_diagnostic_id = f"{ROUTE_TRACE_PREFIX}{auth_probe_id}"
    maas_auth_policy_ready = wait_maas_auth_policy_ready(not_before=route_reconcile_started_at)
    callback_logs_before = callback_log_snapshot()
    policy_status_before = auth_policy_status_snapshot()
    ready_conditions_before_request = policy_status_before.get("maas_auth_policy", [])
    record("maas_auth_policy_ready_before_authenticated_request",
           any(condition.get("type") == "Ready" and condition.get("status") == "True"
               for condition in ready_conditions_before_request), {
               "ready_wait": maas_auth_policy_ready,
               "conditions": ready_conditions_before_request,
           })
    authconfig_before_request = safe_authconfig_snapshot(
        "before-authenticated-request", created_after=route_reconcile_started_at)
    authpolicy_config_before_request = safe_authpolicy_config_snapshot("before-authenticated-request")
    rc, off_status, off_raw = curl_request(
        "feature-off", f"http://127.0.0.1:{GATEWAY_PORT}/v1/messages", anthropic_off,
        auth_file=auth_cfg, extra_headers=[
            f"x-ipp-diagnostic-id: {auth_probe_diagnostic_id}",
        ],
        correlation_id=auth_probe_id,
    )
    valid_auth_input_trace = capture_auth_policy_input_trace(
        auth_probe_diagnostic_id, "shared-feature-off", shared_route_trace_gateway_pod)
    valid_auth_input_trace_ok = (
        valid_auth_input_trace.get("captured") is True
        and valid_auth_input_trace.get("authority") == GATEWAY_AUTHORITY
        and valid_auth_input_trace.get("path") == "/v1/messages"
        and valid_auth_input_trace.get("authorization_present") is True
        and valid_auth_input_trace.get("authorization_scheme", "").lower() == "bearer"
        and valid_auth_input_trace.get("x_api_key_present") is False
        and valid_auth_input_trace.get("client_identity_header_names") == []
    )
    shared_off_trace = capture_runtime_route_trace(
        auth_probe_diagnostic_id, "shared-feature-off", shared_route_trace_gateway_pod)
    save_json(EVIDENCE / "shared-feature-off-envoy-runtime-route.json", shared_off_trace)
    shared_off_trace_ok = bool(
        shared_off_trace.get("captured") and shared_off_trace.get("route_name")
        and shared_off_trace.get("response_flags") is not None)

    authorino_trace = authorino_request_trace(
        shared_off_trace.get("envoy_request_id") or auth_probe_id)
    authorino_expression_hashes = {
        event.get("expression_sha256") for event in authorino_trace.get("matched_events", [])
        if event.get("expression_sha256")
    }
    authconfig_after_request = safe_authconfig_snapshot(
        "after-authenticated-request", authorino_expression_hashes, route_reconcile_started_at)
    callback_logs_after = callback_log_snapshot()
    callback_ids_before = {str(event.get("request_id")) for event in callback_logs_before.get("events", [])}
    callback_events = [event for event in callback_logs_after.get("events", [])
                       if str(event.get("request_id")) not in callback_ids_before]
    callback_trace = {
        "client_correlation_id": auth_probe_id,
        "envoy_request_id": shared_off_trace.get("envoy_request_id"),
        "direct_api_key_validation": {
            "http_status": validation_status or "unknown",
            "valid": validation_valid,
            "tenant": validation_body.get("tenant"),
            "username": validation_body.get("username"),
            "groups": validation_body.get("groups", []),
            "subscription": validation_body.get("subscription"),
            "key_value_saved": False,
        },
        "direct_subscription_selection": {
            "http_status": selection_diagnostic["http_status"],
            "requested_model": selection_diagnostic["requested_model"],
            "requested_subscription_matches_key": selection_diagnostic["requested_subscription_matches_key"],
            "access_allowed": selection_diagnostic["access_allowed"],
            "phase": selection_diagnostic["phase"],
            "resolved_model": selection_diagnostic["resolved_model"],
        },
        "gateway_request": {
            "method": "POST",
            "scheme": "http",
            "authority_expected_from_port_forward": GATEWAY_AUTHORITY,
            "path": "/v1/messages",
            "body_model": "demo",
            "tenant_namespace": NAMESPACE,
            "authorization_header_sent": True,
            "authorization_scheme": "Bearer",
            "authorization_token_family": "sk-oai" if KEY_VALUE.startswith("sk-oai-") else "other",
            "authorization_value_saved": False,
            "x_api_key_header_sent": False,
            "client_identity_headers_sent": [],
            "client_model_header_sent": False,
            "gateway_model_at_auth_policy": valid_auth_input_trace.get("gateway_model"),
            "client_correlation_id": auth_probe_id,
        },
        "envoy_trace": shared_off_trace,
        "auth_policy_input_header_facts": valid_auth_input_trace,
        "policy_status_before_request": policy_status_before,
        "authorino_authconfigs": {
            "before_request_count": len(authconfig_before_request.get("items", [])),
            "before_request_pending_refs": authconfig_before_request.get("referenced_pending_authconfigs", []),
            "after_request_count": len(authconfig_after_request.get("items", [])),
            "after_request_pending_refs": authconfig_after_request.get("referenced_pending_authconfigs", []),
            "route_created_config_names": [item.get("name") for item in authconfig_after_request.get("items", [])
                                            if item.get("created_after_route_setup")],
            "matched_request_expression_fields": authconfig_after_request.get("matched_request_expression_hashes", []),
            "gateway_authpolicy_generation": authpolicy_config_before_request.get("generation"),
            "gateway_authpolicy_authentication_methods": sorted(
                authpolicy_config_before_request.get("authentication", {}).keys()),
            "gateway_authpolicy_metadata_endpoints": authpolicy_config_before_request.get("metadata", {}),
            "raw_secrets_or_expressions_saved": False,
        },
        "authorino_decision": authorino_trace,
        "maas_callback_events_since_baseline": callback_events,
        "callback_event_correlation": {
            "matched_client_request_id": any(
                event.get("request_id") == auth_probe_id for event in callback_events),
            "callback_event_count": len(callback_events),
            "callback_results_by_path_and_status": [
                {"path": event.get("path"), "status": event.get("status"),
                 "request_id_matches_client": event.get("request_id") == auth_probe_id,
                 "authorization_header_present": event.get("authorization_header_present"),
                 "x_api_key_header_present": event.get("x_api_key_header_present")}
                for event in callback_events
            ],
            "raw_callback_bodies_saved": False,
        },
    }
    save_json(EVIDENCE / "auth-callback-request-trace.json", callback_trace)
    RESULT.setdefault("diagnostics", []).append({
        "name": "shared_route_authorino_callback_trace",
        "client_correlation_id": auth_probe_id,
        "gateway_status": off_status,
        "authorino_authorized": next((event.get("authorized") for event in authorino_trace.get("matched_events", [])
                                         if event.get("message_class") == "authorization_response"), None),
        "authorino_response_status": next((event.get("response_status") for event in authorino_trace.get("matched_events", [])
                                            if event.get("message_class") == "authorization_response"), None),
        "callback_events": [{"path": event.get("path"), "status": event.get("status")}
                            for event in callback_events],
        "raw_credentials_saved": False,
    })
    save_json(RESULTS, RESULT)

    no_client_key_rc, no_client_key_status, no_client_key_raw = curl_request(
        "feature-off-no-client-key-diagnostic", f"http://127.0.0.1:{GATEWAY_PORT}/v1/messages",
        no_client_key_body,
        extra_headers=[f"x-ipp-diagnostic-id: {no_client_key_id}"],
        correlation_id=no_client_key_id,
    )
    no_key_auth_input_trace = capture_auth_policy_input_trace(
        no_client_key_id, "shared-feature-off-no-client-key", shared_route_trace_gateway_pod)
    no_key_auth_input_trace_ok = (
        no_key_auth_input_trace.get("captured") is True
        and no_key_auth_input_trace.get("authority") == GATEWAY_AUTHORITY
        and no_key_auth_input_trace.get("path") == "/v1/messages"
        and no_key_auth_input_trace.get("authorization_present") is False
        and no_key_auth_input_trace.get("x_api_key_present") is False
    )
    no_client_key_trace = capture_runtime_route_trace(
        no_client_key_id, "shared-feature-off-no-client-key", shared_route_trace_gateway_pod)
    no_key_authorino_trace = authorino_request_trace(
        no_client_key_trace.get("envoy_request_id") or no_client_key_id)
    no_client_key_seen = any(no_client_key_marker in item.get("message_text", []) for item in observations())
    save_json(EVIDENCE / "feature-off-no-client-key-auth-diagnostic.json", {
        "curl_exit_status": no_client_key_rc,
        "http_status": no_client_key_status,
        "upstream_observed": no_client_key_seen,
        "downstream_response_bytes": len(no_client_key_raw),
        "envoy_trace": no_client_key_trace,
        "authorino_decision": no_key_authorino_trace,
        "authorization_api_key_header_sent": False,
        "client_api_key_header_sent": False,
    })
    record("maas_auth_denies_without_client_credentials",
           no_client_key_rc == 0 and no_client_key_status == "401" and not no_client_key_seen,
           {"http_status": no_client_key_status,
            "upstream_observed": no_client_key_seen,
            "authorization_api_key_header_sent": False,
            "client_api_key_header_sent": False})
    record("no_client_key_request_auth_policy_input_headers", no_key_auth_input_trace_ok, {
        "captured": no_key_auth_input_trace.get("captured"),
        "authority_matches_gateway": no_key_auth_input_trace.get("authority") == GATEWAY_AUTHORITY,
        "path": no_key_auth_input_trace.get("path"),
        "authorization_present": no_key_auth_input_trace.get("authorization_present"),
        "x_api_key_present": no_key_auth_input_trace.get("x_api_key_present"),
        "gateway_model": no_key_auth_input_trace.get("gateway_model"),
    })
    record("valid_request_auth_policy_input_headers", valid_auth_input_trace_ok, {
        "captured": valid_auth_input_trace.get("captured"),
        "authority_matches_gateway": valid_auth_input_trace.get("authority") == GATEWAY_AUTHORITY,
        "path": valid_auth_input_trace.get("path"),
        "authorization_present": valid_auth_input_trace.get("authorization_present"),
        "authorization_scheme": valid_auth_input_trace.get("authorization_scheme"),
        "credential_family": valid_auth_input_trace.get("credential_family"),
        "x_api_key_present": valid_auth_input_trace.get("x_api_key_present"),
        "gateway_model": valid_auth_input_trace.get("gateway_model"),
        "client_identity_header_names": valid_auth_input_trace.get("client_identity_header_names"),
    })
    record("shared_feature_off_runtime_route_trace", shared_off_trace_ok, {
        "captured": shared_off_trace.get("captured"),
        "route_name": shared_off_trace.get("route_name"),
        "upstream_cluster": shared_off_trace.get("upstream_cluster"),
        "response_flags": shared_off_trace.get("response_flags"),
        "response_code": shared_off_trace.get("response_code"),
        "trusted_model_header": shared_off_trace.get("model_header"),
    })
    RESULT.setdefault("diagnostics", []).append({"name": "feature_off_without_client_credentials",
                                                  "curl_exit_status": no_client_key_rc,
                                                  "http_status": no_client_key_status,
                                                  "upstream_observed": no_client_key_seen,
                                                  "route_name": no_client_key_trace.get("route_name"),
                                                  "response_flags": no_client_key_trace.get("response_flags"),
                                                  "trusted_model_header": no_client_key_trace.get("model_header"),
                                                  "authorino_authorized": next((event.get("authorized") for event in no_key_authorino_trace.get("matched_events", [])
                                                                                 if event.get("message_class") == "authorization_response"), None)})
    save_json(RESULTS, RESULT)

    # Compare the known protected OpenAI route with the failing /v1/messages
    # route using the same MaaS API key, current gateway policy, and model.
    # Keep this diagnostic before the expected shared-route failure aborts the run.
    comparison_marker = "ipp-auth-comparison-openai-route"
    comparison_id = "ipp-auth-compare-" + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(4)
    comparison_diagnostic_id = f"{ROUTE_TRACE_PREFIX}{comparison_id}"
    comparison_body = {"model": "demo", "max_tokens": 8,
                       "messages": [{"role": "user", "content": comparison_marker}]}
    comparison_callbacks_before = callback_log_snapshot()
    comparison_authconfig_before = safe_authconfig_snapshot(
        "known-route-before-authenticated-request", created_after=route_reconcile_started_at)
    comparison_rc, comparison_status, comparison_raw = curl_request(
        "known-protected-openai-route-auth-comparison",
        f"http://127.0.0.1:{GATEWAY_PORT}/v1/chat/completions", comparison_body,
        auth_file=auth_cfg,
        extra_headers=[f"x-ipp-diagnostic-id: {comparison_diagnostic_id}"],
        correlation_id=comparison_id,
    )
    comparison_input = capture_auth_policy_input_trace(
        comparison_diagnostic_id, "known-protected-openai-route", shared_route_trace_gateway_pod)
    comparison_route = capture_runtime_route_trace(
        comparison_diagnostic_id, "known-protected-openai-route", shared_route_trace_gateway_pod)
    comparison_authorino = authorino_request_trace(
        comparison_route.get("envoy_request_id") or comparison_id)
    comparison_callbacks_after = callback_log_snapshot()
    callback_keys_before = {
        (event.get("request_id"), event.get("timestamp"), event.get("path"), event.get("status"))
        for event in comparison_callbacks_before.get("events", [])
    }
    comparison_callbacks = [
        event for event in comparison_callbacks_after.get("events", [])
        if (event.get("request_id"), event.get("timestamp"), event.get("path"), event.get("status"))
        not in callback_keys_before
    ]
    comparison_authconfig_after = safe_authconfig_snapshot(
        "known-route-after-authenticated-request", created_after=route_reconcile_started_at)
    comparison_scope = auth_scope_for_route_path("/v1/chat/completions")
    selected_authconfig = next((item for item in comparison_authconfig_after.get("items", [])
                                if item.get("name") == comparison_scope.get("scope")), None)
    comparison_upstream = any(comparison_marker in item.get("message_text", []) for item in observations())
    comparison_decision = next((event for event in comparison_authorino.get("matched_events", [])
                                if event.get("message_class") == "authorization_response"), {})
    auth_comparison = {
        "request": {"path": "/v1/chat/completions", "model": "demo",
                    "same_temporary_maas_key_as_messages_request": True,
                    "authorization_value_saved": False,
                    "x_api_key_header_sent": False,
                    "correlation_id": comparison_id},
        "gateway_authpolicy": "maas-system/maas-gateway-auth",
        "envoy_auth_scope": comparison_scope,
        "selected_authconfig_summary": ({"name": selected_authconfig.get("name"),
                                         "authentication_types": selected_authconfig.get("authentication_types"),
                                         "authorization_types": selected_authconfig.get("authorization_types"),
                                         "status_conditions": selected_authconfig.get("status_conditions")}
                                        if selected_authconfig else None),
        "previous_known_pass": {
            "attempt": "e2e-api-translation-attempt-02",
            "request": "unrelated-openai-stream on /v1/chat/completions",
            "route_policy": "models-as-a-service/api-translation-openai-auth",
            "policy_kind": "HTTPRoute-targeted AuthPolicy with direct MaaS API-key validation",
        },
        "current_request": {"curl_exit_status": comparison_rc, "http_status": comparison_status,
                            "downstream_response_bytes": len(comparison_raw),
                            "upstream_recorder_observed": comparison_upstream,
                            "auth_input_trace": comparison_input,
                            "envoy_route_trace": comparison_route,
                            "authorino_decision": comparison_authorino,
                            "new_maas_callback_events": [{"path": event.get("path"),
                                                          "status": event.get("status"),
                                                          "request_id": event.get("request_id"),
                                                          "authorization_header_present": event.get("authorization_header_present"),
                                                          "x_api_key_header_present": event.get("x_api_key_header_present")}
                                                         for event in comparison_callbacks],
                            "authorization_response": comparison_decision,
                            "raw_credentials_saved": False},
    }
    save_json(EVIDENCE / "known-protected-route-auth-comparison.json", auth_comparison)
    RESULT.setdefault("diagnostics", []).append({
        "name": "known_protected_openai_route_auth_comparison",
        "http_status": comparison_status,
        "envoy_route": comparison_route.get("route_name"),
        "selected_authconfig": comparison_scope.get("scope"),
        "authorino_authorized": comparison_decision.get("authorized"),
        "authorino_response_status": comparison_decision.get("response_status"),
        "maas_callback_count": len(comparison_callbacks),
        "upstream_recorder_observed": comparison_upstream,
        "raw_credentials_saved": False,
    })
    save_json(RESULTS, RESULT)

    off_observed = wait_observation(lambda o: off_marker in o.get("message_text", []), "feature-off request")
    save_json(EVIDENCE / "feature-off-response.json", json.loads(off_raw) if off_raw else {})
    record("feature_off_passthrough", rc == 0 and off_status == "200"
           and off_observed.get("path") == "/v1/messages"
           and off_observed.get("authorization_present") is True
           and off_observed.get("authorization_scheme") == "Bearer"
           and off_observed.get("x_api_key_present") is False
           and off_observed.get("model") == "demo",
           {"http_status": off_status, "upstream_path": off_observed.get("path"),
            "model": off_observed.get("model"),
            "authorization_forwarded": off_observed.get("authorization_present"),
            "authorization_scheme": off_observed.get("authorization_scheme"),
            "x_api_key_absent": off_observed.get("x_api_key_present") is False,
            "body_marker_preserved": off_marker in off_observed.get("message_text", [])})
    if rc != 0 or off_status != "200":
        raise RuntimeError("feature-off request failed; see feature-off response and Envoy diagnostics")
    off_response = json.loads(off_raw)
    record("feature_off_response_untranslated", "choices" in off_response and off_response.get("type") != "message",
           {"response_shape": "OpenAI Chat Completions passthrough"})

    kubectl("-n", NAMESPACE, "patch", "maastenantconfig", "default-tenant", "--type=merge",
            "-p", '{"spec":{"apiTranslation":{"enabled":true}}}')
    enabled = False
    for _ in range(90):
        shared_config = kubectl("-n", "maas-system", "get", "configmap", "payload-processing-plugins", "-o", "json", check=False)
        if shared_config.returncode == 0:
            config_text = json.loads(shared_config.stdout).get("data", {}).get("extproc.yaml", "")
            if "anthropic_messages_request" in config_text and "anthropic_messages_to_chat_completions" in config_text and "path_rewrite" in config_text:
                enabled = True
                break
        time.sleep(1)
    if not enabled:
        capture_shared_runtime("feature-on-timeout")
        raise RuntimeError("shared ExtProc did not render the enabled translator chain")
    kubectl("-n", "maas-system", "rollout", "status", "deployment/payload-processing", "--timeout=180s")
    extproc_on, pods_on = wait_shared_runtime_convergence("feature-on")
    names = ["anthropic_messages_request", "anthropic_messages_to_chat_completions",
             "anthropic_messages_to_chat_completions_stream", "headers", "path_rewrite"]
    positions = [extproc_on.find(name) for name in names]
    record("feature_on_filter_order_and_rollout", all(p >= 0 for p in positions) and positions == sorted(positions)
           and bool(pods_on) and all(p["ready"] and p["mounted_config"] == extproc_on for p in pods_on),
           {"filters": names, "ordered": positions == sorted(positions),
            "ready_pods": [p["name"] for p in pods_on if p["ready"]],
            "mounted_matches_published": all(p["mounted_config"] == extproc_on for p in pods_on)})
    capture_envoy("feature-on")

    on_marker = "phase1-feature-on-marker"
    anthropic_on = {"model": "demo", "max_tokens": 32,
                    "messages": [{"role": "user", "content": [{"type": "text", "text": on_marker}]}]}
    rc, on_status, on_raw = curl_request(
        "feature-on-json", f"http://127.0.0.1:{GATEWAY_PORT}/v1/messages", anthropic_on,
        auth_file=auth_cfg, extra_headers=["x-api-key: client-marker"],
    )
    on_observed = wait_observation(lambda o: on_marker in o.get("message_text", []), "feature-on JSON request")
    (EVIDENCE / "feature-on-json-response.json").write_bytes(on_raw)
    translated = json.loads(on_raw) if on_raw else {}
    record("feature_on_request_translation", rc == 0 and on_status == "200"
           and on_observed.get("path") == "/v1/chat/completions"
           and on_observed.get("model") == "demo"
           and on_marker in on_observed.get("message_text", [])
           and on_observed.get("x_api_key_present") is False
           and on_observed.get("authorization_present") is True
           and on_observed.get("authorization_scheme") == "Bearer",
           {"http_status": on_status, "upstream_path": on_observed.get("path"),
            "upstream_model": on_observed.get("model"), "body_marker_preserved": on_marker in on_observed.get("message_text", []),
            "client_x_api_key_removed": on_observed.get("x_api_key_present") is False,
            "upstream_authorization_present": on_observed.get("authorization_present"),
            "upstream_authorization_scheme": on_observed.get("authorization_scheme")})
    record("feature_on_json_response_translation", translated.get("type") == "message"
           and any(block.get("text") == "translated reply" for block in translated.get("content", [])
                   if isinstance(block, dict)),
           {"downstream_type": translated.get("type"), "content": translated.get("content"),
            "stop_reason": translated.get("stop_reason")})

    stream_marker = "phase1-feature-on-stream-marker"
    anthropic_stream = {"model": "demo", "max_tokens": 32, "stream": True,
                        "messages": [{"role": "user", "content": stream_marker}]}
    rc, stream_status, stream_raw, stream_first_ms, stream_total_ms, stream_content_type = progressive_stream_request(
        "feature-on-stream", f"http://127.0.0.1:{GATEWAY_PORT}/v1/messages", anthropic_stream,
        extra_headers=["x-api-key: client-marker"],
    )
    stream_observed = wait_observation(lambda o: stream_marker in o.get("message_text", []), "feature-on SSE request")
    events = sse_events(stream_raw)
    (EVIDENCE / "feature-on-stream-events.json").write_text(json.dumps(events, indent=2) + "\n")
    event_names = [e["event"] for e in events if e["event"]]
    event_text = [e.get("data", {}).get("delta", {}).get("text") for e in events if isinstance(e.get("data"), dict)]
    record("feature_on_sse_translation", rc == 0 and stream_status == "200"
           and stream_observed.get("path") == "/v1/chat/completions"
           and stream_observed.get("stream") is True
           and "message_start" in event_names and "message_stop" in event_names
           and "translated stream reply" in event_text
           and stream_content_type.startswith("text/event-stream")
           and stream_total_ms - stream_first_ms >= 500,
           {"http_status": stream_status, "upstream_path": stream_observed.get("path"),
            "upstream_stream": stream_observed.get("stream"), "event_names": event_names,
            "text_deltas": [v for v in event_text if v], "content_type": stream_content_type,
            "first_event_latency_ms": round(stream_first_ms, 1),
            "total_response_ms": round(stream_total_ms, 1),
            "progressive_delivery_gap_ms": round(stream_total_ms - stream_first_ms, 1)})

    openai_body = {"model": "demo", "messages": [{"role": "user", "content": "openai-route-regression"}], "stream": True}
    rc, openai_status, openai_raw, openai_first_ms, openai_total_ms, openai_content_type = progressive_stream_request(
        "unrelated-openai-stream", f"http://127.0.0.1:{GATEWAY_PORT}/v1/chat/completions", openai_body,
        extra_headers=["x-api-key: client-marker"],
    )
    openai_observed = wait_observation(
        lambda o: "openai-route-regression" in o.get("message_text", []), "unrelated OpenAI SSE request")
    openai_events = sse_events(openai_raw)
    (EVIDENCE / "unrelated-openai-stream-events.json").write_text(json.dumps(openai_events, indent=2) + "\n")
    openai_data = [e["data"] for e in openai_events]
    record("unrelated_openai_sse_passthrough", rc == 0 and openai_status == "200"
           and openai_observed.get("path") == "/v1/chat/completions"
           and openai_observed.get("stream") is True
           and openai_observed.get("x_api_key_present") is True
           and any(isinstance(d, dict) and "choices" in d for d in openai_data)
           and "[DONE]" in openai_data
           and openai_content_type.startswith("text/event-stream")
           and openai_total_ms - openai_first_ms >= 500,
           {"http_status": openai_status, "upstream_path": openai_observed.get("path"),
            "upstream_stream": openai_observed.get("stream"), "openai_sse_events": len(openai_events),
            "x_api_key_preserved_on_openai_route": openai_observed.get("x_api_key_present"),
            "termination": "[DONE]" in openai_data, "content_type": openai_content_type,
            "first_event_latency_ms": round(openai_first_ms, 1),
            "total_response_ms": round(openai_total_ms, 1),
            "progressive_delivery_gap_ms": round(openai_total_ms - openai_first_ms, 1)})

    if args.scope != "shared":
        test_externalmodel_provider_aware_translation(auth_cfg, gateway_auth_context)

    if LIVE_PROVIDER_SELECTION:
        selected = ("anthropic", "openai", "bedrock") if LIVE_PROVIDER_SELECTION == "all" else (LIVE_PROVIDER_SELECTION,)
        for provider in selected:
            try:
                qualify_live_provider(provider, auth_cfg, gateway_auth_context)
            except Exception as exc:
                append_live_provider_result(provider, "BLOCKED", {
                    "reason": redact_text(str(exc)),
                    "exception_type": type(exc).__name__,
                    "direct_provider_request": False,
                    "credential_value_saved": False,
                })
        live_statuses = [item["status"] for item in RESULT.get("live_provider_results", [])]
        RESULT["deterministic_status"] = "PASS"
        RESULT["status"] = "PASS" if live_statuses and all(status == "PASS" for status in live_statuses) else "PARTIAL"

    all_obs = observations()
    save_json(EVIDENCE / "upstream-observations.json", all_obs)
    save_json(EVIDENCE / "provider-aware-upstream-observations.json", provider_aware_observations())
    kubectl("-n", NAMESPACE, "get", "maastenantconfig", "default-tenant", "-o", "json",
            check=True).stdout
    current_mtc = json.loads(kubectl("-n", NAMESPACE, "get", "maastenantconfig", "default-tenant", "-o", "json").stdout)
    save_json(EVIDENCE / "maastenantconfig-feature-on.json", current_mtc)
    save_json(EVIDENCE / "request-outcomes.json", RESULT["requests"])
    sanitize_scan()
    # A live-provider failure or skip must remain visible to the caller. The
    # deterministic suite passing does not turn a blocked live qualification
    # into an overall pass.
    if LIVE_PROVIDER_SELECTION:
        live_statuses = [item["status"] for item in RESULT.get("live_provider_results", [])]
        RESULT["status"] = "PASS" if live_statuses and all(status == "PASS" for status in live_statuses) else "PARTIAL"
    else:
        RESULT["status"] = "PASS"
    save_json(RESULTS, RESULT)


def main_wrapper():
    try:
        main()
    except Exception as exc:
        RESULT["status"] = "FAIL"
        RESULT["failure"] = redact_text(str(exc))
        save_json(RESULTS, RESULT)
        (EVIDENCE / "failure.txt").write_text(RESULT["failure"] + "\n")
        capture_auth_diagnostics_on_failure()
        deployment = kubectl("-n", NAMESPACE, "get", "deployment", "api-translation-openai-mock",
                             "-o", "json", check=False)
        if deployment.returncode == 0:
            save_json(EVIDENCE / "provider-recorder-deployment-on-failure.json", json.loads(deployment.stdout))
            logs = kubectl("-n", NAMESPACE, "logs", "deploy/api-translation-openai-mock", check=False)
            (EVIDENCE / "provider-recorder-on-failure.log").write_text(redact_text(logs.stdout + logs.stderr))
            save_json(EVIDENCE / "upstream-observations-on-failure.json", {
                "reader": "test-only HTTP endpoint over a namespace-scoped port-forward",
                "observations": observations(),
            })
        if PROVIDER_AWARE_CREATED:
            provider_deployment = kubectl(
                "-n", "maas-system", "get", "deployment", "api-translation-provider-aware-mock",
                "-o", "json", check=False)
            if provider_deployment.returncode == 0:
                save_json(EVIDENCE / "provider-aware-deployment-on-failure.json",
                          json.loads(provider_deployment.stdout))
                provider_logs = kubectl(
                    "-n", "maas-system", "logs", "deploy/api-translation-provider-aware-mock",
                    check=False)
                (EVIDENCE / "provider-aware-recorder-on-failure.log").write_text(
                    redact_text(provider_logs.stdout + provider_logs.stderr))
            save_json(EVIDENCE / "provider-aware-observations-on-failure.json", {
                "reader": "test-only HTTPS observations endpoint over a namespace-scoped port-forward",
                "observations": provider_aware_observations(),
            })
        raise
    finally:
        cleanup()
        if CLEANUP_FAILURES:
            RESULT["status"] = "FAIL"
            RESULT["cleanup_failures"] = CLEANUP_FAILURES
            save_json(RESULTS, RESULT)
        if RESULTS.exists():
            current = json.loads(RESULTS.read_text())
            if current.get("status") == "RUNNING":
                current["status"] = "FAIL"
                save_json(RESULTS, current)
        if RESULT.get("status") == "PARTIAL":
            raise SystemExit(3)


if __name__ == "__main__":
    main_wrapper()

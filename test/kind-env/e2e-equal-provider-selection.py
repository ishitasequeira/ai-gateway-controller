#!/usr/bin/env python3
"""Run the equal-provider Kind qualification against the existing ExtProc image."""
import base64
import datetime
import hashlib
import json
import os
import pathlib
import re
import secrets
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
CLUSTER = os.environ.get("LOCAL_ENV_CLUSTER", "external-model-two-plane")
NAMESPACE = "models-as-a-service"
API_NAMESPACE = "maas-system"
MODEL = "demo-model"
DEPLOYMENT = "payload-processing-external-model"
CONFIG_MAP = "payload-processing-external-model-plugins"
EXTPROC_IMAGE = os.environ.get(
    "EXTPROC_IMAGE",
    "praxis-extproc:dev",
)
AI_CONTROLLER_IMAGE = os.environ.get(
    "AI_CONTROLLER_IMAGE",
    "ai-gateway-controller:external-model-two-plane",
)
PROVIDER_RECORDER_IMAGE = os.environ.get(
    "PROVIDER_RECORDER_IMAGE",
    "provider-recorder:issue28-kind",
)
PRAXIS_EXTPROC_REPO = os.environ.get("PRAXIS_EXTPROC_REPO", "")
EXPECTED_EXTPROC_MAIN_SHA = os.environ.get("EXPECTED_EXTPROC_MAIN_SHA", "")
EVIDENCE = pathlib.Path(os.environ.get("LOCAL_ENV_SELECTION_EVIDENCE") or (
    ROOT / "evidence" / (datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-equal-provider-selection")
))
EVIDENCE.mkdir(parents=True, exist_ok=True)
RESULT = {"status": "RUNNING", "assertions": [], "samples": [], "transitions": []}
PROCESSES = []
SECRET_VALUES = []
CREATED_MODEL_SECRETS = set()
MODEL_BEFORE = None
CONFIG_BEFORE = None
MODEL_CHANGED = False
CONFIG_CHANGED = False
CONTROLLER_PAUSED = False
CONTROLLER_REPLICAS_BEFORE = 1
MAIN_FAILURE = False
API_KEY_ID = None
AUTHORIZATION = None
API_DB_EGRESS_POLICY = False
ISTIO_MESH_ORIGINAL = None
ISTIO_ACCESS_LOG_CHANGED = False
KNOWN_OVERLAYS = {}
PORT = 19000 + (os.getpid() % 1000)
GATEWAY_PORT = PORT + 1


def run(args, *, input_text=None, check=True, timeout=90):
    proc = subprocess.run(args, input=input_text, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=timeout)
    if check and proc.returncode:
        raise RuntimeError(f"command failed rc={proc.returncode}: {' '.join(args)}: {proc.stderr[-1200:]}")
    return proc


def kubectl(*args, **kwargs):
    kwargs.setdefault("timeout", 180)
    return run(["kubectl", "--context", f"kind-{CLUSTER}", *args], **kwargs)


def write_json(path, value):
    pathlib.Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def scrub(value):
    if isinstance(value, str):
        for secret in SECRET_VALUES:
            value = value.replace(secret, "[REDACTED]")
            value = value.replace(base64.b64encode(secret.encode()).decode(), "[REDACTED]")
        return value
    if isinstance(value, list):
        return [scrub(item) for item in value]
    if isinstance(value, dict):
        return {key: scrub(item) for key, item in value.items()}
    return value


def record(name, passed, details):
    assertion = {"name": name, "result": "PASS" if passed else "FAIL", "details": details}
    RESULT["assertions"].append(assertion)
    write_json(EVIDENCE / "results.json", RESULT)
    print(f"{assertion['result']} {name}: {details}")
    if not passed:
        raise RuntimeError(f"assertion failed: {name}")


def capture_providers(stage="provider-poll"):
    for name in ("provider-proof-a", "provider-proof-b"):
        obj = kubectl("-n", NAMESPACE, "get", "externalprovider", name, "-o", "json", check=False)
        if obj.returncode == 0:
            data = json.loads(obj.stdout)
            safe = {
                "name": data["metadata"]["name"],
                "namespace": data["metadata"]["namespace"],
                "generation": data["metadata"].get("generation"),
                "endpoint": data.get("spec", {}).get("endpoint"),
                "secret_reference": data.get("spec", {}).get("auth", {}).get("secretRef", {}).get("name"),
                "status": scrub(data.get("status", {})),
            }
            write_json(EVIDENCE / f"{name}-status-{stage}.json", safe)
        events = kubectl("-n", NAMESPACE, "get", "events", "-o", "json", check=False)
        if events.returncode == 0:
            items = json.loads(events.stdout).get("items", [])
            selected = [e for e in items if e.get("involvedObject", {}).get("name") == name]
            write_json(EVIDENCE / f"{name}-events-{stage}.json", [
                scrub({k: e.get(k) for k in ("type", "reason", "message", "lastTimestamp", "count")})
                for e in selected
            ])
        secret_name = name + "-credentials"
        secret = kubectl("-n", NAMESPACE, "get", "secret", secret_name, "-o", "json", check=False)
        if secret.returncode == 0:
            data = json.loads(secret.stdout)
            write_json(EVIDENCE / f"{secret_name}-reference.json", {
                "name": secret_name, "namespace": NAMESPACE,
                "keys": sorted(data.get("data", {}).keys()),
            })


def provider_ingress_counters():
    counters = {}
    for suffix in ("a", "b"):
        provider = f"proof-{suffix}"
        pods = json.loads(kubectl("-n", API_NAMESPACE, "get", "pods", "-l",
                                  f"app=provider-selection-proof-{suffix}", "-o", "json").stdout)
        total = 0
        statuses = {}
        checks = 0
        pod_names = []
        for pod in pods.get("items", []):
            name = pod.get("metadata", {}).get("name", "")
            if not name:
                continue
            pod_names.append(name)
            logs = kubectl("-n", API_NAMESPACE, "logs", f"pod/{name}", "-c", "recorder", check=False).stdout
            for line in logs.splitlines():
                match = re.search(rf"provider={re.escape(provider)} status=(\d+) tls=(true|false) sni_ok=(true|false) authority_ok=(true|false) credential_ok=(true|false)", line)
                if not match:
                    continue
                total += 1
                status = match.group(1)
                statuses[status] = statuses.get(status, 0) + 1
                if all(match.group(index) == "true" for index in range(2, 6)):
                    checks += 1
        counters[provider] = {"pods": pod_names, "total_requests": total,
                              "status_counts": statuses, "all_transport_and_credential_checks": checks}
    return counters


def capture_provider_ingress_events(label, request_ids):
    wanted = set(request_ids)
    events = []
    for suffix in ("a", "b"):
        provider = f"proof-{suffix}"
        pods = json.loads(kubectl("-n", API_NAMESPACE, "get", "pods", "-l",
                                  f"app=provider-selection-proof-{suffix}", "-o", "json").stdout)
        for pod in pods.get("items", []):
            name = pod.get("metadata", {}).get("name", "")
            if not name:
                continue
            logs = kubectl("-n", API_NAMESPACE, "logs", f"pod/{name}", "-c", "recorder",
                           check=False).stdout
            for line in logs.splitlines():
                fields = dict(re.findall(r"([a-z0-9_]+)=([^ ]*)", line))
                if fields.get("request_id") not in wanted:
                    continue
                events.append({key: fields.get(key, "") for key in (
                    "provider", "request_id", "status", "tls", "sni_ok",
                    "authority_ok", "credential_ok", "session_id_sha256")})
    write_json(EVIDENCE / f"provider-ingress-events-{label}.json", events)
    return events


def counter_delta(before, after):
    result = {}
    for provider in ("proof-a", "proof-b"):
        prior = before.get(provider, {})
        current = after.get(provider, {})
        statuses = {}
        for status in set(prior.get("status_counts", {})) | set(current.get("status_counts", {})):
            delta = current.get("status_counts", {}).get(status, 0) - prior.get("status_counts", {}).get(status, 0)
            if delta:
                statuses[status] = delta
        result[provider] = {
            "requests": current.get("total_requests", 0) - prior.get("total_requests", 0),
            "status_counts": statuses,
            "all_transport_and_credential_checks": current.get("all_transport_and_credential_checks", 0)
            - prior.get("all_transport_and_credential_checks", 0),
        }
    return result


def capture_provenance(label):
    source_diff = run(["git", "-C", str(ROOT), "diff", "--no-ext-diff", "--", "api", "pkg"]).stdout
    extproc_head = run(["git", "-C", PRAXIS_EXTPROC_REPO, "rev-parse", "HEAD"], check=False).stdout.strip() if PRAXIS_EXTPROC_REPO else ""
    extproc_diff = run(["git", "-C", PRAXIS_EXTPROC_REPO, "diff", "--no-ext-diff"], check=False).stdout if PRAXIS_EXTPROC_REPO else ""
    extproc_status = run(["git", "-C", PRAXIS_EXTPROC_REPO, "status", "--porcelain"], check=False).stdout.strip() if PRAXIS_EXTPROC_REPO else ""
    source = {
        "aigc_head": run(["git", "-C", str(ROOT), "rev-parse", "HEAD"]).stdout.strip(),
        "aigc_api_pkg_worktree_diff_sha256": hashlib.sha256(source_diff.encode()).hexdigest(),
        "aigc_controller_image": AI_CONTROLLER_IMAGE,
        "extproc_image": EXTPROC_IMAGE,
        "extproc_source_repo": PRAXIS_EXTPROC_REPO,
        "extproc_source_head": extproc_head,
        "extproc_source_worktree_diff_sha256": hashlib.sha256(extproc_diff.encode()).hexdigest(),
        "extproc_source_worktree_status": extproc_status,
        "extproc_source_worktree_clean": not extproc_status,
        "extproc_source_mode": "local-build-from-main",
        "provider_recorder_image": "provider-recorder:issue28-kind",
    }
    images = {}
    for name, tag in (("controller", source["aigc_controller_image"]),
                      ("extproc", source["extproc_image"]),
                      ("provider_recorder", source["provider_recorder_image"])):
        inspected = run(["docker", "image", "inspect", tag], check=False)
        if inspected.returncode == 0:
            image_data = json.loads(inspected.stdout)[0]
            images[name] = {"tag": tag, "id": image_data.get("Id"), "repo_digests": image_data.get("RepoDigests"),
                            "labels": image_data.get("Config", {}).get("Labels", {})}
        else:
            images[name] = {"tag": tag, "inspect_error": inspected.stderr[-500:]}
    cluster_pods = json.loads(kubectl("get", "pods", "--all-namespaces", "-o", "json").stdout)
    extproc_pods = []
    for pod in cluster_pods.get("items", []):
        containers = []
        for container in pod.get("spec", {}).get("containers", []):
            if "praxis-extproc" not in container.get("image", ""):
                continue
            status = next((item for item in pod.get("status", {}).get("containerStatuses", [])
                           if item.get("name") == container.get("name")), {})
            containers.append({"name": container.get("name"), "image": container.get("image"),
                               "imageID": status.get("imageID"), "ready": status.get("ready"),
                               "restartCount": status.get("restartCount")})
        if containers:
            extproc_pods.append({"namespace": pod["metadata"]["namespace"], "name": pod["metadata"]["name"],
                                 "uid": pod["metadata"]["uid"], "deleting": bool(pod["metadata"].get("deletionTimestamp")),
                                 "containers": containers})
    controller_pods = json.loads(kubectl("-n", "opendatahub", "get", "pods", "-l",
        "control-plane=ai-gateway-controller", "-o", "json").stdout)
    controller_status = [
        {"name": pod["metadata"]["name"], "uid": pod["metadata"]["uid"], "containers": [
            {k: c.get(k) for k in ("name", "image", "imageID", "ready", "restartCount")}
            for c in pod.get("status", {}).get("containerStatuses", [])]}
        for pod in controller_pods.get("items", [])
    ]
    write_json(EVIDENCE / f"provenance-{label}.json", {
        "source": source, "local_images": images, "extproc_pods": extproc_pods,
        "controller_pods": controller_status,
    })


def verify_provider_resolver_loaded(node_image_id):
    """Verify the resolver file is mounted and accepted by this ExtProc build."""
    configmaps = json.loads(kubectl("get", "configmaps", "--all-namespaces", "-o", "json").stdout)
    matches = []
    for configmap in configmaps.get("items", []):
        for key, value in configmap.get("data", {}).items():
            if isinstance(value, str) and "llmisvc_model_provider_resolver" in value:
                matches.append({
                    "namespace": configmap["metadata"]["namespace"],
                    "name": configmap["metadata"]["name"],
                    "key": key,
                    "content": value,
                    "sha256": hashlib.sha256(value.encode()).hexdigest(),
                    "data_keys": list(configmap.get("data", {})),
                })
    if not matches:
        raise RuntimeError("no live ConfigMap contains llmisvc_model_provider_resolver")

    deployments = json.loads(kubectl("get", "deployments", "--all-namespaces", "-o", "json").stdout)
    pods = json.loads(kubectl("get", "pods", "--all-namespaces", "-o", "json").stdout)
    loaded = []
    for config in matches:
        namespace, cm_name = config["namespace"], config["name"]
        for deployment in deployments.get("items", []):
            if deployment["metadata"]["namespace"] != namespace:
                continue
            pod_spec = deployment.get("spec", {}).get("template", {}).get("spec", {})
            containers = [container for container in pod_spec.get("containers", [])
                          if container.get("image") == EXTPROC_IMAGE]
            if not containers:
                continue
            volumes = [volume for volume in pod_spec.get("volumes", [])
                       if volume.get("configMap", {}).get("name") == cm_name]
            if not volumes:
                continue
            selector = deployment.get("spec", {}).get("selector", {}).get("matchLabels", {})
            selected = [pod for pod in pods.get("items", [])
                        if pod["metadata"]["namespace"] == namespace
                        and all(pod.get("metadata", {}).get("labels", {}).get(key) == value
                                for key, value in selector.items())
                        and not pod.get("metadata", {}).get("deletionTimestamp")]
            for pod in selected:
                pod_statuses = {item.get("name"): item for item in
                                pod.get("status", {}).get("containerStatuses", [])}
                for container in containers:
                    status = pod_statuses.get(container["name"], {})
                    image_id = status.get("imageID", "").rsplit("://", 1)[-1]
                    if not status.get("ready") or status.get("restartCount", 0) != 0:
                        continue
                    if image_id != node_image_id:
                        continue
                    pod_volumes = {volume.get("name"): volume for volume in
                                   pod_spec.get("volumes", [])
                                   if volume.get("configMap", {}).get("name") == cm_name}
                    mounted_files = []
                    for mount in container.get("volumeMounts", []):
                        volume = pod_volumes.get(mount.get("name"))
                        if not volume:
                            continue
                        config_map = volume["configMap"]
                        source_items = config_map.get("items") or [
                            {"key": data_key, "path": data_key} for data_key in config["data_keys"]
                        ]
                        relative = next((item.get("path", config["key"]) for item in source_items
                                         if item.get("key") == config["key"]), config["key"])
                        mounted_path = mount.get("mountPath", "").rstrip("/") + "/" + relative
                        if mount.get("subPath"):
                            mounted_path = mount["mountPath"]
                        read = kubectl("-n", namespace, "exec", f"pod/{pod['metadata']['name']}",
                                       "-c", container["name"], "--", "cat", mounted_path, check=False)
                        if read.returncode == 0 and "llmisvc_model_provider_resolver" in read.stdout:
                            mounted_files.append({
                                "path": mounted_path,
                                "sha256": hashlib.sha256(read.stdout.encode()).hexdigest(),
                            })
                    if not mounted_files:
                        continue
                    startup = kubectl("-n", namespace, "logs", f"pod/{pod['metadata']['name']}",
                                      "-c", container["name"], check=False).stdout
                    startup = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", startup)
                    startup_lines = [line for line in startup.splitlines()
                                     if "starting ExtProc server" in line]
                    if not startup_lines:
                        continue
                    loaded.append({
                        "namespace": namespace,
                        "deployment": deployment["metadata"]["name"],
                        "pod": pod["metadata"]["name"],
                        "pod_uid": pod["metadata"].get("uid"),
                        "container": container["name"],
                        "image": container["image"],
                        "image_id": image_id,
                        "restart_count": status.get("restartCount", 0),
                        "configmap": f"{namespace}/{cm_name}",
                        "config_key": config["key"],
                        "config_sha256": config["sha256"],
                        "filter_config_line": next(
                            line.strip() for line in config["content"].splitlines()
                            if "llmisvc_model_provider_resolver" in line
                        ),
                        "mounted_files": mounted_files,
                        "startup_line": startup_lines[-1],
                    })
    result = {
        "required_filter": "llmisvc_model_provider_resolver",
        "configmaps": [{key: value for key, value in config.items()
                        if key not in ("content", "data_keys")}
                       for config in matches],
        "loaded_pods": loaded,
    }
    write_json(EVIDENCE / "provider-resolver-loaded.json", result)
    if not loaded:
        raise RuntimeError(
            "llmisvc_model_provider_resolver was not mounted in a ready, source-matched ExtProc pod with a successful startup"
        )
    record("provider_resolver_loaded", True, json.dumps(result, sort_keys=True))


def patch_object(namespace, kind, name, patch):
    kubectl("-n", namespace, "patch", kind, name, "--type=merge", "-p", json.dumps(patch))


def create_secret(namespace, name, value):
    existing = kubectl("-n", namespace, "get", "secret", name, check=False)
    if existing.returncode == 0:
        raise RuntimeError(f"refusing to overwrite existing fixture Secret {namespace}/{name}")
    obj = {
        "apiVersion": "v1", "kind": "Secret",
        "metadata": {"name": name, "namespace": namespace,
                     "labels": {"external-model-e2e/purpose": "provider-selection-proof"}},
        "type": "Opaque", "data": {"api-key": base64.b64encode(value.encode()).decode()},
    }
    run(["kubectl", "--context", f"kind-{CLUSTER}", "create", "-f", "-"],
        input_text=json.dumps(obj))
    if namespace == NAMESPACE:
        CREATED_MODEL_SECRETS.add(name)


def backend_credential(namespace, name, provider):
    existing = kubectl("-n", namespace, "get", "secret", name, "-o", "json", check=False)
    if existing.returncode == 0:
        deployment = kubectl("-n", namespace, "get", "deployment", f"provider-selection-proof-{provider}",
                             "-o", "json", check=False)
        if deployment.returncode:
            raise RuntimeError(f"cannot verify owner of existing backend fixture Secret {namespace}/{name}")
        pod_spec = json.loads(deployment.stdout).get("spec", {}).get("template", {}).get("spec", {})
        owned = (
            json.loads(deployment.stdout).get("metadata", {}).get("labels", {}).get(
                "app.kubernetes.io/managed-by") == "external-model-e2e"
            and any(v.get("secret", {}).get("secretName") == name for v in pod_spec.get("volumes", []))
        )
        if not owned:
            raise RuntimeError(f"refusing to reuse backend Secret without matching fixture ownership: {namespace}/{name}")
        encoded = json.loads(existing.stdout).get("data", {}).get("api-key")
        if not encoded:
            raise RuntimeError(f"existing backend fixture Secret lacks api-key: {namespace}/{name}")
        value = base64.b64decode(encoded).decode()
        if not value:
            raise RuntimeError(f"existing backend fixture Secret has empty api-key: {namespace}/{name}")
        SECRET_VALUES.append(value)
        return value
    value = secrets.token_hex(32)
    SECRET_VALUES.append(value)
    create_secret(namespace, name, value)
    return value


def model_credential(name, expected_value):
    existing = kubectl("-n", NAMESPACE, "get", "secret", name, "-o", "json", check=False)
    if existing.returncode == 0:
        obj = json.loads(existing.stdout)
        if obj.get("metadata", {}).get("labels", {}).get("external-model-e2e/purpose") != "provider-selection-proof":
            raise RuntimeError(f"refusing to reuse unowned model Secret {NAMESPACE}/{name}")
        encoded = obj.get("data", {}).get("api-key")
        value = base64.b64decode(encoded).decode() if encoded else ""
        if not value or value != expected_value:
            raise RuntimeError(f"existing model fixture Secret does not match its backend Secret: {NAMESPACE}/{name}")
        SECRET_VALUES.append(value)
        return
    create_secret(NAMESPACE, name, expected_value)


def provider_ready(name):
    p = kubectl("-n", NAMESPACE, "get", "externalprovider", name, "-o", "json", check=False)
    if p.returncode:
        return False
    obj = json.loads(p.stdout)
    return (
        obj.get("status", {}).get("phase") == "Ready"
        and obj.get("status", {}).get("observedGeneration") == obj.get("metadata", {}).get("generation")
        and any(c.get("type") == "Ready" and c.get("status") == "True"
                for c in obj.get("status", {}).get("conditions", []))
    )


def wait_providers(suffixes):
    names = [f"provider-proof-{suffix}" for suffix in suffixes]
    for _ in range(90):
        if all(provider_ready(name) for name in names):
            capture_providers()
            return
        time.sleep(2)
    capture_providers()
    raise RuntimeError(f"referenced ExternalProviders did not become Ready: {', '.join(names)}")


def wait_model():
    for _ in range(120):
        p = kubectl("-n", NAMESPACE, "get", "externalmodel", MODEL, "-o", "json", check=False)
        if p.returncode == 0:
            obj = json.loads(p.stdout)
            if (obj.get("status", {}).get("phase") == "Ready"
                    and obj.get("status", {}).get("observedGeneration") == obj.get("metadata", {}).get("generation")):
                return obj
        time.sleep(2)
    raise RuntimeError("ExternalModel did not reach Ready at its current generation")


def set_refs(refs, wait=True):
    global MODEL_CHANGED
    MODEL_CHANGED = True
    patch_object(NAMESPACE, "externalmodel", MODEL,
                 {"spec": {"externalProviderRefs": refs}})
    return wait_model() if wait else None


def make_ref(name, weight=1):
    return {
        "ref": {"name": name}, "targetModel": "demo", "apiFormat": "openai-chat",
        "path": "/v1/chat/completions",
        "weight": weight,
    }


def overlay_snapshot(label):
    cm = json.loads(kubectl("-n", NAMESPACE, "get", "configmap", "routing-overlay", "-o", "json").stdout)
    published = cm.get("metadata", {}).get("annotations", {}).get(
        "inference.opendatahub.io/routing-overlay-content-digest", "")
    wire = json.loads(cm["data"]["routing-overlay.json"])
    mounted = run(["kubectl", "--context", f"kind-{CLUSTER}", "-n", NAMESPACE, "exec",
                   f"deploy/{DEPLOYMENT}", "--", "cat", "/etc/praxis/routing/routing-overlay.json"],
                  check=False, timeout=15)
    mounted_path = EVIDENCE / f"{label}-mounted.json"
    mounted_path.write_text(mounted.stdout if mounted.returncode == 0 else "")
    mounted_digest = run([str(EVIDENCE / "recompute-digest"), str(mounted_path)], check=False).stdout.strip()
    logs = kubectl("-n", NAMESPACE, "logs", f"deploy/{DEPLOYMENT}", "--tail=300", check=False).stdout
    logs = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", logs)
    revision_line = next((line for line in reversed(logs.splitlines()) if "accepted_revision" in line), "")
    accepted_match = re.search(r'accepted_revision.*?="([0-9a-f]{64})".*?serving_revision.*?="([0-9a-f]{64})"', revision_line)
    accepted, serving = accepted_match.groups() if accepted_match else ("", "")
    dep = json.loads(kubectl("-n", NAMESPACE, "get", "deployment", DEPLOYMENT, "-o", "json").stdout)
    config_hash = dep.get("spec", {}).get("template", {}).get("metadata", {}).get("annotations", {}).get(
        "ai-gateway-controller.opendatahub.io/extproc-config-sha256", "")
    config = kubectl("-n", NAMESPACE, "get", "configmap", CONFIG_MAP, "-o", "json").stdout
    extproc = json.loads(config).get("data", {}).get("extproc.yaml", "")
    cluster_line = next((line.strip() for line in extproc.splitlines() if "provider_hop_clusters:" in line), "")
    cluster_match = re.search(r"provider_hop_clusters:\s*(\[.*\])", cluster_line)
    runtime_clusters = json.loads(cluster_match.group(1)) if cluster_match else []
    credential_entries = []
    credential_pattern = re.compile(
        r"(?m)^\s+- name: ([^\n]+)\n\s+namespace: ([^\n]+)\n\s+key: ([^\n]+)\n"
        r"\s+strategy: ([^\n]+)\n\s+file: ([^\n]+)$"
    )
    for match in credential_pattern.finditer(extproc):
        credential_entries.append({
            "name": match.group(1).strip(), "namespace": match.group(2).strip(),
            "key": match.group(3).strip(), "strategy": match.group(4).strip(),
            "file": match.group(5).strip(),
        })
    mounted_secrets = []
    for volume in dep.get("spec", {}).get("template", {}).get("spec", {}).get("volumes", []):
        if volume.get("name") != "provider-credentials":
            continue
        for source in volume.get("projected", {}).get("sources", []):
            secret = source.get("secret", {})
            for item in secret.get("items", []):
                mounted_secrets.append({"name": secret.get("name"), "key": item.get("key"), "path": item.get("path")})
    extproc_hash = hashlib.sha256(extproc.encode()).hexdigest() if extproc else ""
    mounted_config = run(["kubectl", "--context", f"kind-{CLUSTER}", "-n", NAMESPACE, "exec",
                          f"deploy/{DEPLOYMENT}", "--", "cat", "/etc/praxis/extproc.yaml"],
                         check=False, timeout=15)
    mounted_config_hash = hashlib.sha256(mounted_config.stdout.encode()).hexdigest() if mounted_config.returncode == 0 else ""
    dep_hash = dep.get("spec", {}).get("template", {}).get("metadata", {}).get("annotations", {}).get(
        "ai-gateway-controller.opendatahub.io/extproc-config-sha256", "")
    state = {
        "label": label, "published_digest": published, "mounted_digest": mounted_digest,
        "accepted_revision": accepted, "serving_revision": serving,
        "extproc_config_sha256": config_hash, "provider_hop_clusters": cluster_line,
        "runtime_clusters": runtime_clusters, "credential_entries": credential_entries,
        "mounted_secrets": mounted_secrets, "configmap_extproc_sha256": extproc_hash,
        "mounted_extproc_sha256": mounted_config_hash, "deployment_extproc_sha256": dep_hash,
        "selection_policy": wire.get("overlay", {}).get("selection_policy"),
        "deployment_generation": dep.get("metadata", {}).get("generation"),
        "deployment_observed_generation": dep.get("status", {}).get("observedGeneration"),
        "deployment_replicas": dep.get("status", {}).get("replicas"),
        "deployment_updated_replicas": dep.get("status", {}).get("updatedReplicas"),
        "deployment_ready_replicas": dep.get("status", {}).get("readyReplicas"),
        "deployment_available_replicas": dep.get("status", {}).get("availableReplicas"),
        "deployment_unavailable_replicas": dep.get("status", {}).get("unavailableReplicas", 0),
        "deployment_terminating_replicas": dep.get("status", {}).get("terminatingReplicas", 0),
        "candidates": [
            {k: c.get(k) for k in ("cluster", "kind", "name", "selection_group", "traffic_weight")}
            for c in wire.get("overlay", {}).get("candidates", [])
        ],
    }
    return state


def enable_test_session_affinity():
    global CONFIG_CHANGED, CONTROLLER_PAUSED
    controller = json.loads(kubectl("-n", "opendatahub", "get", "deployment", "ai-gateway-controller", "-o", "json").stdout)
    replicas = controller.get("spec", {}).get("replicas", 1)
    if replicas < 1:
        raise RuntimeError("controller must be running before the session-affinity fixture")
    global CONTROLLER_REPLICAS_BEFORE
    CONTROLLER_REPLICAS_BEFORE = replicas
    kubectl("-n", "opendatahub", "scale", "deployment/ai-gateway-controller", "--replicas=0")
    CONTROLLER_PAUSED = True
    for _ in range(60):
        remaining = kubectl("-n", "opendatahub", "get", "pods", "-l", "control-plane=ai-gateway-controller", "-o", "name").stdout.strip()
        if not remaining:
            break
        time.sleep(1)
    else:
        raise RuntimeError("controller did not stop for the bounded affinity-only fixture")
    config_obj = json.loads(kubectl("-n", NAMESPACE, "get", "configmap", CONFIG_MAP, "-o", "json").stdout)
    for field in ("resourceVersion", "uid", "creationTimestamp", "managedFields"):
        config_obj.get("metadata", {}).pop(field, None)
    config_obj.pop("status", None)
    config = config_obj["data"]["extproc.yaml"]
    reload_line = "        reload: {enabled: true, debounce_ms: 500}\n"
    if config.count(reload_line) != 1:
        raise RuntimeError("could not place test-only session affinity in the intelligent_route config")
    config = config.replace(
        reload_line,
        reload_line + "        session_affinity: {enabled: true, header: x-session-id, ttl_secs: 3600}\n",
    )
    config_obj["data"]["extproc.yaml"] = config
    CONFIG_CHANGED = True
    patch_object(NAMESPACE, "configmap", CONFIG_MAP,
                 {"data": {"extproc.yaml": config}})
    config_hash = hashlib.sha256(config.encode()).hexdigest()
    run(["kubectl", "--context", f"kind-{CLUSTER}", "-n", NAMESPACE, "patch", "deployment", DEPLOYMENT,
         "--type=merge", "-p", json.dumps({"spec": {"template": {"metadata": {"annotations": {
             "ai-gateway-controller.opendatahub.io/extproc-config-sha256": config_hash
         }}}}})])
    kubectl("-n", NAMESPACE, "rollout", "status", f"deployment/{DEPLOYMENT}", "--timeout=180s")
    current = json.loads(kubectl("-n", NAMESPACE, "get", "configmap", CONFIG_MAP, "-o", "json").stdout)
    applied = current["data"]["extproc.yaml"]
    (EVIDENCE / "test-provider-runtime-config.txt").write_text("\n".join(
        line.strip() for line in applied.splitlines()
        if "provider_hop_clusters:" in line or "session_affinity:" in line
    ) + "\n")
    if "session_affinity: {enabled: true, header: x-session-id, ttl_secs: 3600}" not in applied:
        raise RuntimeError("test ExtProc config did not retain session affinity")
    state = overlay_snapshot("session-affinity-runtime")
    if not (state["configmap_extproc_sha256"] == state["mounted_extproc_sha256"]
            == state["deployment_extproc_sha256"]):
        raise RuntimeError("session-affinity runtime config did not match the mounted ExtProc file")
    transport_state = wait_for_gateway_extproc_transport()
    record("test_session_affinity_config", True,
           "test-only affinity is mounted; Gateway reports a healthy ExtProc cluster and the EndpointSlice has one serving target; controller remains paused for this bounded check")
    write_json(EVIDENCE / "session-affinity-extproc-transport.json", transport_state)
    return state


def resume_controller():
    global CONTROLLER_PAUSED
    if not CONTROLLER_PAUSED:
        return
    kubectl("-n", "opendatahub", "scale", "deployment/ai-gateway-controller",
            f"--replicas={CONTROLLER_REPLICAS_BEFORE}")
    kubectl("-n", "opendatahub", "rollout", "status", "deployment/ai-gateway-controller", "--timeout=180s")
    CONTROLLER_PAUSED = False


def wait_for_published_candidates(expected, label):
    for _ in range(450):
        result = kubectl("-n", NAMESPACE, "get", "configmap", "routing-overlay", "-o", "json", check=False)
        if result.returncode == 0:
            cm = json.loads(result.stdout)
            try:
                wire = json.loads(cm["data"]["routing-overlay.json"])
                clusters = {c.get("cluster") for c in wire.get("overlay", {}).get("candidates", [])}
            except (KeyError, TypeError, json.JSONDecodeError):
                clusters = set()
            if clusters == expected:
                snapshot = {
                    "label": label,
                    "published_digest": cm.get("metadata", {}).get("annotations", {}).get(
                        "inference.opendatahub.io/routing-overlay-content-digest", ""),
                    "candidate_clusters": sorted(clusters),
                    "selection_policy": wire.get("overlay", {}).get("selection_policy"),
                }
                write_json(EVIDENCE / f"first-published-{label}.json", snapshot)
                return snapshot
        time.sleep(0.2)
    raise RuntimeError(f"expected candidate set was not published: {label}")


def capture_pod_revisions(label):
    deployment = json.loads(kubectl("-n", NAMESPACE, "get", "deployment", DEPLOYMENT, "-o", "json").stdout)
    selector = deployment.get("spec", {}).get("selector", {}).get("matchLabels", {})
    label_selector = ",".join(f"{key}={value}" for key, value in sorted(selector.items()))
    pods = json.loads(kubectl("-n", NAMESPACE, "get", "pods", "-l", label_selector, "-o", "json").stdout)
    snapshots = []
    for pod in pods.get("items", []):
        metadata = pod.get("metadata", {})
        name = metadata.get("name", "")
        if not name:
            continue
        overlay = run(["kubectl", "--context", f"kind-{CLUSTER}", "-n", NAMESPACE,
                       "exec", f"pod/{name}", "--", "cat", "/etc/praxis/routing/routing-overlay.json"],
                      check=False, timeout=15)
        mounted_digest = ""
        mounted_clusters = []
        if overlay.returncode == 0:
            mounted_path = EVIDENCE / f".mounted-{label}-{name}.json"
            mounted_path.write_text(overlay.stdout)
            mounted_digest = run([str(EVIDENCE / "recompute-digest"), str(mounted_path)], check=False).stdout.strip()
            try:
                mounted_wire = json.loads(overlay.stdout)
                mounted_clusters = sorted({c.get("cluster") for c in mounted_wire.get("overlay", {}).get("candidates", [])})
            except json.JSONDecodeError:
                pass
            mounted_path.unlink(missing_ok=True)
        extproc = run(["kubectl", "--context", f"kind-{CLUSTER}", "-n", NAMESPACE,
                       "exec", f"pod/{name}", "--", "cat", "/etc/praxis/extproc.yaml"],
                      check=False, timeout=15)
        extproc_text = extproc.stdout if extproc.returncode == 0 else ""
        runtime_line = next((line.strip() for line in extproc_text.splitlines()
                             if "provider_hop_clusters:" in line), "")
        runtime_match = re.search(r"provider_hop_clusters:\s*(\[.*\])", runtime_line)
        mounted_runtime_clusters = json.loads(runtime_match.group(1)) if runtime_match else []
        credential_pattern = re.compile(
            r"(?m)^\s+- name: ([^\n]+)\n\s+namespace: ([^\n]+)\n\s+key: ([^\n]+)\n"
            r"\s+strategy: ([^\n]+)\n\s+file: ([^\n]+)$"
        )
        mounted_credentials = [
            {"name": m.group(1).strip(), "namespace": m.group(2).strip(),
             "key": m.group(3).strip(), "file": m.group(5).strip()}
            for m in credential_pattern.finditer(extproc_text)
        ]
        projected_secrets = []
        for volume in pod.get("spec", {}).get("volumes", []):
            if volume.get("name") != "provider-credentials":
                continue
            for source in volume.get("projected", {}).get("sources", []):
                secret = source.get("secret", {})
                projected_secrets.extend(
                    {"name": secret.get("name"), "key": item.get("key"), "path": item.get("path")}
                    for item in secret.get("items", [])
                )
        logs = run(["kubectl", "--context", f"kind-{CLUSTER}", "-n", NAMESPACE,
                    "logs", f"pod/{name}", "-c", "payload-processing", "--tail=300"],
                   check=False, timeout=15).stdout
        logs = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", logs)
        revision_line = next((line for line in reversed(logs.splitlines()) if "accepted_revision" in line), "")
        match = re.search(r'accepted_revision.*?="([0-9a-f]{64})".*?serving_revision.*?="([0-9a-f]{64})"', revision_line)
        accepted_revision = match.group(1) if match else ""
        serving_revision = match.group(2) if match else ""
        ready = any(c.get("type") == "Ready" and c.get("status") == "True"
                    for c in pod.get("status", {}).get("conditions", []))
        snapshots.append({
            "pod": name,
            "pod_ip": pod.get("status", {}).get("podIP"),
            "phase": pod.get("status", {}).get("phase"),
            "uid": metadata.get("uid"),
            "template_hash": metadata.get("labels", {}).get("pod-template-hash"),
            "deleting": bool(metadata.get("deletionTimestamp")),
            "ready": ready,
            "restarts": sum(c.get("restartCount", 0) for c in pod.get("status", {}).get("containerStatuses", [])),
            "mounted_digest": mounted_digest,
            "mounted_candidates": mounted_clusters,
            "mounted_extproc_sha256": hashlib.sha256(extproc_text.encode()).hexdigest() if extproc_text else "",
            "mounted_runtime_clusters": mounted_runtime_clusters,
            "mounted_credentials": mounted_credentials,
            "projected_secrets": projected_secrets,
            "accepted_revision": accepted_revision,
            "serving_revision": serving_revision,
            "accepted_overlay": KNOWN_OVERLAYS.get(accepted_revision),
            "serving_overlay": KNOWN_OVERLAYS.get(serving_revision),
        })
    return snapshots


def capture_ready_endpoints():
    service = kubectl("-n", NAMESPACE, "get", "service", DEPLOYMENT, "-o", "json", check=False)
    if service.returncode:
        return {"service": DEPLOYMENT, "error": "service-not-found", "endpoints": []}
    service_obj = json.loads(service.stdout)
    slices = kubectl("-n", NAMESPACE, "get", "endpointslices", "-l",
                     f"kubernetes.io/service-name={DEPLOYMENT}", "-o", "json", check=False)
    if slices.returncode:
        return {"service": DEPLOYMENT, "error": "endpointslice-query-failed", "endpoints": []}
    endpoints = []
    for item in json.loads(slices.stdout).get("items", []):
        for endpoint in item.get("endpoints", []):
            ref = endpoint.get("targetRef", {})
            endpoints.append({
                "pod": ref.get("name"),
                "addresses": endpoint.get("addresses", []),
                "ready": endpoint.get("conditions", {}).get("ready"),
                "serving": endpoint.get("conditions", {}).get("serving"),
                "terminating": endpoint.get("conditions", {}).get("terminating"),
            })
    return {
        "service": DEPLOYMENT,
        "publish_not_ready_addresses": service_obj.get("spec", {}).get("publishNotReadyAddresses", False),
        "selector": service_obj.get("spec", {}).get("selector", {}),
        "endpoints": endpoints,
    }


def wait_for_gateway_extproc_transport():
    """Wait for the serving EndpointSlice and Gateway's ExtProc cluster to settle."""
    stable = 0
    previous = None
    state = {}
    for _ in range(60):
        endpoints = capture_ready_endpoints()
        serving_endpoints = [endpoint for endpoint in endpoints["endpoints"]
                             if endpoint.get("ready") is True
                             and endpoint.get("terminating") is not True]
        gateways = json.loads(kubectl("-n", API_NAMESPACE, "get", "pods", "-l",
            "gateway.networking.k8s.io/gateway-name=maas-default-gateway", "-o", "json").stdout)
        gateway_states = []
        for pod in gateways.get("items", []):
            if pod.get("metadata", {}).get("deletionTimestamp"):
                continue
            name = pod.get("metadata", {}).get("name", "")
            if not name or not any(condition.get("type") == "Ready" and condition.get("status") == "True"
                                   for condition in pod.get("status", {}).get("conditions", [])):
                continue
            admin = kubectl("-n", API_NAMESPACE, "exec", f"pod/{name}", "-c", "istio-proxy", "--",
                            "pilot-agent", "request", "GET", "clusters?format=json", check=False)
            cluster_state = None
            if admin.returncode == 0:
                try:
                    clusters = json.loads(admin.stdout).get("cluster_statuses", [])
                    cluster_state = next((cluster for cluster in clusters
                                          if cluster.get("name") == "payload-processing-external-model-extproc"), None)
                except json.JSONDecodeError:
                    cluster_state = None
            hosts = (cluster_state or {}).get("host_statuses", [])
            gateway_states.append({
                "pod": name,
                "pod_uid": pod.get("metadata", {}).get("uid"),
                "extproc_cluster_present": cluster_state is not None,
                "healthy_extproc_hosts": [
                    host.get("address", {}).get("socket_address", {})
                    for host in hosts
                    if host.get("health_status", {}).get("eds_health_status") == "HEALTHY"
                ],
            })
        state = {
            "extproc_service_endpoints": endpoints,
            "ready_serving_endpoint_count": len(serving_endpoints),
            "ready_gateway_extproc_clusters": gateway_states,
        }
        all_gateways_healthy = bool(gateway_states) and all(
            gateway["extproc_cluster_present"] and gateway["healthy_extproc_hosts"]
            for gateway in gateway_states
        )
        if len(serving_endpoints) == 1 and all_gateways_healthy:
            stable = stable + 1 if state == previous else 1
            if stable >= 3:
                write_json(EVIDENCE / "gateway-extproc-transport-ready.json", state)
                return state
        else:
            stable = 0
        previous = state
        time.sleep(1)
    write_json(EVIDENCE / "gateway-extproc-transport-timeout.json", state)
    raise RuntimeError("Gateway ExtProc cluster or serving ExtProc EndpointSlice did not stabilize")


def scrub_log_line(line):
    line = scrub(line)
    line = re.sub(
        r"(?i)(authorization|bearer|api[_-]?key|token|password|secret|credential)([=:\s\"]+)[^,\s\"'}]+",
        r"\1\2<redacted>", line,
    )
    return line[:2000]


def capture_request_logs(request_id):
    """Capture only request-correlated gateway lines and nearby ExtProc errors."""
    result = {"gateway_access_logs": [], "extproc_errors": []}
    gateway_service = kubectl("-n", API_NAMESPACE, "get", "service", "maas-default-gateway", "-o", "json", check=False)
    gateway_pods = []
    if gateway_service.returncode == 0:
        selector = json.loads(gateway_service.stdout).get("spec", {}).get("selector", {})
        if selector:
            pod_selector = ",".join(f"{key}={value}" for key, value in sorted(selector.items()))
            listed = kubectl("-n", API_NAMESPACE, "get", "pods", "-l", pod_selector, "-o", "json", check=False)
            if listed.returncode == 0:
                gateway_pods = [p.get("metadata", {}).get("name", "")
                                for p in json.loads(listed.stdout).get("items", [])]
    for pod in gateway_pods:
        logs = kubectl("-n", API_NAMESPACE, "logs", f"pod/{pod}", "-c", "istio-proxy",
                       "--since=2m", "--timestamps=true", check=False)
        for line in logs.stdout.splitlines():
            if request_id not in line:
                continue
            payload = line
            try:
                parsed = json.loads(line[line.find("{"):])
                payload = {key: parsed.get(key) for key in (
                    "start_time", "request_id", "test_request_id", "routing_candidate",
                    "routing_revision", "response_code", "response_flags",
                    "response_code_details", "connection_termination_details",
                    "upstream_cluster", "upstream_host", "upstream_transport_failure_reason",
                ) if key in parsed}
            except (ValueError, TypeError):
                payload = scrub_log_line(line)
            result["gateway_access_logs"].append({"pod": pod, "entry": payload})

    pods = json.loads(kubectl("-n", NAMESPACE, "get", "pods", "-l",
                              f"app=payload-processing-external-model,maas.opendatahub.io/tenant-instance={DEPLOYMENT}",
                              "-o", "json").stdout)
    for item in pods.get("items", []):
        pod = item.get("metadata", {}).get("name", "")
        if not pod:
            continue
        logs = kubectl("-n", NAMESPACE, "logs", f"pod/{pod}", "-c", "payload-processing",
                       "--since=30s", "--timestamps=true", check=False)
        matches = []
        for line in logs.stdout.splitlines():
            if request_id in line or re.search(r"(?i)\b(extproc|error|warning|warn|503)\b", line):
                matches.append({"pod": pod, "line": scrub_log_line(line)})
        result["extproc_errors"].extend(matches[-20:])
    return result


def configure_correlated_gateway_access_logs():
    global ISTIO_MESH_ORIGINAL, ISTIO_ACCESS_LOG_CHANGED
    configmap = json.loads(kubectl("-n", "istio-system", "get", "configmap", "istio", "-o", "json").stdout)
    ISTIO_MESH_ORIGINAL = configmap.get("data", {}).get("mesh", "")
    if not ISTIO_MESH_ORIGINAL:
        raise RuntimeError("Istio mesh ConfigMap has no data.mesh to preserve")
    (EVIDENCE / "istio-mesh-original.yaml").write_text(ISTIO_MESH_ORIGINAL)
    mesh_path = EVIDENCE / ".istio-mesh-correlation.yaml"
    mesh_path.write_text(ISTIO_MESH_ORIGINAL)
    access_format = (
        '{"start_time":"%START_TIME%","test_request_id":"%REQ(X-ISSUE28-REQUEST-ID)%",'
        '"session_id":"%REQ(X-SESSION-ID)%",'
        '"request_id":"%REQ(X-REQUEST-ID)%","routing_candidate":"%REQ(X-AI-ROUTING-CANDIDATE)%",'
        '"routing_revision":"%REQ(X-AI-ROUTING-REVISION)%","method":"%REQ(:METHOD)%",'
        '"response_code":"%RESPONSE_CODE%","response_flags":"%RESPONSE_FLAGS%",'
        '"response_code_details":"%RESPONSE_CODE_DETAILS%",'
        '"connection_termination_details":"%CONNECTION_TERMINATION_DETAILS%",'
        '"upstream_cluster":"%UPSTREAM_CLUSTER%","upstream_host":"%UPSTREAM_HOST%",'
        '"upstream_transport_failure_reason":"%UPSTREAM_TRANSPORT_FAILURE_REASON%"}'
    )
    expression = (
        '.accessLogFile = "/dev/stdout" | .accessLogEncoding = "JSON" | .accessLogFormat = '
        + json.dumps(access_format)
    )
    run(["yq", "-i", expression, str(mesh_path)])
    updated_mesh = mesh_path.read_text()
    (EVIDENCE / "istio-mesh-correlation.yaml").write_text(updated_mesh)
    kubectl("-n", "istio-system", "patch", "configmap", "istio", "--type=merge", "-p",
            json.dumps({"data": {"mesh": updated_mesh}}))
    ISTIO_ACCESS_LOG_CHANGED = True
    # Istiod reads the MeshConfig when it starts. Restart it so the Gateway
    # receives the temporary request-correlated access-log format below.
    kubectl("-n", "istio-system", "rollout", "restart", "deployment/istiod")
    kubectl("-n", "istio-system", "rollout", "status", "deployment/istiod", "--timeout=180s")
    kubectl("-n", "maas-system", "rollout", "restart", "deployment/maas-default-gateway-istio")
    kubectl("-n", "maas-system", "rollout", "status", "deployment/maas-default-gateway-istio", "--timeout=180s")
    current = json.loads(kubectl("-n", "istio-system", "get", "configmap", "istio", "-o", "json").stdout)
    if current.get("data", {}).get("mesh") != updated_mesh:
        raise RuntimeError("Istio mesh access-log configuration did not persist")
    deployment = None
    ready_pods = []
    for _ in range(90):
        deployment = json.loads(kubectl("-n", "maas-system", "get", "deployment",
                                        "maas-default-gateway-istio", "-o", "json").stdout)
        status = deployment.get("status", {})
        desired = deployment.get("spec", {}).get("replicas", 1)
        if (status.get("observedGeneration", 0) >= deployment.get("metadata", {}).get("generation", 0)
                and status.get("replicas", 0) == desired
                and status.get("updatedReplicas", 0) == desired
                and status.get("availableReplicas", 0) == desired
                and status.get("terminatingReplicas", 0) == 0):
            gateway = json.loads(kubectl("-n", "maas-system", "get", "pods", "-l",
                                         "gateway.networking.k8s.io/gateway-name=maas-default-gateway",
                                         "-o", "json").stdout)
            ready_pods = [pod for pod in gateway.get("items", [])
                          if not pod.get("metadata", {}).get("deletionTimestamp")
                          and any(c.get("type") == "Ready" and c.get("status") == "True"
                                  for c in pod.get("status", {}).get("conditions", []))]
            if len(ready_pods) == desired:
                break
        time.sleep(1)
    else:
        raise RuntimeError("MaaS Gateway Deployment did not settle to one ready Pod after the access-log restart")
    pod = ready_pods[0]
    dump = kubectl("-n", "maas-system", "exec", f"pod/{pod['metadata']['name']}",
                   "-c", "istio-proxy", "--", "pilot-agent", "request", "GET", "config_dump")
    if "test_request_id" not in dump.stdout or "X-ISSUE28-REQUEST-ID" not in dump.stdout:
        raise RuntimeError("Gateway Envoy config_dump does not contain the request-correlated access log")
    write_json(EVIDENCE / "gateway-access-log-runtime.json", {
        "pod": pod["metadata"]["name"],
        "pod_uid": pod["metadata"].get("uid"),
        "config_dump_bytes": len(dump.stdout),
        "config_dump_sha256": hashlib.sha256(dump.stdout.encode()).hexdigest(),
        "contains_test_request_id_field": "test_request_id" in dump.stdout,
        "contains_issue28_request_header": "X-ISSUE28-REQUEST-ID" in dump.stdout,
    })


def parse_gateway_access_entries(logs, wanted, pod_name):
    entries = []
    for line in logs.splitlines():
        start = line.find("{")
        if start < 0:
            continue
        try:
            entry = json.loads(line[start:])
        except json.JSONDecodeError:
            continue
        request_id = entry.get("test_request_id")
        if request_id not in wanted:
            continue
        session_id = entry.pop("session_id", "")
        entry["session_id_sha256"] = hashlib.sha256(session_id.encode()).hexdigest() if session_id else ""
        entry["gateway_pod"] = pod_name
        entries.append(entry)
    return entries


def capture_gateway_access_event(request_id):
    """Wait briefly for and hash one Gateway access-log record."""
    pods = json.loads(kubectl("-n", "maas-system", "get", "pods", "-l",
                              "gateway.networking.k8s.io/gateway-name=maas-default-gateway",
                              "-o", "json").stdout)
    gateway_pods = [pod for pod in pods.get("items", [])
                    if not pod.get("metadata", {}).get("deletionTimestamp")]
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        for pod in gateway_pods:
            name = pod.get("metadata", {}).get("name", "")
            if not name:
                continue
            logs = kubectl("-n", "maas-system", "logs", f"pod/{name}", "-c", "istio-proxy",
                           "--since=5m", check=False).stdout
            entries = parse_gateway_access_entries(logs, {request_id}, name)
            if entries:
                return entries
        time.sleep(0.1)
    return []


def capture_gateway_access_events(label, expected_request_session_hashes, entries):
    expected_hashes = set(expected_request_session_hashes.values())
    write_json(EVIDENCE / f"gateway-access-events-{label}.json", entries)
    observed_by_request = {
        entry.get("test_request_id"): entry.get("session_id_sha256")
        for entry in entries
    }
    mismatches = [request_id for request_id, expected_hash in expected_request_session_hashes.items()
                  if observed_by_request.get(request_id) != expected_hash]
    seen_hashes = {entry.get("session_id_sha256") for entry in entries if entry.get("session_id_sha256")}
    return {
        "events": entries,
        "expected_request_count": len(expected_request_session_hashes),
        "observed_request_count": len({entry.get("test_request_id") for entry in entries}),
        "session_hashes_seen": sorted(seen_hashes),
        "all_session_headers_observed": bool(expected_hashes) and not mismatches,
        "requests_with_missing_or_mismatched_session_headers": mismatches,
    }


def restore_correlated_gateway_access_logs():
    global ISTIO_ACCESS_LOG_CHANGED
    if not ISTIO_ACCESS_LOG_CHANGED or ISTIO_MESH_ORIGINAL is None:
        return
    kubectl("-n", "istio-system", "patch", "configmap", "istio", "--type=merge", "-p",
            json.dumps({"data": {"mesh": ISTIO_MESH_ORIGINAL}}))
    kubectl("-n", "istio-system", "rollout", "restart", "deployment/istiod")
    kubectl("-n", "istio-system", "rollout", "status", "deployment/istiod", "--timeout=180s")
    kubectl("-n", "maas-system", "rollout", "restart", "deployment/maas-default-gateway-istio")
    kubectl("-n", "maas-system", "rollout", "status", "deployment/maas-default-gateway-istio", "--timeout=180s")
    current = json.loads(kubectl("-n", "istio-system", "get", "configmap", "istio", "-o", "json").stdout)
    if current.get("data", {}).get("mesh") != ISTIO_MESH_ORIGINAL:
        raise RuntimeError("original Istio mesh ConfigMap was not restored exactly")
    (EVIDENCE / "temporary-istio-access-logging-restored.txt").write_text(
        "Original Istio mesh ConfigMap restored exactly and the run-owned gateway restarted.\n")
    ISTIO_ACCESS_LOG_CHANGED = False


def current_provider_path_snapshot():
    overlay_cm = json.loads(kubectl("-n", NAMESPACE, "get", "configmap", "routing-overlay", "-o", "json").stdout)
    wire = json.loads(overlay_cm.get("data", {}).get("routing-overlay.json", "{}"))
    published = overlay_cm.get("metadata", {}).get("annotations", {}).get(
        "inference.opendatahub.io/routing-overlay-content-digest", "")
    KNOWN_OVERLAYS[published] = {
        "candidates": [
            {key: candidate.get(key) for key in ("cluster", "kind", "name", "selection_group", "traffic_weight")}
            for candidate in wire.get("overlay", {}).get("candidates", [])
        ],
        "selection_policy": wire.get("overlay", {}).get("selection_policy"),
    }
    config_obj = json.loads(kubectl("-n", NAMESPACE, "get", "configmap", CONFIG_MAP, "-o", "json").stdout)
    extproc = config_obj.get("data", {}).get("extproc.yaml", "")
    runtime_line = next((line.strip() for line in extproc.splitlines() if "provider_hop_clusters:" in line), "")
    runtime_match = re.search(r"provider_hop_clusters:\s*(\[.*\])", runtime_line)
    runtime_clusters = json.loads(runtime_match.group(1)) if runtime_match else []
    credential_pattern = re.compile(
        r"(?m)^\s+- name: ([^\n]+)\n\s+namespace: ([^\n]+)\n\s+key: ([^\n]+)\n"
        r"\s+strategy: ([^\n]+)\n\s+file: ([^\n]+)$"
    )
    credentials = [
        {"name": m.group(1).strip(), "namespace": m.group(2).strip(),
         "key": m.group(3).strip(), "file": m.group(5).strip()}
        for m in credential_pattern.finditer(extproc)
    ]
    deployment = json.loads(kubectl("-n", NAMESPACE, "get", "deployment", DEPLOYMENT, "-o", "json").stdout)
    projected = []
    for volume in deployment.get("spec", {}).get("template", {}).get("spec", {}).get("volumes", []):
        if volume.get("name") != "provider-credentials":
            continue
        for source in volume.get("projected", {}).get("sources", []):
            secret = source.get("secret", {})
            projected.extend({"name": secret.get("name"), "key": item.get("key"), "path": item.get("path")}
                             for item in secret.get("items", []))
    return {
        "published_digest": published,
        "published_candidates": KNOWN_OVERLAYS[published]["candidates"],
        "published_selection_policy": KNOWN_OVERLAYS[published]["selection_policy"],
        "runtime_config_sha256": hashlib.sha256(extproc.encode()).hexdigest() if extproc else "",
        "runtime_provider_hop_clusters": runtime_clusters,
        "runtime_credentials": credentials,
        "deployment_template_projected_secret_keys": projected,
        "deployment_generation": deployment.get("metadata", {}).get("generation"),
        "deployment_replicas": deployment.get("status", {}).get("replicas"),
        "deployment_updated_replicas": deployment.get("status", {}).get("updatedReplicas"),
        "deployment_available_replicas": deployment.get("status", {}).get("availableReplicas"),
    }


def transition_probe(name, allowed_providers, publication, requests=20):
    config_obj = json.loads(kubectl("-n", NAMESPACE, "get", "configmap", CONFIG_MAP, "-o", "json").stdout)
    deployment = json.loads(kubectl("-n", NAMESPACE, "get", "deployment", DEPLOYMENT, "-o", "json").stdout)
    extproc = config_obj.get("data", {}).get("extproc.yaml", "")
    cluster_line = next((line.strip() for line in extproc.splitlines() if "provider_hop_clusters:" in line), "")
    cluster_match = re.search(r"provider_hop_clusters:\s*(\[.*\])", cluster_line)
    runtime_clusters = json.loads(cluster_match.group(1)) if cluster_match else []
    credential_pattern = re.compile(
        r"(?m)^\s+- name: ([^\n]+)\n\s+namespace: ([^\n]+)\n\s+key: ([^\n]+)\n"
        r"\s+strategy: ([^\n]+)\n\s+file: ([^\n]+)$"
    )
    credential_names = [match.group(1).strip() for match in credential_pattern.finditer(extproc)]
    pod_revisions = capture_pod_revisions(name)
    projected = []
    for volume in deployment.get("spec", {}).get("template", {}).get("spec", {}).get("volumes", []):
        if volume.get("name") == "provider-credentials":
            for source in volume.get("projected", {}).get("sources", []):
                secret = source.get("secret", {})
                projected.extend({"name": secret.get("name"), "key": item.get("key"), "path": item.get("path")}
                                 for item in secret.get("items", []))
    runtime_snapshot = {
        "captured_after_first_publication": name,
        "published_digest": publication["published_digest"],
        "provider_hop_clusters": runtime_clusters,
        "credential_names": [name.strip() for name in credential_names],
        "projected_secret_keys": projected,
        "configmap_extproc_sha256": hashlib.sha256(extproc.encode()).hexdigest() if extproc else "",
        "deployment_extproc_sha256": deployment.get("spec", {}).get("template", {}).get("metadata", {}).get(
            "annotations", {}).get("ai-gateway-controller.opendatahub.io/extproc-config-sha256", ""),
        "deployment_generation": deployment.get("metadata", {}).get("generation"),
        "deployment_observed_generation": deployment.get("status", {}).get("observedGeneration"),
        "deployment_updated_replicas": deployment.get("status", {}).get("updatedReplicas"),
        "deployment_available_replicas": deployment.get("status", {}).get("availableReplicas"),
        "deployment_replicas": deployment.get("status", {}).get("replicas"),
        "pod_revisions": pod_revisions,
    }
    write_json(EVIDENCE / f"runtime-at-{name}.json", runtime_snapshot)
    ingress_before = provider_ingress_counters()
    write_json(EVIDENCE / f"provider-ingress-before-{name}.json", ingress_before)
    counts = {provider: 0 for provider in allowed_providers}
    failures = 0
    status_counts = {}
    response_detail_counts = {}
    failed_requests = []
    for request_index in range(requests):
        response = gateway_request()
        status_counts[str(response["status"])] = status_counts.get(str(response["status"]), 0) + 1
        detail = response.get("response_details", {}).get("code_details") or "none"
        response_detail_counts[detail] = response_detail_counts.get(detail, 0) + 1
        if response["status"] != 200 or not response["checks"] or response["provider"] not in allowed_providers:
            failures += 1
            failure = {
                "request_id": response.get("request_id"),
                "status": response["status"],
                "response_source": response.get("response_source", {}),
                "selected_provider_metadata": response.get("selected_provider_metadata", {}),
                "response_details": response.get("response_details", {}),
                "error_type": response.get("error_type"),
                "observed_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }
            # Preserve the response's Gateway-side classification while the
            # request is still in the proxy log window. This is especially
            # useful for distinguishing a withdrawn upstream (503/UH) from a
            # route disappearance (404/NR) during the separate #108 probe.
            failure["gateway_access_event"] = capture_gateway_access_event(response["request_id"])
            failure.update(capture_request_logs(response["request_id"]))
            if response["status"] == 503:
                failure["provider_ingress_counters_at_observation"] = provider_ingress_counters()
                failure["provider_path_state_at_observation"] = current_provider_path_snapshot()
                failure["service_endpoints_at_observation"] = capture_ready_endpoints()
                failure["per_pod_revisions_at_observation"] = capture_pod_revisions(
                    f"{name}-failure-{request_index}")
            elif response["status"] == 404 and not any(
                    item.get("status") == 404 for item in failed_requests):
                failure["provider_ingress_counters_at_observation"] = provider_ingress_counters()
                failure["provider_path_state_at_observation"] = current_provider_path_snapshot()
                failure["service_endpoints_at_observation"] = capture_ready_endpoints()
                failure["per_pod_revisions_at_observation"] = capture_pod_revisions(
                    f"{name}-first-404-{request_index}")
            failed_requests.append(failure)
        else:
            counts[response["provider"]] += 1
    ingress_after = provider_ingress_counters()
    ingress_delta = counter_delta(ingress_before, ingress_after)
    write_json(EVIDENCE / f"provider-ingress-after-{name}.json", ingress_after)
    write_json(EVIDENCE / f"provider-ingress-delta-{name}.json", ingress_delta)
    diagnostic = {"name": name, "requests": requests, "provider_counts": counts,
                  "failures": failures, "status_counts": status_counts,
                  "response_code_details": response_detail_counts,
                  "failed_request_metadata": failed_requests,
                  "provider_ingress_delta": ingress_delta,
                  "first_published": publication, "runtime_snapshot_file": f"runtime-at-{name}.json"}
    if name == "two_to_one_first_publication":
        diagnostic["tracking_issue"] = "https://github.com/opendatahub-io/ai-gateway-controller/issues/108"
        diagnostic["known_behavior"] = "withdrawal ordering; failures remain test failures"
    RESULT.setdefault("transition_probes", []).append(diagnostic)
    if failures:
        RESULT["transition_failure"] = True
    write_json(EVIDENCE / "results.json", RESULT)
    print(f"OBSERVED {name}: {diagnostic}")


def converged(expected, label):
    last = None
    stable = 0
    last_state = {}
    for _ in range(120):
        model_result = kubectl("-n", NAMESPACE, "get", "externalmodel", MODEL, "-o", "json", check=False)
        model = json.loads(model_result.stdout) if model_result.returncode == 0 else {}
        model_status = model.get("status", {})
        model_ready = (model_status.get("phase") == "Ready"
                       and model_status.get("observedGeneration") == model.get("metadata", {}).get("generation"))
        state = overlay_snapshot(label)
        deployment = json.loads(kubectl("-n", NAMESPACE, "get", "deployment", DEPLOYMENT, "-o", "json").stdout)
        selector = deployment.get("spec", {}).get("selector", {}).get("matchLabels", {})
        label_selector = ",".join(f"{key}={value}" for key, value in sorted(selector.items()))
        clusters = {c["cluster"] for c in state["candidates"]}
        desired_ok = clusters == expected
        expected_secret_names = {cluster.removeprefix("provider-") + "-credentials" for cluster in expected}
        runtime_clusters = set(state["runtime_clusters"])
        credential_names = {c["name"] for c in state["credential_entries"]}
        mounted_secret_names = {s["name"] for s in state["mounted_secrets"]}
        runtime_ok = (runtime_clusters == expected
                      and credential_names == expected_secret_names
                      and mounted_secret_names == expected_secret_names)
        expected_providers = [cluster.removeprefix("provider-") for cluster in expected]
        providers_ready = all(provider_ready(name) for name in expected_providers)
        config_revision_ok = (
            bool(state["configmap_extproc_sha256"])
            and state["configmap_extproc_sha256"] == state["mounted_extproc_sha256"]
            == state["deployment_extproc_sha256"] == state["extproc_config_sha256"]
        )
        digests_ok = bool(state["published_digest"]) and state["published_digest"] == state["mounted_digest"] == state["accepted_revision"] == state["serving_revision"]
        desired_replicas = 1
        settled_pods = []
        latest_template_hash = ""
        if (desired_ok and runtime_ok and providers_ready and config_revision_ok and digests_ok and model_ready):
            settled_pods = capture_pod_revisions(label)
            rs_json = json.loads(kubectl("-n", NAMESPACE, "get", "replicasets", "-l", label_selector,
                                         "-o", "json").stdout)
            owner_uid = deployment["metadata"]["uid"]
            owned_rs = [rs for rs in rs_json.get("items", [])
                        if any(owner.get("uid") == owner_uid for owner in rs.get("metadata", {}).get("ownerReferences", []))]
            newest_rs = max(owned_rs, key=lambda rs: int(rs.get("metadata", {}).get("annotations", {}).get(
                "deployment.kubernetes.io/revision", "0")), default={})
            latest_template_hash = newest_rs.get("metadata", {}).get("labels", {}).get("pod-template-hash", "")
        ready_pods = [pod for pod in settled_pods if pod["ready"]]
        serving_revisions_ok = all(
            pod["mounted_digest"] == state["published_digest"]
            and pod["accepted_revision"] == state["published_digest"]
            and pod["serving_revision"] == state["published_digest"]
            and pod["mounted_extproc_sha256"] == state["configmap_extproc_sha256"]
            for pod in ready_pods
        )
        no_old_ready_pods = bool(latest_template_hash) and all(
            pod["template_hash"] == latest_template_hash for pod in ready_pods
        )
        deployment_ok = (
            state["deployment_observed_generation"] == state["deployment_generation"]
            and state["deployment_replicas"] == desired_replicas
            and state["deployment_updated_replicas"] == desired_replicas
            and state["deployment_ready_replicas"] == desired_replicas
            and state["deployment_available_replicas"] == desired_replicas
            and state["deployment_unavailable_replicas"] == 0
            and state["deployment_terminating_replicas"] == 0
            and len([pod for pod in ready_pods if not pod["deleting"]]) == desired_replicas
            and no_old_ready_pods
            and serving_revisions_ok
        )
        state["pod_revisions"] = settled_pods
        state["latest_template_hash"] = latest_template_hash
        if (desired_ok and runtime_ok and providers_ready and config_revision_ok
                and digests_ok and deployment_ok and model_ready):
            if state == last:
                stable += 1
                if stable >= 2:
                    KNOWN_OVERLAYS[state["published_digest"]] = {
                        "candidates": state["candidates"],
                        "selection_policy": state["selection_policy"],
                    }
                    state["known_serving_overlay"] = KNOWN_OVERLAYS[state["published_digest"]]
                    write_json(EVIDENCE / f"revision-{label}.json", state)
                    return state
            else:
                stable = 0
        else:
            stable = 0
        RESULT["transitions"].append(state)
        write_json(EVIDENCE / "results.json", RESULT)
        last = state
        last_state = state
        time.sleep(2)
    write_json(EVIDENCE / f"revision-{label}-last.json", last_state)
    raise RuntimeError(f"revision failed to converge: {label}")


def start_port_forward(namespace, service, local_port, remote_port, logfile):
    f = open(logfile, "w")
    p = subprocess.Popen(["kubectl", "--context", f"kind-{CLUSTER}", "-n", namespace,
                          "port-forward", f"svc/{service}", f"{local_port}:{remote_port}"],
                         stdout=f, stderr=subprocess.STDOUT)
    PROCESSES.append((p, f))
    for _ in range(60):
        try:
            with __import__("socket").create_connection(("127.0.0.1", local_port), timeout=0.4):
                return p
        except OSError:
            time.sleep(1)
    raise RuntimeError(f"port-forward not ready for {service}")


def gateway_request(session=None):
    headers = {"Authorization": AUTHORIZATION, "Content-Type": "application/json"}
    request_id = "issue28-" + secrets.token_hex(8)
    headers["X-Request-ID"] = request_id
    # The ingress Envoy may replace the external X-Request-ID. This separate
    # test-only ID is included in access logs for direct request correlation.
    headers["X-Issue28-Request-ID"] = request_id
    if session:
        headers["X-Session-ID"] = session
    req = urllib.request.Request(
        f"http://127.0.0.1:{GATEWAY_PORT}/{NAMESPACE}/demo/v1/chat/completions",
        data=b'{"model":"demo","messages":[{"role":"user","content":"selection-check"}]}',
        headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            status = response.status
            body = response.read()
            response_headers = response.headers
            response_details = {
                "code_details": response.headers.get("x-envoy-response-code-details", ""),
                "flags": response.headers.get("x-envoy-response-flags", ""),
            }
    except urllib.error.HTTPError as e:
        status, body = e.code, e.read()
        response_headers = e.headers
        response_details = {
            "code_details": e.headers.get("x-envoy-response-code-details", ""),
            "flags": e.headers.get("x-envoy-response-flags", ""),
        }
    except Exception as exc:
        return {"status": 0, "provider": "", "checks": False, "error_type": type(exc).__name__,
                "request_id": request_id, "response_details": {}, "response_source": {},
                "selected_provider_metadata": {}}
    try:
        data = json.loads(body)
    except Exception:
        data = {}
    source_headers = (
        "server", "x-envoy-upstream-host", "x-envoy-upstream-cluster",
        "x-envoy-decorator-operation", "x-envoy-response-code-details",
        "x-envoy-response-flags", "x-request-id",
    )
    response_source = {}
    for name in source_headers:
        value = response_headers.get(name, "")
        if value and re.fullmatch(r"[A-Za-z0-9_{}=,:./-]{1,240}", value):
            response_source[name] = value
    selected_provider_metadata = {}
    for name in ("provider", "selected_provider", "selectedProvider", "candidate",
                 "selected_candidate", "selectedCandidate", "cluster", "upstream"):
        value = data.get(name)
        if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.:/-]{1,200}", value):
            selected_provider_metadata[name] = value
    return {
        "status": status, "provider": data.get("provider", ""),
        "checks": all(data.get(k) is True for k in ("tls", "sni_ok", "authority_ok", "credential_ok")),
        "request_id": request_id,
        "response_source": response_source,
        "selected_provider_metadata": selected_provider_metadata,
        "response_details": {
            k: value for k, value in response_details.items()
            if value and re.fullmatch(r"[A-Za-z0-9_{}=,:.-]{1,160}", value)
        },
    }


def sample(name, a_min=35, a_max=65, b_min=35, b_max=65, label="roughly_even_split", require_a_gt_b=False):
    a = b = failures = 0
    status_counts = {}
    for _ in range(100):
        response = gateway_request()
        status_counts[str(response["status"])] = status_counts.get(str(response["status"]), 0) + 1
        if response["status"] != 200 or not response["checks"] or response["provider"] not in ("proof-a", "proof-b"):
            failures += 1
        elif response["provider"] == "proof-a":
            a += 1
        else:
            b += 1
    # Broad bands absorb ordinary random variation while still catching
    # uniform-vs-proportional skew (equal: 35–65; 80/20: prefer A and a > b).
    biased = a_min <= a <= a_max and b_min <= b <= b_max
    if require_a_gt_b:
        biased = biased and a > b
    result = {"name": name, "requests": 100, "proof_a": a, "proof_b": b, "failures": failures,
              "status_counts": status_counts,
              "strict_https_sni_authority_credential": True,
              label: biased, "a_gt_b": a > b,
              "accepted_proof_a": f"{a_min}-{a_max}", "accepted_proof_b": f"{b_min}-{b_max}"}
    RESULT["samples"].append(result)
    write_json(EVIDENCE / "results.json", RESULT)
    record(name, failures == 0 and biased, str(result))


def sample_single_provider(name, expected_provider):
    counts = {"proof-a": 0, "proof-b": 0}
    failures = 0
    status_counts = {}
    for _ in range(100):
        response = gateway_request()
        status_counts[str(response["status"])] = status_counts.get(str(response["status"]), 0) + 1
        if response["status"] != 200 or not response["checks"] or response["provider"] != expected_provider:
            failures += 1
        elif response["provider"] in counts:
            counts[response["provider"]] += 1
    return {"name": name, "requests": 100, "proof_a": counts["proof-a"],
            "proof_b": counts["proof-b"], "failures": failures, "status_counts": status_counts,
            "strict_https_sni_authority_credential": True}


def main():
    global MODEL_BEFORE, CONFIG_BEFORE, API_KEY_ID, AUTHORIZATION, API_DB_EGRESS_POLICY
    if not re.fullmatch(r"praxis-extproc:[A-Za-z0-9._-]+", EXTPROC_IMAGE):
        raise RuntimeError(f"Kind qualification requires a local ExtProc image tag, got {EXTPROC_IMAGE!r}")
    if not PRAXIS_EXTPROC_REPO:
        raise RuntimeError("PRAXIS_EXTPROC_REPO must identify the source checkout used to build ExtProc")
    source_image_id = run(["docker", "image", "inspect", EXTPROC_IMAGE, "--format", "{{.Id}}"], check=False)
    if source_image_id.returncode != 0:
        raise RuntimeError("the locally built ExtProc image must be present")
    source_image_id = source_image_id.stdout.strip()
    source_head = run(["git", "-C", PRAXIS_EXTPROC_REPO, "rev-parse", "HEAD"]).stdout.strip()
    source_branch = run(["git", "-C", PRAXIS_EXTPROC_REPO, "branch", "--show-current"]).stdout.strip()
    source_status = run(["git", "-C", PRAXIS_EXTPROC_REPO, "status", "--porcelain"]).stdout.strip()
    if source_branch != "main":
        raise RuntimeError(f"ExtProc source checkout must be on main, got {source_branch!r}")
    if source_status:
        raise RuntimeError("ExtProc main checkout must be clean")
    if EXPECTED_EXTPROC_MAIN_SHA and source_head != EXPECTED_EXTPROC_MAIN_SHA:
        raise RuntimeError(f"ExtProc main SHA mismatch: expected={EXPECTED_EXTPROC_MAIN_SHA}, got={source_head}")
    node = f"{CLUSTER}-control-plane"
    node_images = run(["docker", "exec", node, "crictl", "images", "-o", "json"])
    expected_node_tag = f"docker.io/library/{EXTPROC_IMAGE}"
    matching_node_images = [image for image in json.loads(node_images.stdout).get("images", [])
                            if expected_node_tag in image.get("repoTags", [])]
    if len(matching_node_images) != 1:
        raise RuntimeError(f"expected exactly one loaded Kind image for {EXTPROC_IMAGE}, found {len(matching_node_images)}")
    node_image_id = matching_node_images[0]["id"]
    capture_provenance("before")
    configure_correlated_gateway_access_logs()
    run(["go", "build", "-o", str(EVIDENCE / "recompute-digest"), str(ROOT / "test/kind-env/recompute_digest.go")])
    original = kubectl("-n", NAMESPACE, "get", "externalmodel", MODEL, "-o", "json")
    MODEL_BEFORE = json.loads(original.stdout)
    CONFIG_BEFORE = kubectl("-n", NAMESPACE, "get", "configmap", CONFIG_MAP, "-o", "json").stdout
    extproc = json.loads(CONFIG_BEFORE)["data"]["extproc.yaml"]
    (EVIDENCE / "original-provider-runtime-config.txt").write_text(
        "\n".join(line.strip() for line in extproc.splitlines() if "provider_hop_clusters:" in line) + "\n")
    for name in ("provider-proof-a", "provider-proof-b"):
        existing = kubectl("-n", NAMESPACE, "get", "externalprovider", name, "-o", "json", check=False)
        if existing.returncode == 0:
            labels = json.loads(existing.stdout).get("metadata", {}).get("labels", {})
            if labels.get("external-model-e2e/purpose") != "provider-selection-proof":
                raise RuntimeError(f"refusing to overwrite unowned ExternalProvider {NAMESPACE}/{name}")
    pods = json.loads(kubectl("-n", NAMESPACE, "get", "pods", "-l",
        f"app=payload-processing-external-model,maas.opendatahub.io/tenant-instance={DEPLOYMENT}", "-o", "json").stdout)
    runtime_pods = []
    for pod in pods.get("items", []):
        if pod.get("metadata", {}).get("deletionTimestamp"):
            continue
        statuses = {c.get("name"): c for c in pod.get("status", {}).get("containerStatuses", [])}
        images = []
        for container in pod.get("spec", {}).get("containers", []):
            if "praxis-extproc" not in container.get("image", ""):
                continue
            status = statuses.get(container.get("name"), {})
            images.append({
                "name": container.get("name"),
                "image": container.get("image"),
                "imageID": status.get("imageID"),
                "ready": status.get("ready"),
                "restartCount": status.get("restartCount"),
            })
        runtime_pods.append({"uid": pod["metadata"].get("uid"), "name": pod["metadata"].get("name"),
                             "ready": any(c.get("ready") for c in images), "containers": images})
    write_json(EVIDENCE / "extproc-runtime-image.json", {
        "requested_image": EXTPROC_IMAGE,
        "source_repository": PRAXIS_EXTPROC_REPO,
        "source_branch": source_branch,
        "source_commit": source_head,
        "source_worktree_clean": not source_status,
        "source_worktree_status": source_status,
        "local_image_id": source_image_id,
        "kind_node_image_id": node_image_id,
        "pods": runtime_pods,
    })
    ready_runtime_images = [container for pod in runtime_pods if pod["ready"]
                            for container in pod["containers"]]
    if (not ready_runtime_images
            or any(container["image"] != EXTPROC_IMAGE
                   or container.get("imageID", "").rsplit("://", 1)[-1] != node_image_id
                   for container in ready_runtime_images)):
        raise RuntimeError("no ready serving ExtProc pod matches the loaded Kind image ID; see extproc-runtime-image.json")
    record("local_extproc_runtime_identity", True,
           json.dumps({"reference": EXTPROC_IMAGE, "source_branch": source_branch,
                       "source_commit": source_head, "local_image_id": source_image_id,
                       "kind_node_image_id": node_image_id,
                       "runtime_image_ids": sorted({c["imageID"] for c in ready_runtime_images})}, sort_keys=True))
    verify_provider_resolver_loaded(node_image_id)

    for suffix in ("a", "b"):
        value = backend_credential(API_NAMESPACE, f"provider-proof-{suffix}-backend-credential", suffix)
        model_credential(f"provider-proof-{suffix}-credentials", value)
    proof_manifest = (ROOT / "test/kind-env/manifests/provider-selection-proof.yaml").read_text()
    proof_manifest = proof_manifest.replace("provider-recorder:issue28-kind", PROVIDER_RECORDER_IMAGE)
    run(["kubectl", "--context", f"kind-{CLUSTER}", "apply", "-f", "-"], input_text=proof_manifest)
    for suffix in ("a", "b"):
        kubectl("-n", "maas-system", "rollout", "status", f"deployment/provider-selection-proof-{suffix}",
                "--timeout=120s")
    run(["kubectl", "--context", f"kind-{CLUSTER}", "apply", "-f",
         str(ROOT / "test/kind-env/manifests/provider-proof-external-providers.yaml")])
    capture_providers("created")
    record("proof_provider_objects_created", True,
           "both proof providers applied; readiness is set by ExternalModel reconciliation after each reference is selected")

    # The Kind Postgres fixture uses app=maas-postgres, while the generated
    # MaaS API egress rule is absent in this local setup. Allow only this test's
    # default API pod to reach that fixture on the Postgres port.
    kubectl("apply", "-f", str(ROOT / "test/kind-env/manifests/provider-selection-kind-networkpolicy.yaml"))
    API_DB_EGRESS_POLICY = True
    record("kind_api_database_egress", True,
           "run-owned policy permits default maas-api to reach only the maas-postgres fixture on TCP 5432")

    ca = kubectl("-n", "kuadrant-system", "get", "configmap", "authorino-maas-api-ca",
                 "-o", "jsonpath={.data.ca\\.crt}").stdout
    if not ca.strip():
        raise RuntimeError("Authorino MaaS API CA ConfigMap has no ca.crt data")
    (EVIDENCE / "maas-api-ca.crt").write_text(ca)
    start_port_forward(API_NAMESPACE, "maas-api", PORT, 8443, EVIDENCE / "port-forward-api.log")
    start_port_forward(API_NAMESPACE, "maas-default-gateway", GATEWAY_PORT, 80, EVIDENCE / "port-forward-gateway.log")
    admin_cfg = EVIDENCE / ".admin-curl.cfg"
    admin_cfg.write_text('header = "X-MaaS-Username: kind-user"\nheader = "X-MaaS-Group: [\\"system:authenticated\\"]"\n')
    key_response = run(["curl", "--noproxy", "*", "--cacert", str(EVIDENCE / "maas-api-ca.crt"),
        "--http1.1", "--connect-timeout", "5", "--max-time", "20",
        "--resolve", f"maas-api.maas-system.svc.cluster.local:{PORT}:127.0.0.1",
        "-sS", "-X", "POST", "-o", "-", "--config", str(admin_cfg),
        "-H", "content-type: application/json", "--data",
        '{"name":"issue28-provider-selection","ephemeral":true,"subscription":"kind-e2e-subscription"}',
        f"https://maas-api.maas-system.svc.cluster.local:{PORT}/v1/api-keys"]).stdout
    key = json.loads(key_response)
    API_KEY_ID = key.get("id") or key.get("keyId") or key.get("metadata", {}).get("id")
    token = key.get("key") or key.get("apiKey") or key.get("token")
    if not API_KEY_ID or not token:
        raise RuntimeError("MaaS API did not return an ephemeral API key")
    AUTHORIZATION = "Bearer " + token
    del token, key
    record("api_key_creation", True, "ephemeral key created; value not recorded")

    ref_a, ref_b = make_ref("provider-proof-a"), make_ref("provider-proof-b")
    if os.environ.get("LOCAL_ENV_SELECTION_TRANSITION_ONLY") == "1":
        set_refs([ref_a, ref_b])
        converged({"provider-provider-proof-a", "provider-provider-proof-b"}, "two-provider-transition-only")
        wait_providers(("a", "b"))
        set_refs([ref_a], wait=False)
        first_one = wait_for_published_candidates({"provider-provider-proof-a"}, "two-to-one-transition-only")
        transition_probe("two_to_one_first_publication", {"proof-a", "proof-b"}, first_one)
        one = converged({"provider-provider-proof-a"}, "two-to-one-transition-only")
        record("two_to_one_revision", True, json.dumps(one, sort_keys=True))
        return

    set_refs([ref_a])
    one = converged({"provider-provider-proof-a"}, "one-provider")
    wait_providers(("a",))
    capture_providers("one-provider-ready")
    record("proof_provider_a_ready", True, "provider A Ready condition captured after its first model reference")
    record("one_provider_revision", True, json.dumps(one, sort_keys=True))
    one_sample = sample_single_provider("sample_one_provider", "proof-a")
    RESULT["samples"].append(one_sample)
    write_json(EVIDENCE / "results.json", RESULT)
    record("sample_one_provider", one_sample["failures"] == 0 and one_sample["proof_a"] == 100,
           str(one_sample))
    deterministic = [gateway_request() for _ in range(20)]
    record("one_provider_deterministic",
           all(r["status"] == 200 and r["provider"] == "proof-a" and r["checks"] for r in deterministic),
           "20 requests all attributed to proof-a with strict HTTPS checks")

    set_refs([ref_a, ref_b], wait=False)
    first_two = wait_for_published_candidates({"provider-provider-proof-a", "provider-provider-proof-b"}, "one-to-two")
    transition_probe("one_to_two_first_publication", {"proof-a", "proof-b"}, first_two)
    two = converged({"provider-provider-proof-a", "provider-provider-proof-b"}, "two-provider")
    wait_providers(("a", "b"))
    capture_providers("two-providers-ready")
    record("proof_providers_a_b_ready", True, "both Ready conditions and events captured after the 1-to-2 reconcile")
    groups = {c.get("selection_group") for c in two["candidates"]}
    record("one_to_two_revision", True, json.dumps(two, sort_keys=True))
    record("equal_weight_wire_metadata",
           two["selection_policy"] == {"mode": "random"} and groups == {0},
           "random policy and numeric group zero on both candidates")

    # Add the affinity setting before the bounded routing samples so the
    # ExtProc rollout and connection warm-up cannot interrupt the affinity
    # observations themselves. The fixture changes only the test runtime
    # ConfigMap; the product controller remains paused only until this
    # configuration is mounted and verified.
    affinity_runtime_overlay = enable_test_session_affinity()
    sample("sample_1")
    sample("sample_2")

    # Re-read the running config after the samples. Affinity is considered
    # configured only when the ConfigMap, Deployment template, and serving
    # pod's mounted file all contain the same bytes.
    affinity_runtime_overlay = overlay_snapshot("two-provider-session-affinity")
    expected_affinity = "session_affinity: {enabled: true, header: x-session-id, ttl_secs: 3600}"
    mounted_affinity_config = run(["kubectl", "--context", f"kind-{CLUSTER}", "-n", NAMESPACE,
        "exec", f"deploy/{DEPLOYMENT}", "--", "cat", "/etc/praxis/extproc.yaml"], timeout=15).stdout
    if expected_affinity not in mounted_affinity_config:
        raise RuntimeError("serving ExtProc config does not contain the expected session-affinity contract")
    record("session_affinity_runtime_still_loaded", True,
           "the test session-affinity setting remains in the serving ExtProc configuration")
    affinity_before = provider_ingress_counters()
    sessions = {}
    attempts = []
    all_request_ids = []
    captured_gateway_events = []
    for i in range(60):
        session = f"issue28-affinity-{i}"
        session_hash = hashlib.sha256(session.encode()).hexdigest()
        response = gateway_request(session)
        captured_gateway_events.extend(capture_gateway_access_event(response["request_id"]))
        attempt = {"session_id_sha256": session_hash, "response": response}
        if response["status"] != 200:
            attempt["diagnostics"] = capture_request_logs(response["request_id"])
        attempts.append(attempt)
        all_request_ids.append(response["request_id"])
        if (response["status"] == 200 and response["provider"] in ("proof-a", "proof-b")
                and response["checks"]):
            sessions.setdefault(response["provider"], {
                "session": session,
                "session_id_sha256": session_hash,
                "initial_response": response,
            })
        if len(sessions) == 2:
            break
    affinity_sessions = []
    for provider in ("proof-a", "proof-b"):
        binding = sessions.get(provider)
        if binding is None:
            continue
        repeats = [gateway_request(binding["session"]) for _ in range(10)]
        for response in repeats:
            captured_gateway_events.extend(capture_gateway_access_event(response["request_id"]))
        for response in repeats:
            if response["status"] != 200:
                response["diagnostics"] = capture_request_logs(response["request_id"])
        all_request_ids.extend(response["request_id"] for response in repeats)
        affinity_sessions.append({
            "initial_provider": provider,
            "session_id_sha256": binding["session_id_sha256"],
            "initial_response": binding["initial_response"],
            "repeat_responses": repeats,
            "reused_provider_for_all_repeats": all(
                response["status"] == 200 and response["provider"] == provider and response["checks"]
                for response in repeats),
        })
    expected_request_session_hashes = {
        attempt["response"]["request_id"]: attempt["session_id_sha256"] for attempt in attempts
    }
    for binding in affinity_sessions:
        for response in binding["repeat_responses"]:
            expected_request_session_hashes[response["request_id"]] = binding["session_id_sha256"]
    gateway_events = capture_gateway_access_events(
        "session-affinity", expected_request_session_hashes, captured_gateway_events)
    provider_events = capture_provider_ingress_events("session-affinity", all_request_ids)
    affinity_after = provider_ingress_counters()
    gateway_request_hashes = {
        event.get("test_request_id"): event.get("session_id_sha256")
        for event in gateway_events["events"]
    }
    header_name_matches = "X-Session-ID".lower() == "x-session-id".lower()
    gateway_headers_match = all(
        gateway_request_hashes.get(request_id) == expected_hash
        for request_id, expected_hash in expected_request_session_hashes.items()
    )
    all_affinity_requests_successful = all(
        attempt["response"]["status"] == 200
        and attempt["response"]["provider"] in ("proof-a", "proof-b")
        and attempt["response"]["checks"]
        for attempt in attempts
    )
    all_affinity_requests_successful = all_affinity_requests_successful and all(
        response["status"] == 200 and response["checks"]
        for binding in affinity_sessions for response in binding["repeat_responses"]
    )
    affinity_result = {
        "configured_header": "x-session-id",
        "sent_header": "X-Session-ID",
        "header_name_matches_case_insensitively": header_name_matches,
        "all_affinity_requests_successful": all_affinity_requests_successful,
        "gateway_headers_match_requests": gateway_headers_match,
        "provider_forwarded_session_header_requests": sum(
            bool(event.get("session_id_sha256")) for event in provider_events),
        "runtime_config_excerpt": (EVIDENCE / "test-provider-runtime-config.txt").read_text().splitlines(),
        "runtime_overlay": affinity_runtime_overlay,
        "initial_selection_attempts": attempts,
        "sessions": affinity_sessions,
        "gateway_access_observation": {k: v for k, v in gateway_events.items() if k != "events"},
        "provider_ingress_delta": counter_delta(affinity_before, affinity_after),
        "provider_ingress_event_count": len(provider_events),
    }
    write_json(EVIDENCE / "session-affinity-check.json", affinity_result)
    affinity_passed = (
        set(sessions) == {"proof-a", "proof-b"}
        and len(affinity_sessions) == 2
        and all(item["reused_provider_for_all_repeats"] for item in affinity_sessions)
        and header_name_matches
        and all_affinity_requests_successful
        and gateway_headers_match
        and gateway_events["observed_request_count"] == len(set(all_request_ids))
        and gateway_events["all_session_headers_observed"]
    )
    record("session_affinity", affinity_passed,
           json.dumps({k: v for k, v in affinity_result.items() if k != "initial_selection_attempts"},
                      sort_keys=True))

    resume_controller()
    converged({"provider-provider-proof-a", "provider-provider-proof-b"}, "two-provider-after-affinity")

    # Proportional 80/20 selection (#122). Band is intentionally wide: prove
    # bias toward A without requiring an exact ratio over 100 draws.
    ref_a80, ref_b20 = make_ref("provider-proof-a", 80), make_ref("provider-proof-b", 20)
    set_refs([ref_a80, ref_b20])
    weighted = converged({"provider-provider-proof-a", "provider-provider-proof-b"}, "unequal-weight-80-20")
    weights = {c.get("cluster"): c.get("traffic_weight") for c in weighted["candidates"]}
    record("unequal_weight_wire_metadata",
           weighted["selection_policy"] == {"mode": "weightedRandom"}
           and weights.get("provider-provider-proof-a") == 80
           and weights.get("provider-provider-proof-b") == 20
           and {c.get("selection_group") for c in weighted["candidates"]} == {0},
           json.dumps({"selection_policy": weighted["selection_policy"], "traffic_weights": weights},
                      sort_keys=True))
    sample("sample_unequal_80_20", a_min=55, a_max=95, b_min=5, b_max=45,
           label="roughly_proportional_80_20", require_a_gt_b=True)

    # weight 0 disables the second provider while keeping the ref.
    set_refs([ref_a, make_ref("provider-proof-b", 0)])
    disabled = converged({"provider-provider-proof-a"}, "weight-zero-disabled")
    record("weight_zero_wire_metadata",
           len(disabled["candidates"]) == 1
           and disabled["candidates"][0].get("cluster") == "provider-provider-proof-a"
           and not disabled.get("selection_policy"),
           json.dumps(disabled, sort_keys=True))
    disabled_sample = sample_single_provider("sample_weight_zero_disabled", "proof-a")
    RESULT["samples"].append(disabled_sample)
    write_json(EVIDENCE / "results.json", RESULT)
    record("sample_weight_zero_disabled",
           disabled_sample["failures"] == 0 and disabled_sample["proof_a"] == 100,
           str(disabled_sample))

    set_refs([ref_a, ref_b])
    converged({"provider-provider-proof-a", "provider-provider-proof-b"}, "two-provider-before-withdrawal")
    set_refs([ref_a], wait=False)
    first_one = wait_for_published_candidates({"provider-provider-proof-a"}, "two-to-one")
    transition_probe("two_to_one_first_publication", {"proof-a", "proof-b"}, first_one)
    one_again = converged({"provider-provider-proof-a"}, "two-to-one")
    record("two_to_one_revision", True, json.dumps(one_again, sort_keys=True))
    deterministic = [gateway_request() for _ in range(20)]
    record("one_provider_after_withdrawal",
           all(r["status"] == 200 and r["provider"] == "proof-a" and r["checks"] for r in deterministic),
           "20 requests all attributed to proof-a after 2-to-1")


def cleanup():
    global MODEL_CHANGED, CONFIG_CHANGED
    capture_providers("before-fixture-cleanup")
    try:
        resume_controller()
    except Exception as e:
        RESULT["cleanup_failure"] = True
        (EVIDENCE / "controller-resume-error.txt").write_text(str(e) + "\n")
    if MODEL_CHANGED and MODEL_BEFORE:
        try:
            original_refs = MODEL_BEFORE.get("spec", {}).get("externalProviderRefs")
            patch_object(NAMESPACE, "externalmodel", MODEL,
                         {"spec": {"externalProviderRefs": original_refs}})
            wait_model()
            MODEL_CHANGED = False
        except Exception as e:
            RESULT["cleanup_failure"] = True
            (EVIDENCE / "model-restore-error.txt").write_text(str(e) + "\n")
    if CONFIG_BEFORE:
        try:
            before = json.loads(CONFIG_BEFORE)
            current = json.loads(kubectl("-n", NAMESPACE, "get", "configmap", CONFIG_MAP, "-o", "json").stdout)
            if current.get("data", {}).get("extproc.yaml") != before.get("data", {}).get("extproc.yaml"):
                CONFIG_CHANGED = True
                patch_object(NAMESPACE, "configmap", CONFIG_MAP,
                             {"data": {"extproc.yaml": before.get("data", {}).get("extproc.yaml", "")}})
                kubectl("-n", NAMESPACE, "rollout", "restart", f"deployment/{DEPLOYMENT}")
                kubectl("-n", NAMESPACE, "rollout", "status", f"deployment/{DEPLOYMENT}", "--timeout=180s")
            CONFIG_CHANGED = False
        except Exception as e:
            RESULT["cleanup_failure"] = True
            (EVIDENCE / "config-restore-error.txt").write_text(str(e) + "\n")
    capture_providers("after-model-restore")
    try:
        restore_correlated_gateway_access_logs()
    except Exception as e:
        RESULT["cleanup_failure"] = True
        (EVIDENCE / "istio-access-log-restore-error.txt").write_text(str(e) + "\n")
    if MODEL_CHANGED or CONFIG_CHANGED:
        RESULT["cleanup_failure"] = True
        (EVIDENCE / "cleanup.txt").write_text("provider proof resources retained; restore incomplete\n")
    elif (not MAIN_FAILURE and RESULT["assertions"] and all(a["result"] == "PASS" for a in RESULT["assertions"])
          and not RESULT.get("transition_failure")):
        kubectl("-n", NAMESPACE, "delete", "externalprovider", "provider-proof-a", "provider-proof-b",
                "--ignore-not-found", "--wait=true", check=False)
        kubectl("-n", NAMESPACE, "delete", "secret", "provider-proof-a-credentials",
                "provider-proof-b-credentials", "--ignore-not-found", "--wait=true", check=False)
        if API_DB_EGRESS_POLICY:
            kubectl("-n", API_NAMESPACE, "delete", "networkpolicy", "provider-selection-api-to-fixture-postgres",
                    "--ignore-not-found", "--wait=true", check=False)
        (EVIDENCE / "cleanup.txt").write_text("provider conditions and events captured before fixture Secret and CR cleanup; Kind API-to-Postgres policy removed\n")
    else:
        (EVIDENCE / "cleanup.txt").write_text("provider proof resources preserved for diagnosis\n")
    if API_KEY_ID:
        try:
            run(["curl", "--noproxy", "*", "--cacert", str(EVIDENCE / "maas-api-ca.crt"),
                 "--http1.1", "--connect-timeout", "5", "--max-time", "20",
                 "--resolve", f"maas-api.maas-system.svc.cluster.local:{PORT}:127.0.0.1",
                 "-sS", "-o", "/dev/null", "-w", "%{http_code}",
                 "-H", "X-MaaS-Username: kind-user", "-H", "X-MaaS-Group: [\"system:authenticated\"]",
                 "-X", "DELETE", f"https://maas-api.maas-system.svc.cluster.local:{PORT}/v1/api-keys/{API_KEY_ID}"],
                check=False)
        except Exception:
            pass
    for proc, stream in PROCESSES:
        proc.terminate()
        proc.wait(timeout=5)
        stream.close()
    for path in (EVIDENCE / ".admin-curl.cfg",):
        if path.exists():
            path.unlink()
    try:
        capture_provenance("after")
    except Exception as e:
        RESULT["cleanup_failure"] = True
        (EVIDENCE / "provenance-after-error.txt").write_text(str(e) + "\n")
    RESULT["status"] = "PASS" if (
        all(a["result"] == "PASS" for a in RESULT["assertions"])
        and not MAIN_FAILURE
        and not RESULT.get("transition_failure")
        and not RESULT.get("cleanup_failure")
        and not MODEL_CHANGED and not CONFIG_CHANGED and not CONTROLLER_PAUSED
    ) else "FAIL"
    write_json(EVIDENCE / "results.json", RESULT)


if __name__ == "__main__":
    exit_code = 0
    try:
        main()
    except Exception as exc:
        MAIN_FAILURE = True
        exit_code = 1
        (EVIDENCE / "failure.txt").write_text(str(exc) + "\n")
        print(f"FAIL qualification: {exc}")
    finally:
        cleanup()
    if RESULT.get("transition_failure") or RESULT.get("cleanup_failure"):
        exit_code = 1
    raise SystemExit(exit_code)

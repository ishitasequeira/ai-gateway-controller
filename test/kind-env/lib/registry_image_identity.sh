#!/usr/bin/env bash

# Match a pushed image by its immutable registry digest and local runtime tag.
# Containerd may list both an OCI index digest and the resolved platform
# manifest digest, so callers must not assume either digest is first.
containerd_image_matches_pushed_ref() {
  local runtime_info=$1 expected_repo_digest=$2 expected_tag=$3
  [[ -n "$expected_repo_digest" && -n "$expected_tag" ]] || return 1
  jq -e --arg digest "$expected_repo_digest" --arg tag "$expected_tag" '
    ((.status.repoDigests // []) | index($digest)) != null and
    ((.status.repoTags // []) | index($tag)) != null
  ' <<<"$runtime_info" >/dev/null
}

image_reference_is_immutable_digest() {
  [[ "$1" =~ ^[^[:space:]]+@sha256:[0-9a-f]{64}$ ]]
}

extproc_image_mode_is_valid() {
  local mode=$1 image=$2 source_sha=$3 platform=$4 praxis_ai_repo=$5
  case "$mode" in
    source)
      [[ -n "$image" && "$image" != *@sha256:* ]]
      ;;
    published)
      image_reference_is_immutable_digest "$image" \
        && [[ "$source_sha" =~ ^[0-9a-f]{40}$ ]] \
        && [[ "$platform" == linux/amd64 || "$platform" == linux/arm64 ]] \
        && [[ -z "$praxis_ai_repo" ]]
      ;;
    *) return 1 ;;
  esac
}

containerd_image_has_exact_digest_reference() {
  local runtime_info=$1 requested_reference=$2
  image_reference_is_immutable_digest "$requested_reference" || return 1
  jq -e --arg ref "$requested_reference" '(.status.repoDigests // []) | index($ref) != null' \
    <<<"$runtime_info" >/dev/null
}

# A pod may report either the containerd config image ID or a pullable digest
# in imageID. Accept the latter only when that digest is present in the exact
# inspected runtime image's RepoDigests list.
containerd_pod_image_id_matches_runtime() {
  local runtime_info=$1 pod_image_id=$2
  jq -e --arg podImageID "$pod_image_id" '
    (.status.id // "") as $runtimeID |
    ($podImageID | sub("^[^:]+://"; "")) as $podID |
    if $runtimeID == "" or $podID == "" then false
    elif $runtimeID == $podID then true
    elif ($podID | contains("@sha256:")) then
      ($podID | split("@")[1]) as $podDigest |
      any(.status.repoDigests[]?; endswith("@" + $podDigest))
    else false end
  ' <<<"$runtime_info" >/dev/null
}

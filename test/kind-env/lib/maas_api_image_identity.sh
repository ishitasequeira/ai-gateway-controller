#!/usr/bin/env bash

# MaaS API Deployments share base selector labels; scope pod checks with the
# tenant-instance label from each Deployment's pod template as well.
maas_api_deployment_pod_selector() {
  jq -er '
    .spec.template.metadata.labels["maas.opendatahub.io/tenant-instance"] as $instance |
    if ($instance | type) != "string" or $instance == "" then
      error("MaaS API Deployment has no tenant-instance pod label")
    else
      .spec.selector.matchLabels + {"maas.opendatahub.io/tenant-instance": $instance}
      | to_entries | map(.key + "=" + .value) | join(",")
    end
  ' <<<"$1"
}

# Require a non-terminating, Running, Ready maas-api pod that requested this
# run's registry tag and whose runtime digest belongs to the pulled image. The
# expected pushed digest and the node's complete repoDigests list are recorded
# together by crictl inspecti. Containerd may report a prior digest alias in a
# pod's imageID when identical image content already has multiple repo digests.
maas_api_pods_have_exact_ready_image() {
  local pods_json=$1 expected_image=$2 expected_image_digest=$3 node_repo_digests=$4
  jq -e --arg image "$expected_image" --arg image_digest "$expected_image_digest" \
    --arg node_repo_digests "$node_repo_digests" '
    ($node_repo_digests | split(",") | map(select(length > 0))) as $digests |
    ($digests | index($image_digest)) != null and
    any(.items[]?;
      .metadata.deletionTimestamp == null and .status.phase == "Running" and
      any(.spec.containers[]?;
        .name == "maas-api" and .image == $image
      ) and
      any(.status.containerStatuses[]?;
        .name == "maas-api" and .ready == true and
        ((.imageID // "" | sub("^containerd://"; "")) as $pod_digest |
          ($digests | index($pod_digest)) != null)
      )
    )
  ' <<<"$pods_json" >/dev/null
}

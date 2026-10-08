#!/usr/bin/env bash
set -euo pipefail

script=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/provision.sh

early_status_line=$(grep -nF "cleanup-complete || \"\$MTC_STATUS\" == steady" "$script" | head -1 | cut -d: -f1)
early_controller_line=$(grep -nF 'apply_rendered 20-controller.yaml' "$script" | head -1 | cut -d: -f1)
model_fixture_line=$(grep -nF 'apply_rendered 40-model-fixtures.yaml' "$script" | head -1 | cut -d: -f1)
maas_fixture_line=$(grep -nF 'apply_rendered 50-maas-fixtures.yaml' "$script" | head -1 | cut -d: -f1)
late_status_line=$(grep -nF 'MTC_STEADY_DEADLINE=' "$script" | head -1 | cut -d: -f1)
late_snapshot_line=$(grep -nF 'maastenantconfig-after-controller-fixtures.json' "$script" | head -1 | cut -d: -f1)

for marker in early_status_line early_controller_line model_fixture_line maas_fixture_line late_status_line late_snapshot_line; do
  [[ -n "${!marker}" ]] || { echo "missing handoff-order marker: $marker" >&2; exit 1; }
done

(( early_status_line < early_controller_line )) || { echo "early cleanup-complete/steady gate must precede controller apply" >&2; exit 1; }
(( early_controller_line < model_fixture_line )) || { echo "controller must be applied before model fixtures" >&2; exit 1; }
(( model_fixture_line < maas_fixture_line )) || { echo "model fixtures must precede MaaS fixtures" >&2; exit 1; }
(( maas_fixture_line < late_status_line && late_status_line < late_snapshot_line )) || {
  echo "steady gate and snapshot must follow controller and ExternalModel fixtures" >&2
  exit 1
}
grep -qF "[[ \"\$MTC_STATUS\" == steady ]] && break" "$script" || {
  echo "post-fixture gate must require steady" >&2
  exit 1
}
grep -qF 'maastenantconfig-steady-timeout.json' "$script" || {
  echo "post-fixture steady timeout diagnostics are missing" >&2
  exit 1
}

echo 'OpenShift handoff ordering: PASS'

/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    20|Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tenant

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// configureExtProcGrpcRecvMsgSize sets GRPC_MAX_RECV_MSG_SIZE on ExtProc
// Deployments to maxBodyBytes + framing headroom so tonic rejects oversized
// ExtProc messages before the process holds a full decode buffer.
func configureExtProcGrpcRecvMsgSize(resources []unstructured.Unstructured, maxBodyBytes int64) error {
	if maxBodyBytes < 1 {
		return fmt.Errorf("extproc max body bytes must be >= 1, got %d", maxBodyBytes)
	}
	recv := maxBodyBytes + grpcRecvFramingHeadroomBytes
	value := strconv.FormatInt(recv, 10)
	patched := 0
	for i := range resources {
		dep := &resources[i]
		if dep.GetKind() != "Deployment" || !isExtProcDeployment(dep.GetName()) {
			continue
		}
		if err := setContainerEnvValue(dep, grpcMaxRecvMsgSizeEnv, value); err != nil {
			return fmt.Errorf("deployment %s/%s: %w", dep.GetNamespace(), dep.GetName(), err)
		}
		patched++
	}
	if patched == 0 {
		return fmt.Errorf("no ExtProc Deployments found to apply GRPC_MAX_RECV_MSG_SIZE=%s", value)
	}
	return nil
}

func isExtProcDeployment(name string) bool {
	return name == PayloadProcessingName ||
		name == PayloadPreProcessingName ||
		strings.HasPrefix(name, PayloadProcessingName+"-") ||
		strings.HasPrefix(name, PayloadPreProcessingName+"-") ||
		strings.HasPrefix(name, PayloadProcessingExternalModelName)
}

func setContainerEnvValue(dep *unstructured.Unstructured, envName, value string) error {
	containers, found, err := unstructured.NestedSlice(dep.Object, "spec", "template", "spec", "containers")
	if err != nil {
		return fmt.Errorf("read containers: %w", err)
	}
	if !found || len(containers) == 0 {
		return errors.New("containers not found")
	}
	container, ok := containers[0].(map[string]any)
	if !ok {
		return errors.New("container[0] is not an object")
	}
	env, _, err := unstructured.NestedSlice(container, "env")
	if err != nil {
		return fmt.Errorf("read env: %w", err)
	}
	updated := false
	for i, raw := range env {
		entry, ok := raw.(map[string]any)
		if !ok || entry["name"] != envName {
			continue
		}
		entry["value"] = value
		delete(entry, "valueFrom")
		env[i] = entry
		updated = true
		break
	}
	if !updated {
		env = append(env, map[string]any{"name": envName, "value": value})
	}
	container["env"] = env
	containers[0] = container
	if err := unstructured.SetNestedSlice(dep.Object, containers, "spec", "template", "spec", "containers"); err != nil {
		return fmt.Errorf("write containers: %w", err)
	}
	return nil
}

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
	"strconv"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestConfigureExtProcGrpcRecvMsgSize(t *testing.T) {
	resources := []unstructured.Unstructured{
		{
			Object: map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata":   map[string]any{"name": PayloadPreProcessingName},
				"spec": map[string]any{
					"template": map[string]any{
						"spec": map[string]any{
							"containers": []any{
								map[string]any{
									"name": PayloadPreProcessingName,
									"env": []any{
										map[string]any{"name": grpcMaxRecvMsgSizeEnv, "value": "11Mi"},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			Object: map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata":   map[string]any{"name": "unrelated"},
				"spec": map[string]any{
					"template": map[string]any{
						"spec": map[string]any{
							"containers": []any{map[string]any{"name": "other"}},
						},
					},
				},
			},
		},
	}

	const wantBody int64 = 20 * 1024 * 1024
	if err := configureExtProcGrpcRecvMsgSize(resources, wantBody); err != nil {
		t.Fatalf("configureExtProcGrpcRecvMsgSize: %v", err)
	}

	wantRecv := strconv.FormatInt(wantBody+grpcRecvFramingHeadroomBytes, 10)
	containers, _, _ := unstructured.NestedSlice(resources[0].Object, "spec", "template", "spec", "containers")
	container, ok := containers[0].(map[string]any)
	if !ok {
		t.Fatalf("container[0] = %T, want object", containers[0])
	}
	env, _ := container["env"].([]any)
	found := false
	for _, raw := range env {
		entry, ok := raw.(map[string]any)
		if !ok || entry["name"] != grpcMaxRecvMsgSizeEnv {
			continue
		}
		if entry["value"] != wantRecv {
			t.Fatalf("GRPC_MAX_RECV_MSG_SIZE = %v, want %s", entry["value"], wantRecv)
		}
		found = true
	}
	if !found {
		t.Fatal("GRPC_MAX_RECV_MSG_SIZE missing after configure")
	}

	otherContainers, _, _ := unstructured.NestedSlice(resources[1].Object, "spec", "template", "spec", "containers")
	otherContainer, ok := otherContainers[0].(map[string]any)
	if !ok {
		t.Fatalf("other container[0] = %T, want object", otherContainers[0])
	}
	if _, hasEnv := otherContainer["env"]; hasEnv {
		t.Fatalf("unrelated Deployment env mutated: %#v", otherContainer["env"])
	}
}

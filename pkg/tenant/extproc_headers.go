/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tenant

// guardedRequestHeaders lists headers that must not reach an upstream model.
// It is a superset of maas-controller's deny-client-identity-headers AuthPolicy
// predicate list because Authorino injects trusted values under those names
// after authentication. Keep both lists in sync when adding an x-maas-* header.
var guardedRequestHeaders = []string{
	"authorization",
	"x-api-key",
	"x-origin-cluster",
	"x-maas-username",
	"x-maas-username-oc",
	"x-maas-username-token",
	"x-maas-group",
	"x-maas-group-oc",
	"x-maas-group-token",
	"x-maas-keyname",
	"x-maas-key-id",
	"x-maas-subscription",
	"x-maas-subscription-rate-limit-id",
	"x-maas-cost-center",
	"x-maas-organization-id",
	"x-maas-identity",
	"x-maas-admin-scope",
	"x-maas-tenant",
	"x-maas-invocation-mode",
	"x-maas-policy-revision",
	"x-maas-responses-owner",
	"x-maas-return-all-models",
	"x-maas-model",
}

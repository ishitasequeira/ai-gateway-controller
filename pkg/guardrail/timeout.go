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

package guardrail

import "time"

// ProviderTimeoutTooFine reports a positive provider timeout Praxis cannot
// represent. The ExtProc config records timeout_ms in whole milliseconds, and
// Praxis rejects zero, so a duration below 1ms would be rendered as
// timeout_ms: 0 and the post-auth rollout would stall.
func ProviderTimeoutTooFine(d time.Duration) bool {
	return d > 0 && d < time.Millisecond
}

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

import (
	"testing"
	"time"
)

func TestProviderTimeoutTooFine(t *testing.T) {
	cases := []struct {
		name string
		d    time.Duration
		want bool
	}{
		{name: "zero", d: 0},
		{name: "negative", d: -time.Millisecond},
		{name: "500us", d: 500 * time.Microsecond, want: true},
		{name: "999us", d: 999 * time.Microsecond, want: true},
		{name: "1ms", d: time.Millisecond},
		{name: "2s", d: 2 * time.Second},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ProviderTimeoutTooFine(c.d); got != c.want {
				t.Fatalf("ProviderTimeoutTooFine(%s) = %v, want %v", c.d, got, c.want)
			}
		})
	}
}

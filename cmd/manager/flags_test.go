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

package main

import (
	"flag"
	"reflect"
	"testing"
)

// newTestFlagSet registers the flags this build understands. --metrics-secure
// is deliberately absent: it is the flag RHAI-15815 crashlooped on, so every
// case below exercises it as the unknown one.
func newTestFlagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("manager", flag.ContinueOnError)
	fs.Bool("leader-elect", false, "")
	fs.String("image", "", "")
	fs.String("metrics-bind-address", ":8443", "")
	fs.Bool("skip-network-policy", false, "")
	return fs
}

func TestDropUnknownFlagsKeepsKnownFlags(t *testing.T) {
	args := []string{"--leader-elect", "--image=repo/extproc:tag", "--metrics-bind-address=:9090"}

	kept, dropped := dropUnknownFlags(newTestFlagSet(), args)

	assertEqual(t, "kept", kept, args)
	assertEqual(t, "dropped", dropped, []string{})
}

func TestDropUnknownFlagsDropsUnknownFlagWithInlineValue(t *testing.T) {
	// The exact RHAI-15815 command line.
	args := []string{"--leader-elect", "--metrics-secure=true", "--image=repo/extproc:tag"}

	kept, dropped := dropUnknownFlags(newTestFlagSet(), args)

	assertEqual(t, "kept", kept, []string{"--leader-elect", "--image=repo/extproc:tag"})
	assertEqual(t, "dropped", dropped, []string{"--metrics-secure"})
}

func TestDropUnknownFlagsKeepsLaterKnownFlagAfterDroppedOne(t *testing.T) {
	// The regression ContinueOnError alone would introduce: parsing stops at an
	// unknown flag, so a later --image must still reach the kept slice.
	args := []string{"--metrics-secure=true", "--image=repo/extproc:tag"}

	kept, _ := dropUnknownFlags(newTestFlagSet(), args)

	assertEqual(t, "kept", kept, []string{"--image=repo/extproc:tag"})
}

func TestDropUnknownFlagsConsumesSeparatedUnknownValue(t *testing.T) {
	args := []string{"--metrics-secure", "true", "--leader-elect"}

	kept, dropped := dropUnknownFlags(newTestFlagSet(), args)

	assertEqual(t, "kept", kept, []string{"--leader-elect"})
	assertEqual(t, "dropped", dropped, []string{"--metrics-secure"})
}

func TestDropUnknownFlagsDoesNotSwallowFollowingFlag(t *testing.T) {
	// A separated value is only assumed when the next token is not itself a
	// flag; otherwise a real flag would vanish along with the unknown one.
	args := []string{"--metrics-secure", "--leader-elect"}

	kept, dropped := dropUnknownFlags(newTestFlagSet(), args)

	assertEqual(t, "kept", kept, []string{"--leader-elect"})
	assertEqual(t, "dropped", dropped, []string{"--metrics-secure"})
}

func TestDropUnknownFlagsKeepsSeparatedKnownValue(t *testing.T) {
	args := []string{"--image", "repo/extproc:tag", "--leader-elect"}

	kept, dropped := dropUnknownFlags(newTestFlagSet(), args)

	assertEqual(t, "kept", kept, []string{"--image", "repo/extproc:tag", "--leader-elect"})
	assertEqual(t, "dropped", dropped, []string{})
}

func TestDropUnknownFlagsDoesNotConsumeValueForBoolFlag(t *testing.T) {
	// Bool flags never take a separate value, so the next token is the next arg.
	args := []string{"--leader-elect", "--image=repo/extproc:tag"}

	kept, _ := dropUnknownFlags(newTestFlagSet(), args)

	assertEqual(t, "kept", kept, args)
}

func TestDropUnknownFlagsKeepsHelpFlags(t *testing.T) {
	for _, help := range []string{"-h", "--help", "-help"} {
		kept, dropped := dropUnknownFlags(newTestFlagSet(), []string{help})
		assertEqual(t, "kept for "+help, kept, []string{help})
		assertEqual(t, "dropped for "+help, dropped, []string{})
	}
}

func TestDropUnknownFlagsKeepsPositionalTailVerbatim(t *testing.T) {
	// flag.Parse stops interpreting flags at the first positional argument, so
	// anything after it is never a flag — including a string that looks like one.
	args := []string{"--leader-elect", "leftover", "--metrics-secure=true"}

	kept, dropped := dropUnknownFlags(newTestFlagSet(), args)

	assertEqual(t, "kept", kept, args)
	assertEqual(t, "dropped", dropped, []string{})
}

func TestDropUnknownFlagsStopsAtTerminator(t *testing.T) {
	args := []string{"--metrics-secure=true", "--", "--also-unknown"}

	kept, dropped := dropUnknownFlags(newTestFlagSet(), args)

	assertEqual(t, "kept", kept, []string{"--", "--also-unknown"})
	assertEqual(t, "dropped", dropped, []string{"--metrics-secure"})
}

func TestDropUnknownFlagsDoesNotConsumeBoundary(t *testing.T) {
	// "--" and a lone "-" end flag interpretation. Consuming either as an
	// unknown flag's value would silently move a later flag into the positional
	// tail, where flag.Parse no longer applies it.
	for _, boundary := range []string{"--", "-"} {
		args := []string{"--metrics-secure", boundary, "--image=repo/extproc:tag"}

		kept, dropped := dropUnknownFlags(newTestFlagSet(), args)

		assertEqual(t, "kept for "+boundary, kept, []string{boundary, "--image=repo/extproc:tag"})
		assertEqual(t, "dropped for "+boundary, dropped, []string{"--metrics-secure"})
	}
}

func TestDropUnknownFlagsReportsNamesNotValues(t *testing.T) {
	// dropped is logged, so it must never carry a value that could be a
	// credential (CWE-532).
	cases := []struct {
		name string
		args []string
	}{
		{name: "inline", args: []string{"--api-key=s3cret"}},
		{name: "separated", args: []string{"--api-key", "s3cret"}},
	}

	for _, tc := range cases {
		_, dropped := dropUnknownFlags(newTestFlagSet(), tc.args)
		assertEqual(t, "dropped for "+tc.name, dropped, []string{"--api-key"})
	}
}

func TestDropUnknownFlagsHandlesSingleDashSpellings(t *testing.T) {
	args := []string{"-leader-elect", "-metrics-secure=true", "-image=repo/extproc:tag"}

	kept, dropped := dropUnknownFlags(newTestFlagSet(), args)

	assertEqual(t, "kept", kept, []string{"-leader-elect", "-image=repo/extproc:tag"})
	assertEqual(t, "dropped", dropped, []string{"--metrics-secure"})
}

func TestDropUnknownFlagsEmptyArgs(t *testing.T) {
	kept, dropped := dropUnknownFlags(newTestFlagSet(), nil)

	assertEqual(t, "kept", kept, []string{})
	assertEqual(t, "dropped", dropped, []string{})
}

func TestDropUnknownFlagsOutputParsesCleanly(t *testing.T) {
	// End to end: a manifest/newer flag must not prevent the surviving flags
	// from being applied.
	fs := newTestFlagSet()
	args, dropped := dropUnknownFlags(fs, []string{
		"--leader-elect", "--metrics-secure=true", "--image=repo/extproc:tag",
	})
	if len(dropped) != 1 {
		t.Fatalf("dropped = %v, want one flag", dropped)
	}
	if err := fs.Parse(args); err != nil {
		t.Fatalf("Parse(%v) = %v, want nil", args, err)
	}
	if got := fs.Lookup("leader-elect").Value.String(); got != "true" {
		t.Fatalf("leader-elect = %q, want true", got)
	}
	if got := fs.Lookup("image").Value.String(); got != "repo/extproc:tag" {
		t.Fatalf("image = %q, want repo/extproc:tag", got)
	}
}

func assertEqual(t *testing.T, what string, got, want []string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s = %#v, want %#v", what, got, want)
	}
}

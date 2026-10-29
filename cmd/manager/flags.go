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
	"strings"
)

// helpFlagNames are handled by the flag package itself and are never
// registered on a FlagSet, so Lookup cannot resolve them. They are kept
// explicitly — dropping them would turn a request for usage into a silent
// start-up.
var helpFlagNames = map[string]bool{"h": true, "help": true}

// dropUnknownFlags splits args into the slice fs can parse and the names of
// the flags fs does not define.
//
// dropped holds flag names only, normalised to the "--name" spelling, never a
// flag's value: an unknown flag's value is unknown by definition and may be a
// credential, and this list is written to the log (CWE-532).
//
// This exists because the command line is authored by a different repository
// on a different release cadence than the binary that consumes it: the
// operator vendors this Deployment's args at a pinned commit and pairs them
// with an image built from another branch, so a manifest newer than its image
// is a normal upgrade state (RHAI-15815). Go's parser stops at the first
// unknown flag and an ExitOnError FlagSet then exits, so a single unrecognised
// arg crashloops the pod. Filtering first — rather than switching to
// ContinueOnError — is deliberate: ContinueOnError abandons the rest of the
// command line at the offending flag, so a later --image would be silently
// lost.
//
// Everything from the first positional argument onward is kept verbatim,
// matching flag.Parse, which stops interpreting flags there.
func dropUnknownFlags(fs *flag.FlagSet, args []string) (kept, dropped []string) {
	kept = make([]string, 0, len(args))
	dropped = []string{}

	for i := 0; i < len(args); i++ {
		arg := args[i]

		// Non-flags, "--" and "-" all end flag interpretation. Keeping the tail
		// verbatim matches flag.Parse, which stops there and leaves the rest
		// to the caller.
		if !isFlagArg(arg) {
			kept = append(kept, args[i:]...)
			break
		}

		name, hasInlineValue := splitFlagArg(arg)
		f := fs.Lookup(name)

		switch {
		case f != nil:
			kept = append(kept, arg)
			// A non-bool flag without "=value" takes the next token as its
			// value, whatever it looks like — this mirrors flag.Parse.
			if !hasInlineValue && !isBoolFlag(f) && i+1 < len(args) {
				i++
				kept = append(kept, args[i])
			}
		case helpFlagNames[name]:
			kept = append(kept, arg)
		default:
			dropped = append(dropped, "--"+name)
			// An unknown flag's type is unknowable, so a following bare token
			// is taken as its value. A token that looks like a flag is not:
			// it is far more likely the next arg, and swallowing it would
			// drop a flag this build does understand. The same goes for a
			// parsing boundary, which can never be a value.
			if !hasInlineValue && i+1 < len(args) &&
				!isFlagArg(args[i+1]) && !isParsingBoundary(args[i+1]) {
				i++
			}
		}
	}

	return kept, dropped
}

// isFlagArg reports whether arg is a flag rather than a positional argument.
// Parsing boundaries (see isParsingBoundary) are not flags.
func isFlagArg(arg string) bool {
	return len(arg) > 1 && arg[0] == '-' && arg != "--"
}

// isParsingBoundary reports whether arg ends flag interpretation rather than
// being a flag or a value: "--" terminates parsing, and a lone "-" is
// positional by convention. flag.Parse stops at either, so dropUnknownFlags
// must keep them and must never consume one as a value.
func isParsingBoundary(arg string) bool {
	return arg == "--" || arg == "-"
}

// splitFlagArg returns the flag name in arg and whether a value was attached
// with "=". Both "-flag" and "--flag" spellings are accepted, as in flag.Parse.
func splitFlagArg(arg string) (name string, hasValue bool) {
	name, _, hasValue = strings.Cut(strings.TrimPrefix(arg[1:], "-"), "=")
	return name, hasValue
}

// isBoolFlag reports whether f is a boolean flag, which never consumes the
// following token as its value.
func isBoolFlag(f *flag.Flag) bool {
	bf, ok := f.Value.(interface{ IsBoolFlag() bool })
	return ok && bf.IsBoolFlag()
}

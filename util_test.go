// Copyright 2014, Orchestrate.IO, Inc.

package gorc

import (
	"flag"

	"github.com/liquidgecka/testlib"
	"github.com/orchestrate-io/dvr"
)

// This is the auth token  that will be used for queries against Orchestrate.
// If the dvr library is in recording or pass through mode then this is
// the real token that will be used against Orchestrate. If it is in
// replay mode then this will be a fake, obfuscated token that exposes
// no real security credentials.
var AuthToken string

// This is the obviously obfuscated token that is used when the dvr library
// is in recording mode.
var ObfuscatedAuthToken = "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE"

func init() {
	// We need to ensure that all of our testing is done with the dvr library
	// firmly in place. This allows us to quickly replay existing queries
	// rather than constantly having to go back to Orchestrate in testing.
	//
	// This also allows contributors to submit pull requests and such without
	// having to expose real auth tokens..

	// First put the Obfuscator in place.
	dvr.Obfuscator = dvr.BasicAuthObfuscator(ObfuscatedAuthToken, "")

	// Ensure that replay mode is default in the dvr Library.
	dvr.DefaultReplay = true

	// Next inject a dvr.RoundTripper into the DefaultTransport so that all
	// testing queries default to using the RoundTripper.
	DefaultTransport = dvr.NewRoundTripper(DefaultTransport)

	// And lastly insert a flag that allows the user to provide an auth token
	// via the command line.
	flag.StringVar(&AuthToken, "auth_token", "",
		"The Orchestrate auth token for a given application.")
}

// This call will create a client and ensure that all the arguments needed
// have been passed.
func cleanTestingClient(T *testlib.T) *Client {
	if dvr.IsReplay() {
		return NewClient(ObfuscatedAuthToken)
	} else {
		return NewClient(AuthToken)
	}
}

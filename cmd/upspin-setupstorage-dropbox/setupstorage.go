// Copyright 2017 The Upspin Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The upspin-setupstorage-dropbox command is an external upspin subcommand that
// executes the second step in establishing an upspinserver for Dropbox.
// Run upspin setupstorage-dropbox -help for more information.
package main // import "dropbox.upspin.io/cmd/upspin-setupstorage-dropbox"

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"golang.org/x/oauth2"

	"upspin.io/subcmd"
)

type state struct {
	*subcmd.State
}

const help = `
Setupstorage-dropbox is the second step in establishing an upspinserver.
It sets up Dropbox for your Upspin installation. You may skip this step
if you wish to store Upspin data on your server's local disk.
The first step is 'setupdomain' and the final step is 'setupserver'.
Setupstorage-dropbox creates the server configuration files in $where/$domain/ to use
the specified authorization code to access your Dropbox.

Before running this command, you must obtain an authorization code:

1. Go to https://www.dropbox.com/oauth2/authorize?client_id=wt1281n3q768jj3&response_type=code&state=state
2. Click "Allow" (you might have to log in first).
3. Copy the authorization code
4. Run setupstorage-dropbox -domain <domain.tld> <authorization_code>
`

func main() {
	const name = "setupstorage-dropbox"

	log.SetFlags(0)
	log.SetPrefix("upspin setupstorage-dropbox: ")

	s := &state{
		State: subcmd.NewState(name),
	}

	where := flag.String("where", filepath.Join(os.Getenv("HOME"), "upspin", "deploy"), "`directory` to store private configuration files")
	domain := flag.String("domain", "", "domain `name` for this Upspin installation")

	s.ParseFlags(flag.CommandLine, os.Args[1:], help,
		"setupstorage-dropbox -domain=<name> <authorization_code>")
	if flag.NArg() != 1 {
		s.Exitf("a valid authorization code must be provided")
	}
	if *domain == "" {
		s.Exitf("the -domain  flags must be provided")
	}

	authCode := flag.Arg(0)

	cfgPath := filepath.Join(*where, *domain)
	cfg := s.ReadServerConfig(cfgPath)

	cfg.StoreConfig = []string{
		"backend=DROPBOX",
		"token=" + s.token(authCode),
	}
	s.WriteServerConfig(cfgPath, cfg)

	fmt.Fprintf(os.Stderr, "You should now deploy the upspinserver binary and run 'upspin setupserver'.\n")

	s.ExitNow()
}

func (s *state) token(code string) string {
	conf := &oauth2.Config{
		ClientID:     "wt1281n3q768jj3",
		ClientSecret: "blk944sx4oyf6aq",
		Endpoint: oauth2.Endpoint{
			AuthURL:  "https://www.dropbox.com/oauth2/authorize",
			TokenURL: "https://api.dropboxapi.com/oauth2/token",
		},
	}
	conf.AuthCodeURL("state")

	token, err := conf.Exchange(oauth2.NoContext, code)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error in fetching oauth2 token: %v.\n", err)
	}

	return token.AccessToken
}

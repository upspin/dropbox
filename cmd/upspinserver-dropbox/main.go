// Copyright 2017 The Upspin Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Command upspinserver-dropbox is a combined DirServer and StoreServer for use on
// stand-alone machines. It provides the production implementations of the
// dir and store servers (dir/server and store/server) with support for storage
// in Dropbox.
package main // import "dropbox.upspin.io/cmd/upspinserver-dropbox"

import (
	"upspin.io/cloud/https"
	"upspin.io/serverutil/upspinserver"

	// Storage on Dropbox.
	_ "dropbox.upspin.io/cloud/storage/dropbox"
)

func main() {
	ready := upspinserver.Main()
	https.ListenAndServe(ready, https.OptionsFromFlags())
}

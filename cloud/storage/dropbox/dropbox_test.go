// Copyright 2016 The Upspin Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dropbox

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"upspin.io/cloud/storage"
)

var (
	client      storage.Storage
	testDataStr = fmt.Sprintf("This is test at %v", time.Now())
	testData    = []byte(testDataStr)
	fileName    = fmt.Sprintf("test-file-%d", time.Now().Second())

	authCode   = flag.String("code", "", "dropbox authentication code")
	useDropbox = flag.Bool("use_dropbox", false, "enable to run dropbox tests; requires authentication code")
)

// This is more of a regression test as it uses the running cloud
// storage in prod. However, since Dropbox is always available, we accept
// to rely on it.
func TestPutGetAndDownload(t *testing.T) {
	err := client.Put(fileName, testData)
	if err != nil {
		t.Fatal(err)
	}
	data, err := client.Download(fileName)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != testDataStr {
		t.Errorf("Expected %q got %q", testDataStr, string(data))
	}
	// Check that Download yields the same data
	bytes, err := client.Download(fileName)
	if err != nil {
		t.Fatal(err)
	}
	if string(bytes) != testDataStr {
		t.Errorf("Expected %q got %q", testDataStr, string(bytes))
	}
}

func TestDelete(t *testing.T) {
	err := client.Put(fileName, testData)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Delete(fileName)
	if err != nil {
		t.Fatalf("Expected no errors, got %v", err)
	}
	// Test the side effect after Delete.
	_, err = client.Download(fileName)
	if err == nil {
		t.Fatal("Expected an error, but got none")
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	if !*useDropbox {
		log.Printf(`
cloud/storage/dropbox: skipping test as it requires Dropbox access. To enable this test,
on the first run get an authentication code by visiting:

https://www.dropbox.com/oauth2/authorize?client_id=ufhy41x7g4obzqz&response_type=code

Copy the code and pass it by the -code flag. This will get an oAuth2 access token, store
it and reuse it in successive test calls.

`)
		os.Exit(0)
	}

	t, err := token()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error in getting oauth2 token: %v.\n", err)
	}

	// Create client that writes to your Dropbox.
	client, err = storage.Dial("Dropbox",
		storage.WithKeyValue("token", t))
	if err != nil {
		log.Fatalf("cloud/storage/dropbox: couldn't set up client: %v", err)
	}

	code := m.Run()

	os.Exit(code)
}

func token() (string, error) {
	tokenFile := path.Join(os.TempDir(), "upspin-test-token")

	token, _ := ioutil.ReadFile(tokenFile)
	if err == nil {
		return string(token), nil
	}

	conf := &oauth2.Config{
		ClientID:     "ufhy41x7g4obzqz",
		ClientSecret: "vuhgmucmxm93dp5",
		Endpoint: oauth2.Endpoint{
			AuthURL:  "https://www.dropbox.com/oauth2/authorize",
			TokenURL: "https://api.dropboxapi.com/oauth2/token",
		},
	}

	tok, err := conf.Exchange(oauth2.NoContext, *authCode)
	if err != nil {
		return "", err
	}

	if err := ioutil.WriteFile(tokenFile, []byte(tok.AccessToken), 0600); err != nil {
		return "", err
	}

	return tok.AccessToken, nil
}

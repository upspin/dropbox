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
	"upspin.io/upspin"
)

var (
	client      storage.Storage
	testDataStr = fmt.Sprintf("This is test at %v", time.Now())
	testData    = []byte(testDataStr)
	fileName    = fmt.Sprintf("test-file-%d", time.Now().Second())

	authCode   = flag.String("code", "", "dropbox authentication code")
	useDropbox = flag.Bool("use_dropbox", false, "enable to run dropbox tests; requires authentication code")
)

func TestList(t *testing.T) {
	ls, ok := client.(storage.Lister)
	if !ok {
		t.Fatal("impl does not provide List method")
	}

	refs, next, err := ls.List("")
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 0 {
		t.Errorf("list returned %d refs, want 0", len(refs))
	}
	if next != "" {
		t.Errorf("list returned page token %q, want empty", next)
	}

	// Test pagination by reducing the results per page to 2.
	oldMaxResults := maxResults
	defer func() { maxResults = oldMaxResults }()
	maxResults = 2

	const nFiles = 6 // Must be evenly divisible by maxResults.
	for i := 0; i < nFiles; i++ {
		fn := fmt.Sprintf("test-%d", i)
		err = client.Put(fn, testData)
		if err != nil {
			t.Fatal(err)
		}
		// clean up
		defer client.Delete(fn)
	}

	seen := make(map[upspin.Reference]bool)
	for i := 0; i < nFiles/2; i++ {
		refs, next, err = ls.List(next)
		if err != nil {
			t.Fatal(err)
		}
		if len(refs) != 2 {
			t.Errorf("got %d refs, want 2", len(refs))
		}
		if i == nFiles/2-1 {
			if next != "" {
				t.Errorf("got page token %q, want empty", next)
			}
		} else if next == "" {
			t.Error("got empty page token, want non-empty")
		}
		for _, ref := range refs {
			if seen[ref.Ref] {
				t.Errorf("saw duplicate ref %q", ref.Ref)
			}
			seen[ref.Ref] = true
			if got, want := ref.Size, int64(len(testData)); got != want {
				t.Errorf("ref %q has size %d, want %d", ref.Ref, got, want)
			}
		}
	}
}

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

	token, err := ioutil.ReadFile(tokenFile)
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

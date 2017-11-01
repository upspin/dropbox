// Copyright 2016 The Upspin Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package dropbox implements a storage backend that saves data to a User
// Dropbox.
package dropbox // import "dropbox.upspin.io/cloud/storage/dropbox"

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"upspin.io/cloud/storage"
	"upspin.io/errors"
	"upspin.io/upspin"
)

// apiToken is the key for the dial options in the storage.Storage interface.
const apiToken = "token"

// New initializes a Storage implementation that stores data to Dropbox.
func New(opts *storage.Opts) (storage.Storage, error) {
	const op = "cloud/storage/dropbox.New"

	tok, ok := opts.Opts[apiToken]
	if !ok {
		return nil, errors.E(op, errors.Invalid, errors.Errorf("%q option is required", apiToken))
	}

	return &dropboxImpl{
		client: http.DefaultClient,
		token:  tok,
	}, nil
}

func init() {
	storage.Register("Dropbox", New)
}

// dropboxImpl is an implementation of Storage that connects to a Dropbox backend.
type dropboxImpl struct {
	client *http.Client
	token  string
}

// Guarantee we implement the Storage interface
var _ storage.Storage = (*dropboxImpl)(nil)

// LinkBase implements Storage.
func (d *dropboxImpl) LinkBase() (base string, err error) {
	return "", upspin.ErrNotSupported
}

// Download implements Storage.
func (d *dropboxImpl) Download(ref string) ([]byte, error) {
	const op = "cloud/storage/dropbox.Download"

	arg, _ := json.Marshal(struct {
		Path string `json:"path"`
	}{"/" + ref})

	req, err := d.newRequest("https://content.dropboxapi.com/2/files/download", nil, string(arg))
	if err != nil {
		return nil, errors.E(op, errors.Other, err)
	}

	data, err := d.doRequest(req)
	if err != nil {
		if derr, ok := err.(DropboxAPIError); ok && derr.StatusCode() == 404 {
			return nil, errors.E(op, errors.NotExist, derr)
		}

		return nil, errors.E(op, errors.IO, err)
	}
	return data, nil
}

// Put implements Storage.
func (d *dropboxImpl) Put(ref string, contents []byte) error {
	const op = "cloud/storage/dropbox.Put"

	arg, _ := json.Marshal(struct {
		Path   string `json:"path"`
		Mode   string `json:"mode"`
		Rename bool   `json:"autorename"`
		Mute   bool   `json:"mute"`
	}{
		"/" + ref,
		"overwrite",
		true,
		false,
	})

	body := bytes.NewReader(contents)

	// The endpoint has an upload limit of 150 MB which is fine for the Upspin
	// default blocksize. If the Upspin blocksize is set larger than this limit,
	// the "upload_session/start" endpoint should be used.
	req, err := d.newRequest("https://content.dropboxapi.com/2/files/upload", body, string(arg))
	if err != nil {
		return errors.E(op, errors.Other, err)
	}

	_, err = d.doRequest(req)
	if err != nil {
		return errors.E(op, errors.IO, err)
	}

	return nil
}

// Delete implements Storage.
func (d *dropboxImpl) Delete(ref string) error {
	const op = "cloud/storage/dropbox.Delete"

	arg, _ := json.Marshal(struct {
		Path string `json:"path"`
	}{"/" + ref})

	body := bytes.NewReader(arg)

	req, err := d.newRequest("https://api.dropboxapi.com/2/files/delete_v2", body, "")
	if err != nil {
		return errors.E(op, errors.Other, err)
	}
	req.Header.Set("Content-Type", "application/json")

	_, err = d.doRequest(req)
	if err != nil {
		return errors.E(op, errors.IO, err)
	}

	return nil
}

// Close implements Storage.
func (d *dropboxImpl) Close() {
	// not yet implemented
}

func (d *dropboxImpl) newRequest(path string, body io.Reader, arg string) (*http.Request, error) {
	req, err := http.NewRequest("POST", path, body)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Authorization", "Bearer "+d.token)
	req.Header.Add("Content-Type", "application/octet-stream")

	if arg != "" {
		req.Header.Add("Dropbox-API-Arg", arg)
	}

	return req, nil
}

func (d *dropboxImpl) doRequest(req *http.Request) ([]byte, error) {
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusConflict {
		var dbxErr DropboxAPIError
		err := json.Unmarshal(body, &dbxErr)
		if err != nil {
			return nil, err
		}

		return nil, dbxErr
	}

	if resp.StatusCode != 200 {
		return nil, errors.Errorf(resp.Status)
	}

	return body, nil
}

type DropboxAPIError struct {
	ErrorSummary string `json:"error_summary"`
}

func (e DropboxAPIError) StatusCode() int {
	if strings.Contains(e.ErrorSummary, "not_found") {
		return 404
	}

	return 0
}

func (e DropboxAPIError) Error() string {
	return e.ErrorSummary
}

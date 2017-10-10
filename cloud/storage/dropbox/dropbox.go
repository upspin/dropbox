// Copyright 2016 The Upspin Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package dropbox implements a storage backend that saves data to a User
// Dropbox.
package dropbox // import "dropbox.upspin.io/cloud/storage/dropbox"

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"upspin.io/cloud/storage"
	"upspin.io/errors"
	"upspin.io/upspin"
)

// Keys used for storing dial options.
const apiToken = "token"

// New initializes a Storage implementation that stores data to Dropbox.
func New(opts *storage.Opts) (storage.Storage, error) {
	const op = "cloud/storage/dropbox.New"

	tok, ok := opts.Opts[apiToken]
	if !ok {
		return nil, errors.E(op, errors.Invalid, errors.Errorf("%q option is required", apiToken))
	}

	return &dropboxImpl{
		client: &http.Client{},
		token:  tok,
	}, nil
}

func init() {
	storage.Register("DROPBOX", New)
}

// dropboxImpl is an implementation of Storage that connects a Dropbox backend.
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

	arg := fmt.Sprintf("{\"path\": \"/%s\"}", ref)

	req, err := http.NewRequest("POST", "https://content.dropboxapi.com/2/files/download", nil)
	req.Header.Add("Authorization", "Bearer "+d.token)
	req.Header.Add("Dropbox-API-Arg", arg)
	if err != nil {
		return nil, errors.E(op, errors.Other, err)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, errors.E(op, errors.Other, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, errors.E(op, errors.Other, errors.Errorf(
			"got an error from the endpoint: %s"), resp.Status)
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.E(op, errors.IO, err)
	}

	return data, nil
}

// Put implements Storage.
func (d *dropboxImpl) Put(ref string, contents []byte) error {
	const op = "cloud/storage/dropbox.Put"

	arg, err := json.Marshal(map[string]interface{}{
		"path":       "/" + ref,
		"mode":       "overwrite",
		"autorename": true,
		"mute":       false,
	})
	if err != nil {
		return err
	}

	body := bytes.NewReader(contents)

	// TODO(mazebuhu): This version of Put has a limitation on the file size
	// (150 MB). For larger files the "upload_session/start" endpoint should be
	// used.
	req, err := http.NewRequest("POST", "https://content.dropboxapi.com/2/files/upload", body)
	req.Header.Add("Authorization", "Bearer "+d.token)
	req.Header.Add("Dropbox-API-Arg", string(arg))
	req.Header.Add("Content-Type", "application/octet-stream")
	if err != nil {
		return errors.E(op, errors.Other, err)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return errors.E(op, errors.IO, err)
	}

	if resp.StatusCode != 200 {
		return errors.E(op, errors.Other, errors.Errorf(
			"got an error from the endpoint: %s"), resp.Status)
	}

	return nil
}

// Delete implements Storage.
func (d *dropboxImpl) Delete(ref string) error {
	const op = "cloud/storage/dropbox.Delete"

	arg := fmt.Sprintf("{\"path\": \"/%s\"}", ref)
	body := strings.NewReader(arg)

	req, err := http.NewRequest("POST", "https://api.dropboxapi.com/2/files/delete_v2", body)
	req.Header.Add("Authorization", "Bearer "+d.token)
	req.Header.Add("Content-Type", "application/json")
	if err != nil {
		return errors.E(op, errors.Other, err)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return errors.E(op, errors.IO, err)
	}

	if resp.StatusCode != 200 {
		return errors.E(op, errors.Other, errors.Errorf(
			"got an error from the endpoint: %s"), resp.Status)
	}

	return nil
}

// Close implements Storage.
func (d *dropboxImpl) Close() {
	// not yet implemented
}

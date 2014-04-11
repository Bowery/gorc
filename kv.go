// Copyright 2014, Orchestrate.IO, Inc.

package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strconv"
)

type KVResults struct {
	Count   uint64     `json:"count"`
	Results []KVResult `json:"results"`
	Next    string     `json:"next,omitempty"`
}

type KVResult struct {
	Path     Path            `json:"path"`
	RawValue json.RawMessage `json:"value"`
}

func (client *Client) Get(collection, key string) (*KVResult, error) {
	return client.GetPath(&Path{Collection: collection, Key: key})
}

func (client *Client) GetPath(path *Path) (*KVResult, error) {
	resp, err := client.doRequest("GET", path.trailingGetURI(), nil, nil)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, newError(resp)
	}

	// TODO: Check for a content-length header so we can pre-allocate buffer
	// space.
	buf := bytes.NewBuffer(nil)
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return nil, err
	}

	return &KVResult{Path: *path, RawValue: buf.Bytes()}, nil
}

func (client *Client) Put(collection string, key string, value interface{}) (*Path, error) {
	buf := new(bytes.Buffer)
	encoder := json.NewEncoder(buf)

	if err := encoder.Encode(value); err != nil {
		return nil, err
	}

	return client.PutRaw(collection, key, buf)
}

func (client *Client) PutRaw(collection string, key string, value io.Reader) (*Path, error) {
	resp, err := client.doRequest("PUT", collection+"/"+key, nil, value)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		return nil, newError(resp)
	}

	return &Path{
		Collection: collection,
		Key:        key,
		Ref:        resp.Header.Get("Location"),
	}, err
}

func (client *Client) PutIfUnmodified(path *Path, value interface{}) (*Path, error) {
	buf := new(bytes.Buffer)
	encoder := json.NewEncoder(buf)

	if err := encoder.Encode(value); err != nil {
		return nil, err
	}

	return client.PutIfUnmodifiedRaw(path, buf)
}

func (client *Client) PutIfUnmodifiedRaw(path *Path, value io.Reader) (*Path, error) {
	headers := map[string]string{
		"If-Match": fmt.Sprintf("\"%s\"", path.Ref),
	}

	resp, err := client.doRequest("PUT", path.trailingPutURI(), headers, value)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		return nil, newError(resp)
	}

	return &Path{
		Collection: path.Collection,
		Key:        path.Key,
		Ref:        resp.Header.Get("Location"),
	}, err
}

func (client *Client) PutIfAbsent(collection string, key string, value interface{}) (*Path, error) {
	buf := new(bytes.Buffer)
	encoder := json.NewEncoder(buf)

	if err := encoder.Encode(value); err != nil {
		return nil, err
	}

	return client.PutIfAbsentRaw(collection, key, buf)
}

func (client *Client) PutIfAbsentRaw(collection string, key string, value io.Reader) (*Path, error) {
	headers := map[string]string{
		"If-None-Match": "\"*\"",
	}

	resp, err := client.doRequest("PUT", collection+"/"+key, headers, value)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		return nil, newError(resp)
	}

	return &Path{
		Collection: collection,
		Key:        key,
		Ref:        resp.Header.Get("Location"),
	}, err
}

func (client *Client) Delete(collection, key string) error {
	resp, err := client.doRequest("DELETE", collection+"/"+key, nil, nil)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		return newError(resp)
	}

	return nil
}

func (client *Client) Purge(collection, key string) error {
	resp, err := client.doRequest("DELETE", collection+"/"+key+"?purge=true", nil, nil)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		return newError(resp)
	}

	return nil
}

func (client *Client) List(collection string, limit int) (*KVResults, error) {
	queryVariables := url.Values{
		"limit": []string{strconv.Itoa(limit)},
	}

	trailingUri := fmt.Sprintf("%s?%s", collection, queryVariables.Encode())

	return client.doList(trailingUri)
}

func (client *Client) ListAfter(collection string, after string, limit int) (*KVResults, error) {
	queryVariables := url.Values{
		"limit":    []string{strconv.Itoa(limit)},
		"afterKey": []string{after},
	}

	trailingUri := fmt.Sprintf("%s?%s", collection, queryVariables.Encode())

	return client.doList(trailingUri)
}

func (client *Client) ListStart(collection string, start string, limit int) (*KVResults, error) {
	queryVariables := url.Values{
		"limit":    []string{strconv.Itoa(limit)},
		"startKey": []string{start},
	}

	trailingUri := fmt.Sprintf("%s?%s", collection, queryVariables.Encode())

	return client.doList(trailingUri)
}

func (client *Client) ListGetNext(results *KVResults) (*KVResults, error) {
	return client.doList(results.Next[4:])
}

func (client *Client) doList(trailingUri string) (*KVResults, error) {
	resp, err := client.doRequest("GET", trailingUri, nil, nil)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, newError(resp)
	}

	decoder := json.NewDecoder(resp.Body)
	result := new(KVResults)
	if err := decoder.Decode(result); err != nil {
		return result, err
	}

	return result, nil
}

func (results *KVResults) HasNext() bool {
	return results.Next != ""
}

func (result *KVResult) Value(value interface{}) error {
	return json.Unmarshal(result.RawValue, value)
}

func (path *Path) trailingGetURI() string {
	if path.Ref != "" {
		return fmt.Sprintf("%s/%s/ref/$s", path.Collection, path.Key, path.Ref)
	}
	return fmt.Sprintf("%s/%s", path.Collection, path.Key)
}

func (path *Path) trailingPutURI() string {
	return fmt.Sprintf("%s/%s", path.Collection, path.Key)
}

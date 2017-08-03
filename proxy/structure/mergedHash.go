// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package structure

import (
	"bytes"

	"encoding/json"
	"github.com/Mansfield6/tikv-proxy-demo/proxy/util"
	"github.com/juju/errors"
)

// HSet sets the string value of a hash field.
func (t *TxStructure) MergedHSet(key []byte, field []byte, value []byte) (int, error) {
	if t.readWriter == nil {
		return 0, errWriteOnSnapshot
	}
	metaKey := t.EncodeMetaKey(key)
	meta, err := t.loadHashMeta(metaKey)
	if err != nil {
		return 0, errors.Trace(err)
	}

	dataKey := t.encodeMergedHashDataKey(key)
	jsonData, err := t.loadHashValue(dataKey)
	if err != nil {
		return 0, errors.Trace(err)
	}

	oldMap := make(map[string][]byte)
	if len(jsonData)>0{
		if err = json.Unmarshal(jsonData, &oldMap);err != nil {
			return 0, errors.Trace(err)
		}
	}
	res := 0
	fkey := string(field)
	ov,has := oldMap[fkey]
	if !has {
		meta.FieldCount++
		res = 1
	}

	if !bytes.Equal(ov, value) {
		oldMap[fkey] = value
		newJsonData, err := json.Marshal(oldMap)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if err = t.readWriter.Set(dataKey, newJsonData); err != nil {
			return 0, errors.Trace(err)
		}

		if err = t.readWriter.Set(metaKey, EncodeHashMetaValue(meta.ExpireAt, meta.FieldCount)); err != nil {
			return 0, errors.Trace(err)
		}
	}

	return res, errors.Trace(err)
}

func (t *TxStructure) MergedHGet(key []byte, field []byte) ([]byte, error) {
	dataKey := t.encodeMergedHashDataKey(key)
	jsonData, err := t.loadHashValue(dataKey)
	if err != nil || len(jsonData) ==0{
		return nil, errors.Trace(err)
	}

	var oldMap map[string][]byte
	if err = json.Unmarshal(jsonData, &oldMap);err != nil {
		return nil, errors.Trace(err)
	}

	return oldMap[string(field)], errors.Trace(err)
}

func (t *TxStructure) MergedHMSet(key []byte, elements []*HashPair) ([]byte, error) {
	if t.readWriter == nil {
		return nil, errWriteOnSnapshot
	}

	metaKey := t.EncodeMetaKey(key)
	meta, err := t.loadHashMeta(metaKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dataKey := t.encodeMergedHashDataKey(key)
	jsonData, err := t.loadHashValue(dataKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	oldMap := make(map[string][]byte)
	if len(jsonData)>0 {
		if err = json.Unmarshal(jsonData, &oldMap);err != nil {
			return nil, errors.Trace(err)
		}
	}

	ms := &util.MarkSet{}
	for _, e := range elements {
		field := string(e.Field)
		if _,has :=oldMap[field];!has {
			ms.Set(e.Field)
		}
		oldMap[field] = e.Value
	}

	newJsonData, err := json.Marshal(oldMap)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err = t.readWriter.Set(dataKey, newJsonData); err != nil {
		return nil, errors.Trace(err)
	}

	meta.FieldCount += int64(ms.Len())
	if err = t.readWriter.Set(metaKey, EncodeHashMetaValue(meta.ExpireAt, meta.FieldCount)); err != nil {
		return nil, errors.Trace(err)
	}
	return []byte("OK"), nil
}

func (t *TxStructure) MergedHGetAll(key []byte) ([][]byte, error) {
	dataKey := t.encodeMergedHashDataKey(key)
	jsonData, err := t.loadHashValue(dataKey)
	if err != nil || len(jsonData)==0 {
		return nil, errors.Trace(err)
	}

	var oldMap map[string][]byte
	err = json.Unmarshal(jsonData, &oldMap)

	if err != nil {
		return nil, errors.Trace(err)
	}
	rets := make([][]byte, len(oldMap)*2)
	i := 0
	for k, v := range oldMap {
		rets[i*2], rets[i*2+1] = []byte(k), v
		i++
	}

	return rets, errors.Trace(err)
}

// HDel deletes one or more hash fields.
func (t *TxStructure) MergedHDel(key []byte, fields [][]byte) (int, error) {
	if t.readWriter == nil {
		return 0, errWriteOnSnapshot
	}
	metaKey := t.EncodeMetaKey(key)
	meta, err := t.loadHashMeta(metaKey)
	if err != nil || meta.IsEmpty() {
		return 0, errors.Trace(err)
	}

	dataKey := t.encodeMergedHashDataKey(key)
	jsonData, err := t.loadHashValue(dataKey)
	if err != nil|| len(jsonData)==0 {
		return 0, errors.Trace(err)
	}

	var oldMap map[string][]byte
	if err = json.Unmarshal(jsonData, &oldMap);err != nil {
		return 0, errors.Trace(err)
	}

	res := 0
	for _, field := range fields {
		f := string(field)
		if _,ok := oldMap[f];ok {
			delete(oldMap, f)
			res++
			meta.FieldCount--
		}
	}

	if res == 0 {
		return res, errors.Trace(err)
	}

	newJsonData, err := json.Marshal(oldMap)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if err = t.readWriter.Set(dataKey, newJsonData); err != nil {
		return 0, errors.Trace(err)
	}

	if err = t.readWriter.Set(metaKey, EncodeHashMetaValue(meta.ExpireAt, meta.FieldCount)); err != nil {
		return 0, errors.Trace(err)
	}
	return res, errors.Trace(err)
}

func (t *TxStructure) MergedHClear(key []byte) error {
	metaKey := t.EncodeMetaKey(key)
	meta, err := t.loadHashMeta(metaKey)
	if err != nil || meta.IsEmpty() {
		return errors.Trace(err)
	}

	if err = t.readWriter.Delete(metaKey);err != nil {
		return errors.Trace(err)
	}

	dataKey := t.encodeMergedHashDataKey(key)
	err = t.readWriter.Delete(dataKey)

	return errors.Trace(err)
}

func (t *TxStructure) MergedHKeys(key []byte) ([][]byte, error) {
	var keys [][]byte
	dataKey := t.encodeMergedHashDataKey(key)
	jsonData, err := t.loadHashValue(dataKey)
	if err != nil||len(jsonData)==0 {
		return nil, errors.Trace(err)
	}

	var oldMap map[string][]byte
	err = json.Unmarshal(jsonData, &oldMap)

	if err != nil {
		return nil, errors.Trace(err)
	}

	for k,_ := range oldMap {
		keys = append(keys,[]byte(k))
	}

	return keys, errors.Trace(err)
}

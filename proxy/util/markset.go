// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package util

import "bytes"

type MarkSet struct {
	one []byte
	set map[string]bool
}

func (s *MarkSet) Set(key []byte) {
	if s.one == nil {
		s.one = key
	} else {
		if s.set == nil {
			s.set = make(map[string]bool)
			s.set[string(s.one)] = true
		}
		s.set[string(key)] = true
	}
}

func (s *MarkSet) Len() int {
	if s.set != nil {
		return len(s.set)
	}
	if s.one != nil {
		return 1
	} else {
		return 0
	}
}

func (s *MarkSet) Has(key []byte) bool {
	if s.set != nil {
		return s.set[string(key)]
	}
	if s.one != nil {
		return bytes.Equal(key, s.one)
	} else {
		return false
	}
}

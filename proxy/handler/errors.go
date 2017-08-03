package handler

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
)

var (
	ErrBegionTXN = errors.New("begin transaction error")
	ErrKeySize   = errors.New("invalid key size")
	ErrValueSize = errors.New("invalid value size")
)

func errArguments(format string, v ...interface{}) error {
	err := errors.Errorf(format, v...)
	log.Warningf("call store function with invalid arguments - %s", err)
	return errors.Trace(err)
}

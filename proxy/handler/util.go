package handler

import (
	"github.com/Mansfield6/tikv-proxy-demo/proxy/uuid"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"time"
	"github.com/juju/errors"
)

const (
	MaxRetryCount int = 5
	//max key size
	MaxKeySize int = 1024
	//max value size
	MaxValueSize int = 1024 * 1024 * 1024
)

func checkKeySize(key []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return ErrKeySize
	}
	return nil
}

func checkValueSize(value []byte) error {
	if len(value) > MaxValueSize {
		return ErrValueSize
	}

	return nil
}

func nowms() int64 {
	return int64(time.Now().UnixNano()) / int64(time.Millisecond)
}

type RequestContext struct {
	id  uuid.UUID
	cmd string
}

func newRequestContext(commond string) *RequestContext {
	return &RequestContext{
		id:  uuid.NewV4(),
		cmd: commond,
	}
}

func CallWithRetry(context *RequestContext, fn func() (interface{}, error)) (interface{}, error) {
	curCount := 0
	for {
		curCount++
		res, err := fn()
		if err == nil {
			return res, err
		}
		log.Errorf("%s retry:%d error:%s", context.id, curCount,errors.ErrorStack(err) )
		if curCount >= MaxRetryCount {
			log.Errorf("%s Retry reached max count %d error: %s", context.id, curCount, err)
			return res, err
		}

		if !kv.IsRetryableError(err) {
			return res, err
		}
	}

}

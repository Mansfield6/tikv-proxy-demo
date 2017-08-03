package handler

import (
	"github.com/pingcap/tidb/kv"
)

type TxTikvHandler struct {
	Store kv.Storage
}

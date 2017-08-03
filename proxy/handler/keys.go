package handler

import (
	"github.com/Mansfield6/tikv-proxy-demo/proxy/structure"
	"github.com/juju/errors"
	"github.com/ngaut/log"
)

func (h *TxTikvHandler) DEL(keys [][]byte) (int, error) {
	if len(keys) == 0 {
		return 0, errArguments("len(args) = %d, expect != 0", len(keys))
	}

	context := newRequestContext("del")
	log.Infof("%s del %s", context.id, keys)
	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return 0, errors.Trace(ErrBegionTXN)
		}

		tx := structure.NewStructure(txn, txn, []byte{0x00})
		res, ierr := tx.DEL(keys)

		if ierr == nil {
			ierr = txn.Commit()
		}

		if ierr != nil {
			txn.Rollback()
			log.Error(ierr)
		}
		return res, ierr
	})
	return res.(int), err

}


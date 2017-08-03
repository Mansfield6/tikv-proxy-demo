package handler

import (
	"github.com/Mansfield6/tikv-proxy-demo/proxy/structure"
	"github.com/juju/errors"
	"github.com/ngaut/log"
)

func (h *TxTikvHandler) SET(args [][]byte) ([]byte, error) {
	context := newRequestContext("set")
	if len(args) != 2 {
		return nil, errArguments("len(args) = %d, expect = 2", len(args))
	}

	key := args[0]
	value := args[1]

	log.Infof("%s set %s %s", context.id, key, value)
	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return nil, errors.Trace(ErrBegionTXN)
		}

		tx := structure.NewStructure(txn, txn, []byte{0x00})
		res, ierr := tx.Set(key, value)
		if ierr == nil {
			ierr = txn.Commit()
		}

		if ierr != nil {
			txn.Rollback()
		}
		return res, ierr
	})

	return res.([]byte), err

}

func (h *TxTikvHandler) GET(key []byte) ([]byte, error) {
	//if kerr := checkKeySize(key); kerr != nil {
	//	return nil, kerr
	//}
	context := newRequestContext("set")
	log.Infof("%s get %s", context.id, key)

	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return nil, errors.Trace(ErrBegionTXN)
		}

		tx := structure.NewStructure(txn, txn, []byte{0x00})
		res, ierr := tx.Get(key)

		if ierr == nil {
			ierr = txn.Commit()
		}

		if ierr != nil {
			txn.Rollback()
			log.Error(ierr)
		}
		return res, ierr
	})

	return res.([]byte), err
}

func (h *TxTikvHandler) MSET(args [][]byte) ([]byte, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return nil, errArguments("len(args) = %d, expect != 0 && mod 2 = 0", len(args))
	}

	//for i := len(args)/2 - 1; i >= 0; i-- {
	//	key, value := args[i*2], args[i*2+1]
	//	if kerr := checkKeySize(key); kerr != nil {
	//		return nil, kerr
	//	}
	//
	//	if verr := checkValueSize(value); verr != nil {
	//		return nil, verr
	//	}
	//
	//}

	context := newRequestContext("mset")
	log.Infof("%s mset %s", context.id, args)

	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return nil, errors.Trace(ErrBegionTXN)
		}

		tx := structure.NewStructure(txn, txn, []byte{0x00})
		var ierr error
		for i := len(args)/2 - 1; i >= 0; i-- {
			key, value := args[i*2], args[i*2+1]

			_, err := tx.Set(key, value)
			if err != nil {
				ierr = err
				break
			}

		}

		if ierr == nil {
			ierr = txn.Commit()
		}

		if ierr != nil {
			txn.Rollback()
		}
		return []byte("OK"), ierr
	})
	return res.([]byte), err
}

func (h *TxTikvHandler) MGET(args [][]byte) ([][]byte, error) {
	if len(args) == 0 {
		return nil, errArguments("len(args) = %d, expect != 0", len(args))
	}

	keys := args

	for _, key := range keys {
		if kerr := checkKeySize(key); kerr != nil {
			return nil, kerr
		}
	}

	values := make([][]byte, len(keys))

	context := newRequestContext("mget")
	log.Infof("%s mget %s", context.id, args)
	_, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return nil, errors.Trace(ErrBegionTXN)
		}

		tx := structure.NewStructure(txn, txn, []byte{0x00})
		var ierr error

		for i, key := range keys {
			res, err := tx.Get(key)
			if err != nil {
				ierr = err
				break
			}
			values[i] = res
		}

		if ierr == nil {
			ierr = txn.Commit()
		}

		if ierr != nil {
			txn.Rollback()
		}
		return nil, ierr
	})

	return values, err
}

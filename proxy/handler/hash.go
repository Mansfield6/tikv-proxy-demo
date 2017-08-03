package handler

import (
	"github.com/Mansfield6/tikv-proxy-demo/proxy/structure"
	"github.com/juju/errors"
	"github.com/ngaut/log"
)

func (h *TxTikvHandler) HSET(args [][]byte) (int, error) {
	if len(args) != 3 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	key := args[0]
	field := args[1]
	value := args[2]

	context := newRequestContext("hset")
	log.Infof("%s hset %s", context.id, args)
	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return 0, errors.Trace(ErrBegionTXN)
		}

		tx := structure.NewStructure(txn, txn, []byte{0x00})
		//res, ierr := tx.HSet(key, field, value)
		res, ierr := tx.MergedHSet(key, field, value)

		if ierr == nil {
			ierr = txn.Commit()
		}

		if ierr != nil {
			txn.Rollback()
		}
		return res, ierr
	})

	return res.(int), errors.Trace(err)
}

func (h *TxTikvHandler) HMSET(args [][]byte) ([]byte, error) {
	if len(args) == 1 || len(args)%2 != 1 {
		return nil, errArguments("len(args) = %d, expect != 1 && mod 2 = 1", len(args))
	}
	key := args[0]

	var eles = make([]*structure.HashPair, len(args)/2)
	for i := 0; i < len(eles); i++ {
		e := &structure.HashPair{}
		e.Field = args[i*2+1]
		e.Value = args[i*2+2]
		eles[i] = e
	}

	context := newRequestContext("hmset")
	log.Infof("%s hmset %s %s", context.id, key, args)
	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return 0, errors.Trace(ErrBegionTXN)
		}

		tx := structure.NewStructure(txn, txn, []byte{0x00})
		//res, ierr := tx.HMSet(key, eles)
		res, ierr := tx.MergedHMSet(key, eles)

		if ierr == nil {
			ierr = txn.Commit()
		}

		if ierr != nil {
			txn.Rollback()
		}
		return res, ierr
	})

	return res.([]byte), errors.Trace(err)
}

func (h *TxTikvHandler) HGET(args [][]byte) ([]byte, error) {
	if len(args) != 2 {
		return nil, errArguments("len(args) = %d, expect = 2", len(args))
	}

	key := args[0]
	field := args[1]

	context := newRequestContext("hget")
	log.Infof("%s hget %s", context.id, args)
	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return nil, errors.Trace(ErrBegionTXN)
		}

		tx := structure.NewStructure(txn, txn, []byte{0x00})
		//res, ierr := tx.HGet(key, field)
		res, ierr := tx.MergedHGet(key, field)
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

func (h *TxTikvHandler) HGETALL(key []byte) ([][]byte, error) {
	context := newRequestContext("hgetall")
	log.Infof("%s hget %s", context.id, key)
	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return nil, errors.Trace(ErrBegionTXN)
		}

		tx := structure.NewStructure(txn, txn, []byte{0x00})
		//res, ierr := tx.HGetAll(key)
		res, ierr := tx.MergedHGetAll(key)
		if ierr == nil {
			ierr = txn.Commit()
		}

		if ierr != nil {
			txn.Rollback()
		}
		return res, ierr
	})
	return res.([][]byte), err

}

func (h *TxTikvHandler) HDEL(key []byte, args [][]byte) (int, error) {
	if len(args) < 1 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	context := newRequestContext("hdel")
	log.Infof("%s hdel %s", context.id, args)
	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return 0, errors.Trace(ErrBegionTXN)
		}

		tx := structure.NewStructure(txn, txn, []byte{0x00})
		//res, ierr := tx.HDel(key, args)
		res, ierr := tx.MergedHDel(key, args)

		if ierr == nil {
			ierr = txn.Commit()
		}

		if ierr != nil {
			txn.Rollback()
		}
		return res, ierr
	})

	return res.(int), err
}

func (h *TxTikvHandler) HKEYS(key []byte) ([][]byte, error) {

	context := newRequestContext("hkeys")
	log.Infof("%s hkeys %s", context.id, key)
	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return nil, errors.Trace(ErrBegionTXN)
		}

		tx := structure.NewStructure(txn, txn, []byte{0x00})
		//res, ierr := tx.HKeys(key)
		res, ierr := tx.MergedHKeys(key)
		if ierr == nil {
			ierr = txn.Commit()
		}

		if ierr != nil {
			txn.Rollback()
		}
		return res, ierr
	})
	return res.([][]byte), err

}

func (h *TxTikvHandler) HLEN(key []byte) (int, error) {
	context := newRequestContext("hlen")
	log.Infof("%s hlen %s", context.id, key)
	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return nil, errors.Trace(ErrBegionTXN)
		}

		tx := structure.NewStructure(txn, txn, []byte{0x00})
		res, ierr := tx.HLen(key)

		if ierr == nil {
			ierr = txn.Commit()
		}

		if ierr != nil {
			txn.Rollback()
		}
		return res, ierr
	})
	return res.(int), err
}

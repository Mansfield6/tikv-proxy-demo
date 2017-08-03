package structure

import (
	"github.com/Mansfield6/tikv-proxy-demo/proxy/util"
	"github.com/juju/errors"
)

var (
	InvalidFlag = errors.New("invalid flag")
)

func (t *TxStructure) DEL(keys [][]byte) (int, error) {
	if t.readWriter == nil {
		return 0, errWriteOnSnapshot
	}
	ms := &util.MarkSet{}

	var intererr error
	for _, key := range keys {
		if !ms.Has(key) {
			mk := t.EncodeMetaKey(key)
			mv, _ := t.reader.Get(mk)
			if mv != nil {
				flag, _, _ := DecodeMetaValue(mv)
				switch flag {
				case StringData:
					err := t.Clear(key)
					if err != nil {
						return 0, err
					}
					ms.Set(key)
				case HashData:
					//err := t.HClear(key)
					err := t.MergedHClear(key)
					if err != nil {
						return 0, err
					}
					ms.Set(key)
				default:
					return 0, InvalidFlag
				}

			}

		}
	}

	return int(ms.Len()), intererr

}

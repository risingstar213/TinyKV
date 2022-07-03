package mvcc

import (
	"reflect"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	nxtKey   []byte
	startTS  uint64
	txn      *MvccTxn
	iter     engine_util.DBIterator
	finished bool
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scan := &Scanner{
		nxtKey:   startKey,
		startTS:  txn.StartTS,
		txn:      txn,
		iter:     txn.Reader.IterCF(engine_util.CfWrite),
		finished: false,
	}
	return scan
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	if scan != nil {
		scan.iter.Close()
	}
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.finished {
		return nil, nil, nil
	}
	nowKey := scan.nxtKey
	for {
		scan.iter.Seek(EncodeKey(nowKey, scan.startTS))
		if !scan.iter.Valid() {
			return nil, nil, nil
		}

		item := scan.iter.Item()
		itemKey := item.Key()
		key := DecodeUserKey(itemKey)

		if !reflect.DeepEqual(nowKey, key) {
			nowKey = key
			continue
		}

		value, _ := item.Value()
		write, _ := ParseWrite(value)

		for {
			scan.iter.Next()
			if !scan.iter.Valid() {
				scan.finished = true
				break
			}
			tItem := scan.iter.Item()
			tItemKey := tItem.Key()
			tKey := DecodeUserKey(tItemKey)
			if !reflect.DeepEqual(tKey, key) {
				scan.nxtKey = tKey
				break
			}
		}

		if write.Kind == WriteKindDelete {
			return key, nil, nil
		} else if write.Kind == WriteKindPut {
			val, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
			if err != nil {
				return key, val, err
			}
			return key, val, nil
		}
	}
}

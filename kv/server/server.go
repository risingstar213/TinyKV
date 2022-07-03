package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())

	rsp := &kvrpcpb.GetResponse{
		RegionError: nil,
		Error:       nil,
		Value:       nil,
		NotFound:    false,
	}

	lock, err := txn.GetLock(req.Key)
	// The key is locked
	if err != nil {
		return nil, err
	}

	// The key is locked
	if lock != nil && lock.Ts <= req.GetVersion() {
		rsp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.Key,
				LockTtl:     lock.Ttl,
			},
		}
		return rsp, nil
	}

	value, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if value == nil || len(value) == 0 {
		rsp.NotFound = true
		return rsp, nil
	}
	rsp.Value = value
	return rsp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	keyErrors := make([]*kvrpcpb.KeyError, 0)
	for _, mut := range req.Mutations {
		write, timestamp, err := txn.MostRecentWrite(mut.GetKey())
		if err != nil {
			return nil, err
		}
		// The write come across conflict
		if write != nil && timestamp >= txn.StartTS {
			keyErr := kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    txn.StartTS,
					ConflictTs: timestamp,
					Key:        mut.GetKey(),
					Primary:    nil,
				},
			}
			keyErrors = append(keyErrors, &keyErr)
			continue
		}
		lock, err := txn.GetLock(mut.GetKey())
		if err != nil {
			return nil, err
		}
		// The key is locked by other transaction
		if lock != nil && lock.Ts != txn.StartTS {
			keyErr := kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         mut.GetKey(),
					LockTtl:     lock.Ttl,
				},
			}
			keyErrors = append(keyErrors, &keyErr)
			continue
		}
	}

	if len(keyErrors) > 0 {
		rsp := &kvrpcpb.PrewriteResponse{
			RegionError: nil,
			Errors:      keyErrors,
		}
		return rsp, nil
	}
	for _, mut := range req.Mutations {
		switch mut.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mut.Key, mut.Value)
			txn.PutLock(mut.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      txn.StartTS,
				Ttl:     req.GetLockTtl(),
				Kind:    mvcc.WriteKindPut,
			})
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mut.Key)
			txn.PutLock(mut.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      txn.StartTS,
				Ttl:     req.GetLockTtl(),
				Kind:    mvcc.WriteKindDelete,
			})
		}
	}
	err = server.storage.Write(req.GetContext(), txn.Writes())
	if err != nil {
		return nil, err
	}
	rsp := &kvrpcpb.PrewriteResponse{
		RegionError: nil,
		Errors:      nil,
	}
	return rsp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	rsp := &kvrpcpb.CommitResponse{
		RegionError: nil,
		Error:       nil,
	}
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			write, ts, err := txn.MostRecentWrite(key)
			if err != nil {
				return nil, err
			}

			// has been commited
			if write.StartTS == req.GetStartVersion() && ts == req.GetCommitVersion() {
				value, err := txn.GetValueByStartTS(key, write.StartTS)
				if err != nil {
					return nil, err
				}
				if value != nil {
					continue
				}
			}
			rsp.Error = &kvrpcpb.KeyError{}
			return rsp, nil
		}

		if lock.Ts != req.GetStartVersion() {
			rsp.Error = &kvrpcpb.KeyError{}
			return rsp, nil
		}
		write := &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    lock.Kind,
		}
		txn.PutWrite(key, req.GetCommitVersion(), write)
		txn.DeleteLock(key)
	}
	err = server.storage.Write(req.GetContext(), txn.Writes())
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())
	scan := mvcc.NewScanner(req.GetStartKey(), txn)
	defer scan.Close()
	pairs := make([]*kvrpcpb.KvPair, 0)

	for i := uint32(0); i < req.Limit; {
		key, value, err := scan.Next()
		if err != nil {
			return nil, err
		}
		if key == nil && value == nil {
			break
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}

		if lock != nil && lock.Ts <= req.GetVersion() {
			error := &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         key,
						LockTtl:     lock.Ttl,
					},
				},
			}
			pairs = append(pairs, error)
			i += 1
			continue
		}
		if value != nil {
			pair := &kvrpcpb.KvPair{
				Key:   key,
				Value: value,
			}
			pairs = append(pairs, pair)
			i += 1
		}
	}
	rsp := &kvrpcpb.ScanResponse{
		RegionError: nil,
		Pairs:       pairs,
	}
	return rsp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetLockTs())

	rsp := &kvrpcpb.CheckTxnStatusResponse{}

	write, ts, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}
	if write != nil {
		if write.Kind != mvcc.WriteKindRollback {
			rsp.CommitVersion = ts
		}
		return rsp, nil
	}

	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if lock == nil {
		txn.PutWrite(req.PrimaryKey, req.GetLockTs(), &mvcc.Write{
			StartTS: req.GetLockTs(),
			Kind:    mvcc.WriteKindRollback,
		})
		err = server.storage.Write(req.GetContext(), txn.Writes())
		if err != nil {
			return nil, err
		}
		rsp.Action = kvrpcpb.Action_LockNotExistRollback
	} else if mvcc.PhysicalTime(req.LockTs)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, req.GetLockTs(), &mvcc.Write{
			StartTS: req.GetLockTs(),
			Kind:    mvcc.WriteKindRollback,
		})
		err = server.storage.Write(req.GetContext(), txn.Writes())
		if err != nil {
			return nil, err
		}
		rsp.Action = kvrpcpb.Action_TTLExpireRollback
	}
	return rsp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	rsp := &kvrpcpb.BatchRollbackResponse{
		RegionError: nil,
		Error:       nil,
	}

	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				continue
			} else {
				rsp.Error = &kvrpcpb.KeyError{
					Abort: "aborted",
				}
				return rsp, nil
			}
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts == txn.StartTS {
			txn.DeleteLock(key)
			txn.DeleteValue(key)
		}
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	err = server.storage.Write(req.GetContext(), txn.Writes())
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	rsp := &kvrpcpb.ResolveLockResponse{
		RegionError: nil,
		Error:       nil,
	}

	lockKeys := make([][]byte, 0)
	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close()
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		value, err := item.Value()
		if err != nil {
			return nil, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return nil, err
		}
		if lock.Ts == req.StartVersion {
			lockKeys = append(lockKeys, item.KeyCopy(nil))
		}
	}
	if len(lockKeys) == 0 {
		return rsp, nil
	}
	if req.CommitVersion == 0 {
		req := &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         lockKeys,
		}
		subRsp, err := server.KvBatchRollback(nil, req)
		rsp.Error = subRsp.Error
		rsp.RegionError = subRsp.RegionError
		return rsp, err
	} else {
		req := &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          lockKeys,
			CommitVersion: req.CommitVersion,
		}
		subRsp, err := server.KvCommit(nil, req)
		rsp.Error = subRsp.Error
		rsp.RegionError = subRsp.RegionError
		return rsp, err
	}
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}

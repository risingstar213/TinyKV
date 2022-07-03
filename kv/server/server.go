package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
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
			rsp := &kvrpcpb.CommitResponse{
				RegionError: nil,
				Error:       &kvrpcpb.KeyError{},
			}
			return rsp, nil
		}
		if lock.Ts != req.GetStartVersion() {
			rsp := &kvrpcpb.CommitResponse{
				RegionError: nil,
				Error:       &kvrpcpb.KeyError{},
			}
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
	rsp := &kvrpcpb.CommitResponse{
		RegionError: nil,
		Error:       nil,
	}
	return rsp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
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

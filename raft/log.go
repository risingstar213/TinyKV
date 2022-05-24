// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	l := &RaftLog{
		storage: storage,
	}
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	entries, _ := storage.Entries(firstIndex, lastIndex+1)
	l.stabled = lastIndex
	l.committed = firstIndex - 1
	l.applied = firstIndex - 1
	l.firstIndex = firstIndex
	l.entries = entries
	return l
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.stabled-l.firstIndex+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.applied-l.firstIndex+1 : l.committed-l.firstIndex+1]
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	len := len(l.entries)
	if len == 0 {
		return l.committed
	} else {
		return l.entries[len-1].Index
	}
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 && i >= l.firstIndex {
		return l.entries[i-l.firstIndex].Term, nil
	}

	return 0, nil
}

func (l *RaftLog) getSliceIndex(i uint64) int {
	idx := int(i - l.firstIndex)
	if idx < 0 {
		panic("toSliceIndex: index < 0")
	}
	return idx
}

func (l *RaftLog) maybeAppend(m pb.Message) (uint64, bool) {
	if m.Index > l.LastIndex() {
		return l.LastIndex(), false
	}
	var term uint64
	term, _ = l.Term(m.Index)
	// Cannot ensure the modification correct
	if term != m.LogTerm {
		// sliceIndex := l.getSliceIndex(m.Index)
		// l.entries = l.entries[:sliceIndex]
		// l.stabled = min(l.stabled, m.Index - 1)
		return m.Index - 1, false
	}
	for i := 0; i < len(m.Entries); i++ {
		if m.Entries[i].Index <= l.LastIndex() {
			term, _ = l.Term(m.Entries[i].Index)
			sliceIndex := l.getSliceIndex(m.Entries[i].Index)
			if term != m.Entries[i].Term {
				l.entries[sliceIndex] = *m.Entries[i]
				l.entries = l.entries[:sliceIndex+1]
				l.stabled = min(l.stabled, m.Entries[i].Index-1)
			}
		} else {
			l.entries = append(l.entries, *m.Entries[i])
		}
	}
	return None, true
}

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

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

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
	LastAppend uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	entries, _ := storage.Entries(firstIndex, lastIndex+1)
	hardState, _, _ := storage.InitialState()

	return &RaftLog{
		storage:         storage,
		committed:       hardState.Commit,
		applied:         firstIndex - 1, // not clear
		stabled:         lastIndex,
		entries:         entries,
		pendingSnapshot: nil,
		LastAppend:      firstIndex,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	first, _ := l.storage.FirstIndex()
	if first > l.LastAppend {
		//log.Infof("need to compact")
		if len(l.entries) > 0 {
			entries := l.entries[first-l.LastAppend:]
			l.entries = make([]pb.Entry, len(entries))
			copy(l.entries, entries)
		}
		l.LastAppend = first
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) <= 0 {
		return nil
	}
	return l.entries[l.stabled-l.LastAppend+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) <= 0 {
		return nil
	}
	offset := l.LastAppend - 1
	if l.applied < offset {
		return l.allEntries()
	}
	return l.entries[l.applied-offset : l.committed-offset]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	res := l.LastAppend + uint64(len(l.entries)) - 1
	return res
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 && i >= l.LastAppend {
		return l.entries[i-l.LastAppend].Term, nil
	}
	term, err := l.storage.Term(i)
	if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot) {
		index := l.pendingSnapshot.Metadata.Index
		if i == index {
			err = nil
			term = l.pendingSnapshot.Metadata.Term
		} else if i < index {
			err = ErrCompacted
		}
	}
	return term, err
}

func (l *RaftLog) getEntries(start, end uint64) (ents []pb.Entry) {
	if start < l.LastAppend || end > l.LastIndex() {
		panic("get entries out of range")
	}
	ents = l.entries[start-l.LastAppend : end-l.LastAppend+1]
	return ents
}

package raft

import (
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
	"github.com/ethereum/go-ethereum/log"
)

func (pm *ProtocolManager) openWAL(maybeRaftSnapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(pm.waldir) {
		// On a CIFS filesytem we need to use the 'wal_windows' implementation in the
		// version of Raft included in Quorum. This performs a rename of raft-wal.tmp
		// to raft-wal as art of wal.Create()
		// As of Go 1.8, in UNIX/Docker you cannot rename a directory to replace a directory
		// see - https://golang.org/doc/go1.8
		//
		// As such, we do not create the waldir in the Quorum layer, but rather leave
		// it to the wal code itself to create it.
		//
		// if err := os.Mkdir(pm.waldir, 0750); err != nil {
		// 	fatalf("cannot create waldir: %s", err)
		// }

		wal, err := wal.Create(pm.waldir, nil)
		if err != nil {
			fatalf("failed to create waldir: %s", err)
		}
		wal.Close()
	}

	walsnap := walpb.Snapshot{}

	log.Info("loading WAL", "term", walsnap.Term, "index", walsnap.Index)

	if maybeRaftSnapshot != nil {
		walsnap.Index, walsnap.Term = maybeRaftSnapshot.Metadata.Index, maybeRaftSnapshot.Metadata.Term
	}

	wal, err := wal.Open(pm.waldir, walsnap)
	if err != nil {
		fatalf("error loading WAL: %s", err)
	}

	return wal
}

func (pm *ProtocolManager) replayWAL(maybeRaftSnapshot *raftpb.Snapshot) (*wal.WAL, []raftpb.Entry) {
	log.Info("replaying WAL")
	wal := pm.openWAL(maybeRaftSnapshot)

	_, hardState, entries, err := wal.ReadAll()
	if err != nil {
		fatalf("failed to read WAL: %s", err)
	}

	pm.raftStorage.SetHardState(hardState)
	pm.raftStorage.Append(entries)

	return wal, entries
}

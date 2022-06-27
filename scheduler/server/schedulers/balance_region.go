// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

type storePair struct {
	regionSize int64
	id         uint64
}

type storePairSlice []*storePair

func (s storePairSlice) Len() int {
	return len(s)
}

func (s storePairSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s storePairSlice) Less(i, j int) bool {
	return s[i].regionSize > s[j].regionSize
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	stores := cluster.GetStores()
	slice := make(storePairSlice, 0)
	for _, store := range stores {
		// suitable
		if store.IsUp() && store.DownTime() <= cluster.GetMaxStoreDownTime() {
			slice = append(slice, &storePair{regionSize: store.GetRegionSize(), id: store.GetID()})
		}
	}

	if len(slice) < 2 {
		return nil
	}

	sort.Sort(slice)

	var region *core.RegionInfo
	var hasFound int = -1

	for i := 0; i < len(slice); i++ {
		var regions core.RegionsContainer
		cluster.GetPendingRegionsWithLock(slice[i].id, func(rc core.RegionsContainer) { regions = rc })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			hasFound = i
			break
		}

		cluster.GetFollowersWithLock(slice[i].id, func(rc core.RegionsContainer) { regions = rc })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			hasFound = i
			break
		}

		cluster.GetLeadersWithLock(slice[i].id, func(rc core.RegionsContainer) { regions = rc })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			hasFound = i
			break
		}
	}

	if hasFound >= 0 {
		srcStore := cluster.GetStore(slice[hasFound].id)
		var dstStore *core.StoreInfo = nil
		storeIds := region.GetStoreIds()

		if len(storeIds) < cluster.GetMaxReplicas() {
			return nil
		}
		for j := len(slice) - 1; j > hasFound; j-- {
			if _, ok := storeIds[slice[j].id]; !ok {
				dstStore = cluster.GetStore(slice[j].id)
				break
			}
		}
		if dstStore == nil {
			return nil
		}

		if srcStore.GetRegionSize()-dstStore.GetRegionSize() < region.GetApproximateSize()*2 {
			return nil
		}

		peer, err := cluster.AllocPeer(dstStore.GetID())
		if err != nil {
			return nil
		}
		desc := fmt.Sprintf("move from %d to %d", srcStore.GetID(), dstStore.GetID())

		opt, err := operator.CreateMovePeerOperator(desc, cluster, region, operator.OpBalance, srcStore.GetID(), dstStore.GetID(), peer.Id)
		if err != nil {
			return nil
		}
		return opt
	}
	return nil
}

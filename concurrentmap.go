package logger

import (
	"encoding/json"
	"sync"
)

const totalShardCount = 32

type ConcurrentMap []*ConcurrentMapShared

type ConcurrentMapShared struct {
	items map[string]interface{}
	sync.RWMutex
}

func NewConcurrentMap() ConcurrentMap {
	m := make(ConcurrentMap, totalShardCount)
	for i := 0; i < totalShardCount; i++ {
		m[i] = &ConcurrentMapShared{items: make(map[string]interface{})}
	}
	return m
}

func (m ConcurrentMap) GetShard(key string) *ConcurrentMapShared {
	return m[uint(fnv32(key))%uint(totalShardCount)]
}

func (m ConcurrentMap) MSet(data map[string]interface{}) {
	for key, value := range data {
		shard := m.GetShard(key)
		shard.Lock()
		shard.items[key] = value
		shard.Unlock()
	}
}

func (m ConcurrentMap) Set(key string, value interface{}) {
	shard := m.GetShard(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

type UpdateOrInsertCb func(exist bool, valueInMap interface{}, newValue interface{}) interface{}

func (m ConcurrentMap) UpdateOrInsert(key string, value interface{}, cb UpdateOrInsertCb) (res interface{}) {
	shard := m.GetShard(key)

	shard.Lock()
	v, ok := shard.items[key]
	res = cb(ok, v, value)
	shard.items[key] = res
	shard.Unlock()

	return res
}

func (m ConcurrentMap) SetIfAbsent(key string, value interface{}) bool {
	shard := m.GetShard(key)

	shard.Lock()
	_, ok := shard.items[key]
	if !ok {
		shard.items[key] = value
	}
	shard.Unlock()

	return !ok
}

func (m ConcurrentMap) Get(key string) (interface{}, bool) {
	shard := m.GetShard(key)

	shard.RLock()
	val, ok := shard.items[key]
	shard.RUnlock()

	return val, ok
}

func (m ConcurrentMap) Count() int {
	count := 0
	for i := 0; i < totalShardCount; i++ {
		shard := m[i]

		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

func (m ConcurrentMap) Has(key string) bool {
	shard := m.GetShard(key)

	shard.RLock()
	_, ok := shard.items[key]
	shard.RUnlock()

	return ok
}

func (m ConcurrentMap) Remove(key string) {
	shard := m.GetShard(key)

	shard.Lock()
	delete(shard.items, key)

	shard.Unlock()
}

func (m ConcurrentMap) Pop(key string) (v interface{}, exists bool) {
	shard := m.GetShard(key)

	shard.Lock()
	v, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()

	return v, exists
}

func (m ConcurrentMap) IsEmpty() bool {
	return m.Count() == 0
}

type Tuple struct {
	Key string
	Val interface{}
}

func (m ConcurrentMap) Iterator() <-chan Tuple {
	channels := snapshot(m)
	ch := make(chan Tuple)
	go fanIn(channels, ch)
	return ch
}

func (m ConcurrentMap) IteratorBuffered() <-chan Tuple {
	channels := snapshot(m)
	total := 0
	for _, c := range channels {
		total += cap(c)
	}
	ch := make(chan Tuple, total)
	go fanIn(channels, ch)
	return ch
}

func snapshot(m ConcurrentMap) (channels []chan Tuple) {
	channels = make([]chan Tuple, totalShardCount)
	wg := sync.WaitGroup{}
	wg.Add(totalShardCount)

	for index, shard := range m {
		go func(index int, shard *ConcurrentMapShared) {
			shard.RLock()
			channels[index] = make(chan Tuple, len(shard.items))
			wg.Done()
			for key, val := range shard.items {
				channels[index] <- Tuple{key, val}
			}
			shard.RUnlock()
			close(channels[index])
		}(index, shard)
	}
	wg.Wait()
	return channels
}

func fanIn(channels []chan Tuple, out chan Tuple) {
	wg := sync.WaitGroup{}
	wg.Add(len(channels))
	for _, ch := range channels {
		go func(ch chan Tuple) {
			for t := range ch {
				out <- t
			}
			wg.Done()
		}(ch)
	}
	wg.Wait()
	close(out)
}

func (m ConcurrentMap) Items() map[string]interface{} {
	tmp := make(map[string]interface{})

	for item := range m.IteratorBuffered() {
		tmp[item.Key] = item.Val
	}

	return tmp
}

// Iterator callback,called for every key,value found in
// maps. RLock is held for all calls for a given shard
// therefore callback sess consistent view of a shard,
// but not across the shards
type IteratorCb func(key string, v interface{})

func (m ConcurrentMap) IteratorCb(fn IteratorCb) {
	for idx := range m {
		shard := (m)[idx]
		shard.RLock()
		for key, value := range shard.items {
			fn(key, value)
		}
		shard.RUnlock()
	}
}

func (m ConcurrentMap) Keys() []string {
	count := m.Count()
	ch := make(chan string, count)
	go func() {
		wg := sync.WaitGroup{}
		wg.Add(totalShardCount)
		for _, shard := range m {
			go func(shard *ConcurrentMapShared) {
				shard.RLock()
				for key := range shard.items {
					ch <- key
				}
				shard.RUnlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()

	keys := make([]string, 0, count)
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}

func (m ConcurrentMap) MarshalJSON() ([]byte, error) {
	tmp := make(map[string]interface{})

	for item := range m.IteratorBuffered() {
		tmp[item.Key] = item.Val
	}

	return json.Marshal(tmp)
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

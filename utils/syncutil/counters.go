// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package syncutil

import "sync/atomic"

// counter is an element in a Counters struct, using atomic operations
// for lock-free concurrent access.
type counter struct {
	count atomic.Int64
}

// Counters provides a wrapper to a list of counters that supports
// concurrent update-only operations using lock-free atomics.
type Counters []counter

// NewCounters returns an initialized Counters of the given length.
func NewCounters(length int) Counters {
	return Counters(make([]counter, length))
}

// Len returns the number of counters in the Counters.
func (c Counters) Len() int {
	return len(c)
}

// Get returns the count of the counter at index i.
func (c Counters) Get(i int) int {
	return int(c[i].count.Load())
}

// Set sets the count of the counter at index i to count v.
func (c Counters) Set(i, v int) {
	c[i].count.Store(int64(v))
}

// Increment increments the count of the counter at index i.
func (c Counters) Increment(i int) {
	c[i].count.Add(1)
}

// Decrement decrements the count of the counter at index i.
func (c Counters) Decrement(i int) {
	c[i].count.Add(-1)
}

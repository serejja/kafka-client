package client

import (
	"math"
	"sync"
)

func inLock(lock *sync.Mutex, fun func()) {
	lock.Lock()
	defer lock.Unlock()

	fun()
}

func inReadLock(lock *sync.RWMutex, fun func()) {
	lock.RLock()
	defer lock.RUnlock()

	fun()
}

func inWriteLock(lock *sync.RWMutex, fun func()) {
	lock.Lock()
	defer lock.Unlock()

	fun()
}

func maxInt64(ints ...int64) int64 {
	var max int64
	max = math.MinInt64
	for _, value := range ints {
		if value > max {
			max = value
		}
	}
	return max
}

func minInt64(ints ...int64) int64 {
	var min int64
	min = math.MaxInt64
	for _, value := range ints {
		if value < min {
			min = value
		}
	}
	return min
}

package maghash

import (
	"errors"
	"sync"
)

// MagHash is an interface providing Maglev Hashing functions
type MagHash interface {
	// Add backends to Maglev hashing, those added items will be considered
	// as available backends. This will internally RW lock backend related
	// data structures. It will trigger a permutation and lookup table re-calculation
	AddBackends(backends []string) (err error)

	// Remove backends from backend list, we don't need recalculate permutation
	// but a lookup table populate is required.
	RemoveBackends(backends []string)

	// Get the current number of backends.
	BackendsNum() (count int)

	// Get the m value for hash calculation.
	M() (m int)

	// Get the backend lookup result for given flow.
	GetBackend(flow string) (backend string)

	// Get the lookup table (list of backends)
	LookupTable() (backends []string)
}

type magHash struct {
	// pMu protects permutation table as well as m value
	pMu sync.RWMutex
	// Prime number for modular
	m int32
	// Permutation table for all backend
	// it stores the values(score) for each (backend, entry) pair
	permutation [][]int

	// bMu protects backends[] and backend number n
	bMu      sync.RWMutex
	bIndex   map[string]int
	backends [][]byte
	// Backend number
	n int32

	// eMu protects entry[] and next[]
	eMu sync.RWMutex
	// entry is the result of backend selection for each possible
	// hash value entry - Lookup table
	entry []int
}

// Creates Maghash with M, M must larger than backend number.
// A greater M value increasing backend selection equalization
// while decreasing performance.
func NewMagHash(m int) (mh MagHash, err error) {
	if m == 0 {
		m = defaultPrime
	}
	if !isPrime(m) {
		err = ErrInvalidPrime
		return
	}
	return &magHash{
		m:           int32(m),
		permutation: make([][]int, 0),
		bIndex:      make(map[string]int),
		backends:    make([][]byte, 0),
		entry:       make([]int, m),
	}, nil
}

var (
	ErrInvalidIndex = errors.New("invalid index")
	ErrInvalidPrime = errors.New("invalid prime number")
)

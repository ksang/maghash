/*
maghash implements Google's load balance solution - Maglev's consistent
hashing algorithm.
ref:
	http://static.googleusercontent.com/media/research.google.com/zh-CN//pubs/archive/44824.pdf
*/
package maghash

import (
	"hash"
	"hash/fnv"
	"sync/atomic"
)

// offset and index generates offset/index value given backend index number
// we assume backend/permutation resouces are locked and can't be changed
// during the whole permutation table calculation.
// formula: offset = hash1(backend) mod M
//			skip   = hash2(backend) mode (M-1) + 1
func (p *magHash) offset(backendIdx int) (offset int, err error) {
	fnv := fnv.New64()
	h1, err := p.getHash(backendIdx, fnv)
	if err != nil {
		return
	}
	m := int(p.m)
	return int(h1 % uint64(m)), nil
}

func (p *magHash) skip(backendIdx int) (offset int, err error) {
	fnva := fnv.New64a()
	h2, err := p.getHash(backendIdx, fnva)
	m := int(p.m)
	return int(h2%uint64(m-1)) + 1, nil
}

func (p *magHash) getHash(backendIdx int, h hash.Hash64) (hash uint64, err error) {
	n := int(p.n)
	if backendIdx >= int(n) {
		err = ErrInvalidIndex
		return
	}
	var backend []byte
	if backendIdx < len(p.backends) {
		backend = p.backends[backendIdx]
	} else {
		err = ErrInvalidIndex
		return
	}
	h.Write(backend)
	return h.Sum64(), nil
}

// Calculate permutation table incrementally.
// Values for old backends are re-used.
// To re-create permutation table, simply empty it.
// formula: P[i][j] = (offset + j*skip) mod M
func (p *magHash) spawnPermutation() (err error) {
	p.bMu.RLock()
	p.pMu.Lock()
	defer func() {
		p.bMu.RUnlock()
		p.pMu.Unlock()
	}()
	// n and m are protected by bMu and pMu.
	n := int(p.n)
	m := int(p.m)
	if int(n) != len(p.backends) {
		panic("backend number inconsistent, something wrong.")
	}
	calced := len(p.permutation)
	for i := calced; i < n; i++ {
		buf := make([]int, m)
		for j := 0; j < m; j++ {
			offset, err := p.offset(i)
			skip, err := p.skip(i)
			if err != nil {
				return err
			}
			buf[j] = (offset + j*skip) % int(m)
		}
		p.permutation = append(p.permutation, buf)
	}
	return
}

// Populate lookup table (entry[]) based on permutation table.
func (p *magHash) populate() {
	p.pMu.RLock()
	p.bMu.RLock()
	p.eMu.Lock()
	defer func() {
		p.pMu.RUnlock()
		p.bMu.RUnlock()
		p.eMu.Unlock()
	}()

	// n and m are protected by bMu and pMu.
	n := int(p.n)
	m := int(p.m)
	// tracking the next index in entries to be considered for
	// backend i, go to the next entry if it is alreay taken.
	next := make([]int, n)
	for i := 0; i < m; i++ {
		p.entry[i] = -1
	}

	j := 0
	for {
		for i := 0; i < n; i++ {
			c := p.permutation[i][next[i]]
			for p.entry[c] >= 0 {
				next[i] = next[i] + 1
				c = p.permutation[i][next[i]]
			}
			p.entry[c] = i
			next[i] = next[i] + 1
			j++
			if j == m {
				return
			}
		}
	}
}

func (p *magHash) AddBackends(backends []string) (err error) {
	p.bMu.Lock()
	defer p.bMu.Unlock()
	for _, b := range backends {
		if _, ok := p.bIndex[b]; ok {
			continue
		}
		p.backends = append(p.backends, []byte(b))
		p.bIndex[b] = int(p.n)
		p.n++
	}

	go func() {
		p.spawnPermutation()
		p.populate()
	}()

	return
}

func (p *magHash) BackendsNum() (count int) {
	return int(atomic.LoadInt32(&p.n))
}

func (p *magHash) M() (m int) {
	return int(atomic.LoadInt32((&p.m)))
}

func (p *magHash) GetBackend(flow string) (backend string) {
	fnv := fnv.New64()
	fnv.Write([]byte(flow))
	fhash := fnv.Sum64()

	p.eMu.RLock()
	p.bMu.RLock()
	defer func() {
		p.eMu.RUnlock()
		p.bMu.RUnlock()
	}()

	m := int(atomic.LoadInt32((&p.m)))

	if len(p.entry) != m {
		panic("internal index inconsistent")
	}

	bIdx := p.entry[int(fhash%uint64(m))]
	return string(p.backends[bIdx])
}

func (p *magHash) LookupTable() (lookup []string) {
	p.eMu.RLock()
	p.bMu.RLock()
	defer func() {
		p.eMu.RUnlock()
		p.bMu.RUnlock()
	}()

	m := len(p.entry)
	lookup = make([]string, m)
	for i, bIdx := range p.entry {
		lookup[i] = string(p.backends[bIdx])
	}
	return
}

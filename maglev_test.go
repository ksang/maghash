package maghash

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestOffsetSkip(t *testing.T) {
	magh := magHash{
		m:        int32(91),
		backends: make([][]byte, 0),
		bIndex:   make(map[string]int),
		entry:    make([]int, 91),
	}
	bes := make([]string, 0)
	for i := 0; i < 100; i++ {
		bes = append(bes, strconv.FormatInt(rand.Int63(), 10))
	}

	if err := magh.AddBackends(bes); err != nil {
		t.Error(err)
		return
	}

	t.Log("Backend num:", magh.BackendsNum())

	for i, b := range bes {
		offset, err := magh.offset(i)
		if err != nil {
			t.Error("backend:", b, "offset() error:", err)
			continue
		}
		skip, err := magh.skip(i)
		if err != nil {
			t.Error("backend:", b, "skip() error:", err)
			continue
		}
		t.Log("backend:", b, "offset:", offset, "skip:", skip)
	}
}

func printPermutation(p [][]int) {
	if len(p) == 0 {
		return
	}
	n := len(p)
	m := len(p[0])
	for j := 0; j < m; j++ {
		for i := 0; i < n; i++ {
			fmt.Printf("%v\t", p[i][j])
		}
		fmt.Printf("\n")
	}
}

func printLookup(entry []int) {
	for i, e := range entry {
		fmt.Printf("entry: %v\t - backend: %v\n", i, e)
	}
}

func TestPermutation(t *testing.T) {
	magh := magHash{
		// m value must be a prime
		m:        int32(17),
		backends: make([][]byte, 0),
		bIndex:   make(map[string]int),
		entry:    make([]int, 17),
	}
	bes := make([]string, 10)
	for i := 0; i < 10; i++ {
		bes[i] = strconv.FormatInt(rand.Int63(), 10)
	}
	if err := magh.AddBackends(bes); err != nil {
		t.Error(err)
		return
	}
	magh.spawnPermutation(0)
	if magh.m != 17 || magh.n != 10 || len(magh.backends) != 10 {
		t.Error("M/N/len(backends) incorrect")
		t.Errorf("M:%v N:%v len(backends):%v len(bes):%v",
			magh.m, magh.n, len(magh.backends), len(bes))
		return
	}
	if len(magh.permutation) != 10 {
		t.Error("permutation size incorrect:", len(magh.permutation))
		return
	}
	printPermutation(magh.permutation)
}

func TestPopulation(t *testing.T) {
	magh := magHash{
		m:        int32(17),
		backends: make([][]byte, 0),
		bIndex:   make(map[string]int),
		entry:    make([]int, 17),
	}
	bes := make([]string, 10)
	for i := 0; i < 10; i++ {
		bes[i] = strconv.FormatInt(rand.Int63(), 10)
	}
	if err := magh.AddBackends(bes); err != nil {
		t.Error(err)
	}
	magh.spawnPermutation(0)
	printPermutation(magh.permutation)
	magh.populate()
	printLookup(magh.entry)
}

func printSelectionResult(res map[string]int) {
	for backend, selected := range res {
		fmt.Printf("%v \t- selected %v times\n", backend, selected)
	}
	fmt.Println("Total:", len(res))
}

func printLookupTable(lookup []string) {
	backends := make(map[string]int)
	for _, b := range lookup {
		_, ok := backends[b]
		if ok {
			backends[b]++
		} else {
			backends[b] = 1
		}
	}
	for b, c := range backends {
		fmt.Printf("backend:%v \t - %v entries\n", b, c)
	}
}

func TestSingle(t *testing.T) {
	const (
		M int = 65537
		N int = 10
	)

	mh, err := NewMagHash(M)
	if err != nil {
		t.Fatal(err)
	}
	res := make(map[string]int)
	bes := make([]string, N)
	for i := 0; i < N; i++ {
		bes[i] = strconv.FormatInt(rand.Int63(), 10)
	}
	if err := mh.AddBackends(bes); err != nil {
		t.Error(err)
		return
	}
	time.Sleep(time.Second)
	for i := 0; i < 10000; i++ {
		b := mh.GetBackend(strconv.FormatInt(rand.Int63(), 10))
		_, ok := res[b]
		if ok {
			res[b]++
		} else {
			res[b] = 1
		}
	}
	printLookupTable(mh.LookupTable())
	printSelectionResult(res)
}

func TestRemove(t *testing.T) {
	bes := []string{"1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4", "5.5.5.5"}
	rbes := []string{"2.2.2.2", "5.5.5.5"}
	mh := &magHash{
		m:        int32(11),
		backends: make([][]byte, 0),
		bIndex:   make(map[string]int),
		entry:    make([]int, 11),
	}
	if err := mh.AddBackends(bes); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	fmt.Printf("%#+v\n", mh)
	mh.RemoveBackends(rbes)
	time.Sleep(time.Second)
	fmt.Printf("%#+v\n", mh)
	_ = "breakpoint"
}

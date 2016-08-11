# Maghash
Maglev hashing algorithm implementation in Golang for load balance backend selection

####Interface
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

####Documentation:
[Godoc](https://godoc.org/github.com/ksang/maghash)

####Example
	maghash.go:

	package main

	import (
		"github.com/ksang/maghash"
		"log"
		"time"
	)

	func main() {
		// 0 means use the default M value: 65537
		mh, err := maghash.NewMagHash(0)
		if err != nil {
			log.Fatal(err)
		}

		backends := []string{
			"1.1.1.1",
			"2.2.2.2",
			"3.3.3.3",
			"4.4.4.4",
			"5.5.5.5",
		}

		flows := []string{
			"10.0.0.1:80|10.0.0.2:8080|tcp",
			"10.0.0.1:80|10.0.0.2:8081|tcp",
			"10.0.1.1:80|10.0.1.2:65535|udp",
		}

		// Add backend servers to Maglev Hashing
		if err := mh.AddBackends(backends); err != nil {
			log.Fatal(err)
		}

		// The lookup table calculation is Asyncronize, so need to wait
		time.Sleep(time.Second)
		for _, f := range flows {
			log.Println("Backend selected:", mh.GetBackend(f))
		}

	}

######Output
	$ go run maghash.go
	2016/07/24 13:36:32 Backend selected: 4.4.4.4
	2016/07/24 13:36:32 Backend selected: 1.1.1.1
	2016/07/24 13:36:32 Backend selected: 3.3.3.3
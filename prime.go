package maghash

import "math"

const (
	// Prime number is used for modular, it should be larger than
	// the number if backend servers.
	defaultPrime int = 65537
)

func isPrime(n int) bool {
	if n < 2 {
		return false
	}
	for i := 2; i < int(math.Sqrt(float64(n)))+1; i++ {
		if n%i == 0 {
			return false
		}
	}
	return true
}

// A prime number generator, it will return a channel
// and keep generating prime numbers to that channel.
// impelements sieve of Eratosthenes
func primeGenerator() (pc chan int) {
	pc = make(chan int)

	go func() {
		// The running integer that's checked for primeness
		q := 2
		// Maps composites to primes witnessing their compositeness.
		// This is memory efficient, as the sieve is not "run forward"
		// indefinitely, but only as long as required by the current
		// number being tested.
		dict := make(map[int][]int)

		for {
			primes, ok := dict[q]
			if !ok {
				// q is a new prime.
				// Send it and mark its first multiple that isn't
				// already marked in previous iterations
				pc <- q
				dict[q*q] = []int{q}
			} else {
				// q is composite. dict[q] is the list of primes that
				// divide it. Since we've reached q, we no longer
				// need it in the map, but we'll mark the next
				// multiples of its witnesses to prepare for larger
				// numbers
				for _, p := range primes {
					ps, ok := dict[p+q]
					if !ok {
						dict[p+q] = []int{p}
					} else {
						dict[p+q] = append(ps, p)
					}
				}
				delete(dict, q)
			}
			q++
		}
	}()
	return
}

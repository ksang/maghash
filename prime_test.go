package maghash

import (
	"log"
	"testing"
)

func TestIsPrime(t *testing.T) {
	nums := []int{1, 7, 49, 99, 10767, 65537, 705636879}
	res := []bool{false, true, false, false, false, true, false}
	for i, n := range nums {
		log.Println("Testing num:", n)
		if res[i] != isPrime(n) {
			t.Error("Test number:", n, "expected result:", res[i], "actual result:", isPrime(n))
		}
	}
}

func TestPrimeGenerator(t *testing.T) {
	pc := primeGenerator()
	for i := 0; i < 100; i++ {
		p := <-pc
		if !isPrime(p) {
			t.Error("Number generated is not prime:", p)
		} else {
			log.Println("Got prime: ", p)
		}
	}
}

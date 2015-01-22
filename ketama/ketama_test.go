package ketama

import (
	"strconv"
	"testing"
)

func BenchmarkDistribution(b *testing.B) {
	cont, _ := NewFromFile("../testdata/servers.test")
	for i := 0; i < b.N; i++ {
		cont.PickServer(strconv.Itoa(i))
	}
}

func TestContinuumDistribution(t *testing.T) {
	cont, err := NewFromFile("../testdata/servers.test")
	if err != nil {
		t.Fatalf("Failed to create continuum: %v", err)
	}
	serverMap := make(map[string]int)
	for i := 0; i < 10000; i++ {
		server, _ := cont.PickServer(strconv.Itoa(i))
		serverMap[server.String()] += 1
	}

	if len(serverMap) != 10 {
		t.Fatalf("Did not pick 1 or more servers")
	}

	// Value should be roughly equal to 10000 / num_servers
	target := float64(10000) / float64(len(serverMap))
	error := float64(target * .15)
	for _, v := range serverMap {
		v := float64(v)
		if v > target+error || v < target-error {
			t.Errorf("Server had %v keys, should have %v (+- 15%%)", v, target)
		}
	}
}

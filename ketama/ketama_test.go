package ketama

import (
	"fmt"
	"net"
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
	errorRange := float64(target * .18)
	for k, v := range serverMap {
		v := float64(v)
		if v > target+errorRange || v < target-errorRange {
			t.Errorf("Server address %s had %v keys, should have %v (+/- 18%%)", k, v, target)
		}
	}
}

func TestNonWeightedDistribution(t *testing.T) {
	var servers []ServerInfo
	for i := 1; i < 11; i++ {
		ss := fmt.Sprintf("%d0.%d0.%d0.%d0:11211", i, i, i, i)
		a, err := net.ResolveTCPAddr("tcp", ss)
		if err != nil {
			t.Fatalf("Failed resolving: %v", err)
		}
		servers = append(servers, ServerInfo{a, 0})
	}

	cont := New(servers, nil)

	serverMap := make(map[string]int)
	for i := 0; i < 10000; i++ {
		server, err := cont.PickServer(strconv.Itoa(i))
		if err != nil {
			t.Fatalf("Failed picking: %v", err)
		}
		// log.Printf("picked %s %s", server, err)
		serverMap[server.String()] += 1
	}

	if len(serverMap) != 10 {
		t.Fatalf("Did not pick 1 or more servers")
	}

	// Value should be roughly equal to 10000 / num_servers
	target := float64(10000) / float64(len(serverMap))
	errorRange := float64(target * .25)
	for k, v := range serverMap {
		v := float64(v)
		if v > target+errorRange || v < target-errorRange {
			t.Errorf("Server address %s had %v keys, should have %v (+/- 25%%)", k, v, target)
		}
	}
}

func TestContinuumEach(t *testing.T) {
	cont, err := NewFromFile("../testdata/servers.test")
	if err != nil {
		t.Fatalf("Failed to create continuum: %v", err)
	}
	var count int
	cont.Each(func(n net.Addr) error {
		count += 1
		return nil
	})
	if count != 10 {
		t.Fatalf("did not Each() all servers")
	}
}

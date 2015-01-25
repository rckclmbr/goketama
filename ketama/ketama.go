package ketama

import (
	"bufio"
	"crypto/md5"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	ErrNoServers       = errors.New("No valid server definitions found")
	ErrMalformedServer = errors.New("One of the servers is in an invalid format")
)

type mcs struct {
	point uint
	addr  net.Addr
}

type mcsArray []mcs

// A non-zero value for Memory enables ketama weighted
type ServerInfo struct {
	Addr   net.Addr
	Memory uint64
}

type Continuum struct {
	numpoints int
	modtime   time.Time
	array     mcsArray
	newHash   func() hash.Hash
}

func (s mcsArray) Less(i, j int) bool { return s[i].point < s[j].point }
func (s mcsArray) Len() int           { return len(s) }
func (s mcsArray) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s mcsArray) Sort()              { sort.Sort(s) }

// Should be "servername:port\tmemory"
func readServerDefinitions(filename string) (ss []ServerInfo, err error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReader(file)
	ss = make([]ServerInfo, 0)

	for {
		data, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		line := string(data)
		if strings.HasPrefix(line, "#") {
			continue
		}
		addr, mem, err := getServerAddr(line)
		if err != nil {
			return nil, err
		}

		s := ServerInfo{
			Addr:   addr,
			Memory: mem,
		}
		ss = append(ss, s)
	}
	return ss, nil
}

func getServerAddr(line string) (addr net.Addr, mem uint64, err error) {
	record := strings.Split(string(line), "\t")
	if len(record) != 2 {
		return nil, 0, ErrMalformedServer
	}
	mem, err = strconv.ParseUint(record[1], 10, 0)
	if err != nil {
		return nil, 0, ErrMalformedServer
	}
	addr, err = ServerAddr(record[0])
	return addr, mem, err
}

func ServerAddr(addr string) (net.Addr, error) {
	if strings.Contains(addr, "/") {
		return net.ResolveUnixAddr("unix", addr)
	} else {
		return net.ResolveTCPAddr("tcp", addr)
	}
}

func (cont *Continuum) GetHash(in string, offset int) uint {
	h := cont.newHash()
	h.Write([]byte(in))
	digest := h.Sum(nil)
	return ((uint(digest[3+offset*4]) << 24) |
		(uint(digest[2+offset*4]) << 16) |
		(uint(digest[1+offset*4]) << 8) |
		uint(digest[offset*4]))
}

func NewFromFile(filename string) (*Continuum, error) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	serverList, err := readServerDefinitions(filename)
	if err != nil {
		return nil, err
	}
	continuum := New(serverList, nil)
	continuum.modtime = fileInfo.ModTime()
	return continuum, nil
}

// Construct a new Continum for the given servers and hashing function
func New(serverList []ServerInfo, newHash func() hash.Hash) *Continuum {
	numServers := len(serverList)
	if numServers == 0 {
		panic(ErrNoServers)
	}

	var totalMemory uint64
	for i := range serverList {
		totalMemory += serverList[i].Memory
	}

	if newHash == nil {
		newHash = md5.New
	}

	pointsPerServer := 100
	pointsPerHash := 1
	if totalMemory > 0 {
		pointsPerServer = 160
		pointsPerHash = 4
	}

	continuum := &Continuum{
		array:   make([]mcs, numServers*pointsPerServer),
		newHash: newHash,
	}

	cont := 0

	for _, server := range serverList {
		ks := pointsPerServer / pointsPerHash
		if totalMemory > 0 {
			pct := float64(server.Memory) / float64(totalMemory)
			ks = int(math.Floor(pct * 40.0 * float64(numServers)))
		}

		for k := 0; k < ks; k++ {
			ss := fmt.Sprintf("%s-%d", server.Addr, k)
			for h := 0; h < pointsPerHash; h++ {
				continuum.array[cont].point = continuum.GetHash(ss, h)
				continuum.array[cont].addr = server.Addr
				cont++
			}
		}
	}

	continuum.array.Sort()
	continuum.numpoints = cont

	return continuum
}

func (cont *Continuum) PickServer(key string) (net.Addr, error) {

	if len(cont.array) == 0 {
		return nil, ErrNoServers
	}

	h := cont.GetHash(key, 0)
	i := sort.Search(len(cont.array), func(i int) bool { return cont.array[i].point >= h })
	if i >= len(cont.array) {
		i = 0
	}
	return cont.array[i].addr, nil
}

// Each iterates over each server calling the given function
func (cont *Continuum) Each(f func(net.Addr) error) error {
	seen := make(map[net.Addr]bool)
	for _, a := range cont.array {
		if seen[a.addr] {
			continue
		}
		seen[a.addr] = true
		if err := f(a.addr); err != nil {
			return err
		}
	}
	return nil
}

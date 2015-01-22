package ketama

import (
	"bufio"
	"crypto/md5"
	"errors"
	"fmt"
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

type ServerInfo struct {
	Addr   net.Addr
	Memory uint64
}

type Continuum struct {
	numpoints int
	modtime   time.Time
	array     mcsArray
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

func md5Digest(in []byte) []byte {
	h := md5.New()
	h.Write(in)
	return h.Sum(nil)
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

func GetHash(in string) uint {
	digest := md5Digest([]byte(in))
	return ((uint(digest[3]) << 24) |
		(uint(digest[2]) << 16) |
		(uint(digest[1]) << 8) |
		uint(digest[0]))
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
	continuum := New(serverList)
	continuum.modtime = fileInfo.ModTime()
	return continuum, nil
}

func New(serverList []ServerInfo) *Continuum {
	numServers := len(serverList)
	if numServers == 0 {
		panic(ErrNoServers)
	}

	var totalMemory uint64
	for i := range serverList {
		totalMemory += serverList[i].Memory
	}

	continuum := &Continuum{
		array: make([]mcs, numServers*160),
	}

	cont := 0

	for _, server := range serverList {
		pct := float64(server.Memory) / float64(totalMemory)
		ks := int(math.Floor(pct * 40.0 * float64(numServers)))

		for k := 0; k < ks; k++ {
			ss := fmt.Sprintf("%s-%v", server.Addr, k)
			digest := md5Digest([]byte(ss))

			for h := 0; h < 4; h++ {
				continuum.array[cont].point = ((uint(digest[3+h*4]) << 24) |
					(uint(digest[2+h*4]) << 16) |
					(uint(digest[1+h*4]) << 8) |
					uint(digest[h*4]))
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

	h := GetHash(key)
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

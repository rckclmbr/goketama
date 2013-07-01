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
    addr   net.Addr
    memory uint64
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
func readServerDefinitions(filename string) (ss []ServerInfo, memory uint64, err error) {
    file, err := os.Open(filename)
    if err != nil {
        return nil, 0, err
    }
    reader := bufio.NewReader(file)
    ss = make([]ServerInfo, 0)
    memory = uint64(0)

    for {
        data, _, err := reader.ReadLine()
        if err == io.EOF {
            break
        } else if err != nil {
            return nil, 0, err
        }
        line := string(data)
        if strings.HasPrefix(line, "#") {
            continue
        }
        addr, mem, err := getServerAddr(line)
        if err != nil {
            return nil, 0, err
        }

        s := ServerInfo{
            addr:   addr,
            memory: mem,
        }
        ss = append(ss, s)
        memory += mem
    }
    return ss, memory, nil
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
    if strings.Contains(record[0], "/") {
        addr, err = net.ResolveUnixAddr("unix", record[0])
    } else {
        addr, err = net.ResolveTCPAddr("tcp", record[0])
    }
    return
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
    serverList, memory, err := readServerDefinitions(filename)
    if err != nil {
        return nil, err
    }
    numServers := len(serverList)
    if numServers < 1 {
        return nil, ErrNoServers
    }

    continuum := &Continuum{
        array: make([]mcs, numServers*160),
    }

    cont := 0

    for _, server := range serverList {
        pct := float64(server.memory) / float64(memory)
        ks := int(math.Floor(pct * 40.0 * float64(numServers)))

        for k := 0; k < ks; k++ {
            ss := fmt.Sprintf("%s-%v", server.addr.String(), k)
            digest := md5Digest([]byte(ss))

            for h := 0; h < 4; h++ {
                continuum.array[cont].point = ((uint(digest[3+h*4]) << 24) |
                    (uint(digest[2+h*4]) << 16) |
                    (uint(digest[1+h*4]) << 8) |
                    uint(digest[h*4]))
                continuum.array[cont].addr = server.addr
                cont++
            }
        }
    }

    continuum.array.Sort()
    continuum.numpoints = cont
    continuum.modtime = fileInfo.ModTime()

    return continuum, nil
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

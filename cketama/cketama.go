package cketama

/*
#cgo LDFLAGS: -lketama
#include <ketama.h>
#include <stdlib.h>
char* get_ip(mcs* m) {
    return m->ip;
}
char* get_ketama_ip(char* key, ketama_continuum cont) {
    mcs* m = ketama_get_server(key, cont);
    return m->ip;
}
*/
import "C"
import (
    "errors"
    "net"
    "reflect"
    "strings"
    "unsafe"
)

type Continuum struct {
    cont    *C.ketama_continuum
    addrMap map[string]net.Addr
}

func GetHash(in string) uint {
    val := C.CString(in)
    defer C.free(unsafe.Pointer(val))
    hashInt := C.ketama_hashi(val)
    return uint(hashInt)
}

func NewFromFile(filename string) (*Continuum, error) {
    var cont C.ketama_continuum
    fn := C.CString(filename)
    defer C.free(unsafe.Pointer(fn))
    rv := C.ketama_roll(&cont, fn)
    if rv == 0 {
        errMsg := C.GoString(C.ketama_error())
        return nil, errors.New(errMsg)
    }

    addrMap := make(map[string]net.Addr)

    var mcsSlice reflect.SliceHeader
    mcsSlice.Data = uintptr(cont.array)
    mcsSlice.Len = int(cont.numpoints)
    mcsSlice.Cap = int(cont.numpoints)
    mcss := *(*[]C.mcs)(unsafe.Pointer(&mcsSlice))
    for _, mcs := range mcss {
        ip := C.GoString(C.get_ip(&mcs))
        addrMap[ip], _ = getServerAddr(ip)
    }

    return &Continuum{&cont, addrMap}, nil
}

func (cont *Continuum) String() {
    C.ketama_print_continuum(*cont.cont)
}

func (cont *Continuum) PickServer(key string) (net.Addr, error) {
    ckey := C.CString(key)
    defer C.free(unsafe.Pointer(ckey))
    cip := C.get_ketama_ip(ckey, *cont.cont)
    return cont.addrMap[C.GoString(cip)], nil
}

func getServerAddr(ip string) (addr net.Addr, err error) {
    if strings.Contains(ip, "/") {
        addr, err = net.ResolveUnixAddr("unix", ip)
    } else {
        addr, err = net.ResolveTCPAddr("tcp", ip)
    }
    return
}

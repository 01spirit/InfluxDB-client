/*
Copyright 2011 The gomemcache AUTHORS

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package memcache

import (
	"hash/crc32"
	"net"
	"strings"
	"sync"
)

// ServerSelector is the interface that selects a memcache server		根据 item 的 key 选择 memcache server
// as a function of the item's key.
//
// All ServerSelector implementations must be safe for concurrent use	必须支持并发
// by multiple goroutines.
type ServerSelector interface {
	// PickServer returns the server address that a given item
	// should be shared onto.
	PickServer(key string) (net.Addr, error) //返回给定 item 应该被分享到的服务器地址
	Each(func(net.Addr) error) error
}

// ServerList is a simple ServerSelector. Its zero value is usable.
type ServerList struct {
	mu    sync.RWMutex //锁
	addrs []net.Addr   //服务器地址数组
}

// staticAddr caches the Network() and String() values from any net.Addr.
type staticAddr struct {
	ntw, str string
}

func newStaticAddr(a net.Addr) net.Addr {
	return &staticAddr{
		ntw: a.Network(),
		str: a.String(),
	}
}

func (s *staticAddr) Network() string { return s.ntw }
func (s *staticAddr) String() string  { return s.str }

// SetServers changes a ServerList's set of servers at runtime and is
// safe for concurrent use by multiple goroutines.
//
// Each server is given equal weight. A server is given more weight
// if it's listed multiple times.
//
// SetServers returns an error if any of the server names fail to
// resolve. No attempt is made to connect to the server. If any error
// is returned, no changes will be made to the ServerList.
func (ss *ServerList) SetServers(servers ...string) error { //从一组服务器字符串中解析出地址并存入 ServerList
	naddr := make([]net.Addr, len(servers))
	for i, server := range servers {
		if strings.Contains(server, "/") { //server 中是否包含 "/"
			addr, err := net.ResolveUnixAddr("unix", server) // ResolveUnixAddr returns an address of Unix domain socket end point.
			if err != nil {
				return err
			}
			naddr[i] = newStaticAddr(addr)
		} else {
			tcpaddr, err := net.ResolveTCPAddr("tcp", server) // ResolveTCPAddr returns an address of TCP end point.
			if err != nil {
				return err
			}
			naddr[i] = newStaticAddr(tcpaddr)
		}
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.addrs = naddr //把解析出的地址存入 ServerList		地址可能是 Unix 或 TCP
	return nil
}

// Each iterates over each server calling the given function	遍历每个调用该函数的服务器
func (ss *ServerList) Each(f func(net.Addr) error) error {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	for _, a := range ss.addrs {
		if err := f(a); nil != err {
			return err
		}
	}
	return nil
}

// keyBufPool returns []byte buffers for use by PickServer's call to
// crc32.ChecksumIEEE to avoid allocations. (but doesn't avoid the
// copies, which at least are bounded in size and small)
var keyBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 256)
		return &b
	},
}

// 根据 key 返回相应的服务器地址
func (ss *ServerList) PickServer(key string) (net.Addr, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	if len(ss.addrs) == 0 {
		return nil, ErrNoServers
	}
	if len(ss.addrs) == 1 {
		return ss.addrs[0], nil
	}
	bufp := keyBufPool.Get().(*[]byte)
	n := copy(*bufp, key)
	cs := crc32.ChecksumIEEE((*bufp)[:n]) // ChecksumIEEE returns the CRC-32 checksum of data
	keyBufPool.Put(bufp)

	return ss.addrs[cs%uint32(len(ss.addrs))], nil
}

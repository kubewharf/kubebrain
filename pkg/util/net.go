// Copyright 2022 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"

	"k8s.io/klog/v2"
)

var (
	onceInitPrivateIPBlocks sync.Once
	privateIPBlocks         []*net.IPNet
)

func getPrivateIPBlocks() []*net.IPNet {
	initPrivateIPBlocks()
	return privateIPBlocks
}

func initPrivateIPBlocks() {
	onceInitPrivateIPBlocks.Do(func() {
		for _, cidr := range []string{
			"10.0.0.0/8",     // RFC1918
			"172.16.0.0/12",  // RFC1918
			"192.168.0.0/16", // RFC1918
			"fd00::/8",       // RFC4193
		} {
			_, block, err := net.ParseCIDR(cidr)
			if err != nil {
				continue
			}
			privateIPBlocks = append(privateIPBlocks, block)
		}
	})
}

// GetHost returns the host for accessing this node
func GetHost() (host string) {
	ips, err := getLocalIPs()
	if err != nil || len(ips) == 0 {
		klog.ErrorS(err, "failed to get valid ip")
		return
	}

	// sort for trying to pin host on a specified
	sort.Slice(ips, preferMinimalPrivate(ips))

	for _, ip := range ips {
		if ip.getHost() != "" {
			return ip.getHost()
		}
	}
	return ""
}

func preferMinimalIPv4(ips []ip) func(i, j int) bool {
	return func(i, j int) bool {
		if ips[i].ipv4 != "" && ips[j].ipv4 == "" {
			return true
		}
		if ips[i].ipv4 == "" && ips[j].ipv4 != "" {
			return false
		}

		if ips[i].ipv4 != "" && ips[j].ipv4 != "" {
			return strings.Compare(ips[i].ipv4, ips[j].ipv4) < 0
		}

		return strings.Compare(ips[i].ipv6, ips[j].ipv6) < 0
	}
}

func preferMinimalPrivate(ips []ip) func(i, j int) bool {
	return func(i, j int) bool {
		// prefer private ips
		// sort priority:
		// 1. A class private address
		// 2. B class private address
		// 3. C class private address
		// 4. other address

		if ips[i].isPrivate && !ips[j].isPrivate {
			return true
		} else if !ips[i].isPrivate && ips[j].isPrivate {
			return false
		}

		return preferMinimalIPv4(ips)(i, j)
	}
}

func isPrivate(ip net.IP) bool {
	privateIPBlocks := getPrivateIPBlocks()
	for _, privateIPBlock := range privateIPBlocks {
		if privateIPBlock.Contains(ip) {
			return true
		}
	}
	return false
}

type ip struct {
	ipv4 string
	ipv6 string

	ip        net.IP
	isPrivate bool
}

func (i ip) getHost() string {
	// prefer ipv4
	if i.ipv4 != "" && !forceInIPv6Mode {
		return i.ipv4
	}

	// ipv6 only
	return fmt.Sprintf("[%s]", i.ipv6)
}

func (i ip) valid() bool {
	return i.ipv4 != "" || i.ipv6 != ""
}

func getLocalIPs() (ret []ip, err error) {
	inters, err := net.Interfaces()
	if err != nil {
		panic(err)
	}

	for _, inter := range inters {
		// check if the interface is opened and it not a loop-back interface
		if inter.Flags&net.FlagUp != 0 && !strings.HasPrefix(inter.Name, "lo") {
			// get all addresses under the interface
			addrs, err := inter.Addrs()
			if err != nil {
				continue
			}

			i := getIp(addrs)
			if !i.valid() {
				continue
			}
			klog.V(klogLevel).InfoS("get ip address on net interface", "name", inter.Name, "ip", i.getHost())
			ret = append(ret, i)
		}
	}
	return
}

func getIp(addrs []net.Addr) (ret ip) {
	for _, addr := range addrs {
		// todo: if it's possible that there are multiple eligible ips?
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && !ipnet.IP.IsLinkLocalUnicast() {
			ip16 := ipnet.IP.To16()
			if ip16.To4() == nil {
				// is ipv6 address
				ret.ipv6 = ip16.String()
			} else if !forceInIPv6Mode {
				// is ipv4 address
				ret.ipv4 = ip16.String()
			}
			ret.ip = ipnet.IP
			ret.isPrivate = isPrivate(ipnet.IP)
			if ret.isPrivate && skipPrivateAddr {
				ret = ip{}
				klog.V(klogLevel).InfoS("skip private address", "addr", ret.getHost())
				continue
			}

			if ret.valid() {
				return
			}
		}
	}
	return
}

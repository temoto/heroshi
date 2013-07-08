package heroshi

import (
	"errors"
	"github.com/miekg/dns"
	"github.com/temoto/heroshi/limitmap" // Temporary location
	"log"
	"net"
	"time"
)

var limit *limitmap.Semaphore

func ResolveName(name, nameserver string) (addrs []net.IP, rtt time.Duration, err error) {
	limit.Acquire()
	defer limit.Release()

	dnsClient := &dns.Client{
		Net:         "udp",
		ReadTimeout: 5 * time.Second,
	}
	dnsMessage := new(dns.Msg)
	dnsMessage.MsgHdr.RecursionDesired = true
	dnsMessage.SetQuestion(dns.Fqdn(name), dns.TypeA)
	addrs = make([]net.IP, 0)

Redo:
	var reply *dns.Msg
	reply, rtt, err = dnsClient.Exchange(dnsMessage, nameserver)
	if err != nil {
		return nil, rtt, err
	}
	if reply.Rcode != dns.RcodeSuccess {
		err = errors.New("ResolveName(" + name + ", " + nameserver + "): " + dns.RcodeToString[reply.Rcode])
		return nil, rtt, err
	}
	for _, a := range reply.Answer {
		if rra, ok := a.(*dns.A); ok {
			addrs = append(addrs, rra.A)
		}
	}
	if reply.MsgHdr.Truncated {
		if dnsClient.Net != "tcp" {
			log.Printf("ResolveName: ;; Truncated, trying TCP\n")
			dnsClient.Net = "tcp"
			goto Redo
		}
		if reply.MsgHdr.Truncated {
			log.Printf("ResolveName: ;; Truncated\n")
		}
	}
	return
}

func init() {
	limit = limitmap.NewSemaphore(2)
}

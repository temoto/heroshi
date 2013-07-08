package heroshi

import (
	"errors"
	"github.com/miekg/dns"
	"github.com/temoto/heroshi/limitmap" // Temporary location
	"log"
	"net"
	"time"
)

var (
	DnsTimeout     = 20 * time.Second
	DnsStepTimeout = 2 * time.Second
	DnsRetryWait   = 1 * time.Second

	dnsLimit *limitmap.Semaphore
)

func ResolveName(name, nameserver string) (addrs []net.IP, duration time.Duration, err error) {
	dnsLimit.Acquire()
	defer dnsLimit.Release()

	dnsClient := &dns.Client{
		Net:          "udp",
		ReadTimeout:  DnsStepTimeout,
		WriteTimeout: DnsStepTimeout,
	}
	dnsMessage := new(dns.Msg)
	dnsMessage.MsgHdr.RecursionDesired = true
	dnsMessage.SetQuestion(dns.Fqdn(name), dns.TypeA)
	addrs = make([]net.IP, 0)
	retryWait := DnsRetryWait

Redo:
	var reply *dns.Msg
	var rtt time.Duration
	reply, rtt, err = dnsClient.Exchange(dnsMessage, nameserver)
	duration += rtt
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
			log.Printf("ResolveName: %s Temporary error %s", name, netErr.Error())
			if duration+retryWait < DnsTimeout {
				time.Sleep(retryWait)
				retryWait *= 2
				goto Redo
			}
		}

		return nil, duration, err
	}
	if reply.Rcode != dns.RcodeSuccess {
		err = errors.New("ResolveName(" + name + ", " + nameserver + "): " + dns.RcodeToString[reply.Rcode])
		return nil, duration, err
	}
	for _, a := range reply.Answer {
		if rra, ok := a.(*dns.A); ok {
			addrs = append(addrs, rra.A)
		}
		if rra6, ok := a.(*dns.AAAA); ok {
			addrs = append(addrs, rra6.AAAA)
		}
	}
	if reply.MsgHdr.Truncated {
		if dnsClient.Net != "tcp" {
			log.Printf("ResolveName: %s Truncated, trying TCP", name)
			dnsClient.Net = "tcp"
			goto Redo
		}
		if reply.MsgHdr.Truncated {
			log.Printf("ResolveName: %s Truncated", name)
		}
	}
	return
}

func SetDnsConcurrency(n uint) {
	dnsLimit = limitmap.NewSemaphore(n)
}

func init() {
	SetDnsConcurrency(2)
}

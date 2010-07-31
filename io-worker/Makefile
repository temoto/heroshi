# Heroshi io-worker in Go.

include $(GOROOT)/src/Make.$(GOARCH)

TARG  := io-worker
GOFMT := gofmt -spaces=true -tabindent=false -tabwidth=4

GOFILES := \
	fetch.go \
	worker.go \
	worker_main.go

include $(GOROOT)/src/Make.cmd

format:
	${GOFMT} -w ${GOFILES}


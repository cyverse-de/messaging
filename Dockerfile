FROM golang:1.14

COPY . /go/src/github.com/cyverse-de/messaging

WORKDIR /go/src/github.com/cyverse-de/messaging

RUN go get github.com/jstemmer/go-junit-report

RUN go build github.com/cyverse-de/messaging

CMD go test -v github.com/cyverse-de/messaging | tee /dev/stderr | go-junit-report

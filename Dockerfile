FROM golang:1.16

ADD https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh /bin/wait-for-it.sh
RUN chmod +x /bin/wait-for-it.sh

COPY . /go/src/github.com/cyverse-de/messaging

WORKDIR /go/src/github.com/cyverse-de/messaging

RUN go get github.com/jstemmer/go-junit-report

CMD go test -v github.com/cyverse-de/messaging | tee /dev/stderr | go-junit-report

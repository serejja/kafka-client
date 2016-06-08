kafka-client
============
[![GoDoc](https://godoc.org/github.com/serejja/kafka-client?status.png)](https://godoc.org/github.com/serejja/kafka-client)
[![Build Status](https://travis-ci.org/serejja/kafka-client.svg?branch=master)](https://travis-ci.org/serejja/kafka-client)
[![Go Report Card](https://goreportcard.com/badge/github.com/serejja/kafka-client)](https://goreportcard.com/report/github.com/serejja/kafka-client)

Apache Kafka low level client in Golang

***Installation:***

1. Install Golang [http://golang.org/doc/install](http://golang.org/doc/install)
2. Make sure env variables GOPATH and GOROOT exist and point to correct places
3. `go get github.com/serejja/kafka-client`
4. `go test -v` to make sure it works

You may also want to spin up a local broker at `localhost:9092` for the functional test to work as well (it will be skipped otherwise).
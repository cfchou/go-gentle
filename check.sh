#! /bin/bash

pushd gentle;
go vet
go fmt
popd

go test -v -race github.com/cfchou/go-gentle/gentle | grep '\-\-\-'

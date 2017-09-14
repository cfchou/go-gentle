PACKAGE := $(shell glide nv)

.DEFAULT_GOAL := format-and-test

.PHONY: format-and-test
format-and-test: format test

.PHONY: clean
clean:
	@rm -rf cover_*.out *.log

.PHONY: test
test: clean
	@rm -rf cover_*.out
	go test -v -race -covermode=atomic -coverprofile=cover_all.out ./extra/log
	go test -v -race -covermode=atomic -coverprofile=cover_extra_metric.out ./extra/metric
	@env grep -v mode cover_extra_metric.out >>cover_all.out
	go test -v -race -covermode=atomic -coverprofile=cover_gentle.out ./gentle -quickchecks 50 -level crit
	@env grep -v mode cover_gentle.out >>cover_all.out

.PHONY: format
format:
	go fmt $(PACKAGE)
	go vet $(PACKAGE)

.PHONY: lint
lint: format
	golint $(PACKAGE)

.PHONY: cover
cover: test
	go tool cover -html=cover_all.out -o cover.html
	make clean

.PHONY: coveralls
coveralls: test
	goveralls -coverprofile=cover_all.out -service=travis-ci
	make clean



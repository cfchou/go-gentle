PACKAGE := $(shell glide nv)

.DEFAULT_GOAL := format-and-test

.PHONY: format-and-test
format-and-test: format test

.PHONY: test
test:
	go test -v -race -cover ./extra/...
	go test -v -race -cover ./gentle/... -quickchecks 50 -level crit

.PHONY: format
format:
	go fmt $(PACKAGE)
	go vet $(PACKAGE)

.PHONY: cover
cover:
	@rm -rf cover.out
	go test -v -coverprofile=cover.out ./gentle/... -quickchecks 10 -level crit
	go tool cover -html=cover.out -o cover.html

.PHONY: lint
lint:
	go fmt $(PACKAGE)
	go vet $(PACKAGE)
	golint $(PACKAGE)
	@# Run again with magic to exit non-zero if golint outputs anything.
	@! (golint ./gentle/... | read dummy)
	go vet $(PACKAGE)

.PHONY: coveralls
coveralls:
	@rm -rf cover.out
	go test -v -covermode=count -coverprofile=cover.out ./gentle/... -quickchecks 50 -level crit
	goveralls -coverprofile=cover.out -service=travis-ci


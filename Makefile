PACKAGE := $(shell glide nv)

.DEFAULT_GOAL := format-and-test

.PHONY: format-and-test
format-and-test: format test

.PHONY: test
test:
	go test -v -race -cover $(PACKAGE) -quickchecks 50 -level crit

.PHONY: format
format:
	go fmt $(PACKAGE)
	go vet $(PACKAGE)

.PHONY: cover
cover:
	@rm -rf cover.out
	go test -coverprofile cover.out $(PACKAGE) -quickchecks 10
	go tool cover -html=cover.out -o cover.html

.PHONY: lint
lint:
	go fmt $(PACKAGE)
	go vet $(PACKAGE)
	golint $(PACKAGE)
	@# Run again with magic to exit non-zero if golint outputs anything.
	@! (golint ./gentle/... | read dummy)
	go vet $(PACKAGE)


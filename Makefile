PACKAGE := $(shell glide nv)

.DEFAULT_GOAL := format-and-test

.PHONY: format-and-test
format-and-test: format test

.PHONY: clean
clean:
	@rm -rf cover_*.out

.PHONY: test
test: clean
	#go test -v -race -cover ./extra/...
	#go test -v -race -cover ./gentle/... -quickchecks 50 -level crit
	@rm -rf cover_*.out
	go test -v -race -covermode=atomic -coverprofile=cover_all.out ./extra/...
	go test -v -race -covermode=atomic -coverprofile=cover_gentle.out ./gentle/... -quickchecks 50 -level crit
	@env grep -v mode cover_gentle.out >>cover_all.out

.PHONY: format
format:
	go fmt $(PACKAGE)
	go vet $(PACKAGE)

.PHONY: lint
lint: format
	golint $(PACKAGE)
	@# Run again with magic to exit non-zero if golint outputs anything.
	@! (golint ./gentle/... | read dummy)
	go vet $(PACKAGE)

.PHONY: cover
cover: test
	go tool cover -html=cover_all.out -o cover.html
	make clean

.PHONY: coveralls
coveralls: test
	goveralls -coverprofile=cover_all.out -service=travis-ci
	make clean



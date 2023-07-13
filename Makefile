.PHONY: all deps ask check

##
##
all:
	@go test ./...

##
##
deps: check ask
	$(shell $(shell go list -u -f "{{if (and (not (or .Main .Indirect)) .Update)}}go get -d {{.Path}}@{{.Update.Version}} ; {{end}}" -m all))
	go mod tidy

check:
	@go list -u -f '{{if (and (not (or .Main .Indirect)) .Update)}}{{.Path}}: {{.Version}} -> {{.Update.Version}}{{end}}' -m all

ask:
	@echo "\nUpdate go.mod? [y/N] " && read ans && [ $${ans:-N} = y ]

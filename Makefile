.PHONY: test build pack

test:
	go test ./...

build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w -extldflags "-static"' -o dist/garage-reconciler ./

pack: build
	tar -C dist -czf dist/garage-reconciler-linux-amd64.tgz garage-reconciler
	sha256sum dist/garage-reconciler-linux-amd64.tgz > dist/garage-reconciler-linux-amd64.tgz.sha256

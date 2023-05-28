REGISTRY_NAME?=docker.io/iejalapeno
IMAGE_VERSION?=latest

.PHONY: all sr-node container push clean test

ifdef V
TESTARGS = -v -args -alsologtostderr -v 5
else
TESTARGS =
endif

all: sr-node

sr-node:
	mkdir -p bin
	$(MAKE) -C ./cmd compile-sr-node

sr-node-container: sr-node
	docker build -t $(REGISTRY_NAME)/sr-node:$(IMAGE_VERSION) -f ./build/Dockerfile.sr-node .

push: sr-node-container
	docker push $(REGISTRY_NAME)/sr-node:$(IMAGE_VERSION)

clean:
	rm -rf bin

test:
	GO111MODULE=on go test `go list ./... | grep -v 'vendor'` $(TESTARGS)
	GO111MODULE=on go vet `go list ./... | grep -v vendor`

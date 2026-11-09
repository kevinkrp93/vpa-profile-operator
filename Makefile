IMAGE ?= ghcr.io/kevinkrp93/vpa-profile-operator:latest

.PHONY: build test image deploy

build:
	go build ./cmd/operator

test:
	go test ./...

image:
	docker build -t $(IMAGE) .

deploy:
	kubectl apply -f deploy/operator.yaml

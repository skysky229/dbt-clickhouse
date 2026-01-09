IMAGE_NAME ?= skysky229/dbt-clickhouse
TAG ?= latest

.PHONY: build push

build:
	docker build -f images/Dockerfile -t $(IMAGE_NAME):$(TAG) .

push:
	docker push $(IMAGE_NAME):$(TAG)

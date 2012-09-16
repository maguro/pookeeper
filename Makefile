HERE = $(shell pwd)
BIN = $(HERE)/bin
PYTHON = $(BIN)/python

PIP_DOWNLOAD_CACHE ?= $(HERE)/.pip_cache
INSTALL = $(BIN)/pip install
INSTALL += --download-cache $(PIP_DOWNLOAD_CACHE) -U --use-mirrors

BUILD_DIRS = bin build include lib lib64 man share .Python .pip_cache

ZOOKEEPER = $(BIN)/zookeeper
ZOOKEEPER_VERSION ?= 3.3.6
ZOOKEEPER_PATH ?= $(ZOOKEEPER)

.PHONY: all build clean test zookeeper clean-zookeeper

all: build

$(PYTHON):
	virtualenv --distribute .

build: $(PYTHON)
	$(INSTALL) -r requirements.txt
	$(PYTHON) setup.py develop

clean:
	rm -rf $(BUILD_DIRS)

test:
	export ZOOKEEPER_PATH=$(ZOOKEEPER_PATH) && \
	$(BIN)/nosetests -d --with-coverage

html:
	cd docs && \
	make html

$(ZOOKEEPER):
	@echo "Installing Zookeeper"
	mkdir -p bin
	cd bin && \
	curl --progress-bar http://apache.osuosl.org/zookeeper/zookeeper-$(ZOOKEEPER_VERSION)/zookeeper-$(ZOOKEEPER_VERSION).tar.gz | tar -zx
	mv bin/zookeeper-$(ZOOKEEPER_VERSION) bin/zookeeper
	cd bin/zookeeper && ant compile
	chmod a+x bin/zookeeper/bin/zkServer.sh
	@echo "Finished installing Zookeeper"

zookeeper: $(ZOOKEEPER)

clean-zookeeper:
	rm -rf zookeeper bin/zookeeper

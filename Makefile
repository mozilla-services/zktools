APPNAME = zktools
HERE = $(shell pwd)
BIN = $(HERE)/bin
NOSE = $(BIN)/nosetests -s --with-xunit
PYTHON = $(BIN)/python
ZOOKEEPER = $(BIN)/zookeeper

.PHONY: zookeeper

$(ZOOKEEPER):
	mkdir -p bin
	cd bin && \
	curl --silent http://mirrors.ibiblio.org/apache//zookeeper/stable/zookeeper-3.3.4.tar.gz | tar -zx
	mv bin/zookeeper-3.3.4 bin/zookeeper
	cd bin/zookeeper && ant compile
	cp zoo.cfg bin/zookeeper/conf/

zookeeper: 	$(ZOOKEEPER)

all: build

$(BIN)/python:
	virtualenv-2.6 --no-site-packages --distribute .

build: $(BIN)/python
	$(PYTHON) setup.py develop
	$(BIN)/pip install nose
	$(BIN)/pip install Mock

test:
	$(BIN)/zookeeper/bin/zkServer.sh start $(HERE)/zoo.cfg
	$(NOSE) --with-coverage --cover-package=$(APPNAME) --cover-inclusive $(APPNAME)
	$(BIN)/zookeeper/bin/zkServer.sh stop $(HERE)/zoo.cfg

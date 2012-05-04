APPNAME = zktools
HERE = $(shell pwd)
BIN = $(HERE)/bin
NOSE = $(BIN)/nosetests -s --with-xunit
PYTHON = $(BIN)/python
ZOOKEEPER = $(BIN)/zookeeper
ZOOKEEPER_VERSION = 3.4.3
SW = sw
BUILD_DIRS = bin build deps include lib lib64 man

.PHONY: all zookeeper
.SILENT: lib python pip $(ZOOKEEPER) zookeeper

all: build

$(BIN)/python:
	python $(SW)/virtualenv.py --distribute . >/dev/null 2>&1
	rm distribute-0.6.*.tar.gz

$(BIN)/pip: $(BIN)/python

$(ZOOKEEPER):
	@echo "Installing Zookeeper"
	mkdir -p bin
	cd bin && \
	curl --silent http://apache.osuosl.org/zookeeper/zookeeper-$(ZOOKEEPER_VERSION)/zookeeper-$(ZOOKEEPER_VERSION).tar.gz | tar -zx
	mv bin/zookeeper-$(ZOOKEEPER_VERSION) bin/zookeeper
	cd bin/zookeeper && ant compile >/dev/null 2>&1
	cd bin/zookeeper/src/c && \
	./configure >/dev/null 2>&1 && \
	make >/dev/null 2>&1
	cd bin/zookeeper/src/contrib/zkpython && \
	mv build.xml old_build.xml && \
	cat old_build.xml | sed 's|executable="python"|executable="../../../../../bin/python"|g' > build.xml && \
	ant install >/dev/null 2>&1
	cd bin/zookeeper/bin && \
	mv zkServer.sh old_zkServer.sh && \
	cat old_zkServer.sh | sed 's|    $$JAVA "-Dzoo|    exec $$JAVA "-Dzoo|g' > zkServer.sh && \
	chmod a+x zkServer.sh
	cp zoo.cfg bin/zookeeper/conf/
	@echo "Finished installing Zookeeper"

zookeeper: 	$(ZOOKEEPER)

clean-env:
	rm -rf $(BUILD_DIRS)

clean-cassandra:
	rm -rf cassandra bin/cassandra

clean-nginx:
	rm -rf bin/nginx

clean-zookeeper:
	rm -rf zookeeper bin/zookeeper

clean: clean-env

build: $(BIN)/python zookeeper
	$(PYTHON) setup.py develop
	$(BIN)/pip install nose
	$(BIN)/pip install Mock

test:
	$(BIN)/zookeeper/bin/zkServer.sh start $(HERE)/zoo.cfg
	$(NOSE) --with-coverage --cover-package=$(APPNAME) --cover-inclusive $(APPNAME)
	$(BIN)/zookeeper/bin/zkServer.sh stop $(HERE)/zoo.cfg

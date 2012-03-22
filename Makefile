HERE = $(shell pwd)
BIN = $(HERE)/bin
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

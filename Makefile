###########################################
# Port Concentrator
# -------------------------------------
# file       : Makefile
# author     : Ben Kietzman
# begin      : 2014-02-26
# copyright  : kietzman.org
# email      : ben@kietzman.org
###########################################

MAKEFLAGS="-j ${C}"
prefix=/usr/local

all: bin/concentrator

bin/concentrator: ../common/libcommon.a obj/concentrator.o
	-if [ ! -d bin ]; then mkdir bin; fi;
	g++ -ggdb -o bin/concentrator obj/concentrator.o $(LDFLAGS) -L/data/extras/lib -L../common -lcommon -lb64 -lcrypto -lexpat -lmjson -lpthread -lssl -ltar -lz

../common/libcommon.a:
	cd ../common; ./configure; make;

obj/concentrator.o: concentrator.cpp
	-if [ ! -d obj ]; then mkdir obj; fi;
	g++ -Wall -ggdb -c concentrator.cpp -o obj/concentrator.o $(CPPFLAGS) -I/data/extras/include -I../common

install: bin/concentrator
	-if [ ! -d $(prefix)/portconcentrator ]; then mkdir $(prefix)/portconcentrator; fi;
	install --mode=777 bin/concentrator $(prefix)/portconcentrator/concentrator_preload
	if [ ! -f /lib/systemd/system/concentrator.service ]; then install --mode=644 concentrator.service /lib/systemd/system/; fi;

clean:
	-rm -fr obj bin

uninstall:
	-rm -fr $(prefix)/portconcentrator

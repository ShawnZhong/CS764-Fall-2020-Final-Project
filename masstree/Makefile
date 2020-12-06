CC=g++
CFLAGS=-Wall -g -std=c++14 -no-pie

.SUFFIXES: .o .cpp .h

SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./system/
INCLUDE = -I. -I./benchmarks -I./concurrency_control -I./storage -I./system \
	-I./libs/cicada-engine/src \
	-I./libs/ \
	-I./libs/cicada-engine/src/mica/util/cityhash

#CFLAGS += $(INCLUDE) -D NOGRAPHITE=1 -Werror -O3
CFLAGS += $(INCLUDE) -D NOGRAPHITE=1 -Wno-unused-function -O3 #-fno-omit-frame-pointer
#CFLAGS += $(INCLUDE) -D NOGRAPHITE=1 -Wno-unused-function -Og -fno-omit-frame-pointer
#CFLAGS += $(INCLUDE) -D NOGRAPHITE=1 -Wno-unused-function -O0 -fno-omit-frame-pointer

#LDFLAGS = -Wall -L. -L./libs -pthread -g -lrt -std=c++0x -O3 -ljemalloc
LDFLAGS = -Wall -L. -L./libs -L./libs/cicada-engine/build -pthread -g -lrt -std=c++14 -lcommon -lnuma -ljemalloc -O3
LDFLAGS += $(CFLAGS)

CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))
OBJS = $(CPPS:.cpp=.o)
DEPS = $(CPPS:.cpp=.d)

all:rundb

rundb : $(OBJS) ./libs/cicada-engine/build/libcommon.a \
	./libs/silo/out-perf.masstree/allocator.o \
	./libs/silo/out-perf.masstree/compiler.o \
	./libs/silo/out-perf.masstree/core.o \
	./libs/silo/out-perf.masstree/counter.o \
	./libs/silo/out-perf.masstree/json.o \
	./libs/silo/out-perf.masstree/straccum.o \
	./libs/silo/out-perf.masstree/string.o \
	./libs/silo/out-perf.masstree/ticker.o \
	./libs/silo/out-perf.masstree/rcu.o
	$(CC) -o $@ $^ $(LDFLAGS)

-include $(OBJS:%.o=%.d)

%.d: %.cpp
	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<

%.o: %.cpp
	$(CC) -c $(CFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f rundb $(OBJS) $(DEPS)

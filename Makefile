
# Use this for strict compilation check(will work on clang 3.8+)
CXX := clang++
EXTRA_CXXFLAGS := -fsanitize=address -Wall -Werror -Weverything -Wno-c++11-long-long -Wno-c++98-compat -Wno-padded
EXTRA_LDFLAGS := -fsanitize=address
#EXTRA_CXXFLAGS := -Wall -Werror -Weverything -Wno-c++11-long-long -Wno-c++98-compat -Wno-padded

.PHONY: clean all

all:
	clang++  $(EXTRA_CXXFLAGS) -std=c++11 -g -O2 -o csv_parser_example csv_parser_example.cc ${EXTRA_LDFLAGS} -pthread


## ====================================
## With Ryu
##
#
#INCFLAGS = -Iryu
#EXTRA_CXXFLAGS += -DNANOCSV_WITH_RYU=1
#
#all: csv_parser_example
#
#csv_parser_example: csv_parser_example.o d2s.o f2s.o s2f.o s2d.o
#	$(CXX) $(EXTRA_LDFLAGS) -g -O2 -o $@ $^
#
#csv_parser_example.o: csv_parser_example.cc nanocsv.h
#	$(CXX)  $(INCFLAGS) $(EXTRA_CXXFLAGS) -std=c++11 -g -O2 -c -o $@ $<
#
#d2s.o: ryu/d2s.c
#	$(CC) $(INCFLAGS) $(CFLAGS) -c -o $@ $<
#
#f2s.o: ryu/f2s.c
#	$(CC) $(INCFLAGS) $(CFLAGS) -c -o $@ $<
#
#s2f.o: ryu/s2f.c
#	$(CC) $(INCFLAGS) $(CFLAGS) -c -o $@ $<
#
#s2d.o: ryu/s2d.c
#	$(CC) $(INCFLAGS) $(CFLAGS) -c -o $@ $<
#
#clean:
#	@rm -rf csv_parser_example csv_parser_example.o d2s.o f2s.o s2d.o s2f.o

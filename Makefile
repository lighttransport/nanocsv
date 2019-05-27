# Use this for strict compilation check(will work on clang 3.8+)
#EXTRA_CXXFLAGS := -fsanitize=address -Wall -Werror -Weverything -Wno-c++11-long-long -Wno-c++98-compat -Wno-padded
EXTRA_CXXFLAGS := -Wall -Werror -Weverything -Wno-c++11-long-long -Wno-c++98-compat -Wno-padded

all:
	clang++  $(EXTRA_CXXFLAGS) -std=c++11 -g -O2 -o csv_parser_example csv_parser_example.cc -pthread

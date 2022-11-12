all:
	clang++ mr-pr-cpp.cpp /usr/lib/x86_64-linux-gnu/libboost_system.a /usr/lib/x86_64-linux-gnu/libboost_iostreams.a /usr/lib/x86_64-linux-gnu/libboost_filesystem.a -pthread -o mr-pr-cpp.o

checker:
	clang++ correctness_checker.cpp -o check

clean:
	rm *.o check
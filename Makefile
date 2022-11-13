pr:
	clang++ mr-pr-cpp.cpp /usr/lib/x86_64-linux-gnu/libboost_system.a /usr/lib/x86_64-linux-gnu/libboost_iostreams.a /usr/lib/x86_64-linux-gnu/libboost_filesystem.a -pthread -o mr-pr-cpp.o

mpi:
	mpic++ -std=c++11 mr-pr-mpi.cpp -lboost_system -lpthread -lboost_iostreams -lboost_filesystem -o mr-pr-mpi.o


checker:
	clang++ correctness_checker.cpp -o check

clean:
	rm *.o check
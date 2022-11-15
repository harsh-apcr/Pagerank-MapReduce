
./mr-pr-cpp.o test/$1.txt -o result/$1-pr-cpp.txt
echo "Running correctness check"
./check result/$1-pr-cpp.txt result/$1-pr-p.txt


mpirun -np 8 --oversubscribe ./mr-pr-mpi.o test/$1.txt -o result/$1-pr-mpi.txt
echo "Running correctness check"
./check result/$1-pr-mpi.txt result/$1-pr-p.txt

mpirun -np 4 --oversubscribe ./mr-pr-mpi-base.o test/$1.txt -o result/$1-pr-mpi-base.txt
echo "Running correctness check"
./check result/$1-pr-mpi-base.txt result/$1-pr-p.txt
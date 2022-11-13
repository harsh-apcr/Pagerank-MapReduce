
if [ $1 ==  "cpp" ] ;
then
    echo "Running pagerank on barabasi-20000.txt dataset"
    ./mr-pr-cpp.o data/barabasi-20000.txt -o result/barabasi-20000-pr-cpp.txt
    echo ""
    echo "Running correctness check of the result, which checks if ranks are within 1e-4"
    echo ""
    ./check result/barabasi-20000-pr-cpp.txt result/barabasi-20000-pr-p.txt
    echo "---------------------------------------------------------------------------------"
    echo ""
    echo "Running pagerank on erdos-20000.txt dataset"
    ./mr-pr-cpp.o data/erdos-20000.txt -o result/erdos-20000-pr-cpp.txt
    echo ""
    echo "Running correctness check of the result, which checks if ranks are within 1e-4"
    echo ""
    ./check result/erdos-20000-pr-cpp.txt result/erdos-20000-pr-p.txt
    echo "---------------------------------------------------------------------------------"
    echo ""
    echo "Running pagerank on erdos-100000.txt dataset"
    ./mr-pr-cpp.o data/erdos-100000.txt -o result/erdos-100000-pr-cpp.txt
    echo ""
    echo "Running correctness check of the result, which checks if ranks are within 1e-4"
    echo ""
    ./check result/erdos-100000-pr-cpp.txt result/erdos-100000-pr-p.txt
    echo "---------------------------------------------------------------------------------"
    echo ""
elif [ $1 == "mpi" ] ;
then
    echo "Running pagerank on barabasi-20000.txt dataset"
    mpirun -np 8 --oversubscribe ./mr-pr-mpi.o data/barabasi-20000.txt -o result/barabasi-20000-pr-mpi.txt
    echo ""
    echo "Running correctness check of the result, which checks if ranks are within 1e-4"
    echo ""
    ./check result/barabasi-20000-pr-mpi.txt result/barabasi-20000-pr-p.txt
    echo "---------------------------------------------------------------------------------"
    echo ""
    echo "Running pagerank on erdos-20000.txt dataset"
    mpirun -np 8 --oversubscribe ./mr-pr-mpi.o data/erdos-20000.txt -o result/erdos-20000-pr-mpi.txt
    echo ""
    echo "Running correctness check of the result, which checks if ranks are within 1e-4"
    echo ""
    ./check result/erdos-20000-pr-mpi.txt result/erdos-20000-pr-p.txt
    echo "---------------------------------------------------------------------------------"
    echo ""
    echo "Running pagerank on erdos-100000.txt dataset"
    mpirun -np 8 --oversubscribe ./mr-pr-mpi.o data/erdos-100000.txt -o result/erdos-100000-pr-mpi.txt
    echo ""
    echo "Running correctness check of the result, which checks if ranks are within 1e-4"
    echo ""
    ./check result/erdos-100000-pr-mpi.txt result/erdos-100000-pr-p.txt
    echo "---------------------------------------------------------------------------------"
    echo ""
else
    echo "invalid input $1 : put one of cpp, mpi, base"
fi



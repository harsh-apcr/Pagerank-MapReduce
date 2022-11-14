#include <cstdio>
#include <unordered_map>
#include <fstream>
#include <string>
#include <vector>
#include <cassert>
#include <regex>
#include <unordered_map>
#include <cmath>
#include <iomanip>
#include "mpi.h"
#include <chrono>


#define SEC_TO_NS(sec) ((sec)*1000000000)
uint64_t nanos()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    uint64_t ns = SEC_TO_NS((uint64_t)ts.tv_sec) + (uint64_t)ts.tv_nsec;
    return ns;
}

int websize;
int hlinksize;

const double DEFAULT_ALPHA = 0.85;
const double DEFAULT_CONVERGENCE = 0.00001;
const unsigned long DEFAULT_MAX_ITERATIONS = 10000;

// split function for `std::string` based on a `char` separator , courtesy : stackoverflow
std::vector<std::string> split(const std::string &s, char seperator) {
    std::vector<std::string> output;
    std::string::size_type prev_pos = 0, pos = 0;
    while((pos = s.find(seperator, pos)) != std::string::npos) {
        std::string substring( s.substr(prev_pos, pos-prev_pos) );
        output.push_back(substring);
        prev_pos = ++pos;
    }
    output.push_back(s.substr(prev_pos, pos-prev_pos)); // Last word
    return output;
}

// parses hyper link file
void
parse_hlfile(std::istream &input_file, std::vector<std::uint32_t> &hyperlink_input, int rank) {
    assert(hyperlink_input.empty());
    std::string line;
    std::regex p("(0|[1-9][0-9]*)\\s(0|[1-9][0-9]*)$");
    unsigned int i = 0;
    while (std::getline(input_file, line)) {
        if (!std::regex_match(line, p)) {
            if (rank == 0) fprintf(stderr, "invalid input at line number : %d", i+1);
            MPI_Abort(MPI_COMM_WORLD,1);
        }
        // regex match
        std::vector<std::string> link = split(line, ' ');
        hyperlink_input.push_back(std::stoi(link[0]));
        hyperlink_input.push_back(std::stoi(link[1]));
        i++;
    }
}

namespace mapreduce {

uint hash(uint x) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x;
}

// only one map worker
class map_task {
public:
    map_task(int num_reduce) 
        : num_reduce(num_reduce) {};

    void run_task(const std::vector<std::uint32_t> &hyperlink) {
        std::uint32_t sendbuf[2];
        for(unsigned i = 0;i < hlinksize;i+=2) {
            sendbuf[0] = hyperlink[i+1];
            sendbuf[1] = hyperlink[i];
            // sendbuf has key-value pair
            MPI_Send(sendbuf, 2, MPI_UINT32_T, mapreduce::hash(sendbuf[0]) % num_reduce + 2, 0, MPI_COMM_WORLD);
            // send will use eager protocol, so it is fast non-blocking send
        }   
        // send is performed `hlinksize / 2` many times
        int exit = -1;
        for(unsigned i = 0;i < num_reduce;i++) {
            MPI_Send(&exit, 1, MPI_INT, i + 2, 0, MPI_COMM_WORLD);
        }
        return;
    };

private:
    int num_reduce;                                   // partitions data according to num_reduce
};

class reduce_task {
public:
    void run_task(std::unordered_map<std::uint32_t, std::vector<std::uint32_t>> &part_incoming) {
        MPI_Status status;
        int recvbuf[2];
        int res;
        do {
            MPI_Recv(recvbuf, 2, MPI_INT, 1/*map worker*/, 0, MPI_COMM_WORLD, &status);
            res = recvbuf[0];
            if (res >= 0) 
                part_incoming[recvbuf[0]].push_back(recvbuf[1]);

        } while (res != -1);
        // we got list(k, list(v))
        return;
    }

};

};

std::vector<double>
run_pgrank(std::vector<std::vector<std::uint32_t>> &incoming,
        std::unordered_map<std::uint32_t, std::uint32_t> &num_outgoing,
        double convergence,
        int max_iterations,
        double alpha) {

    // incoming[i] = vector of all pg ids that have a link to pg i
    // num_outgoing[i] = number of outgoing links out of pg i
    int n = incoming.size();    // websize
    double sum_pr;
    double dangling_pr;
    double diff = 1;
    unsigned long num_iterations = 0;

    std::vector<double> old_pr(n, 0); // prev iteration pgrank table
    std::vector<double> pr(n, 0);     // current pgrank table

    pr[0] = 1;                        // initialize (1,0,...,0)

    while (diff > convergence && num_iterations < max_iterations) {
        sum_pr = 0;
        dangling_pr = 0;
        for(unsigned k = 0;k < n;k++) {
            double cpr= pr[k];
            sum_pr += cpr;
            if (num_outgoing[k] == 0)
                dangling_pr += cpr;
        }

        if (num_iterations == 0) {
            old_pr = pr;
        } else {
            /* Normalize so that we start with sum equal to one */
            for (unsigned i = 0; i < n; i++) {
                old_pr[i] = pr[i] / sum_pr;
            }
        }

        /*
         * After normalisation the elements of the pagerank vector sum
         * to one
         */
        sum_pr = 1;

        /* An element of the A x I vector; all elements are identical */
        double one_Av = alpha * dangling_pr / n;

        /* An element of the 1 x I vector; all elements are identical */
        double one_Iv = (1 - alpha) * sum_pr / n;

        /* The difference to be checked for convergence */
        diff = 0;
        for(unsigned i = 0;i < n;i++) {
            /* The corresponding element of the H multiplication */
            double h = 0.0;
            for(int pg : incoming[i]) {
                // pg -> i
                assert(num_outgoing[pg]);   // TODO: remove this after testing
                h += 1.0 / num_outgoing[pg] * old_pr[pg];
            }
            h *= alpha;
            pr[i] = h + one_Av + one_Iv;
            diff += fabs(pr[i] - old_pr[i]);
        }

        num_iterations++;
    }

    return pr;
}



int main(int argc, char **argv) {

    MPI_Init(&argc, &argv);
    int size, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    
    if (argc != 4) {
        if (rank == 0) fprintf(stderr, "Usage : ./mr-pr-mpi.o ${filename}.txt -o ${filename}-pr-mpi.txt\n");
        MPI_Abort(MPI_COMM_WORLD,1);
    }
    // argc == 4
    if (strcmp(argv[2], "-o")) {
        if (rank == 0) fprintf(stderr, "flag `-o` expected but provided `%s`\n", argv[2]);
        MPI_Abort(MPI_COMM_WORLD,1);
    }
    
    
    // only 1 map worker
    int num_reduce = size - 2;   // 1 master and 1 map-worker
    if (num_reduce <= 0) {
        if (rank == 0) std::cerr << "number of processes spawned must be atleast 3 for doing pgrank using map-reduce, but only " << size << " spawned" << std::endl;
        MPI_Abort(MPI_COMM_WORLD,1);
    }
    
    uint32_t start, end;

    std::filebuf fb1;
    if (fb1.open(argv[1], std::ios::in)) {
        
        std::unordered_map<std::uint32_t, std::uint32_t> num_outgoing;
        std::vector<std::uint32_t> hyperlink;
        if (rank == 0 || rank == 1) {
            // both master and map-worker parses the input file
            std::istream input_stream(&fb1);
            parse_hlfile(input_stream, hyperlink, rank);

            if (rank == 0) {
                // only master needs outgoing, and it also computes websize, hlinksize
                websize = 0;
                for(unsigned i = 0;i < hyperlink.size();i+=2) {
                    // key = hyperlink[i], value = hyperlink[i+1]
                    num_outgoing[hyperlink[i]]++;
                    if (websize < hyperlink[i])
                        websize = hyperlink[i];
                    if (websize < hyperlink[i+1])
                        websize = hyperlink[i+1];
                }
                // websize is largest id of page in web
                websize++;
                hlinksize = hyperlink.size();
            }
            
        }
        
        MPI_Bcast(&websize, 1, MPI_INT, 0, MPI_COMM_WORLD);
        MPI_Bcast(&hlinksize, 1, MPI_INT, 0, MPI_COMM_WORLD);

        int num_send, num_recv;
        // map worker has the hyperlink vector
        if (rank == 0) {
            std::vector<std::vector<std::uint32_t>> incoming(websize);
            num_send = 0;
            MPI_Reduce(&num_send, &num_recv, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
            // got the number of recv
            int pgid;
            MPI_Status status;
            uint32_t recvbuf[websize];
            for(unsigned i = 0;i < num_recv;i++) {
                MPI_Recv(&pgid, 1, MPI_UINT32_T, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
                MPI_Recv(recvbuf, websize, MPI_UINT32_T, status.MPI_SOURCE, 1, MPI_COMM_WORLD, &status);

                int count;
                MPI_Get_count(&status, MPI_UINT32_T, &count);
                for(int i = 0;i < count;i++) {
                    incoming[pgid].push_back(recvbuf[i]);
                }
            }
            MPI_Recv(&start, 1, MPI_UINT64_T, 1, 10, MPI_COMM_WORLD, &status);
            MPI_Recv(&end, 1, MPI_UINT64_T, MPI_ANY_SOURCE, 11, MPI_COMM_WORLD, &status);
            double time = (end - start) / double(1000000000);
            std::cout <<"\nMPI-MapReduce job finished in " << time << "s" << std::endl;
            
            // rank 0 has num_outgoing, incoming
            auto pgstart = std::chrono::high_resolution_clock::now();
            auto pgrankv = run_pgrank(incoming, num_outgoing, DEFAULT_CONVERGENCE, DEFAULT_MAX_ITERATIONS, DEFAULT_ALPHA);
            auto pgend = std::chrono::high_resolution_clock::now();

            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(pgend - pgstart);

            std::cout << "\nPagerank algorithm finished in " << duration.count() << "us" << std::endl;



            std::filebuf fb2;
            if (fb2.open(argv[3], std::ios::out)) {
                std::ostream outs(&fb2);
                outs << std::setprecision(12);
                unsigned i = 0;
                for(auto pr : pgrankv) {
                    outs << i << " = " << pr << std::endl;
                    i++;
                }
                double ranksum = 0;
                for(auto rk : pgrankv)
                    ranksum += rk;
                outs << "s = " << ranksum;
            }

        }
        else if (rank == 1) {
            mapreduce::map_task map_worker(num_reduce);
            start = nanos();
            map_worker.run_task(hyperlink);
            num_send = 0;
            MPI_Reduce(&num_send, &num_recv, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

            MPI_Send(&start, 1, MPI_UINT64_T, 0, 10, MPI_COMM_WORLD);
        }
        // rank > 1
        else {
            // many reduce workers
            mapreduce::reduce_task reduce_worker;
            std::unordered_map<std::uint32_t, std::vector<std::uint32_t>> part_incoming;
            reduce_worker.run_task(part_incoming);
            end = nanos();
            // need to send this part_incoming to master
            num_send = part_incoming.size();
            MPI_Reduce(&num_send, &num_recv, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

            for(auto const &kv : part_incoming) {
                MPI_Send(&kv.first, 1, MPI_UINT32_T, 0, 0, MPI_COMM_WORLD); // send key
                MPI_Send(&kv.second[0], kv.second.size(), MPI_UINT32_T, 0, 1, MPI_COMM_WORLD);
            }

            if (rank == 2) {
                MPI_Send(&end, 1, MPI_UINT64_T, 0, 11, MPI_COMM_WORLD);
            }
        }
        MPI_Finalize();
    }
    return 0;
}

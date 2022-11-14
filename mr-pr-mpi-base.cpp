#include <cstdio>
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
#include "mapreduce-7Apr14/src/mapreduce.h"
#include "mapreduce-7Apr14/src/keyvalue.h"

using namespace MAPREDUCE_NS;

std::unordered_map<std::uint32_t, std::uint32_t> num_outgoing;
std::vector<std::pair<std::uint32_t, std::uint32_t>> hyperlink;
std::uint32_t **incoming;
std::uint32_t *incoming_nval;

int hlinksize;
int websize;
const double DEFAULT_ALPHA = 0.85;
const double DEFAULT_CONVERGENCE = 0.00001;
const unsigned long DEFAULT_MAX_ITERATIONS = 10000;

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


std::vector<double>
run_pgrank(std::unordered_map<std::uint32_t, std::uint32_t> num_outgoing,
        double convergence,
        int max_iterations,
        double alpha) {

    // incoming[i] = vector of all pg ids that have a link to pg i
    // num_outgoing[i] = number of outgoing links out of pg i
    int n = websize;
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
            unsigned sz = incoming_nval[i];
            for(unsigned j = 0;j < sz;j++) {
                std::uint32_t pg = incoming[i][j];
                // pg -> i
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

void fileread(int rank, KeyValue *kv, void *ptr) {
    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    for (int i = rank * (hlinksize / size); i < (rank+1) * (hlinksize / size); i++) {
        kv->add((char *) &hyperlink[i].second,sizeof(std::uint32_t),    // key
                (char *) &hyperlink[i].first,sizeof(std::uint32_t));    // val
    }
}

void collect_incoming(uint64_t /*itask*/, char *key, int keybytes, 
    char *value, int valuebytes, KeyValue *kv, void * ptr) {

    int rank = *reinterpret_cast<int*>(ptr);
    if (rank != 0) return;
    
    std::uint32_t k = *reinterpret_cast<std::uint32_t*>(key);
    incoming[k] = reinterpret_cast<std::uint32_t*>(value);
    incoming_nval[k] = valuebytes / sizeof(std::uint32_t);
    kv->add(key, keybytes, value, valuebytes);
}

void collect(char *key, int keybytes, char *multivalue,
 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) {
    kv->add(key, keybytes, multivalue, sizeof(std::uint32_t) * nvalues);
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc != 4) {
        if (rank == 0) fprintf(stderr, "Usage : ./mr-pr-mpi-base.o ${filename}.txt -o ${filename}-pr-mpi-base.txt\n");
        MPI_Abort(MPI_COMM_WORLD,1);
    }
    // argc == 4
    if (strcmp(argv[2], "-o")) {
        if (rank == 0) fprintf(stderr, "flag `-o` expected but provided `%s`\n", argv[2]);
        MPI_Abort(MPI_COMM_WORLD,1);
    }

    std::filebuf fb1;
    if (fb1.open(argv[1], std::ios::in)) {
        std::istream input_stream(&fb1);
        std::string line;
        std::regex p("(0|[1-9][0-9]*)\\s(0|[1-9][0-9]*)$");
        unsigned int i = 0;
        websize = 0;
        while (std::getline(input_stream, line)) {
            if (!std::regex_match(line, p)) {
                fprintf(stderr, "invalid input at line number : %d", i+1);
                MPI_Abort(MPI_COMM_WORLD,1);
            }
            // regex match
            std::vector<std::string> link = split(line, ' ');
            int pg1 = std::stoi(link[0]);
            int pg2 = std::stoi(link[1]);
            hyperlink.push_back(std::make_pair(pg1, pg2));
            num_outgoing[pg1]++;
            
            if (websize < pg1)
                websize = pg1;
            if (websize < pg2)
                websize = pg2;
            i++;
        }
        websize++;
    } else {
        std::cerr << "cannot open file `" << argv[1] << "`" << std::endl;
    }

    hlinksize = hyperlink.size();
    MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);
    double start = MPI_Wtime();

    mr->map(size, fileread, NULL);
    mr->collate(NULL);              // kv -> kmv
    mr->reduce(collect, NULL);

    MPI_Barrier(MPI_COMM_WORLD);
    double stop = MPI_Wtime();
    if (rank == 0)
        std::cout << "\nBaseMPI-MapReduce job finished in " << (stop - start) << " s" << std::endl;
    
    mr->gather(1);

    if (rank == 0) {
        incoming = new std::uint32_t*[websize];
        memset(incoming, 0, sizeof(std::uint32_t) * websize);
        incoming_nval = new std::uint32_t[websize];
        memset(incoming_nval, 0, sizeof(std::uint32_t) * websize);
    }

    mr->map(mr, collect_incoming, &rank);

    if (rank == 0) {
        auto pgstart = std::chrono::high_resolution_clock::now();
        auto pgrankv = run_pgrank(num_outgoing, DEFAULT_CONVERGENCE, DEFAULT_MAX_ITERATIONS, DEFAULT_ALPHA);
        auto pgend = std::chrono::high_resolution_clock::now();

        delete mr;
        delete [] incoming;
        delete [] incoming_nval;
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
    MPI_Finalize();
}
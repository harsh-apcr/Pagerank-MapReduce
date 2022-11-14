#include <vector>
#include <unordered_map>
#include <iostream>
#include <fstream>
#include <regex>
#include <string>
#include <numeric>
#include <chrono>
#include "include/mapreduce.hpp"

const double DEFAULT_ALPHA = 0.85;
const double DEFAULT_CONVERGENCE = 0.00001;
const unsigned long DEFAULT_MAX_ITERATIONS = 10000;

static bool is_setup = false;

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

namespace pgrank {

template<typename maptask>
class datasource : public mapreduce::detail::noncopyable {
public:
    // pass hyperlink as an rval
    datasource(std::string filename, 
            std::vector<std::pair<std::uint32_t, std::uint32_t>> &hyperlink) : filename(filename), hyperlink(std::move(hyperlink)) {};

    bool const setup_key(typename maptask::key_type &key) {
        key = filename; // key isn't really that important for map
        if (!is_setup) {
            is_setup = true;
            return true;
        }
        return false;
    };

    bool const get_data(typename maptask::key_type const &/*key*/, typename maptask::value_type &value) {
        value = std::move(hyperlink);
        return true;
    }

private:
    std::string filename;
    std::vector<std::pair<std::uint32_t, std::uint32_t>> hyperlink; // useless after one get data
};

// there is exactly one map worker
struct map_task : public mapreduce::map_task<std::string,                                                    // map key   - filename 
                                            std::vector<std::pair<std::uint32_t, std::uint32_t>> >   {      // map value - memory mapped file contents (list of pairs val.first --> val.second)  
                                            
    template<typename Runtime>
    void operator()(Runtime &runtime, const key_type &/*key*/, value_type &value)  const {
        for (auto const &val : value) {
            runtime.emit_intermediate(val.second, val.first);   // val.first links to val.second
        }
    }

};

struct reduce_task : public mapreduce::reduce_task<std::uint32_t,      // key type for result of reduce phase (a page id)
                                                std::uint32_t> {    // value type for result of reduce phase (list of page ids that are pointing to key)
    template<typename Runtime, typename It>
    void operator()(Runtime &runtime,key_type const &key, It it, It ite) {
        for(auto itr = it;itr != ite;itr++) {
            runtime.emit(key, *itr);
        }
    }
};

typedef
mapreduce::job<pgrank::map_task,
             pgrank::reduce_task,
             mapreduce::null_combiner,
             pgrank::datasource<pgrank::map_task>> 
job;

// parses hyper link file
void
parse_hlfile(std::istream &input_file, std::vector<std::pair<std::uint32_t, std::uint32_t>> &hyperlink_input) {
    assert(hyperlink_input.empty());
    std::string line;
    std::regex p("(0|[1-9][0-9]*)\\s(0|[1-9][0-9]*)$");
    unsigned int i = 0;
    while (std::getline(input_file, line)) {
        if (!std::regex_match(line, p)) {
            fprintf(stderr, "invalid input at line number : %d", i+1);
            exit(1);
        }
        // regex match
        std::vector<std::string> link = split(line, ' ');
        hyperlink_input.push_back(std::make_pair<std::uint32_t, std::uint32_t>
                                    (std::stoi(link[0]),
                                     std::stoi(link[1]))
                                );
        i++;
    }
}

std::vector<double>
run(std::vector<std::vector<std::uint32_t>> &incoming,
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


};   // namespace pgrank


int main(int argc, char **argv) {
    if (argc != 4) {
        std::cerr << "Usage : ./mr-pr-cpp.o ${filename}.txt -o ${filename}-pr-cpp.txt" << std::endl;
        exit(1);
    }
    if (strcmp(argv[2], "-o")) {
        std::cerr << "flag `-o` expected but provided `" << argv[2] << "`" << std::endl;
    }
    // argc == 4 and !strcmp(argv[2], "-o")

    std::filebuf fb1;
    if (fb1.open(argv[1], std::ios::in)) {
        std::istream input_stream(&fb1);
        std::vector<std::pair<std::uint32_t, std::uint32_t>> hyperlink;
        pgrank::parse_hlfile(input_stream, hyperlink);

        std::unordered_map<std::uint32_t, std::uint32_t> num_outgoing;
        unsigned int websize = 0;
        for(unsigned i = 0;i < hyperlink.size();i++) {
            num_outgoing[hyperlink[i].first]++;
            if (websize < std::max(hyperlink[i].first, hyperlink[i].second))
                websize = std::max(hyperlink[i].first, hyperlink[i].second);
        }
        // websize is largest id of page in web
        websize++;
        // mapreduce library stuff to get incoming matrix

        mapreduce::specification spec;
        spec.map_tasks = 1;
        spec.reduce_tasks = std::max(1U, std::thread::hardware_concurrency());

        pgrank::job::datasource_type datasource(std::string(argv[1]), hyperlink);
        // hyperlink is destroyed now

        pgrank::job job(datasource, spec);
        mapreduce::results result;

        // job run sequential policy

        job.run<mapreduce::schedule_policy::cpu_parallel<pgrank::job>> (result);
        std::cout <<"\nMapReduce job finished in " << result.job_runtime.count() << "s with " << std::distance(job.begin_results(), job.end_results()) << " results\n\n";
        
        std::vector<std::vector<std::uint32_t>> incoming(websize);

        for(auto it = job.begin_results(); it != job.end_results() ; ++it) {
            int pgid = it->first;
            incoming[pgid].push_back(it->second);
        }

        // ===== DEBUG verify incoming vector =====
        // for(unsigned i = 0;i < websize;i++) {
        //     printf("incoming for page %d : ", i);
        //     for(int pg : incoming[i]) {
        //         printf("%d ", pg);
        //     }
        //     printf("\n");
        // }
        // ===== DEBUG verify incoming vector =====

        // incoming, num_outgoing sets are set up now
        auto start = std::chrono::high_resolution_clock::now();
        auto pgrankv = pgrank::run(incoming, num_outgoing, DEFAULT_CONVERGENCE, DEFAULT_MAX_ITERATIONS, DEFAULT_ALPHA);
        auto end = std::chrono::high_resolution_clock::now();

        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

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
    return 0;

}



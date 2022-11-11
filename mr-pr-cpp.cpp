#include <vector>
#include <iostream>
#include <fstream>
#include <regex>
#include <string>
#include "include/mapreduce.hpp"


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
    datasource(std::string &filename, 
            std::vector<std::pair<std::uint32_t, std::uint32_t>> &&hyperlink) : filename(filename), hyperlink(hyperlink) {};

    bool const setup_key(typename maptask::key_type &key) {
        key = filename; // key isn't really that important for map
        return true;
    };

    bool const get_data(typename maptask::key_type const &/*key*/, typename maptask::value_type &value) {
        value = hyperlink;
        return true;
    }

private:
    std::string filename;
    std::vector<std::pair<std::uint32_t, std::uint32_t>> hyperlink;
};

class map_task : public mapreduce::map_task<std::string,                                                    // map key   - filename 
                                            std::vector<std::pair<std::uint32_t, std::uint32_t>> >   {      // map value - memory mapped file contents (list of pairs val.first --> val.second)  
                                            
    template<typename Runtime>
    void operator()(Runtime &runtime, const key_type &/*key*/, value_type &value)  const {
        for (auto const &val : value) {
            runtime.emit_intermediate(val.second, val.first);   // val.first links to val.second
        }
    }

};

class reduce_task : public mapreduce::reduce_task<std::uint32_t,      // key type for result of reduce phase (a page id)
                                     std::vector<std::uint32_t>> {    // value type for result of reduce phase (list of page ids that are pointing to key)
    template<typename Runtime, typename It>
    void operator()(Runtime &runtime,key_type const &key, It it, It ite) {
        std::vector<std::uint32_t> pages;
        for(auto itr = it; itr != ite; itr++) {
            pages.push_back(*itr);
        }
        runtime.emit(key, pages);
    }
};

typedef
mapreduce::job<pgrank::map_task,
             pgrank::reduce_task,
             mapreduce::null_combiner,
             pgrank::datasource<pgrank::map_task>> 
job;

// parses hyper link file
std::vector<std::pair<std::uint32_t, std::uint32_t>> 
parse_hlfile(std::istream &input_file) {
    std::vector<std::pair<std::uint32_t, std::uint32_t>> hyperlink_input;
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
    return hyperlink_input;
}

void run(std::vector<std::vector<std::uint32_t>> incoming,
        std::vector<std::uint32_t> num_outgoing,
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
}


};   // namespace pgrank







/*
std::filebuf fb;
if (fb.open(filename, std::ios::in)) {
    std::istream input_stream(&fb);
    value = parse_hlfile(input_stream);
    fb.close();
    return true;
} else {
    return false;
}
*/



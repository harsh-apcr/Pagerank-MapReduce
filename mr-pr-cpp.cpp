#include <vector>
#include <iostream>
#include <fstream>
#include <regex>
#include <string>
#include "include/mapreduce.hpp"


std::vector<std::string> split(const std::string &s, char seperator)
{
   std::vector<std::string> output;

    std::string::size_type prev_pos = 0, pos = 0;

    while((pos = s.find(seperator, pos)) != std::string::npos)
    {
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
    datasource(std::string filename) : filename(filename) {};

    bool const setup_key(typename maptask::key_type &key) {
        key = filename; // key isn't really that important for map
        return true;
    };

    bool const get_data(typename maptask::key_type const &key, typename maptask::value_type &value) {
        std::filebuf fb;
        if (fb.open(filename, std::ios::in)) {
            std::istream input_stream(&fb);
            value = parse_hlfile(input_stream);
            fb.close();
            return true;
        } else {
            return false;
        }
    }

private:
    std::string filename;
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
mapreduce::job<pgrank::map_task, pgrank::reduce_task, mapreduce::null_combiner> 
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

};   // namespace pgrank
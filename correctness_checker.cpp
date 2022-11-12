#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <regex>
#include <cmath>

static bool has_diff = false;

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


int main(int argc, char **argv) {
    if (argc != 3) {
        std::cerr << "Usage : ./checker <filename1> <filename2>" << std::endl;
        exit(1);
    }

    std::filebuf fb1;
    std::filebuf fb2;
    if (fb1.open(argv[1], std::ios::in) && fb2.open(argv[2], std::ios::in)) {
        std::istream ins1(&fb1), ins2(&fb2);
        std::string line1, line2;
        std::regex p("((0|[1-9][0-9]*)|s)\\s=\\s([0-9]*[.])?[0-9]+(e[+|-]?[0-9]+)?");
        unsigned i = 0;
        while (std::getline(ins1, line1) && std::getline(ins2, line2)) {
            if (!std::regex_match(line1, p) || !std::regex_match(line2, p)) {
                std::cerr << "invalid input at line number : " << i+1 << std::endl;
                exit(1);
            }
            double r1 = std::stod(split(line1, ' ')[2]);
            double r2 = std::stod(split(line2, ' ')[2]);
            if (fabs(r1 - r2) > 1e-4) {
                has_diff = true;
                std::cout <<  
                    "files do not match at line number : " <<  i+1 << ", difference is more than 1e-4" 
                    << std::endl;
            }
            i++;
        }

        std::cout << "check passed successfully" << std::endl;

    } else {
        std::cerr << "couldn't open one of the file for checking" << std::endl;
    }
    return 0;
}



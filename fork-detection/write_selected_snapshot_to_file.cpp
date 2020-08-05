#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <queue>
#include <set>
#include <sstream>
#include <stack>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/assign.hpp>
#include <unordered_set>
#include <algorithm>
#include <thread>

const uint64_t MIN_DATE = 1453939200;
std::string uint64_to_string( uint64_t value ) {
    std::ostringstream os;
    os << value;
    return os.str();
}
std::unordered_set<uint64_t> snapshot_ids;
void getSelectedSnapshotIds() {
    auto t_start = std::chrono::high_resolution_clock::now();
    std::cout<<"Get snapshot"<<std::endl;
    std::ifstream myFile("/home/sv/snapshot_fork_date.csv");
    if(!myFile.is_open()) throw std::runtime_error("Could not open file");
    //Iterate lines
    std::string line;
    while(std::getline(myFile, line)) {
        std::stringstream s_stream(line);
		int cnt = 0;
		uint64_t forkID = 1;
        uint64_t snapshotID = 1;
        while(s_stream.good()) {
			std::string substr;
			std::getline(s_stream, substr, ',');
			std::istringstream iss(substr);
			if (cnt == 0) {
				iss>>snapshotID;
			}else if(cnt == 1) {
                iss>>forkID;
            }
			++cnt;
		}
        snapshot_ids.insert(snapshotID);
        snapshot_ids.insert(forkID);
    }
    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end-t_start).count();
    elapsed_time_ms /= 1000.0;
    for(uint64_t id: snapshot_ids){
        std::cout<<id<<std::endl;
    }
    std::cout<<"Finish: "<<elapsed_time_ms<<std::endl;
}
void writeToFile() {
    std::cout<<"Start writing"<<std::endl;
    auto t_start = std::chrono::high_resolution_clock::now();
    //Read from the first command line argument, assume it's gzipped
    std::ifstream file("/mnt/17volume/data/snapshot_revision_git.csv.gz", std::ios_base::in | std::ios_base::binary);
    boost::iostreams::filtering_streambuf<boost::iostreams::input> inbuf;
    inbuf.push(boost::iostreams::gzip_decompressor());
    inbuf.push(file);
    //Convert streambuf to istream
    std::istream instream(&inbuf);
    //Iterate lines
    std::string line;
	getline(instream, line);
    std::ofstream myfile;
    myfile.open("/mnt/17volume/data/selected_snapshot.csv", std::ofstream::out | std::ofstream::app);

    while(std::getline(instream, line)) {
		std::stringstream s_stream(line);
		int cnt = 0;
		uint64_t snapshotID = 1;
		std::string revisionID = "";
		uint64_t date = 1;
		while(s_stream.good()) {
			std::string substr;
			std::getline(s_stream, substr, ',');
			substr.erase(std::remove(substr.begin(),substr.end(),'\"'),substr.end());
			std::istringstream iss(substr);
			if (cnt == 0) {
				iss>>snapshotID;
			}else if (cnt==1) {
				iss>>revisionID;
			}else if(cnt == 2) {
				iss>>date;
			}
			++cnt;
		}
        if(!snapshot_ids.count(snapshotID)) {
            continue;
        }
        if(date>=MIN_DATE) {
            std::cout<<"Snapshot: "<<snapshotID<<std::endl;
            std::string ret="";
            ret += uint64_to_string(snapshotID);
            ret += ",";
            ret += revisionID;
            ret += ",";
            ret += uint64_to_string(date);
            ret += "\n";
            myfile<<ret;
        }
    }
    myfile.close();
    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end-t_start).count();
    elapsed_time_ms /= 1000.0;
    std::cout<<"Finish: "<<elapsed_time_ms<<std::endl;
}

int main() {
    getSelectedSnapshotIds();
    writeToFile();
    return 0;
}
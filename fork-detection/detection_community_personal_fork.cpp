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
#include <unordered_set>
#include <algorithm>
#include <thread>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <chrono>
#include <iomanip>
#define mp std::make_pair
typedef std::unordered_map<std::string, uint32_t> cati;
typedef std::pair<uint32_t, uint32_t> ii;

//=========================== DATA STRUCTURE ==========================
class Revision {
public:
	Revision()
		: revisionID(""), date(1){};
	std::string revisionID;
	uint64_t date;
    uint64_t author;
	Revision(const std::string &revisionID, const uint64_t &date, const uint64_t &author){
		this->revisionID = revisionID;
		this->date = date;
        this->author = author;
	}
};

//=========================== FUNCTIONS ==========================
const uint64_t MIN_DATE = 1517097600;
std::unordered_map<uint64_t, std::vector<Revision>> revisionSnapshots;
std::unordered_set<uint64_t> forks;
std::unordered_set<uint64_t> communityForks;

std::string uint64_to_string( uint64_t value ) {
    std::ostringstream os;
    os << value;
    return os.str();
}

void sortVectorByDate(int start, int end) {
    std::unordered_map<uint64_t, std::vector<Revision>>::iterator it = revisionSnapshots.begin();
    int cnt = 0;
    while(cnt < start) {
        ++cnt;
        ++it;
    }
    for(; it != revisionSnapshots.end() && cnt < end; ++it, ++cnt) {
        uint64_t snapshotID = it->first;
        std::sort(revisionSnapshots[snapshotID].begin(), revisionSnapshots[snapshotID].end(), [](const Revision &lhs, const Revision &rhs){
            return lhs.date < rhs.date;
        });
    }
}
uint64_t getSecondsSince1970Until( std::string dateAndHour ) {
  using namespace std;
  tm tm = {};
  std::stringstream ss( dateAndHour );
  ss >> get_time(&tm, "%Y-%m-%d  %H:%M:%S%z");

  chrono::system_clock::time_point tp = chrono::system_clock::from_time_t(mktime(&tm));


  return
    chrono::duration_cast<chrono::seconds>(
                                           tp.time_since_epoch()).count();

}

void loadRevisionHistory(std::string name)
{
    //Read from the first command line argument, assume it's gzipped
    //Read from the first command line argument, assume it's gzipped
    std::ifstream file(name);
    //Iterate lines
    std::string line;
	auto t_start = std::chrono::high_resolution_clock::now();
    while(std::getline(file, line)) {
		std::stringstream s_stream(line);
		int cnt = 0;
		uint64_t snapshotID = 1;
        uint64_t author = 1;
		std::string revisionID = "";
		uint64_t date = 1;
		while(s_stream.good()) {
			std::string substr;
			std::getline(s_stream, substr, ',');
			substr.erase(std::remove(substr.begin(),substr.end(),'\"'),substr.end());
			std::istringstream iss(substr);
			if (cnt == 0) {
				iss>>snapshotID;
			}else if (cnt == 2) {
                iss>>author;
            }
            else if (cnt==3) {
                std::string _date;
                iss>>_date;
                date = getSecondsSince1970Until(_date);
			}else if(cnt == 5) {
				iss>>revisionID;
			}
			++cnt;
		}
        if(!forks.count(snapshotID)) {
            continue;
        }
		if(date >= MIN_DATE && date <= 1548633600) {
			if(!revisionSnapshots.count(snapshotID)){
                revisionSnapshots[snapshotID] = std::vector<Revision>{};
            }
            revisionSnapshots[snapshotID].emplace_back(Revision(revisionID, date, author));
		}		
    }
    //Cleanup
    file.close();
	auto t_end = std::chrono::high_resolution_clock::now();
	double elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end-t_start).count();
	elapsed_time_ms /= 1000.0;
	std::cout<<"finish read: "<<elapsed_time_ms<<" seconds."<<std::endl;
    //sort date
	t_start = std::chrono::high_resolution_clock::now();
	for(auto& it: revisionSnapshots) {
        std::sort(it.second.begin(), it.second.end(), [](const Revision &lhs, const Revision &rhs){
            return lhs.date < rhs.date;
        });
    }
	t_end = std::chrono::high_resolution_clock::now();
	elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end-t_start).count();
	elapsed_time_ms /= 1000.0;
	std::cout<<"finish sort: "<<elapsed_time_ms<<" seconds."<<std::endl;
}

void readForksFromFile(std::string filename) {
    std::ifstream myFile(filename);
    if(!myFile.is_open()) throw std::runtime_error("Could not open file");
    //Iterate lines
    std::string line;
    //skip the first line
	std::getline(myFile, line);
    while(std::getline(myFile, line)) {
        std::stringstream s_stream(line);
		int cnt = 0;
		uint64_t snapshotID = 1;
        uint64_t forkID = 1;
        while(s_stream.good()) {
			std::string substr;
			std::getline(s_stream, substr, ',');
			std::istringstream iss(substr);
            if (cnt == 0) {
                iss >> snapshotID;
            }
			else if (cnt == 1) {
				iss>>forkID;
			}
			++cnt;
		}
        forks.insert(forkID);
        forks.insert(snapshotID);
    }
    std::cout<<forks.size()<<std::endl;
}

void findCommunityFork(std::string filename) {
    std::ifstream myFile(filename);
    if(!myFile.is_open()) throw std::runtime_error("Could not open file");
    //Iterate lines
    std::string line;
    std::ofstream outFile;
    outFile.open("/home/sv/dup_parameters.csv", std::ofstream::out | std::ofstream::app);
    while(std::getline(myFile, line)) {
        std::stringstream s_stream(line);
		int cnt = 0;
		uint64_t forkID = 1;
        uint64_t date = 1;
        while(s_stream.good()) {
			std::string substr;
			std::getline(s_stream, substr, ',');
			std::istringstream iss(substr);
			if (cnt == 1) {
				iss>>forkID;
			}else if(cnt == 2) {
                iss>>date;
            }
			++cnt;
		}
        if(communityForks.count(forkID)) {
            continue;
        }
        auto t_start = std::chrono::high_resolution_clock::now();
        std::cout<<"Fork id: "<<forkID<<std::endl;
        std::unordered_set<uint64_t> authors;
        int commitCnt = 0;
        for(int i = 0; i < int(revisionSnapshots[forkID].size()); ++i) {
            if(revisionSnapshots[forkID][i].date > date && revisionSnapshots[forkID][i].date <= 1548633600) {
                authors.insert(revisionSnapshots[forkID][i].author);
                ++commitCnt;
            }
        }
        std::cout<<"Fork: "<<forkID<<std::endl;
        std::string ret="";
        ret += uint64_to_string(forkID);
        ret += ",";
        ret += std::to_string(authors.size());
        ret += ",";
        ret += std::to_string(commitCnt);
        ret += "\n";
        outFile<<ret;
        communityForks.insert(forkID);
        auto t_end = std::chrono::high_resolution_clock::now();
        double elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end-t_start).count();
        elapsed_time_ms /= 1000.0;
        std::cout<<"fork id: "<<forkID<<"finish: "<<elapsed_time_ms<<" seconds."<<std::endl;
    }
    outFile.close();
    myFile.close();
}

void exportCommunityFork() {
    std::ofstream myfile;
	myfile.open("/home/sv/community_dup.csv", std::ofstream::out | std::ofstream::app);
	auto t_start = std::chrono::high_resolution_clock::now();
    for(uint64_t forkID: communityForks) {
        std::string ret="";
        ret += uint64_to_string(forkID);
        ret += "\n";
        myfile<<ret;
    }
    auto t_end = std::chrono::high_resolution_clock::now();
	auto elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end-t_start).count();
	elapsed_time_ms /= 1000.0;
	std::cout<<"finish writing community forks: "<<elapsed_time_ms<<" seconds."<<std::endl;
	myfile.close();
}

void exportPersonalFork() {
    std::ofstream myfile;
	myfile.open("/home/sv/personal_dup.csv", std::ofstream::out | std::ofstream::app);
	auto t_start = std::chrono::high_resolution_clock::now();
    for(uint64_t forkID: forks) {
        if (!communityForks.count(forkID)){
            std::string ret="";
            ret += uint64_to_string(forkID);
            ret += "\n";
            myfile<<ret;
        }
    }
    auto t_end = std::chrono::high_resolution_clock::now();
	auto elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end-t_start).count();
	elapsed_time_ms /= 1000.0;
	std::cout<<"finish writing personal forks: "<<elapsed_time_ms<<" seconds."<<std::endl;
	myfile.close();
}

int main() {
    readForksFromFile("/home/sv/origin_dup_date.csv");
    loadRevisionHistory("/home/sv/origin_revision_data.csv");
    findCommunityFork("/home/sv/origin_dup_date.csv");
    // exportCommunityFork();
    // exportPersonalFork();
    return 0;
}


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
	Revision(const std::string &revisionID, const uint64_t &date){
		this->revisionID = revisionID;
		this->date = date;
	}
};

//=========================== FUNCTIONS ==========================
std::vector<uint64_t>snapshots;
const uint64_t MIN_DATE = 1517097600;
std::vector<std::vector<Revision>> revisionSnapshots;

std::unordered_map<std::string,uint64_t> buildSnapshotTable(const std::vector<Revision> &snapshot) {
	std::unordered_map<std::string,uint64_t> result;
	for(Revision revision : snapshot) {
		result[revision.revisionID] = revision.date;
	}
	return result;
}
std::string uint64_to_string( uint64_t value ) {
    std::ostringstream os;
    os << value;
    return os.str();
}

void sortVectorByDate(int start, int end) {
	for(int i = start; i < end; ++i) {
		std::sort(revisionSnapshots[i].begin(), revisionSnapshots[i].end(), [](const Revision &lhs, const Revision &rhs){
			return lhs.date < rhs.date;
		});
	}
}

void findFork(int start, int end) {
	std::ofstream myfile;
	myfile.open("/home/sv/origin_dup_date_3_" + std::to_string(start) + ".csv", std::ofstream::out | std::ofstream::app);
	auto t_start = std::chrono::high_resolution_clock::now();
	for(int i = start; i < end; ++i) {
		auto t_start_build = std::chrono::high_resolution_clock::now();
		std::unordered_map<std::string, uint64_t> snapshotTable = buildSnapshotTable(revisionSnapshots[i]);
		std::cout<<"finish build snapshot table with snapshot: "<<snapshots[i]<<" and number: "<<i<<std::endl;
		auto t_end_build = std::chrono::high_resolution_clock::now();
		double elapsed_time_ms_build = std::chrono::duration<double, std::milli>(t_end_build-t_start_build).count();
		elapsed_time_ms_build /= 1000.0;
		std::cout<<"Time: "<<elapsed_time_ms_build<<" seconds."<<std::endl;
		for(int j = 0; j < int(revisionSnapshots.size()); ++j) {
			if(j != i) {
				int k = revisionSnapshots[j].size()-1;
				while(k >= 0) {
					if(snapshotTable.count(revisionSnapshots[j][k].revisionID)) {
						std::string ret="";
						ret += uint64_to_string(snapshots[i]);
						ret += ",";
						ret += uint64_to_string(snapshots[j]);
						ret += ",";
						ret += uint64_to_string(revisionSnapshots[j][k].date);
						ret += "\n";
						myfile<<ret;
						while(k>=0 && snapshotTable.count(revisionSnapshots[j][k].revisionID)) {
							--k;
						}
					}else {
						--k;
					}
				}
			}
		}
	}
	auto t_end = std::chrono::high_resolution_clock::now();
	auto elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end-t_start).count();
	elapsed_time_ms /= 1000.0;
	std::cout<<"finish writing end at(excluded) "<<end<<": "<<elapsed_time_ms<<" seconds."<<std::endl;
	myfile.close();
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
	int index = 0;
    //Read from the first command line argument, assume it's gzipped
    std::ifstream file(name);
    //Iterate lines
    std::string line;
	auto t_start = std::chrono::high_resolution_clock::now();
    while(std::getline(file, line)) {
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
			}else if (cnt==3) {
				std::string _date;
				iss>>_date;
				date = getSecondsSince1970Until(_date);
			}else if(cnt == 7) {
				iss>>revisionID;
			}
			++cnt;
		}
		if(snapshots.size() == 0){
			snapshots.emplace_back(snapshotID);
			revisionSnapshots.emplace_back(std::vector<Revision>{});
			revisionSnapshots[index].emplace_back(Revision(revisionID, date));
			std::cout<<"Snapshot id: "<<snapshotID<<"with number: "<<snapshots.size()<<std::endl;
		}else {
			if(snapshotID == snapshots[index]){//old snapshot
				revisionSnapshots[index].emplace_back(Revision(revisionID, date));
			}else {//new snapshot
				snapshots.emplace_back(snapshotID);
				revisionSnapshots.emplace_back(std::vector<Revision>{});
				++index;
				revisionSnapshots[index].emplace_back(Revision(revisionID, date));
				std::cout<<"Snapshot id: "<<snapshotID<<"with number: "<<snapshots.size()<<std::endl;
			}
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
	int len = revisionSnapshots.size();
	const int step = 7498;
	int start = 0;
	int end = step;
	std::vector<std::thread> threads;
	while(end < len) {
		threads.emplace_back(std::thread(sortVectorByDate, start, end));
		start = end;
		end = std::min(end+step, len);
	}
	threads.emplace_back(std::thread(sortVectorByDate, start, end));

	for (auto &th : threads) {
		th.join();
	}
	t_end = std::chrono::high_resolution_clock::now();
	elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end-t_start).count();
	elapsed_time_ms /= 1000.0;
	std::cout<<"finish sort: "<<elapsed_time_ms<<" seconds."<<std::endl;

	t_start = std::chrono::high_resolution_clock::now();
	std::vector<std::thread> fork_threads;
	start = 0;
	end = step;
	while(end < len) {
		fork_threads.emplace_back(std::thread(findFork, start, end));
		start = end;
		end = std::min(end+step, len);
	}
	fork_threads.emplace_back(std::thread(findFork, start, end));
	for (auto &th : fork_threads) {
		th.join();
	}
	elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end-t_start).count();
	elapsed_time_ms /= 1000.0;
	std::cout<<"finish writing: "<<elapsed_time_ms<<" seconds."<<std::endl;
}

int main()
{
	std::string filename = "/mnt/17volume/data/origin_revision_data_3.csv";
	loadRevisionHistory(filename);
	return 0;
}

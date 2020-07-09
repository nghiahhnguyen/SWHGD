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

// load the record into the memory
// void loadRevisionHistory(char *filePath)
// {
// 	auto start = chrono::high_resolution_clock::now();
// 	ifstream fin;
// 	fin.open(filePath);

// 	string line;
// 	// skip the header
// 	getline(fin, line);
// 	string id, parentId, parentRankStr;
// 	int parentRank;
// 	while (getline(fin, line)) {
// 		stringstream ss(line);
// 		getline(ss, id, ',');
// 		getline(ss, parentId, ',');
// 		getline(ss, parentRankStr, '\n');
// 		parentRank = stoi(parentRankStr);
// 		uint32_t revisionIdx = mergeRevision(id),
// 				 parentRevisionIdx = mergeRevision(parentId);
// 		graphParents[revisionIdx].push_back(ParentRelationship(parentRank, parentRevisionIdx));
// 		graphChildren[parentRevisionIdx].push_back(ParentRelationship(parentRank, revisionIdx));
// 	}
// 	fin.close();
// 	auto stop = chrono::high_resolution_clock::now();
// 	auto duration = chrono::duration_cast<chrono::minutes>(stop - start);

// 	cout << "Finished loading file " << filePath << " after " << duration.count() << " mins\n"
// 		 << "Total size till now: revisions - " << revisions.size() << endl;
// }

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
	myfile.open("/home/sv/snapshot_fork_date_" + std::to_string(start) + ".csv", std::ofstream::out | std::ofstream::app);
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
				for(int k = revisionSnapshots[j].size()-1; k >= 0; --k) {
					if(snapshotTable.count(revisionSnapshots[j][k].revisionID)) {
						std::string ret="";
						ret += uint64_to_string(snapshots[i]);
						ret += ",";
						ret += uint64_to_string(snapshots[j]);
						ret += ",";
						ret += uint64_to_string(revisionSnapshots[j][k].date);
						ret += "\n";
						myfile<<ret;
						break;
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

void loadRevisionHistory(std::string name)
{
	int index = 0;
    //Read from the first command line argument, assume it's gzipped
    std::ifstream file(name, std::ios_base::in | std::ios_base::binary);
    boost::iostreams::filtering_streambuf<boost::iostreams::input> inbuf;
    inbuf.push(boost::iostreams::gzip_decompressor());
    inbuf.push(file);
    //Convert streambuf to istream
    std::istream instream(&inbuf);
    //Iterate lines
    std::string line;
	getline(instream, line);
	auto t_start = std::chrono::high_resolution_clock::now();
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
		if(date >= MIN_DATE) {
			//the first snapshot
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
	const int step = 60724;
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

	// t_start = std::chrono::high_resolution_clock::now();
	// for(int i = 0; i < int(revisionSnapshots.size()); ++i) {
	// 	auto t_start_build = std::chrono::high_resolution_clock::now();
	// 	std::unordered_map<std::string, uint64_t> snapshotTable = buildSnapshotTable(revisionSnapshots[i]);
	// 	std::cout<<"finish build snapshot table with snapshot: "<<snapshots[i]<<" and number: "<<i<<std::endl;
	// 	auto t_end_build = std::chrono::high_resolution_clock::now();
	// 	double elapsed_time_ms_build = std::chrono::duration<double, std::milli>(t_end_build-t_start_build).count();
	// 	elapsed_time_ms_build /= 1000.0;
	// 	std::cout<<"Time: "<<elapsed_time_ms_build<<" seconds."<<std::endl;
	// 	for(int j = 0; j < int(revisionSnapshots.size()); ++j) {
	// 		if(j != i) {
	// 			for(int k = revisionSnapshots[j].size()-1; k >= 0; --k) {
	// 				if(snapshotTable.count(revisionSnapshots[j][k].revisionID)) {
	// 					std::string ret="";
	// 					ret += uint64_to_string(snapshots[i]);
	// 					ret += ",";
	// 					ret += uint64_to_string(snapshots[j]);
	// 					ret += ",";
	// 					ret += uint64_to_string(revisionSnapshots[j][k].date);
	// 					ret += "\n";
	// 					myfile<<ret;
	// 					break;
	// 				}
	// 			}
	// 		}
	// 	}
	// }
	// t_end = std::chrono::high_resolution_clock::now();
	// elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end-t_start).count();
	// elapsed_time_ms /= 1000.0;
	// std::cout<<"finish writing: "<<elapsed_time_ms<<" seconds."<<std::endl;
	// myfile.close();
}

// Revision findForkPositions(const vector<Revision> &original, const vector<Revision> &potentialFork)
// {
// 	unordered_map<int, Revision> idToRevision;

// 	// store the first repo information to a tree
// 	for (Revision revision : original) {
// 		idToRevision.insert(mp(revision.revisionId, revision));
// 	}
// }

int main()
{
	std::string filename = "/mnt/17volume/data/snapshot_revision_git.csv.gz";
	loadRevisionHistory(filename);
	return 0;
}

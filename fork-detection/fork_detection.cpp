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

std::unordered_set<uint64_t>snapshots;

const uint64_t MIN_DATE = 1517097600;

std::unordered_map<std::string,uint64_t> buildSnapshotTable(const std::vector<Revision*> &snapshot) {
	std::unordered_map<std::string,uint64_t> result;
	for(Revision *revision : snapshot) {
		result[revision->revisionID] = revision->date;
	}
	return result;
}
void loadRevisionHistory(std::string name)
{
	std::vector<std::vector<Revision*>> revisionSnapshots;
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
			if(snapshots.count(snapshotID)) {
				revisionSnapshots[index].emplace_back(new Revision(revisionID, date));
			}else {
				revisionSnapshots.emplace_back(std::vector<Revision*>{});
				revisionSnapshots[index].emplace_back(new Revision(revisionID, date));
				std::cout<<"Snapshot id: "<<snapshotID<<" with number: "<<snapshots.size()<<std::endl;
				snapshots.insert(snapshotID);
			}
		}		
    }
    //Cleanup
    file.close();
	//sort date
	for(int i = 0; i < revisionSnapshots.size(); ++i) {
		sort(revisionSnapshots[i].begin(), revisionSnapshots[i].end(), [](const Revision* lhs, const Revision* rhs){
			return lhs->date < rhs->date;
		});
	}
	for(int i = 0; i < revisionSnapshots.size(); ++i) {
		std::unordered_map<std::string, uint64_t> snapshotTable = buildSnapshotTable(revisionSnapshots[i]); 
		
	}
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

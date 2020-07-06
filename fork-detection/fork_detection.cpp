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
<<<<<<< HEAD
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#define BOOST_IOSTREAMS_NO_LIB

using namespace std;
=======
#include <boost/assign.hpp>
>>>>>>> add fork code

#define mp make_pair
typedef unordered_map<string, uint32_t> cati;
typedef pair<uint32_t, uint32_t> ii;

//=========================== DATA STRUCTURE ==========================
class Revision {
public:
	Revision()
		: revisionId(0), date(0){};
	uint64_t revisionId, date;
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



Revision findForkPositions(const vector<Revision> &original, const vector<Revision> &potentialFork)
{
	unordered_map<int, Revision> idToRevision;

	// store the first repo information to a tree
	for (Revision revision : original) {
		idToRevision.insert(mp(revision.revisionId, revision));
void loadRevisionHistory(char *name)
{
    //Read from the first command line argument, assume it's gzipped
    std::ifstream file(argv[1], std::ios_base::in | std::ios_base::binary);
    boost::iostreams::filtering_streambuf<boost::iostreams::input> inbuf;
    inbuf.push(boost::iostreams::gzip_decompressor());
    inbuf.push(file);
    //Convert streambuf to istream
    std::istream instream(&inbuf);
    //Iterate lines
    std::string line;
	getline(fin, line);
    while(std::getline(instream, line)) {
        std::cout << line << std::endl;
    }
    //Cleanup
    file.close();
	
	string basePath = "/mnt/17volume/data/snapshot_revision_git.csv.gz.part" + name;
	auto start = chrono::high_resolution_clock::now();
	string filePath = basePath + name;
	ifstream fin;
	fin.open(filePath);

	string line;
	// skip the header
	if (name = "aa") {
		
	}
	string id, parentId, ;
	int parentRank;
	while (getline(fin, line)) {
		stringstream ss(line);
		getline(ss, id, ',');
		getline(ss, parentId, ',');
		getline(ss, parentRankStr, '\n');
		parentRank = stoi(parentRankStr);
		uint32_t revisionIdx = mergeRevision(id),
				 parentRevisionIdx = mergeRevision(parentId);
		graphParents[revisionIdx].push_back(ParentRelationship(parentRank, parentRevisionIdx));
		graphChildren[parentRevisionIdx].push_back(ParentRelationship(parentRank, revisionIdx));
	}

	// extract the latest fork position
	for (auto revisionIter = potentialFork.rbegin(); revisionIter != potentialFork.rend(); ++revisionIter) {
		auto seekIter = idToRevision.find(revisionIter->revisionId);
		// if there is match
		if (seekIter != idToRevision.end()) {
			return *revisionIter;
		}
	}
	return Revision();
}

int main(int argc, char **argv)
{

	return 0;
}

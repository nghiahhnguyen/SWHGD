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

using namespace std;

#define mp make_pair
typedef unordered_map<string, uint32_t> cati;
typedef pair<uint32_t, uint32_t> ii;

//=========================== DATA STRUCTURE ==========================


//=========================== FUNCTIONS ==========================

// load the record into the memory
void loadRevisionHistory(char *filePath)
{
	auto start = chrono::high_resolution_clock::now();
	ifstream fin;
	fin.open(filePath);

	string line;
	// skip the header
	getline(fin, line);
	string id, parentId, parentRankStr;
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
	fin.close();
	auto stop = chrono::high_resolution_clock::now();
	auto duration = chrono::duration_cast<chrono::minutes>(stop - start);

	cout << "Finished loading file " << filePath << " after " << duration.count() << " mins\n"
		 << "Total size till now: revisions - " << revisions.size() << endl;
}

int main(int argc, char **argv)
{
    
	return 0;
}

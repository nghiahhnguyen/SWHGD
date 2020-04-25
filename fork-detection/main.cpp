#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <queue>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

using namespace std;

#define mp make_pair
typedef unordered_map<string, uint32_t> cati;

class Revision {
public:
	Revision(string newId) { strcpy(id, newId.c_str()); }
	char id[40];
};

class ParentRelationship {
public:
	ParentRelationship(int rank, string d)
		: parentRank(rank), dest(d){};
	int parentRank;
	string dest;
};

vector<Revision> revisions;
vector<vector<ParentRelationship>> graphParent, graphChildren;
cati idToGraphIdx;

uint32_t mergeRevision(string id)
{
	uint32_t currentIdx;
	currentIdx = uint32_t(graphParent.size());
	unordered_map<string, uint32_t>::const_iterator it = idToGraphIdx.find(id);

	// not exist yet
	if (it == idToGraphIdx.end()) {
		graphParent.push_back(vector<ParentRelationship>());
		graphChildren.push_back(vector<ParentRelationship>());
		revisions.push_back(Revision(id));
		return currentIdx++;
	}
	return it->second;
}

void readRecord()
{
	ifstream fin;
	string filePath = "/tmp/revision_history_14.csv";
	fin.open(filePath);

	string line;
	// skip the header
	getline(fin, line);
	string id, parentId;
    	int parentRank;
	while (getline(fin, line)) {
		stringstream ss(line);
		ss >> id >> parentId >> parentRank;
		cout << id << ' ' << parentId << ' ' << parentRank << endl;
		uint32_t revisionIdx = mergeRevision(id),
				 parentRevisionIdx = mergeRevision(parentId);
		cout << "Indexes: " << revisionIdx << ' ' << parentRevisionIdx << endl;
		graphParent[revisionIdx].push_back(ParentRelationship(parentRank, parentId));
		graphChildren[parentRevisionIdx].push_back(ParentRelationship(parentRank, id));
	}
	cout << "Total size: " << revisions.size() << ' ' << graphParent.size() << ' ' << graphChildren.size() << endl;

	fin.close();
}

int main()
{
	readRecord();
	return 0;
}

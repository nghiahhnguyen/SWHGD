#include <cstring>
#include <fstream>
#include <iostream>
#include <queue>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

typedef pair<int, int> ii;
typedef vector<vector<ii>> vvii;
typedef unordered_map<char[40], uint32_t> cati;

using namespace std;

class Revision {
public:
	Revision(string newId) { strcpy(id, newId.c_str()); }
	char id[40];
};

vector<Revision> revisions;
vvii graphParent, graphChildren;
cati idToGraphIdx;

uint32_t mergeRevision(string id)
{
	uint32_t currentIdx;
	currentIdx = uint32_t(graphParent.size());
	unordered_map<char[40], uint32_t>::const_iterator it = idToGraphIdx.find(id.c_str());

	// not exist yet
	if (it == idToGraphIdx.end()) {
		graphParent.append(vector<ii>());
		graphChildren.append(vector<ii>());
        revisions.push_back(Revision(id));
		return currentIdx++;
	}
	return it->second;
}

void readRecord()
{
	ifstream fin;
	string filePath = "/var/lib/neo4j/import/revision_history_14.csv";
	fin.open(filePath);

	string line;
	// skip the header
	getline(fin, line);
	string id, parentId, parentRank;
	while (getline(fin, line)) {
		stringstream ss(line);
		ss >> id >> parentId >> parentRank;
		uint32_t revisionIdx = mergeRevision(id),
				 parentRevisionIdx = mergeRevision(parentId);
        graphParent[id].push_back({parentId, parentRank});
        graphChildren[parentId].push_back({id, parentRank});
	}

    fin.close();
}

int main()
{
    readRecord();
    return 0;
}
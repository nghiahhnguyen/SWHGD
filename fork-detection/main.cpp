#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <queue>
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

class Revision {
public:
	Revision(string newId) { strcpy(id, newId.c_str()); }
	char id[40];
};

class Snapshot {
public:
	Snapshot(string newId) { strcpy(id, newId.c_str()); }
	char id[40];
};

class Origin {
public:
	Origin(string newId) { strcpy(id, newId.c_str()); }
	char id[40];
};

class ParentRelationship {
public:
	ParentRelationship(int rank, int dest)
		: parentRank(rank), dest(dest){};
	int parentRank;
	uint32_t dest;
};

// list of Revision nodes
vector<Revision> revisions;
// list of Snapshot nodes
vector<Snapshot> snapshots;
// list of Origin nodes
vector<Origin> origins;

// the adjacency list for Revision - Revision relationship
vector<vector<ParentRelationship>> graphParents, graphChildren;
// the adjacency list for the Revision - Snapshot relationship
vector<vector<uint32_t>> graphRevSnap, graphSnapRev;
// the adjacency list for the Snapshot - Origin relationship
vector<vector<uint32_t>> graphSnapOrigin, graphOriginSnap;

//=========================== FUNCTIONS ==========================
// map from the Id of the Revision node to the index in revisions
cati idToGraphIdx;

uint32_t mergeRevision(string id)
{
	uint32_t currentIdx;
	currentIdx = uint32_t(graphParents.size());
	unordered_map<string, uint32_t>::const_iterator it = idToGraphIdx.find(id);

	// not exist yet
	if (it == idToGraphIdx.end()) {
		graphParents.push_back(vector<ParentRelationship>());
		graphChildren.push_back(vector<ParentRelationship>());
		revisions.push_back(Revision(id));
		idToGraphIdx.insert(mp(id, currentIdx));
		return currentIdx++;
	}
	return it->second;
}

// load the record into the memory
void readRecord(char *filePath)
{
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
	cout << "Total size: " << revisions.size() << ' ' << graphParents.size() << ' ' << graphChildren.size() << endl;

	fin.close();
}

void printFork(string id)
{
	uint32_t startNodeIndex = -1;
	cati::iterator it = idToGraphIdx.find(id);
	// if revision does not exist in Revision list
	if (it == idToGraphIdx.end()) {
		printf("Revision does not exists.\n");
		return;
	}
	else {
		startNodeIndex = it->second;
	}

	/*
	Find the potential fork positions, for each position, get the index
	of that Revision node, along with its child on the branch from the
	starting Revision node
	*/
	stack<ii> S;
	vector<ii> potentialForkPoints;
	vector<bool> visited(uint32_t(revisions.size()));
	S.push(mp(startNodeIndex, -1));
	visited[startNodeIndex] = true;
	while (!S.empty()) {
		ii pairNode = S.top();
		S.pop();
		uint32_t curNodeIndex = pairNode.first,
				 prevNodeIndex = pairNode.second;

		if (visited[curNodeIndex])
			continue;

		// if node is potential forking point
		if (uint32_t(graphChildren[curNodeIndex].size()) > 1) {
			potentialForkPoints.push_back(mp(curNodeIndex, prevNodeIndex));
		}

		for (ParentRelationship p : graphParents[curNodeIndex]) {
			uint32_t parentIndex = p.dest;
			if (!visited[parentIndex]) {
				S.push(mp(parentIndex, curNodeIndex));
			}
		}
	}

}

int main(int argc, char **argv)
{
	readRecord(argv[1]);
	return 0;
}

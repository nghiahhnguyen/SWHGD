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

class Revision {
public:
	Revision(string newId) { strcpy(id, newId.c_str()); }
	char id[40];
};

class Node {
public:
	Node(uint32_t id)
		: id(id){};
	uint32_t id;
};

class ParentRelationship {
public:
	ParentRelationship(int rank, int dest)
		: rank(rank), dest(dest){};
	int rank;
	uint32_t dest;
};

// list of Revision nodes
vector<Revision> revisions;
// list of Snapshot nodes
vector<Node> snapshots;
// list of Origin nodes
vector<Node> origins;

// the adjacency list for Revision - Revision relationship
vector<vector<ParentRelationship>> graphParents, graphChildren;
// the adjacency list for the Revision - Snapshot relationship
vector<vector<uint32_t>> graphRevSnap, graphSnapRev;
// the adjacency list for the Snapshot - Origin relationship
vector<vector<uint32_t>> graphSnapOrigin, graphOriginSnap;

// map from the Id of the Revision node to the index in revisions
cati revisionIdToGraphIdx;
unordered_map<uint32_t, uint32_t> originIdToGraphIdx, snapshotIdToGraphIdx;

//=========================== FUNCTIONS ==========================

uint32_t mergeRevision(const string &id)
{
	uint32_t currentIdx;
	currentIdx = uint32_t(graphParents.size());
	unordered_map<string, uint32_t>::const_iterator it = revisionIdToGraphIdx.find(id);

	// not exist yet
	if (it == revisionIdToGraphIdx.end()) {
		graphParents.push_back(vector<ParentRelationship>());
		graphChildren.push_back(vector<ParentRelationship>());
		revisions.push_back(Revision(id));
		revisionIdToGraphIdx.insert(mp(id, currentIdx));
		return currentIdx++;
	}
	return it->second;
}

uint32_t mergeNode(const uint32_t &id, vector<Node> &graph, unordered_map<uint32_t, uint32_t> &idToGraphIdx)
{
	uint32_t currentIdx;
	currentIdx = uint32_t(graph.size());
	unordered_map<uint32_t, uint32_t>::const_iterator it = idToGraphIdx.find(id);

	// not exist yet
	if (it == idToGraphIdx.end()) {
		graph.push_back(Node(id));
		idToGraphIdx.insert(mp(id, currentIdx));
		return currentIdx++;
	}
	return it->second;
}

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

void loadOriginSnapshot(char *filePath)
{
	auto start = chrono::high_resolution_clock::now();
	ifstream fin;
	fin.open(filePath);

	string line;
	// skip the header
	getline(fin, line);
	string origin, snapshot, revision;
	uint32_t originId, snapshotId, revisionIdx;
	while (getline(fin, line)) {
		stringstream ss(line);
		getline(ss, origin, ',');
		getline(ss, snapshot, ',');
		getline(ss, revision, '\n');
		originId = stoi(origin);
		snapshotId = stoi(snapshot);

		cati::iterator it = revisionIdToGraphIdx.find(revision);
		// if revision does not exist
		if (it == revisionIdToGraphIdx.end()) {
			printf("Revision ID not found, skipping this entry\n");
			continue;
		}
		revisionIdx = it->second;
		uint32_t snapshotIdx = mergeNode(snapshotId, snapshots, snapshotIdToGraphIdx),
				 originIdx = mergeNode(originId, origins, originIdToGraphIdx);
		graphSnapOrigin[snapshotIdx].push_back(originIdx);
		graphOriginSnap[originIdx].push_back(snapshotIdx);
		graphSnapRev[snapshotIdx].push_back(revisionIdx);
		graphRevSnap[revisionIdx].push_back(snapshotIdx);
	}
	fin.close();
	auto stop = chrono::high_resolution_clock::now();
	auto duration = chrono::duration_cast<chrono::minutes>(stop - start);
	cout << "Finished loading file " << filePath << " after " << duration.count() << " mins\n"
		 << "Total size till now: revisions - " << revisions.size() << endl;
}

void exportFork(const uint32_t &originId, const string &exportPath)
{
	uint32_t originIdx = originIdToGraphIdx[originId],
			 snapshotIdx = graphOriginSnap[originIdx][0],
			 startNodeIndex = graphSnapRev[snapshotIdx][0];

	/*
	Find potential fork positions, for each position, push the index
	of that Revision node, along with its child on the branch from the
	starting Revision node to `potentialForkPositions`
	*/
	stack<ii> Sii;
	vector<ii> potentialForkPositions;
	vector<bool> visited(uint32_t(revisions.size()));
	Sii.push(mp(startNodeIndex, startNodeIndex));
	visited[startNodeIndex] = true;
	while (!Sii.empty()) {
		ii pairNode = Sii.top();
		Sii.pop();
		uint32_t curNodeIndex = pairNode.first,
				 prevNodeIndex = pairNode.second;

		if (visited[curNodeIndex])
			continue;

		// if node is potential forking point
		if (uint32_t(graphChildren[curNodeIndex].size()) > 1) {
			potentialForkPositions.push_back(mp(curNodeIndex, prevNodeIndex));
		}

		for (ParentRelationship p : graphParents[curNodeIndex]) {
			uint32_t parentIndex = p.dest;
			if (!visited[parentIndex]) {
				visited[parentIndex] = true;
				Sii.push(mp(parentIndex, curNodeIndex));
			}
		}
	}

	/*
	For each potential fork position in `potentialForkPositions`, traverse through its
	child to get fork origins, notice that this does not confirm yet either the original
	origin or the origins found in this step is the real fork
	*/
	stack<uint32_t> Si;
	set<ii> forkSnapshotOrigin;
	for (ii forkPair : potentialForkPositions) {
		uint32_t forkPosition = forkPair.first,
				 prevChild = forkPair.second;
		visited.assign(uint32_t(revisions.size()), false);
		Si.push(forkPosition);
		visited[forkPosition] = true;
		while (!Si.empty()) {
			uint32_t nodeIdx = Si.top();
			Si.pop();
			if (visited[nodeIdx])
				continue;

			// found a point with snapshot
			if (graphRevSnap[nodeIdx].size() > 0) {
				for (uint32_t snapshotIdx : graphRevSnap[nodeIdx]) {
					for (uint32_t originIdx : graphSnapOrigin[snapshotIdx]) {
						forkSnapshotOrigin.insert(mp(snapshotIdx, originIdx));
					}
				}
			}
		}
	}

	ofstream fout;
	string outFilePath = exportPath + "/" + to_string(originId) + ".csv";
	fout << "snapshot_id,origin_id\n";
	for (set<ii>::iterator it = forkSnapshotOrigin.begin(); it != forkSnapshotOrigin.end(); ++it) {
		fout << snapshots[it->first].id << ',' << origins[it->second].id << "\n";
	}
	fout.open(outFilePath);
}

int main(int argc, char **argv)
{
	loadRevisionHistory(argv[1]);
	loadOriginSnapshot(argv[2]);
	string buffer(argv[4]);
	stringstream ss(buffer);
	uint32_t startOriginId;
	ss >> startOriginId;
	exportFork(startOriginId, argv[3]);
	return 0;
}

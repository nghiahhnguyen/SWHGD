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

int main(int argc, char** argv)
{
	printf("Revision max_size: %lld\n", revisions.max_size());
	printf("graphParent max_size: %lld\n", graphParent.max_size());
	return 0;
}

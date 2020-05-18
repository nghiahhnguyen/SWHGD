from neo4j import GraphDatabase

class Neo4jQuery(object):

    def __init__(self):
        uri = "bolt://localhost:7687"
        user = "neo4j"
        password = "password"
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def run_query(self, fp, output_fp, batch_size):
        prefix = "/var/lib/neo4j/import/"
        #with open(prefix + fp, "r") as f:
            #file_row_count = sum(1 for line in f) - 1
        file_row_count = 6000000
        with self._driver.session() as session:
            count = 0
            for i in range(1, file_row_count + 1, batch_size):
                count += 1
                output_fp = output_fp[:-4] + str(count) + output_fp[-4:]
                result = session.write_transaction(self._run_query, fp="file:///"+fp, output_fp=output_fp, skip=i, batch_size=batch_size)
                print(result)

    @staticmethod
    def _run_query(tx, fp, output_fp, skip, batch_size):
        query = "CALL apoc.export.csv.query(\"CALL apoc.load.csv($fp, {skip: $skip, limit: $batch_size}) yield snapshot_id AS query_snapshot_id, node_id AS query_revision_id \
            MATCH (r:Revision{id: query_revision_id}) WITH query_revision_id, query_snapshot_id, r \
                CALL apoc.path.subgraphNodes(r, {relationshipFilter: '<PARENT'}) YIELD node WITH query_revision_id, query_snapshot_id, node MATCH(fork_snapshot:Snapshot)-[:BRANCH]->(node)  WHERE fork_snapshot.type is null and fork_snapshot.id <> query_snapshot_id WITH query_revision_id, query_snapshot_id, fork_snapshot.id AS fork_snapshot_id RETURN DISTINCT query_snapshot_id, query_revision_id, fork_snapshot_id;\", $output_fp, {})"
        result = tx.run(query, fp=fp, output_fp=output_fp, skip=skip, batch_size=batch_size)
        return result.single()[0]

if __name__ == "__main__":
    query = Neo4jQuery()
    fp = "list_of_forks_apoc_subgraph_git_master_6M_snapshots.csv"
    output_fp = "/mnt/17volume/data/list_of_forks_apoc_subgraph_git_master_verified_6M.csv"
    query.run_query(fp=fp, output_fp=output_fp, batch_size=100000)

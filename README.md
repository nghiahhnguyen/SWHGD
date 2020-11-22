We only focus on **git project**

Attention: THIS GUIDANCE IS DEDUCED FROM OUR PROCESS. In authors&#39; opinion, it is better than we did in our thesis.

1. Import revision to Neo4j:

- Export revision from PostgreSQL to file:

COPY (

SELECT encode(id,&#39;hex&#39;) as id, author, date FROM revision

)

TO &#39;/tmp/revision.csv&#39; delimiter &#39;,&#39; CSV HEADER;

- Import revision from file to Neo4j: Remember to put file in directory you set on config file of Neo4j.

USING PERIODIC COMMIT X

LOAD CSV WITH HEADERS FROM &#39;file:///revision.csv&#39; AS row

WITH row WHERE row.id IS NOT NULL

MERGE (r:Revision {id: row.id, author: row.author, date: apoc.date.parse(row.date, &#39;s&#39;, &#39;2017-05-09 06:19:41.823386&#39;, &#39;+07&#39;)});

X can be 100,000 or 1,000,000 depending on your ram memory. We use 100,000 for 64 GB RAM.

1. Import relationship between revisions to Neo4j:

Since we have more than 1 billion relationships in file revison\_history.csv.gz (download from this link: [https://annex.softwareheritage.org/public/dataset/graph/latest/sql/](https://annex.softwareheritage.org/public/dataset/graph/latest/sql/)). We separate it into 100,000 relationships, thus we have 14 files.

COPY (SELECT encode(id,&#39;hex&#39;) as id, encode(parent\_id, &#39;hex&#39;) as parent\_id FROM revision\_history ORDER BY id, parent\_id LIMIT 100000000 OFFSET 0) TO &#39;/tmp/revision\_history\_1.csv&#39; delimiter &#39;,&#39; CSV HEADER;

COPY (SELECT encode(id,&#39;hex&#39;) as id, encode(parent\_id, &#39;hex&#39;) as parent\_id FROM revision\_history ORDER BY id, parent\_id LIMIT 100000000 OFFSET 100000000) TO &#39;/tmp/revision\_history\_2.csv&#39; delimiter &#39;,&#39; CSV HEADER;

COPY (SELECT encode(id,&#39;hex&#39;) as id, encode(parent\_id, &#39;hex&#39;) as parent\_id FROM revision\_history ORDER BY id, parent\_id LIMIT 100000000 OFFSET 200000000) TO &#39;/tmp/revision\_history\_3.csv&#39; delimiter &#39;,&#39; CSV HEADER;

COPY (SELECT encode(id,&#39;hex&#39;) as id, encode(parent\_id, &#39;hex&#39;) as parent\_id FROM revision\_history ORDER BY id, parent\_id LIMIT 100000000 OFFSET 300000000) TO &#39;/tmp/revision\_history\_4.csv.gz&#39; delimiter &#39;,&#39; CSV HEADER;

COPY (SELECT encode(id,&#39;hex&#39;) as id, encode(parent\_id, &#39;hex&#39;) as parent\_id FROM revision\_history ORDER BY id, parent\_id LIMIT 100000000 OFFSET 400000000) TO &#39;/tmp/revision\_history\_5.csv.gz&#39; delimiter &#39;,&#39; CSV HEADER;

COPY (SELECT encode(id,&#39;hex&#39;) as id, encode(parent\_id, &#39;hex&#39;) as parent\_id FROM revision\_history ORDER BY id, parent\_id LIMIT 100000000 OFFSET 500000000) TO &#39;/tmp/revision\_history\_6.csv.gz&#39; delimiter &#39;,&#39; CSV HEADER;

COPY (SELECT encode(id,&#39;hex&#39;) as id, encode(parent\_id, &#39;hex&#39;) as parent\_id FROM revision\_history ORDER BY id, parent\_id LIMIT 100000000 OFFSET 600000000) TO &#39;/tmp/revision\_history\_7.csv.gz&#39; delimiter &#39;,&#39; CSV HEADER;

COPY (SELECT encode(id,&#39;hex&#39;) as id, encode(parent\_id, &#39;hex&#39;) as parent\_id FROM revision\_history ORDER BY id, parent\_id LIMIT 100000000 OFFSET 700000000) TO &#39;/tmp/revision\_history\_8.csv.gz&#39; delimiter &#39;,&#39; CSV HEADER;

COPY (SELECT encode(id,&#39;hex&#39;) as id, encode(parent\_id, &#39;hex&#39;) as parent\_id FROM revision\_history ORDER BY id, parent\_id LIMIT 100000000 OFFSET 800000000) TO &#39;/tmp/revision\_history\_9.csv.gz&#39; delimiter &#39;,&#39; CSV HEADER;

COPY (SELECT encode(id,&#39;hex&#39;) as id, encode(parent\_id, &#39;hex&#39;) as parent\_id FROM revision\_history ORDER BY id, parent\_id LIMIT 100000000 OFFSET 900000000) TO &#39;/tmp/revision\_history\_10.csv.gz&#39; delimiter &#39;,&#39; CSV HEADER;

COPY (SELECT encode(id,&#39;hex&#39;) as id, encode(parent\_id, &#39;hex&#39;) as parent\_id FROM revision\_history ORDER BY id, parent\_id LIMIT 100000000 OFFSET 1000000000) TO &#39;/tmp/revision\_history\_11.csv.gz&#39; delimiter &#39;,&#39; CSV HEADER;

COPY (SELECT encode(id,&#39;hex&#39;) as id, encode(parent\_id, &#39;hex&#39;) as parent\_id FROM revision\_history ORDER BY id, parent\_id LIMIT 100000000 OFFSET 1100000000) TO &#39;/tmp/revision\_history\_12.csv&#39; delimiter &#39;,&#39; CSV HEADER;

COPY (SELECT encode(id,&#39;hex&#39;) as id, encode(parent\_id, &#39;hex&#39;) as parent\_id FROM revision\_history ORDER BY id, parent\_id LIMIT 100000000 OFFSET 1200000000) TO &#39;/tmp/revision\_history\_13.csv&#39; delimiter &#39;,&#39; CSV HEADER;

COPY (SELECT encode(id,&#39;hex&#39;) as id, encode(parent\_id, &#39;hex&#39;) as parent\_id FROM revision\_history ORDER BY id, parent\_id LIMIT 2422013 OFFSET 1300000000) TO &#39;/tmp/revision\_history\_[y].csv&#39; delimiter &#39;,&#39; CSV HEADER;

Then, we import relationship into Neo4j:

1 \&lt;= x \&lt;= 14

USING PERIODIC COMMIT

LOAD CSV WITH HEADERS FROM &quot;file:///revision\_history\_[x].csv&quot; AS row

MATCH (r:Revision{id: row.id})

MATCH (p:Revision{id: row.parent\_id})

MERGE (r)\&lt;-[:PARENT]-(p);

Notice: This step took us 1 month. So any improvement is welcome.

1. Choose subset of repositories from the dataset:

In the thesis, we choose based on the visited date which is a mistake. So we suggest future researchers use the date of the branch to find the active repositories. Moreover, we only choose MASTER branch or HEAD branch, the branch that the pointer currently point to: [https://stackoverflow.com/questions/2304087/what-is-head-in-git](https://stackoverflow.com/questions/2304087/what-is-head-in-git).

- Find the latest date from all of MASTER/HEAD branches

COPY (

SELECT MAX(r.date) as max\_date

FROM origin as o

JOIN origin\_visit as ov ON o.id = ov.origin

JOIN snapshot as ss ON ov.snapshot\_id = ss.object\_id

JOIN snapshot\_branches as sbs ON ss.object\_id = sbs.snapshot\_id

JOIN snapshot\_branch as sb ON sbs.branch\_id = sb.object\_id

JOIN revision as r ON sb.target = r.id

WHERE ov.origin IS NOT NULL AND ss.object\_id IS NOT NULL AND sbs.branch\_id IS NOT NULL AND r.id IS NOT NULL AND o.type = &#39;git&#39; AND (encode(sb.name::bytea, &#39;escape&#39;) ~ &#39;.\*HEAD.\*&#39; OR encode(sb.name::bytea, &#39;escape&#39;) ~\* &#39;.\*master.\*&#39;)

After find the latest date among branches, we will find repositories which have branches from 3 months ago to the current latest date

COPY(

SELECT ov.origin as origin\_id, r.id as revision\_id

FROM origin as o

JOIN origin\_visit as ov ON o.id = ov.origin

JOIN snapshot as ss ON ov.snapshot\_id = ss.object\_id

JOIN snapshot\_branches as sbs ON ss.object\_id = sbs.snapshot\_id

JOIN snapshot\_branch as sb ON sbs.branch\_id = sb.object\_id

JOIN revision as r ON sb.target = r.id

WHERE ov.origin IS NOT NULL AND ss.object\_id IS NOT NULL AND sbs.branch\_id IS NOT NULL AND r.id IS NOT NULL AND o.type = &#39;git&#39; AND (encode(sb.name::bytea, &#39;escape&#39;) ~ &#39;.\*HEAD.\*&#39; OR encode(sb.name::bytea, &#39;escape&#39;) ~\* &#39;.\*master.\*&#39;) AND

(DATE\_PART(&#39;month&#39;, [MAX\_DATE]::date) - DATE\_PART(&#39;month&#39;, r.date)) \&lt;= 3

)

to &#39;/tmp/latest\_repository.csv&#39; delimiter &#39;,&#39; CSV HEADER;

MAX\_DATE: The max date you find put in here in format &#39;yyyy-mm-dd&#39;

Then we import table called latest\_repository:

COPY

latest\_repository(origin\_id, revision\_id)

FROM &#39;temp\latest\_repository.csv&#39; DELIMITER &#39;,&#39; CSV HEADER;

Then, we find the oldest version of those &#39;active&#39; repositories:

COPY(

SELECT ov.origin as origin\_id, min(r.date) as min\_date

FROM latest\_repository as lr

JOIN origin\_visit as ov ON lr.origin\_id = ov.origin

JOIN revision as r ON lr.revision\_id = r.id

WHERE ov.origin IS NOT NULL AND ss.object\_id IS NOT NULL AND sbs.branch\_id IS NOT NULL AND r.id IS NOT NULL

GROUP BY lr.origin\_id, r.date

)

to &#39;/tmp/oldest\_repository\_date.csv&#39; delimiter &#39;,&#39; CSV HEADER;

Import table called oldest\_repository to PostgreSQL:

COPY oldest\_repository

FROM &#39;/tmp/oldest\_repository\_date.csv&#39; delimiter &#39;,&#39; CSV HEADER;

Finally, we find the id of the revision the the oldest version of the active repositories point to :

COPY(

SELECT DISTINCT ov.origin as origin\_id, r.id as revision\_id

FROM oldest\_repository as or

JOIN origin\_visit as ov ON or.origin\_id = ov.origin

JOIN snapshot as ss ON ov.snapshot\_id = ss.object\_id

JOIN snapshot\_branches as sbs ON ss.object\_id = sbs.snapshot\_id

JOIN snapshot\_branch as sb ON sbs.branch\_id = sb.object\_id

JOIN revision as r ON r.date = or.min\_date

WHERE ov.origin IS NOT NULL AND ss.object\_id IS NOT NULL AND sbs.branch\_id IS NOT NULL AND r.id IS NOT NULL

GROUP BY or.origin\_id, r.date

)

to &#39;/tmp/oldest\_repository.csv&#39; delimiter &#39;,&#39; CSV HEADER;

1. Revision traversal

We will create relationship called &#39;BRANCH&#39; from repository to one particular revision for latest repository.

USING PERIODIC COMMIT 100000

LOAD CSV WITH HEADERS FROM &quot;file:///latest\_repository.csv&quot; AS row

MATCH (r:Revision{id: row.revision\_id})

MERGE (r)\&lt;-[:BRANCH]-(s:Repository{id: row.origin\_id});

With oldest\_repository, we have one more property called &quot;oldest&quot;

USING PERIODIC COMMIT 100000

LOAD CSV WITH HEADERS FROM &quot;file:///oldest\_repository.csv&quot; AS row

WITH row.origin\_id as origin\_id, row.id as revision\_id

WHERE revision\_id is not null

MATCH(r:Revision{id:revision\_id})

MERGE (r)\&lt;-[:BRANCH]-(s:Repository{id:origin\_id, oldest: &quot;true&quot;});

1. Export release to file:

COPY(

SELECT DISTINCT encode(id, &quot;hex&quot;) as release\_id, date as date

FROM release)

to &#39;/tmp/release.csv&#39; delimiter &#39;,&#39; CSV HEADER;

1. Import release to Neo4j:

USING PERIODIC COMMIT 100000

LOAD CSV WITH HEADERS FROM &quot;file:///release.csv&quot; AS row

MATCH (r:Revision{id: row.release\_id})

SET r.release\_date = apoc.date.parse(row.date, &#39;s&#39;, &#39;2017-05-09 06:19:41.823386&#39;, &#39;+07&#39;)

1. Extract git repository&#39;s revisions and releases:

CALL apoc.export.csv.query(

&quot;MATCH (s:Repository)-[:BRANCH]-\&gt;(r:Revision)

WITH DISTINCT r, s.id as origin\_id

CALL apoc.path.subgraphNodes(r, {relationshipFilter: \&quot;PARENT\&gt;\&quot;})

YIELD node WITH origin\_id, node.id as id, node.author as author, node.date as date, node.release\_date as release\_date;&quot;, &quot;/mnt/17volume/data/snapshot\_revision\_git.csv&quot;, {});

Conclusion: We have two files: snapshot\_revision\_git.csv and snapshot\_release\_git to extract metrics.

- Extract metrics: commit frequency, total author, total commit, daily/weekly/monthly contributor count

Since the file snapshot\_revision\_git is really big, we split it into 8 files since we have eight core ram. We do that to boost the process of extracting metrics.

The code: **extract\_metrics.py (remember to change tor origin\_id)**

Notice: We should handle the case for example we split the big file into file 1 and file 2, the last origin\_id of the file 1 and the first origin\_id of file 2 belong to the same origin id. Therefore, we cannot extract these metrics from the code above directly. In thesis, we combine the last origin of file 1 and the first origin of file 2 and handle it separately.

- Extract metrics: cycle time release

The code: **cycle\_time.py**

- Extract fork information in one year from the latest date: **fork\_detection.cpp in folder fork-detection** origin\_id, fork\_id, date
- Extract fork metrics: **extract\_fork\_metric.py in folder fork-detection**
- The duplication information do the same by changing the code of fork: fork\_detectin.cpp and extract\_fork\_metric.py
- Extract all metrics to one file: **extract\_metrics\_to\_one\_file.py**

# **Please contact us if there is any concern**

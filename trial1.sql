select * from snapshot_branch as sb limit 1;
select o.id, o.url, o.type, count(ov.origin)
from origin_visit as ov join origin as o on ov.origin = o.id
where o.type = 'git'
group by o.url, o.id, ov.origin
order by o.id;

select max(sbs.snapshot_id) over (partition by ov.origin), sbs.branch_id, ov.origin, o.url, ov.date, count(sbs.branch_id) over (partition by sbs.snapshot_id)
from snapshot_branches as sbs, snapshot as ss, origin_visit as ov, origin as o
where sbs.snapshot_id = ss.object_id and ss.object_id = ov.snapshot_id and ov.origin = o.id and o.type = 'git'
group by ov.origin, sbs.snapshot_id, sbs.branch_id, ov.date, o.id;
-- tra ve list of repo voi latest snapshot
select distinct(max(sbs.snapshot_id) over (partition by ov.origin)) as snapshot_id, encode(sb.target, 'hex') as rev_id
from snapshot_branches as sbs, snapshot as ss, origin_visit as ov, origin as o, snapshot_branch sb
where sbs.snapshot_id = ss.object_id and ss.object_id = ov.snapshot_id and ov.origin = o.id and sbs.branch_id = sb.object_id and ov.snapshot_id = 19091164
group by sbs.snapshot_id, sb.target, sb.target_type, ov.origin;
--với snapshot mới nhất của một project, trả về id của snapshot đó, các branch id và tên của branch id đó
with snapshots(snap_id, branch_id) as(
select distinct(max(sbs.snapshot_id) over (partition by ov.origin)), sbs.branch_id
from snapshot_branches as sbs, snapshot as ss, origin_visit as ov, origin as o
where sbs.snapshot_id = ss.object_id and ss.object_id = ov.snapshot_id and ov.origin = o.id
group by ov.origin, sbs.snapshot_id, sbs.branch_id)
select snaps.snap_id, encode(sb.target, 'hex'), encode(sb.name, 'escape')
from snapshot_branch as sb, snapshots as snaps
where sb.object_id = snaps.branch_id;

select * from snapshot_branches as sbs where sbs.snapshot_id = 56423881;
select distinct(min(sbs.snapshot_id) over (partition by ov.origin)), ov.origin, o.url
from snapshot_branches as sbs, snapshot as ss, origin_visit as ov, origin as o
where sbs.snapshot_id = ss.object_id and ss.object_id = ov.snapshot_id and ov.origin = o.id and o.type = 'git'
group by ov.origin, sbs.snapshot_id, o.url
order by ov.origin;

select o.id, o.url, count(o.id) over(partition by o.id)
from origin o
group by o.url, o.id
order by count(o.id) desc;


 


select encode(r.id, 'hex'), r.date, r.committer_date, encode(r.message,'escape'), r.author, r.committer, d.file_entries, d.dir_entries
		from revision as r 
		inner join snapshot_branch as sb on r.id = sb.target 
		inner join directory as d on r.directory = d.id
		where sb.object_id = 
		ANY(ARRAY(select sbs.branch_id from snapshot_branches as sbs where sbs.snapshot_id = 56423881))
		order by r.committer_date;

select d.dir_entries
		from revision as r 
		inner join snapshot_branch as sb on r.id = sb.target 
		inner join directory as d on r.directory = d.id
		where sb.object_id = 
		ANY(ARRAY(select sbs.branch_id from snapshot_branches as sbs where sbs.snapshot_id = 2))
		order by r.committer_date;
		
select array_agg(c)
from (
  select unnest(d.dir_entries)
  from revision as r 
  inner join snapshot_branch as sb on r.id = sb.target 
  inner join directory as d on r.directory = d.id
  where sb.object_id = 
		ANY(ARRAY(select sbs.branch_id from snapshot_branches as sbs where sbs.snapshot_id = 2))
		order by r.committer_date
) as dt(c);


select ARRAY(select encode(d.id, 'hex')
  from revision as r 
  inner join snapshot_branch as sb on r.id = sb.target 
  inner join directory as d on r.directory = d.id
  where sb.object_id = 
		ANY(ARRAY(select sbs.branch_id from snapshot_branches as sbs where sbs.snapshot_id = 2))
		order by r.committer_date);

-- software health and risk, lay ra nhung cai branch id cua project va metadata cua no dir name, file name
select dir.id, encode(dir.target, 'hex'), encode(dir.name, 'escape'), count(dir.name) over(partition by dir.name order by dir.id)
from directory_entry_dir as dir, directory as d
where dir.id =
ANY( 
	ARRAY(select array_agg(c)
	from (
	  select unnest(d.dir_entries)
	  from revision as r 
	  inner join snapshot_branch as sb on r.id = sb.target 
	  inner join directory as d on r.directory = d.id
	  where sb.object_id = 
			ANY(ARRAY(select sbs.branch_id from snapshot_branches as sbs where sbs.snapshot_id = 2))
			order by r.committer_date
	) as dt(c))
)
group by dir.id, dir.name, dir.target;

select dir.id, encode(dir.target, 'hex'), encode(dir.name, 'escape'), count(dir.name) over(partition by dir.name order by dir.id)
from directory_entry_dir as dir, directory as d
where dir.id =
ANY( 
	ARRAY(select encode(d.id, 'hex')
  from revision as r 
  inner join snapshot_branch as sb on r.id = sb.target 
  inner join directory as d on r.directory = d.id
  where sb.object_id = 
		ANY(ARRAY(select sbs.branch_id from snapshot_branches as sbs where sbs.snapshot_id = 2))
		order by r.committer_date)
)
group by dir.id, dir.name, dir.target;
		
select r.date, r.committer_date, encode(r.message,'escape'), r.author, r.committer, d.file_entries, sb.object_id 
		from revision as r 
		inner join snapshot_branch as sb on r.id = sb.target 
		inner join directory as d on r.directory = d.id
		where sb.object_id = 
		ANY(ARRAY(select sbs.branch_id from snapshot_branches as sbs where sbs.snapshot_id = ANY(ARRAY(select distinct sbs.snapshot_id ))))
		order by r.committer_date;
select encode(rh.id, 'hex'), encode(rh.parent_id, 'hex'), rh.parent_rank from revision_history as rh limit 10;
WITH RECURSIVE Children (id, parent_id, level)
AS
(
	SELECT rh1.id, rh1.parent_id, 0
	FROM revision_history rh1
	where rh1.parent_id IS NULL
	UNION ALL 
	SELECT c.parent_id, rh2.id, (c.level + 1)
	FROM revision_history as rh2, Children AS c
	WHERE rh2.parent_id = c.id
)
SELECT encode(c.parent_id, 'hex'), encode(c.id, 'hex'), c.level 
FROM Children AS c
order by c.level desc
limit 1000 ;

SELECT encode(rh_main.id, 'hex'), encode(rh_main.parent_id, 'hex'), rh_main.parent_rank
FROM revision_history rh_main
WHERE rh_main.id = ANY(ARRAY(
	SELECT rh.id
	FROM revision_history as rh
	EXCEPT
	SELECT r.id
	from revision_history as r
	where r.id = ANY( ARRAY(select r1.parent_id from revision_history r1)))
)
group by rh_main.id, rh_main.parent_id, rh_main.parent_rank;
select count(*) from revision_history;


SELECT encode(r.parent_id, 'hex'), encode(r.id, 'hex') from revision_history as r where encode(r.id, 'hex') = 'deeee44fed25307a4978012fd5d4ade8ab765c29';

SELECT encode(rh1.id, 'hex') as r1, encode(rh2.id, 'hex') as r2, encode(rh1.parent_id, 'hex') as parent, rh1.parent_rank, rh2.parent_rank
FROM revision_history AS rh1, revision_history AS rh2
WHERE rh1.parent_id = rh2.parent_id AND rh1.id <> rh2.id AND rh1.parent_rank <> rh2.parent_rank;

SELECT encode(r1.id, 'hex') as rh1, sbs.snapshot_id, encode(sb.name, 'escape'), sb.target_type, r1.date, r1.committer_date, encode(r1.message, 'escape'), r1.author, r1.committer, ov.origin
FROM revision AS r1, snapshot_branch AS sb, snapshot_branches AS sbs, origin_visit as ov
WHERE encode(r1.id, 'hex') = '2afc4b4e9d986adece0764df047bbf9525199010'
AND r1.id = sb.target AND sbs.branch_id = sb.object_id and sbs.snapshot_id = ov.snapshot_id;

SELECT *, encode(message, 'escape') from revision as r
where encode(r.id, 'hex') = '925b9ec0a554b2c85a46cecc7e1bba03d1407b1e';

SELECT encode(target, 'hex') from snapshot_branch sb WHERE encode(target, 'hex') = 'c3bec0dd68ed1d96c59eadeda2513ec3ee77dfdf';

SELECT encode(target, 'hex') from snapshot_branch sb LIMIT 10;

SELECT encode(r1.id, 'hex') as rh1, sbs.snapshot_id, encode(sb.name, 'escape'), sb.target_type, r1.date, r1.committer_date, encode(r1.message, 'escape'), r1.author, r1.committer
FROM revision AS r1, snapshot_branch AS sb, snapshot_branches AS sbs
WHERE encode(r1.id, 'hex') = '2afc4b4e9d986adece0764df047bbf9525199010'
AND r1.id = sb.target AND sbs.branch_id = sb.object_id;

SELECT encode(rh.id, 'hex'), encode(rh.parent_id, 'hex'), count(rh.id) over (partition by rh.parent_id)
FROM revision_history rh
WHERE rh.parent_id = ANY( 
	SELECT r1.parent_id from revision_history r1
	EXCEPT 
	select r.id from revision_history as r
)
GROUP BY rh.id, rh.parent_id;

SELECT encode(rh.id, 'hex'), encode(rh.parent_id, 'hex')
FROM revision_history rh WHERE encode(rh.id, 'hex') = '32eee6d6c16c2fd65459722edb90d1bf6c1debc6';



SELECT encode(p.id, 'hex'), encode(p.parent_id, 'hex'), p.level FROM cte_parent as p WHERE p.level > 0;

-- select branches from snapshot 2
select sb.object_id, encode(sb.name,'escape'), encode(sb.target, 'hex')
		from snapshot_branch as sb, snapshot_branches as sbs
		where sb.object_id = sbs.branch_id and sbs.snapshot_id = 2;
		
SELECT encode(r1.id, 'hex') as rh1, sbs.snapshot_id, encode(sb.name, 'escape'), sb.target_type, r1.date, r1.committer_date, encode(r1.message, 'escape'), r1.author, r1.committer, ov.origin
FROM revision AS r1, snapshot_branch AS sb, snapshot_branches AS sbs, origin_visit as ov
WHERE encode(r1.id, 'hex') = '56bae8cc54f7626819bca911d6603f7d224f10f4'
AND r1.id = sb.target AND sbs.branch_id = sb.object_id and sbs.snapshot_id = ov.snapshot_id;

select encode(dir.target, 'hex')
from directory_entry_dir as dir, directory as d
where dir.id =
ANY( 
	ARRAY(select array_agg(c)
	from (
	  select unnest(d.dir_entries)
	  from revision as r 
	  inner join snapshot_branch as sb on r.id = sb.target 
	  inner join directory as d on r.directory = d.id
	  where sb.object_id = 
			ANY(ARRAY(select sbs.branch_id from snapshot_branches as sbs where sbs.snapshot_id = 2))
			order by r.committer_date
	) as dt(c))
)
group by dir.target
LIMIT 10;

select encode(r_main.id, 'hex'), r_main.date, r_main.committer_date, encode(r_main.message,'escape'), r_main.author, r_main.committer, d_main.file_entries, d_main.dir_entries
		from revision as r_main, directory as d_main
		where r_main.directory = d_main.id and d_main.id = ANY(ARRAY(select dir.target
						from directory_entry_dir as dir, directory as d
						where dir.id =
						ANY( 
							ARRAY(select array_agg(c)
							from (
							  select unnest(d.dir_entries)
							  from revision as r 
							  inner join snapshot_branch as sb on r.id = sb.target 
							  inner join directory as d on r.directory = d.id
							  where sb.object_id = 
									ANY(ARRAY(select sbs.branch_id from snapshot_branches as sbs where sbs.snapshot_id = 2))
									order by r.committer_date
							) as dt(c))
						)
						group by dir.target));
select d.id from directory as d where d.id = ANY(ARRAY(select dir.target
						from directory_entry_dir as dir, directory as d
						where dir.id =
						ANY( 
							ARRAY(select array_agg(c)
							from (
							  select unnest(d.dir_entries)
							  from revision as r 
							  inner join snapshot_branch as sb on r.id = sb.target 
							  inner join directory as d on r.directory = d.id
							  where sb.object_id = 
									ANY(ARRAY(select sbs.branch_id from snapshot_branches as sbs where sbs.snapshot_id = 2))
									order by r.committer_date
							) as dt(c))
						)
						group by dir.target));
						
select count(*)
from revision as r;

select array_agg(encode(sb.target, 'hex'))
from snapshot_branches as sbs, snapshot_branch as sb
where sbs.snapshot_id = 2 and sbs.branch_id = sb.object_id;


select encode(dir.target, 'hex'), encode(dir.name, 'escape')
from directory_entry_dir as dir
where dir.id = any(ARRAY(
	select unnest(d.dir_entries)
	from revision as r, directory as d
	where r.directory = d.id and encode(r.id, 'hex') = 'b38ec6b92d67547f7590c72a38b577231bd4ecb8'
	order by r.committer_date
));

select encode(dir.target, 'hex'), encode(dir.name, 'escape')
from directory_entry_dir as dir
where dir.id = any(ARRAY(
	select unnest(d.dir_entries)
	from revision as r, directory as d
	where r.directory = d.id and encode(r.id, 'hex') = '9fbd21adbac36be869514e82e2e98505dc47219c'
	order by r.committer_date
));
select encode(d.id, 'hex'), encode(dir.name, 'escape'), unnest(d.dir_entries), d.file_entries, 0
	from directory_entry_dir as dir, directory as d
	where d.id = dir.target and dir.id = any(ARRAY(
		select unnest(d.dir_entries)
		from revision as r, directory as d
		where r.directory = d.id and encode(r.id, 'hex') = '9fbd21adbac36be869514e82e2e98505dc47219c'
		order by r.committer_date
	));
WITH RECURSIVE cte_dir(rev_id, date, committer_date, rev_type, message, author, committer, id, name, dir_entry, file_entries, level) AS(
	select r.id, r.date, r.committer_date, r.type, r.message, r.author, r.committer,dir.id, dir.name, t.i, d.file_entries, 0
	from directory_entry_dir as dir, directory as d, revision as r
	cross join lateral unnest(coalesce(nullif(d.dir_entries,'{}'),array[null::int])) as t(i)
	where d.id = dir.target and dir.id = any(ARRAY(
		select unnest(d.dir_entries)
		from directory as d
		where r.directory = d.id and r.id = any(array(select array_agg(sb.target)
																from snapshot_branches as sbs, snapshot_branch as sb
																where sbs.snapshot_id = 2 and sbs.branch_id = sb.object_id))
		order by r.committer_date
	))
	 
	UNION ALL
	
	select p.rev_id, p.date, p.committer_date, p.rev_type, p.message, p.author, p.committer, dir.id, dir.name, t.i, d.file_entries, (p.level + 1)
	from directory_entry_dir as dir, cte_dir as p, directory as d
	cross join lateral unnest(coalesce(nullif(d.dir_entries,'{}'),array[null::int])) as t(i)
	where d.id = dir.target and dir.id = p.dir_entry
)
select encode(c.rev_id, 'hex'), c.date, c.committer_date, c.rev_type, encode(c.message, 'escape'), c.author, c.committer, c.id, encode(c.name, 'escape'), c.dir_entry, c.file_entries, c.level
from cte_dir as c;
select dir.id, encode(dir.name, 'escape'), d.dir_entries, d.file_entries 
from directory_entry_dir as dir, directory as d 
where dir.id = 381 and dir.target = d.id;
ALTER USER sv WITH SUPERUSER;
copy (select distinct(max(sbs.snapshot_id) OVER (PARTITION BY ov.origin)) as snapshot_id, encode(sb.target, 'hex') as rev_id from snapshot_branches as sbs, snapshot as ss, origin_visit as ov, origin as o, snapshot_branch sb where sbs.snapshot_id = ss.object_id and ss.object_id = ov.snapshot_id and ov.origin = o.id and sbs.branch_id = sb.object_id group by sbs.snapshot_id, sb.target, sb.target_type, ov.origin) to '/home/sv/MSR2020/snap_branch.csv' delimiter ',' CSV HEADER;
select count(*) from snapshot_branches where snapshot_id = 56897980;

select distinct(max(sbs.snapshot_id) OVER (PARTITION BY ov.origin)) as snapshot_id, encode(sb.target, 'hex') as rev_id, encode(sb.name::bytea, 'escape')
from snapshot_branches as sbs, snapshot as ss, origin_visit as ov, origin as o, snapshot_branch sb 
where sbs.snapshot_id = ss.object_id and ss.object_id = ov.snapshot_id and ov.origin = o.id and sbs.branch_id = sb.object_id 
and o.type = 'git' and encode(sb.name::bytea, 'escape') ~* '.*master.*'
group by sbs.snapshot_id, sb.target, ov.origin, sbs.branch_id, sb.name;

select distinct(max(sbs.snapshot_id) OVER (PARTITION BY ov.origin)) as snapshot_id, count(sbs.branch_id) over (partition by sbs.snapshot_id) as cnt, o.type, encode(sb.name, 'escape')
		from snapshot_branches as sbs, snapshot as ss, origin_visit as ov, origin as o, snapshot_branch sb 
		where sbs.snapshot_id = ss.object_id and ss.object_id = ov.snapshot_id and ov.origin = o.id 
		and sbs.branch_id = sb.object_id and o.type != 'git'; 
-- 		group by sbs.snapshot_id, sb.target, sb.target_type, ov.origin, sbs.branch_id, o.ty;
select encode(sb.target, 'hex')
from snapshot_branches as sbs, snapshot_branch as sb
where sbs.snapshot_id = 324663 and encode(sb.name, 'escape') = 'refs/heads/master'
and sbs.branch_id = sb.object_id;

select distinct(max(sbs.snapshot_id) OVER (PARTITION BY ov.origin)) as snapshot_id, encode(sb.target, 'hex') as rev_id
from snapshot_branches as sbs, snapshot as ss, origin_visit as ov, origin as o, snapshot_branch as sb 
where encode(sb.name, 'escape') = 'refs/heads/master' and sbs.snapshot_id = ss.object_id and ss.object_id = ov.snapshot_id and ov.origin = o.id and sbs.branch_id = sb.object_id 
and o.type = 'git'
group by sbs.snapshot_id, sb.target, ov.origin, sbs.branch_id, sb.name;

copy (select sbs.snapshot_id, encode(sb.target, 'hex') from snapshot_branches as sbs, snapshot_branch as sb where sbs.branch_id = sb.object_id and (encode(sb.name::bytea, 'escape') ~ '.*HEAD.*' OR encode(sb.name::bytea, 'escape') ~* '.*master.*') and sbs.snapshot_id = ANY ( ARRAY (select distinct(max(sbs.snapshot_id) OVER (PARTITION BY ov.origin)) as snapshot_id from snapshot_branches as sbs, snapshot as ss, origin_visit as ov, origin as o, snapshot_branch sb where sbs.snapshot_id = ss.object_id and ss.object_id = ov.snapshot_id and ov.origin = o.id and sbs.branch_id = sb.object_id and o.type = 'git')) group by sbs.snapshot_id, sb.target) to '/tmp/snapshot_git.csv' delimiter ',' CSV HEADER;

select distinct(max(sbs.snapshot_id) OVER (PARTITION BY ov.origin)) as snapshot_id, encode(sb.target, 'hex')
		from snapshot_branches as sbs, snapshot as ss, origin_visit as ov, origin as o, snapshot_branch sb 
		where sbs.snapshot_id = ss.object_id and ss.object_id = ov.snapshot_id and ov.origin = o.id 
		and sbs.branch_id = sb.object_id;
CREATE TABLE snapshot_master AS 
select distinct(max(sbs.snapshot_id) OVER (PARTITION BY ov.origin)) as snapshot_id, encode(sb.target, 'hex') as branch_id
		from snapshot_branches as sbs, snapshot as ss, origin_visit as ov, origin as o, snapshot_branch sb 
		where sbs.snapshot_id = ss.object_id and ss.object_id = ov.snapshot_id and ov.origin = o.id 
		and sbs.branch_id = sb.object_id;
select s1.branch_id, s2.branch_id
from snapshot_master as s1 join snapshot_master as s2 on s1.snapshot_id != s2.snapshot_id;

CREATE INDEX snapshot_idx on snapshot_master(snapshot_id);

select count(*) from snapshot_master;
select count(*) from release;

select encode(re.id, 'hex')
from release as re
except
(select encode(re.id, 'hex')
from revision as r, snapshot_branch as sb, snapshot_branches as sbs, "snapshot" as s, origin_visit as ov, origin as o, "release" as re
where r.id = re.target and r.id = sb.target and sb.object_id = sbs.branch_id and sbs.snapshot_id  = s.object_id and s.object_id = ov.snapshot_id and ov.origin = o.id
group by o.id, o.type, o.url, re.date, re.name, re.id
order by o.id, re.date);

select  sbs.snapshot_id, sbs.branch_id
from snapshot_branches as sbs, snapshot_branch as sb, snapshot as s, origin_visit as ov, origin as o
where encode(sb.target, 'hex') = 'e3ac5640285460919158d79ef01cacd0ce1e5fc6' 
and sbs.branch_id = sb.object_id and s.object_id = ov.snapshot_id and ov.origin = o.id;

-- get branch master or HEAD of project type git
select sbs.snapshot_id, encode(sb.target, 'hex')
from snapshot_branches as sbs, snapshot_branch as sb
where sbs.branch_id = sb.object_id and (encode(sb.name::bytea, 'escape') ~ '.*HEAD.*' OR encode(sb.name::bytea, 'escape') ~* '.*master.*')
and sbs.snapshot_id = 
		ANY ( ARRAY (select distinct(max(sbs.snapshot_id) OVER (PARTITION BY ov.origin)) as snapshot_id
		from snapshot_branches as sbs, snapshot as ss, origin_visit as ov, origin as o, snapshot_branch sb 
		where sbs.snapshot_id = ss.object_id and ss.object_id = ov.snapshot_id and ov.origin = o.id 
					 and sbs.branch_id = sb.object_id and o.type = 'git'))
group by sbs.snapshot_id, sb.target;

-- get snapshot-branch of non-git projects
select distinct(max(sbs.snapshot_id) OVER (PARTITION BY ov.origin)) as snapshot_id, encode(sb.name, 'escape')
from snapshot_branches as sbs, snapshot as ss, origin_visit as ov, origin as o, snapshot_branch sb 
where sbs.snapshot_id = ss.object_id and ss.object_id = ov.snapshot_id and ov.origin = o.id
and sbs.branch_id = sb.object_id and o.type != 'git';

-- get revision-id and its parent id
WITH RECURSIVE cte_parent(id, parent_id, level) AS(
	SELECT rh_main.id, rh_main.parent_id, 0
	FROM revision_history rh_main
	WHERE rh_main.id = ANY(ARRAY(
		SELECT rh.id
		FROM revision_history as rh
		EXCEPT
		SELECT r.id
		from revision_history as r
		where r.id = ANY( ARRAY(select r1.parent_id from revision_history r1)))
	)
	 
	UNION ALL
	
	SELECT rh.id, rh.parent_id, (p.level + 1)
	FROM revision_history rh INNER JOIN cte_parent AS p ON p.id = rh.parent_id
)
SELECT encode(c.parent_id, 'hex'), encode(c.id, 'hex'), c.level
FROM cte_parent AS c;

WITH RECURSIVE Children (id, parent_id, level)
AS
(
	SELECT rh1.id, rh1.parent_id, 0
	FROM revision_history rh1
	where rh1.parent_id IS NULL
	UNION ALL 
	SELECT c.parent_id, rh2.id, (c.level + 1)
	FROM revision_history as rh2, Children AS c
	WHERE rh2.parent_id = c.id
)
SELECT encode(c.parent_id, 'hex') as parent_id, encode(c.id, 'hex') as id, c.level 
FROM Children AS c;

-- list all revisions
select encode(r.id, 'hex') as id, r.date as date, r.committer_date as committer_date, r.author, r.committer 
from revision as r
limit 10;

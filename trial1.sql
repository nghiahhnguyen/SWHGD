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
select distinct(max(sbs.snapshot_id) over (partition by ov.origin)) as snapshot_id
from snapshot_branches as sbs, snapshot as ss, origin_visit as ov, origin as o, snapshot_branch sb
where sbs.snapshot_id = ss.object_id and ss.object_id = ov.snapshot_id and ov.origin = o.id and sbs.branch_id = sb.object_id
group by sbs.snapshot_id, sb.target, sb.target_type, ov.origin
LIMIT 1;
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
select sbs.snapshot_id as snapshot_id, encode(sb.target, 'hex') as id
from snapshot_branches as sbs, snapshot as ss, origin as o, snapshot_branch as sb, latest_snapshot as ls 
where sbs.snapshot_id = ss.object_id and ls.origin = o.id and ls.snapshot_id = ss.object_id 
and sbs.branch_id = sb.object_id and o.type != 'git';

select sbs.snapshot_id as snapshot_id, encode(sb.target, 'hex') as id
from snapshot_branches as sbs, snapshot as ss, origin as o, snapshot_branch as sb, latest_snapshot as ls 
where sbs.snapshot_id = ss.object_id and ls.origin = o.id and ls.snapshot_id = ss.object_id 
and sbs.branch_id = sb.object_id and o.type = 'git'
and (encode(sb.name::bytea, 'escape') ~ '.*HEAD.*' OR encode(sb.name::bytea, 'escape') ~* '.*master.*');

COPY (select sbs.snapshot_id as snapshot_id, encode(sb.target, 'hex') as id from snapshot_branches as sbs, snapshot as ss, origin as o, snapshot_branch as sb, latest_snapshot as ls where sbs.snapshot_id is not null and sb.target is not null and sbs.snapshot_id = ss.object_id and ls.origin = o.id and ls.snapshot_id = ss.object_id and sbs.branch_id = sb.object_id and o.type != 'git' group by sbs.snapshot_id, sb.target) TO '/tmp/snapshot_branch_non_git.csv' delimiter ',' CSV HEADER;
COPY (select sbs.snapshot_id as snapshot_id, encode(sb.target, 'hex') as id from snapshot_branches as sbs, snapshot as ss, origin as o, snapshot_branch as sb, oldest_snapshot as os where sbs.snapshot_id = ss.object_id and os.origin = o.id and os.snapshot_id = ss.object_id and sbs.branch_id = sb.object_id and o.type != 'git' group by sbs.snapshot_id, sb.target) TO '/tmp/oldest_snapshot_branch_non_git.csv' delimiter ',' CSV HEADER;

COPY (select ls.origin as origin, sbs.snapshot_id as snapshot_id, encode(sb.target, 'hex') from snapshot_branches as sbs, snapshot as ss, origin as o, snapshot_branch as sb, latest_snapshot as ls  where ls.origin is not null and sbs.snapshot_id is not null and sb.target is not null and sbs.snapshot_id = ss.object_id and ls.origin = o.id and ls.snapshot_id = ss.object_id and sbs.branch_id = sb.object_id and o.type = 'git' and (encode(sb.name::bytea, 'escape') ~ '.*HEAD.*' OR encode(sb.name::bytea, 'escape') ~* '.*master.*') group by sbs.snapshot_id, sb.target, ls.origin) TO '/tmp/snapshot_branch_git.csv' delimiter ',' CSV HEADER;
COPY (select sbs.snapshot_id as snapshot_id, encode(sb.target, 'hex') as id from snapshot_branches as sbs, snapshot as ss, origin as o, snapshot_branch as sb, oldest_snapshot as os where sbs.snapshot_id = ss.object_id and os.origin = o.id and os.snapshot_id = ss.object_id and sbs.branch_id = sb.object_id and o.type = 'git' and (encode(sb.name::bytea, 'escape') ~ '.*HEAD.*' OR encode(sb.name::bytea, 'escape') ~* '.*master.*') group by sbs.snapshot_id, sb.target) TO '/tmp/oldest_snapshot_branch_git.csv' delimiter ',' CSV HEADER;
COPY (select os.origin as origin, sbs.snapshot_id as snapshot_id, encode(sb.target, 'hex') as id from snapshot_branches as sbs, snapshot as ss, origin as o, snapshot_branch as sb, oldest_snapshot as os where os.origin is not null and sbs.snapshot_id is not null and sb.target is not null and sbs.snapshot_id = ss.object_id and os.origin = o.id and os.snapshot_id = ss.object_id and sbs.branch_id = sb.object_id and o.type = 'git' and (encode(sb.name::bytea, 'escape') ~ '.*HEAD.*' OR encode(sb.name::bytea, 'escape') ~* '.*master.*') group by sbs.snapshot_id, sb.target, os.origin) TO '/tmp/oldest_snapshot_branch_git.csv' delimiter ',' CSV HEADER;

select sbs.snapshot_id as snapshot_id, encode(sb.target, 'hex')
from snapshot_branches as sbs, snapshot as ss, origin as o, snapshot_branch as sb, latest_snapshot as ls 
where sbs.snapshot_id is not null and sb.target is not null and sbs.snapshot_id = ss.object_id and ls.origin = o.id and ls.snapshot_id = ss.object_id and sbs.branch_id = sb.object_id and o.type = 'git' and (encode(sb.name::bytea, 'escape') ~ '.*HEAD.*' OR encode(sb.name::bytea, 'escape') ~* '.*master.*') 
group by sbs.snapshot_id, sb.target;
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

SELECT rh_main.id, rh_main.parent_id
FROM revision_history rh_main
WHERE rh_main.id = ANY(ARRAY(
	SELECT rh.id
	FROM revision_history as rh
	EXCEPT
	SELECT r.id
	from revision_history as r
	where r.id = ANY( ARRAY(select r1.parent_id from revision_history r1))))
limit 10;

SELECT rh.id
FROM revision_history as rh
EXCEPT
SELECT r.parent_id
from revision_history as r
LIMIT 1;

SELECT rh1.id, rh1.parent_id
FROM revision_history rh1
where rh1.parent_id IS NULL;

SELECT encode(id,'hex') as id, author, committer, committer_date, date 
FROM revision
LIMIT 1;

COPY (SELECT encode(id,'hex') as id, author, committer, committer_date, date FROM revision) TO PROGRAM 'gzip > /tmp/revision.csv.gz' delimiter ',' CSV HEADER;

COPY (SELECT encode(id,'hex') as id, encode(parent_id, 'hex') as parent_id FROM revision_history) TO PROGRAM 'gzip > /tmp/revision_history.csv.gz' delimiter ',' CSV HEADER;

SELECT encode(id,'hex')as id, encode(parent_id, 'hex') as parent_id FROM revision_history limit 1;

WITH latest_origin(origin_date, origin_id) AS (
	select distinct max(ov.date) over (partition by ov.origin), ov.origin
	from snapshot_branches as sbs, snapshot as ss, origin_visit as ov, origin as o, snapshot_branch sb
	where sbs.snapshot_id = ss.object_id and ss.object_id = ov.snapshot_id and ov.origin = o.id and sbs.branch_id = sb.object_id
)
SELECT ov.snapshot_id, o.type
FROM latest_origin as lo, origin_visit as ov, origin as o
WHERE lo.origin_id = ov.origin and ov.date = lo.origin_date and ov.origin = o.id and ov.snapshot_id is not NULL;
COPY (WITH latest_origin(origin_date, origin_id) AS (select distinct min(ov.date) over (partition by ov.origin), ov.origin from snapshot_branches as sbs, snapshot as ss, origin_visit as ov, origin as o, snapshot_branch sb where sbs.snapshot_id = ss.object_id and ss.object_id = ov.snapshot_id and ov.origin = o.id and sbs.branch_id = sb.object_id) SELECT distinct ov.snapshot_id FROM latest_origin as lo, origin_visit as ov, origin as o WHERE lo.origin_id = ov.origin and ov.date = lo.origin_date and ov.origin = o.id and ov.snapshot_id is not NULL) TO PROGRAM 'gzip > /tmp/origin_snapshot.csv.gz' delimiter ',' CSV HEADER;

CREATE TABLE latest_snapshot(
	origin BIGINT,
	date timestamp,
	snapshot_id BIGINT
);
CREATE TABLE url(
	url text
);
copy latest_snapshot(origin, date, snapshot_id) from '/tmp/origin.csv' delimiter ',' CSV HEADER;

CREATE TABLE oldest_snapshot(
	origin BIGINT,
	date timestamp,
	snapshot_id BIGINT
);
copy oldest_snapshot(origin, date, snapshot_id) from '/tmp/oldest_origin.csv' delimiter ',' CSV HEADER;
create index oldest_snapshot_idx ON oldest_snapshot(origin);
select id from latest_snapshot where id =16;
create index url_index on url(url);
copy url(url) from '/mnt/17volume/data/orign_urls.csv' delimiter ',';
ALTER TABLE latest_snapshot
ADD CONSTRAINT id_pk 
PRIMARY KEY (id); 
	
select id, count(*) as cnt from latest_snapshot group by id having count(*) > 1;
	
SELECT id FROM latest_snapshot limit 10;
COPY (SELECT id FROM latest_snapshot) TO PROGRAM 'gzip > /tmp/snapshot.csv.gz' delimiter ',' CSV HEADER;

CREATE INDEX snapshot_branch_target ON snapshot_branch(target);

CREATE INDEX release_target ON release(target);

CREATE INDEX snapshot_branches_snapshot_id ON snapshot_branches(snapshot_id);

SELECT encode(id, 'hex'), encode(target, 'hex') from release limit 1;

SELECT DISTINCT sbs.snapshot_id, re.date 
FROM snapshot_branch as sb, snapshot_branches as sbs, release as re
WHERE re.target = sb.target and sb.object_id = sbs.branch_id
order by sbs.snapshot_id, re.date;

COPY (SELECT DISTINCT sbs.snapshot_id, re.date FROM snapshot_branch as sb, snapshot_branches as sbs, release as re WHERE re.target = sb.target and sb.object_id = sbs.branch_id ORDER BY sbs.snapshot_id, re.date) to program 'gzip > /tmp/average_cycle_time_mini.csv.gz' delimiter ',' CSV HEADER;

COPY (SELECT DISTINCT encode(target, 'hex') as release_id, date as release_date from release) to program 'gzip > /tmp/release.csv.gz' delimiter ',' CSV HEADER;

SELECT DISTINCT encode(target, 'hex') as release_id, date as release_date 
from release
limit 1;

select encode(target, 'hex') as release_id, date as release_date 
from release
group by target, date
limit 2;
CREATE INDEX revision_history_id_idx ON revision_history(id);

select count(*) from revision_history;
COPY (SELECT encode(id,'hex') as id, encode(parent_id, 'hex') as parent_id FROM revision_history ORDER BY id, parent_id LIMIT 100000000 OFFSET 0) TO PROGRAM 'gzip > /tmp/revision_history_1.csv.gz' delimiter ',' CSV HEADER;
COPY (SELECT encode(id,'hex') as id, encode(parent_id, 'hex') as parent_id FROM revision_history ORDER BY id, parent_id LIMIT 100000000 OFFSET 100000000) TO PROGRAM 'gzip > /tmp/revision_history_2.csv.gz' delimiter ',' CSV HEADER;
COPY (SELECT encode(id,'hex') as id, encode(parent_id, 'hex') as parent_id FROM revision_history ORDER BY id, parent_id LIMIT 100000000 OFFSET 200000000) TO PROGRAM 'gzip > /tmp/revision_history_3.csv.gz' delimiter ',' CSV HEADER;
COPY (SELECT encode(id,'hex') as id, encode(parent_id, 'hex') as parent_id FROM revision_history ORDER BY id, parent_id LIMIT 100000000 OFFSET 300000000) TO PROGRAM 'gzip > /tmp/revision_history_4.csv.gz' delimiter ',' CSV HEADER;
COPY (SELECT encode(id,'hex') as id, encode(parent_id, 'hex') as parent_id FROM revision_history ORDER BY id, parent_id LIMIT 100000000 OFFSET 400000000) TO PROGRAM 'gzip > /tmp/revision_history_5.csv.gz' delimiter ',' CSV HEADER;
COPY (SELECT encode(id,'hex') as id, encode(parent_id, 'hex') as parent_id FROM revision_history ORDER BY id, parent_id LIMIT 100000000 OFFSET 500000000) TO PROGRAM 'gzip > /tmp/revision_history_6.csv.gz' delimiter ',' CSV HEADER;
COPY (SELECT encode(id,'hex') as id, encode(parent_id, 'hex') as parent_id FROM revision_history ORDER BY id, parent_id LIMIT 100000000 OFFSET 600000000) TO PROGRAM 'gzip > /tmp/revision_history_7.csv.gz' delimiter ',' CSV HEADER;
COPY (SELECT encode(id,'hex') as id, encode(parent_id, 'hex') as parent_id FROM revision_history ORDER BY id, parent_id LIMIT 100000000 OFFSET 700000000) TO PROGRAM 'gzip > /tmp/revision_history_8.csv.gz' delimiter ',' CSV HEADER;
COPY (SELECT encode(id,'hex') as id, encode(parent_id, 'hex') as parent_id FROM revision_history ORDER BY id, parent_id LIMIT 100000000 OFFSET 800000000) TO PROGRAM 'gzip > /tmp/revision_history_9.csv.gz' delimiter ',' CSV HEADER;
COPY (SELECT encode(id,'hex') as id, encode(parent_id, 'hex') as parent_id FROM revision_history ORDER BY id, parent_id LIMIT 100000000 OFFSET 900000000) TO PROGRAM 'gzip > /tmp/revision_history_10.csv.gz' delimiter ',' CSV HEADER;
COPY (SELECT encode(id,'hex') as id, encode(parent_id, 'hex') as parent_id FROM revision_history ORDER BY id, parent_id LIMIT 100000000 OFFSET 1000000000) TO PROGRAM 'gzip > /tmp/revision_history_11.csv.gz' delimiter ',' CSV HEADER;
COPY (SELECT encode(id,'hex') as id, encode(parent_id, 'hex') as parent_id FROM revision_history ORDER BY id, parent_id LIMIT 100000000 OFFSET 1100000000) TO PROGRAM 'gzip > /tmp/revision_history_12.csv.gz' delimiter ',' CSV HEADER;
COPY (SELECT encode(id,'hex') as id, encode(parent_id, 'hex') as parent_id FROM revision_history ORDER BY id, parent_id LIMIT 100000000 OFFSET 1200000000) TO PROGRAM 'gzip > /tmp/revision_history_13.csv.gz' delimiter ',' CSV HEADER;
COPY (SELECT encode(id,'hex') as id, encode(parent_id, 'hex') as parent_id FROM revision_history ORDER BY id, parent_id LIMIT 2422013 OFFSET 1300000000) TO PROGRAM 'gzip > /tmp/revision_history_14.csv.gz' delimiter ',' CSV HEADER;
WITH latest_origin(origin_date, origin_id) AS (
        select distinct max(ov.date) over (partition by ov.origin)
        from origin_visit as ov)
SELECT ov.snapshot_id, o.type
FROM latest_origin as lo, origin_visit as ov, origin as o
WHERE lo.origin_id = ov.origin and ov.date = lo.origin_date and ov.origin = o.id and ov.snapshot_id is not NULL;
select max(ov.date) over (partition by ov.origin)
from origin_visit as ov;
select count(*) from origin;
COPY (select max(ov.date) over (partition by ov.origin) as date from origin_visit as ov) to 'origin.csv' delimiter ',' csv header; 
COPY (select min(ov.date) over (partition by ov.origin) as date from origin_visit as ov) to '/tmp/origin.csv' delimiter ',' csv header; 

select count(*) 
from origin
where id = 
(select o.id
from origin as o

select ov.origin
from origin_visit as ov);

COPY (select o.id from origin as o except select ov.origin from origin_visit as ov) to '/tmp/check.csv' delimiter ',' csv header;

COPY (select t.origin as origin, t.date as date, t.snapshot_id as snapshot_id from origin_visit as t inner join ( select origin, min(date) as MinDate from origin_visit group by origin) tm on t.origin = tm.origin and t.date = tm.MinDate) to '/tmp/oldest_origin.csv' delimiter ',' csv header;

(encode(sb.name::bytea, 'escape') ~ '.*HEAD.*' OR encode(sb.name::bytea, 'escape') ~* '.*master.*'
SELECT distinct sf.snapshot_id, sf.fork_id
FROM snapshot_fork as sf, origin_visit as ov1, origin_visit as ov2
WHERE sf.snapshot_id = ov1.snapshot_id and sf.fork_id = ov2.snapshot_id and ov1.origin != ov2.origin;
 
SELECT 10000, rv1.committer_date, rv1.author, 29333801, rv2.committer_date, rv2.author
FROM snapshot_branches as sbs1 join snapshot_branch as sb1 on sbs1.branch_id = sb1.object_id join revision as rv1 on sb1.target = rv1.id,
snapshot_branches as sbs2 join snapshot_branch as sb2 on sbs2.snapshot_id = sb2.object_id join revision as rv2 on sb2.target = rv2.id
WHERE 10000 = sbs1.snapshot_id and 29333801 = sbs2.snapshot_id 
 and (encode(sb1.name::bytea, 'escape') ~ '.*HEAD.*' OR encode(sb1.name::bytea, 'escape') ~* '.*master.*') 
 and (encode(sb2.name::bytea, 'escape') ~ '.*HEAD.*' OR encode(sb2.name::bytea, 'escape') ~* '.*master.*');
 
SELECT distinct 29333801 as snapshot_id, sbs.branch_id, r.author, r.committer_date
FROM snapshot_branches as sbs join snapshot_branch as sb on sbs.branch_id = sb.object_id join revision as r on sb.target = r.id
WHERE 10000 = sbs.snapshot_id 
 and (encode(sb.name::bytea, 'escape') ~ '.*HEAD.*' OR encode(sb.name::bytea, 'escape') ~* '.*master.*');
 
CREATE TABLE snapshot_fork (
	 snapshot_id BIGINT,
	 revision_id sha1_git,
	 fork_id BIGINT
);
copy snapshot_fork(snapshot_id, revision_id, fork_id) from 'snapshot_revision_fork.csv' delimiter ',' CSV HEADER;

CREATE TABLE selected_snapshot (
	snapshot_id BIGINT,
	revision_id text,
	date BIGINT
);

CREATE INDEX revision_id_selected_idx on selected_snapshot(revision_id);
CREATE INDEX origin_id_idx on origin_snapshot(origin_id);
copy selected_snapshot(snapshot_id, revision_id, date) from '/mnt/17volume/data/selected_snapshot.csv' delimiter ',';
copy (SELECT distinct sf.snapshot_id as snapshot_id, sf.revision_id as revision_id, sf.fork_id as fork_id FROM snapshot_fork as sf, origin_visit as ov1, origin_visit as ov2 WHERE sf.snapshot_id = ov1.snapshot_id and sf.fork_id = ov2.snapshot_id and ov1.origin != ov2.origin) to '/tmp/snapshot_revision_fork.csv' delimiter ',' csv header; 
COPY (SELECT encode(id,'hex') as id, encode(parent_id, 'hex') as parent_id, parent_rank as rank FROM revision_history) TO '/tmp/revision_history.csv' delimiter ',' CSV HEADER;

CREATE TABLE snapshot_revision (
	snapshot_id BIGINT,
	revision_id text
);
copy snapshot_revision(snapshot_id, revision_id) from program 'zcat snapshot_revision_git.csv.gz' (format csv);
select o.url from origin_visit as ov join origin as o on ov.origin = o.id where ov.snapshot_id = 28830218;
 
select count(*) from latest_snapshot where DATE_PART('year', now()) - DATE_PART('year', date) = 1;
copy(select ls.snapshot_id as snapshot_id from latest_snapshot as ls where DATE_PART('year', now()) - DATE_PART('year', date) = 1) to '/tmp/latest_snapshot.csv' delimiter ',' CSV HEADER;
copy(select ls.snapshot_id as snapshot_id from latest_snapshot as ls where (DATE_PART('month', '2018-12-31'::date) - DATE_PART('month', ls.date)) <= 2 except select snapshot_id from oldest_snapshot) to '/tmp/latest_snapshot.csv' delimiter ',' CSV HEADER;
  select date_part('month', now()) - date_part('month', '2018-12-01') as num;
select now();
select * from origin where id = 5;
select * from origin where url = 'https://github.com/tensorflow/tensorflow'; 
select * from latest_snapshot where origin = 25254725;
select * from oldest_snapshot where snapshot_id = 59590050; 
select max(date) from latest_snapshot;
 
CREATE TABLE community_fork(
	fork_id  BIGINT PRIMARY KEY
);
copy community_fork(fork_id) from '/mnt/17volume/data/community_fork.csv' delimiter ',';
CREATE TABLE origin_snapshot(
	origin_id BIGINT,
	date date,
	snapshot_id BIGINT
);
\copy origin_snapshot(origin_id, date, snapshot_id) from 'origin.csv' delimiter ','  CSV HEADER;
select o.url
from origin as o join origin_visit as ov on o.id = ov.origin
where ov.snapshot_id = ANY(select fork_id from community_fork);
CREATE INDEX snapshot_id_selected_idx on selected_snapshot(snapshot_id);
copy (select ov.snapshot_id, o.url from origin as o join origin_visit as ov on o.id = ov.origin where ov.snapshot_id = ANY(select fork_id from community_fork)) to '/tmp/community_forks.csv' delimiter ',';
create index snapshot_id_origin_snapshot_idx on origin_snapshot(snapshot_id);
select * from origin limit 1;
select distinct o.id, o.url, r.author, r.date, encode(r.directory,'hex') as directory, encode(r.message,'text') as message
from origin as o inner join origin_snapshot as os on o.id = os.origin_id
 inner join selected_snapshot as ss on os.snapshot_id = ss.snapshot_id
 inner join revision as r on ss.revision_id = r.id
where r.date >= '2018-01-01'::date
order by o.id
limit 1;
copy (select distinct o.id, o.url, r.author, r.date, ss.revision_id, encode(r.directory, 'hex') as directory_id from origin as o inner join origin_snapshot as os on o.id = os.origin_id inner join selected_snapshot as ss on os.snapshot_id = ss.snapshot_id inner join revision as r on ss.revision_id = encode(r.id, 'hex') where r.date >= '2018-01-01'::date order by o.id) to '/tmp/origin_release_data.csv' delimiter ',';
explain select distinct o.id, o.url, r.author, r.date, encode(r.directory,'hex') as directory, encode(r.message,'text') as message
from origin as o inner join origin_snapshot as os on o.id = os.origin_id
 inner join selected_snapshot as ss on os.snapshot_id = ss.snapshot_id
where r.date >= '2018-01-01'::date and ss.revision_id = ANY(select ss.revision_id intersect select id from revision)
order by o.id
limit 1;
select revision_id 
from selected_snapshot
 intersect
 select id
 from revision
 limit 1;
 select * from revision where date >= '2018-01-01'::date limit 1;
 
 select encode(ss.revision_id,'hex') from selected_snapshot as ss limit 1;
 
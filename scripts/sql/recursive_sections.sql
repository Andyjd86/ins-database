DROP TABLE client.testrecursive;

-- Create am empty table with auto_ID ready for the data

CREATE TABLE client.testrecursive2 (
	section_label character varying(30),
	dist_from_last double precision,
-- ADD FID to hapms_master before running this to reduce the ID size to integer
	id bigint[]
);

-- Starting with a particular section the recursive query looks at the endpoint of that segment and the nearest start point of the next segment
-- within 1.00m. it continues in this fashion until it reaches the end of the table or a gap bigger than 1.00m
-- filters can include direction, carriageway type and even an end segment if you want to stop it part way through the table.

WITH RECURSIVE walk_network(geom, section_label, dist_from_last) AS (
	SELECT geom, section_label, 0.00::float AS dist_from_last, ARRAY[section] AS breadcrumb
    	FROM client.hapms_master
    	WHERE section_label = '0900M6/624'
  	UNION ALL
  	SELECT n.geom, n.section_label, ST_Distance(ST_EndPoint(w.geom), ST_StartPoint(n.geom)) AS dist_from_last, breadcrumb || section
    	FROM client.hapms_master n, walk_network w
    	WHERE ST_DWithin(ST_EndPoint(w.geom), ST_StartPoint(n.geom), 6.50)
    	--AND w.section_label != '3400M6/432, 4600M6/803'
    	AND direction_key = 2
    	AND section_function = 1
		AND operational_area_code != 'M6TOLL'
)
INSERT INTO client.testrecursive2 (section_label, dist_from_last, id)
	SELECT section_label, dist_from_last, breadcrumb::bigint[] AS id
    	FROM walk_network
        ORDER BY breadcrumb;

SELECT *
	FROM client.testrecursive2
    ORDER BY id asc;
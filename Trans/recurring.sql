-- Create am empty table with auto_ID ready for the data

CREATE TABLE hapms.testrecursive4 (
	sect_label character varying(30),
	dist_from_last double precision,
	id varchar
);

-- Starting with a particular section the recursive query looks at the endpoint of that segment and the nearest start point of the next segment
-- within 1.00m. it continues in this fashion until it reaches the end of the table or a gap bigger than 1.00m
-- filters can include direction, carriageway type and even an end segment if you want to stop it part way through the table.

WITH RECURSIVE walk_network(geom, sect_label, dist_from_last) AS (
	SELECT geom, sect_label, 0.00::float AS dist_from_last, ARRAY[id] AS breadcrumb
    	FROM hapms.m6_hapms_sections
    	WHERE sect_label = '3400M6/602'
  	UNION ALL
  	SELECT n.geom, n.sect_label, ST_Distance(ST_EndPoint(w.geom), ST_StartPoint(n.geom)) AS dist_from_last, breadcrumb || id
    	FROM hapms.m6_hapms_sections n, walk_network w
    	WHERE ST_DWithin(ST_EndPoint(w.geom), ST_StartPoint(n.geom),5.00)
    	AND w.sect_label != '3400M6/432'
    	AND direc_code = 'SB'
    	AND funct_name = 'Main Carriageway'
)
INSERT INTO hapms.testrecursive4 (sect_label, dist_from_last, id) 
	SELECT sect_label, dist_from_last, breadcrumb::VARCHAR AS id 
    	FROM walk_network 
        ORDER BY breadcrumb;

SELECT *
	FROM hapms.testrecursive4
    ORDER BY breadcrumb
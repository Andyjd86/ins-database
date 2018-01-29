SELECT section_label, section
FROM (
	SELECT section_label, section
	FROM client.hapms_master
	WHERE section
	IN (
		SELECT unnest(id)
		FROM client.testrecursive2
		WHERE section_label = '0900M6/439'
	)
) as t1
WHERE NOT EXISTS (
SELECT section_label
FROM (
	SELECT section_label
	FROM client.hapms_master
	WHERE section
	IN (
		SELECT unnest(id)
		FROM client.testrecursive2
		WHERE section_label = '0900M6/610'
	)
) as t2
WHERE t1.section_label = t2.section_label
);
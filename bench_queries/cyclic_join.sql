SELECT a.id AS a_id, b.id AS b_id, c.id AS c_id 
FROM a
JOIN b ON a.b_id = b.id
JOIN c ON b.c_id = c.id
JOIN a ON a.id = c.a_id

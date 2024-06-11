

SELECT *
FROM a
JOIN b ON a.z = b.z
JOIN (
    SELECT *
    FROM c as c2 JOIN a as a2 ON c2.x = a2.x
) as sub ON b.w = sub.w

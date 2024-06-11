SELECT y
FROM a
JOIN b ON a.z = b.z
JOIN c ON c.x = a.x
JOIN c ON c.w = b.w


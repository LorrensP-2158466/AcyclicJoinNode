SELECT *
FROM stem
JOIN petal as p1 ON stem.flower = p1.flower
JOIN petal as p2 ON stem.flower = p2.flower

CREATE TABLE users(
    uid INT,
    name VARCHAR(32)
);

CREATE TABLE products(
    pid INT,
    name VARCHAR(32)
);

CREATE TABLE orders(
    oid INT,
    pid INT,
    uid INT
);

CREATE TABLE items
(
    id          VARCHAR(50) PRIMARY KEY,
    name        VARCHAR(100),
    description TEXT
);

CREATE TABLE parts
(
    id VARCHAR(50) PRIMARY KEY,
    item_id VARCHAR(50) REFERENCES items (id),
    name    VARCHAR(100)
);
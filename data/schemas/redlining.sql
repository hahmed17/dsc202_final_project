CREATE TABLE IF NOT EXISTS redlinescores (
    holc_id VARCHAR(5),
    holc_grade text,
    community TEXT,
    NID INT,
    POP2010 INT,
    POP2000 INT,
    POPCH INT,
    POPPERCH NUMERIC(10, 6),
    popplus INT,
    popneg INT,
    neighborhood_area NUMERIC,
    region_area NUMERIC
);
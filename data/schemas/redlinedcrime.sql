CREATE TABLE RedlinedCrimeData (
    ID INT PRIMARY KEY,
    holc_id VARCHAR(5),
    holc_grade VARCHAR(1),
    Date TIMESTAMP,
    PrimaryType TEXT,
    Description TEXT,
    LocationDescription TEXT,
    CommunityArea INT
);

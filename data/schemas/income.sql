CREATE TABLE IF NOT EXISTS communitystats(
    CommunityAreaNumber INT PRIMARY KEY,
    CommunityAreaName VARCHAR(255),
    PercentHousingCrowded DECIMAL(4, 1),
    PercentHouseholdsBelowPoverty DECIMAL(4, 1),
    PercentAged16PlusUnemployed DECIMAL(4, 1),
    PercentAged25PlusNoHighSchoolDiploma DECIMAL(4, 1),
    PercentAgedUnder18OrOver64 DECIMAL(4, 1),
    PerCapitaIncome NUMERIC,
    HardshipIndex INT
    );
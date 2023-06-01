CREATE VIEW trades2017clean AS
SELECT *
FROM TRADESDB.trades2017view
WHERE (
    (toHour(Time) = 9 AND toMinute(Time) >= 45) 
    OR 
    (toHour(Time) BETWEEN 10 AND 15) 
    OR 
    (toHour(Time) = 16 AND toMinute(Time) < 45)
)
AND Trade_Volume > 0
AND Trade_Price > 0;

CREATE VIEW trades2018clean AS
SELECT *
FROM TRADESDB.trades2018view
WHERE (
    (toHour(Time) = 9 AND toMinute(Time) >= 45) 
    OR 
    (toHour(Time) BETWEEN 10 AND 15) 
    OR 
    (toHour(Time) = 16 AND toMinute(Time) < 45)
)
AND Trade_Volume > 0
AND Trade_Price > 0;

CREATE VIEW trades2019clean AS
SELECT *
FROM TRADESDB.trades2019view
WHERE (
    (toHour(Time) = 9 AND toMinute(Time) >= 45) 
    OR 
    (toHour(Time) BETWEEN 10 AND 15) 
    OR 
    (toHour(Time) = 16 AND toMinute(Time) < 45)
)
AND Trade_Volume > 0
AND Trade_Price > 0;

CREATE VIEW trades2020clean AS
SELECT *
FROM TRADESDB.trades2020view
WHERE (
    (toHour(Time) = 9 AND toMinute(Time) >= 45) 
    OR 
    (toHour(Time) BETWEEN 10 AND 15) 
    OR 
    (toHour(Time) = 16 AND toMinute(Time) < 45)
)
AND Trade_Volume > 0
AND Trade_Price > 0;

CREATE VIEW trades2021clean AS
SELECT *
FROM TRADESDB.trades2021view
WHERE (
    (toHour(Time) = 9 AND toMinute(Time) >= 45) 
    OR 
    (toHour(Time) BETWEEN 10 AND 15) 
    OR 
    (toHour(Time) = 16 AND toMinute(Time) < 45)
)
AND Trade_Volume > 0
AND Trade_Price > 0;


CREATE VIEW trades2022clean AS
SELECT *
FROM TRADESDB.trades2022view
WHERE (
    (toHour(Time) = 9 AND toMinute(Time) >= 45) 
    OR 
    (toHour(Time) BETWEEN 10 AND 15) 
    OR 
    (toHour(Time) = 16 AND toMinute(Time) < 45)
)
AND Trade_Volume > 0
AND Trade_Price > 0;
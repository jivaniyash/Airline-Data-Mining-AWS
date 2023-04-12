CREATE DATABASE yash_1999;
USE yash_1999;

CREATE TABLE airline_1999 (
    Year INT,
    Month INT,
    DayofMonth INT,
    DayOfWeek INT ,
    DepTime INT,
    CRSDepTime INT,
    ArrTime INT,
    CRSArrTime INT,
    UniqueCarrier STRING,
    FlightNum STRING,
    TailNum STRING,
    ActualElapsedTime INT,
    CRSElapsedTime INT,
    AirTime INT,
    ArrDelay INT,
    DepDelay INT,
    Origin STRING,
    Dest STRING,
    Distance INT,
    TaxiIn INT,
    TaxiOut INT,
    Cancelled INT,
    CancellationCode STRING,
    Diverted INT,
    CarrierDelay INT,
    WeatherDelay INT,
    NASDelay INT,
    SecurityDelay INT,
    LateAircraftDelay INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

CREATE TABLE airport(
    iata STRING,
    airport STRING,	
    city STRING, 
    state STRING, 
    country STRING, 
    lat STRING,	
    long STRING) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar"     = "\"");

"


CREATE TABLE carrier (
    Code STRING, 
    Description STRING) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar"     = "\""
);

"

SELECT
    '1999' AS Year,
    left.Arrival_Airport,
    right.airport AS Airport_Name,
    ROUND((left.Arrival_Delay/60),2),
    ROUND((left.Departure_Delay/60),2),
    IF(
        left.ArrDelay > left.Departure_Delay,
        ROUND((left.Arrival_Delay/60),2),
        ROUND((left.Departure_Delay/60),2)
    ) AS highest_delay,
    ROUND((left.Total_Delay/60),2)    
FROM
    (SELECT
        DESTINATION.Arrival_Airport,
        DESTINATION.Arrival_Delay,
        DEPARTURE.Departure_Delay, 
        (DESTINATION.Arrival_Delay + DEPARTURE.Departure_Delay) AS Total_Delay
    FROM 
        (SELECT
            a.Dest AS Arrival_Airport,
            SUM(
                IF
                (a.ArrDelay < 0,
                    0,
                    a.ArrDelay)) AS Arrival_Delay
        FROM airline_1999 AS a
        GROUP BY a.Dest) AS DESTINATION
    FULL OUTER JOIN 
        (SELECT
            a.Origin AS Departure_Airport,
            SUM(
                IF
                (a.DepDelay < 0,
                    0,
                    a.DepDelay)) AS Departure_Delay
        FROM airline_1999 AS a
        GROUP BY a.Origin) AS DEPARTURE
    ON DESTINATION.Arrival_Airport = DEPARTURE.Departure_Airport
    GROUP BY DESTINATION.Arrival_Airport
    ORDER BY Total_Delay desc
    LIMIT 3) AS left
LEFT OUTER JOIN airport AS right
ON left.Dest = right.iata;




SELECT
    '1999' AS Year,
    left.Carrier_Code,
    right.Description,
    ROUND((left.Arrival_Delay/60),2),
    ROUND((left.Departure_Delay/60),2),
    ROUND((left.Arrival_Delay+left.Departure_Delay)/60,2) AS Total_Delay
FROM
    (SELECT
        a.UniqueCarrier AS Carrier_Code, 
        SUM(
            IF
            (a.ArrDelay < 0,
                0,
                a.ArrDelay)) AS Arrival_Delay,
        SUM(
            IF
            (a.DepDelay < 0,
                0,
                a.DepDelay)) AS Departure_Delay
    FROM
        airline_1999 AS a      
    GROUP BY a.UniqueCarrier) AS left
FULL INNER JOIN carrier AS right
ON left.Carrier_Code = right.Code
ORDER BY Total_Delay desc 
LIMIT 3




SELECT
    '1999' AS Year,
    left.Carrier_Code,
    right.Description,
    ROUND((left.Arrival_Delay/60),2),
    ROUND((left.Departure_Delay/60),2),
    IF(
        MAX(left.Arrival_Delay,left.Departure_Delay) = left.Arrival_Delay,
            'Arrival_Delay',
            'Departure_Delay') AS Delay_Type,
    ROUND(MAX(left.Arrival_Delay,left.Departure_Delay)/60,2) AS Largest_Delay,
    ROUND((left.Arrival_Delay+left.Departure_Delay)/60,2) AS Total_Delay
FROM
    (SELECT
        a.UniqueCarrier AS Carrier_Code, 
        SUM(
            IIF
            (a.ArrDelay < 0,
                0,
                a.ArrDelay)) AS Arrival_Delay,
        SUM(
            IIF
            (a.DepDelay < 0,
                0,
                a.DepDelay)) AS Departure_Delay
    FROM
        airline_1999 AS a      
    GROUP BY a.UniqueCarrier) AS left
INNER JOIN carrier AS right
ON left.Carrier_Code = right.Code
ORDER BY Total_Delay desc 
LIMIT 3
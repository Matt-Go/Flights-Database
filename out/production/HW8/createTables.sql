-- add all your SQL setup statements here. 

-- You can assume that the following base table has been created with data loaded for you when we test your submission 
-- (you still need to create and populate it in your instance however),
-- although you are free to insert extra ALTER COLUMN ... statements to change the column 
-- names / types if you like.

CREATE TABLE FLIGHTS (fid int,
                      month_id int,        -- 1-12
                      day_of_month int,    -- 1-31
                      day_of_week_id int,  -- 1-7, 1 = Monday, 2 = Tuesday, etc
                      carrier_id varchar(7),
                      flight_num int,
                      origin_city varchar(34),
                      origin_state varchar(47),
                      dest_city varchar(34),
                      dest_state varchar(46),
                      departure_delay int, -- in mins
                      taxi_out int,        -- in mins
                      arrival_delay int,   -- in mins
                      canceled int,        -- 1 means canceled
                      actual_time int,     -- in mins
                      distance int,        -- in miles
                      capacity int,
                      price int            -- in $
                    );

CREATE TABLE USERS (
    username varchar(20) PRIMARY KEY,
    password varchar(20),
    balance int);

CREATE TABLE RESERVATIONS (
    reservation_id int PRIMARY KEY,
    paid int,
    fid1 int,
    fid2 int,
    day int,
    total_price int,
    capacity1 int,
    capacity2 int,
    carrier1 varchar(30),
    carrier2 varchar(30),
    flight_num1 varchar(30),
    flight_num2 varchar(30),
    origin_city1 varchar(30),
    origin_city2 varchar(30),
    dest_city1 varchar(30),
    dest_city2 varchar(30),
    duration1 int,
    duration2 int,
    price2 int,
    direct int);

CREATE TABLE ITINERARIES (
    itinerary_id int,
    fid1 int,
    fid2 int,
    day int,
    capacity1 int,
    capacity2 int,
    total_price int,
    carrier1 varchar(30),
    carrier2 varchar(30),
    flight_num1 varchar(30),
    flight_num2 varchar(30),
    origin_city1 varchar(30),
    origin_city2 varchar(30),
    dest_city1 varchar(30),
    dest_city2 varchar(30),
    duration1 int,
    duration2 int,
    price2 int,
    direct int);



CREATE DATABASE cars_price;

USE cars_price;

DROP TABLE IF EXISTS car_raw;

CREATE TABLE car_raw (
    id INT AUTO_INCREMENT PRIMARY KEY,
    `year` INT,
    make VARCHAR(100),
    model VARCHAR(150),
    trim VARCHAR(150),
    body VARCHAR(100),
    transmission VARCHAR(50),
    vin VARCHAR(50),
    state VARCHAR(100),
    `condition` FLOAT,
    odometer FLOAT,
    color VARCHAR(50),
    interior VARCHAR(50),
    seller VARCHAR(255),
    mmr FLOAT,
    sellingprice FLOAT,
    saledate VARCHAR(255)
);

SELECT * FROM car_raw;

# Kreiranje tablice i import podataka

DROP TABLE IF EXISTS make;

CREATE TABLE make (
    id INT AUTO_INCREMENT PRIMARY KEY,
    make VARCHAR(100),
    UNIQUE(make)
);

INSERT INTO make (make)
SELECT DISTINCT make
FROM car_raw;


DROP TABLE IF EXISTS model;

CREATE TABLE model (
    id INT AUTO_INCREMENT PRIMARY KEY,
    model VARCHAR(150),
    make_id INT,
    UNIQUE(model, make_id),
    FOREIGN KEY (make_id) REFERENCES make(id)
);

INSERT INTO model (model, make_id)
SELECT DISTINCT r.model, m.id
FROM car_raw r
JOIN make m 
    ON r.make = m.make;


DROP TABLE IF EXISTS seller;

CREATE TABLE seller (
    id INT AUTO_INCREMENT PRIMARY KEY,
    seller VARCHAR(255),
    UNIQUE(seller)
);

INSERT INTO seller (seller)
SELECT DISTINCT seller
FROM car_raw;


DROP TABLE IF EXISTS car;

CREATE TABLE car (
    id INT AUTO_INCREMENT PRIMARY KEY,
    vin VARCHAR(50),
    `year` INT,
    trim VARCHAR(150),
    odometer FLOAT,
    mmr FLOAT,
    body VARCHAR(100),
    transmission VARCHAR(50),
    `condition` FLOAT,
    color VARCHAR(50),
    interior VARCHAR(50),
    model_id INT,
    UNIQUE(vin),
    FOREIGN KEY (model_id) REFERENCES model(id)
);

INSERT INTO car (
    vin, `year`, trim, odometer, mmr,
    body, transmission, `condition`,
    color, interior, model_id
)
SELECT
    r.vin,
    MAX(r.year),
    MAX(r.trim),
    MAX(r.odometer),
    MAX(r.mmr),
    MAX(r.body),
    MAX(r.transmission),
    MAX(r.`condition`),
    MAX(r.color),
    MAX(r.interior),
    MAX(m.id)
FROM car_raw r
JOIN make mk ON r.make = mk.make
JOIN model m 
    ON r.model = m.model 
    AND m.make_id = mk.id
GROUP BY r.vin;



DROP TABLE IF EXISTS selling;

CREATE TABLE selling (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sellingprice FLOAT,
    saledate VARCHAR(255),
    state VARCHAR(100),
    car_id INT,
    seller_id INT,
    FOREIGN KEY (car_id) REFERENCES car(id),
    FOREIGN KEY (seller_id) REFERENCES seller(id)
);

INSERT INTO selling (
    sellingprice,
    saledate,
    state,
    car_id,
    seller_id
)
SELECT
    r.sellingprice,
    r.saledate,
    r.state,
    c.id,
    s.id
FROM car_raw r
JOIN car c ON r.vin = c.vin
JOIN seller s ON r.seller = s.seller;

# Testiranje točnosti importa

SELECT COUNT(*) FROM car_raw;
SELECT COUNT(*) FROM make;
SELECT COUNT(*) FROM model;
SELECT COUNT(*) FROM car;
SELECT COUNT(*) FROM seller;
SELECT COUNT(*) FROM selling;

SELECT 
SUM(year IS NULL) year_null,
SUM(make IS NULL) make_null,
SUM(model IS NULL) model_null,
SUM(trim IS NULL) trim_null,
SUM(body IS NULL) body_null,
SUM(color IS NULL) color_null,
SUM(interior IS NULL) interior_null
FROM car_raw;

SELECT COUNT(DISTINCT make) FROM car_raw;
SELECT COUNT(*) FROM make;

SELECT COUNT(*) 
FROM (
    SELECT DISTINCT model, make
    FROM car_raw
) t;
SELECT COUNT(*) FROM model;

SELECT COUNT(DISTINCT seller) FROM car_raw;
SELECT COUNT(*) FROM seller;

SELECT *
FROM model m
LEFT JOIN make mk ON m.make_id = mk.id
WHERE mk.id IS NULL;

SELECT *
FROM car c
LEFT JOIN model m ON c.model_id = m.id
WHERE m.id IS NULL;

SELECT *
FROM selling s
LEFT JOIN car c ON s.car_id = c.id
WHERE c.id IS NULL;

SELECT *
FROM selling s
LEFT JOIN seller se ON s.seller_id = se.id
WHERE se.id IS NULL;

SELECT car_id, COUNT(*)
FROM selling
GROUP BY car_id
HAVING COUNT(*) > 1;

SELECT *
FROM selling
WHERE sellingprice <= 0;

SELECT COUNT(*) FROM selling;



# Zadnje testiranje vracanja u csv

SELECT
mk.make,
m.model,
c.year,
c.trim,
c.body,
c.transmission,
c.vin,
c.condition,
c.odometer,
c.color,
c.interior,
s.sellingprice,
s.saledate,
s.state,
se.seller,
c.mmr
FROM selling s
JOIN car c ON s.car_id = c.id
JOIN model m ON c.model_id = m.id
JOIN make mk ON m.make_id = mk.id
JOIN seller se ON s.seller_id = se.id
ORDER BY s.id;



# prebacivanje iz saledate u sale_date_dt i brisanje starijeg atributa


ALTER TABLE selling ADD COLUMN sale_date_dt DATETIME NULL;

UPDATE selling
SET sale_date_dt = STR_TO_DATE(
    SUBSTRING(saledate, 5, 20),
    '%b %e %Y %H:%i:%s'
);

SELECT * FROM selling;
ALTER TABLE selling DROP COLUMN saledate;
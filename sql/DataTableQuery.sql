-- Xóa bảng temp_gold_data
DROP TABLE IF EXISTS temp_gold_data;
DROP TABLE IF EXISTS fact_gold_data;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_symbol;

-- Tạo bảng temp_gold_data
CREATE TEMPORARY TABLE IF NOT EXISTS temp_gold_data (
	date varchar, 
	symbol varchar,
	open_price varchar,
	high_price varchar,
	low_price varchar,
	close_price varchar,
	adj_close_price varchar,
	volumn varchar);

-- Copy data từ file csv vào bảng temp_gold_data
\copy temp_gold_data
FROM '../data/gold_data.csv'
WITH DELIMITER ','
CSV HEADER encoding 'windows-1251';

-- Chuyển đổi dữ liệu cột date sang kiểu DATE
ALTER TABLE temp_gold_data 
ALTER COLUMN date 
TYPE date
USING date::date;

CREATE TABLE IF NOT EXISTS dim_date(
	date varchar PRIMARY KEY,
	week varchar,
	month varchar,
	year varchar
);

INSERT INTO dim_date(date, week, month, year)
SELECT DISTINCT(date), EXTRACT(week FROM date), EXTRACT(month FROM date), EXTRACT(year FROM date)
FROM temp_gold_data;

CREATE TABLE IF NOT EXISTS dim_symbol(
	symbol varchar PRIMARY KEY,
	symbol_name varchar
);

INSERT INTO dim_symbol(symbol)
SELECT DISTINCT(symbol)
FROM temp_gold_data;

CREATE TABLE IF NOT EXISTS fact_gold_data (
	date varchar REFERENCES dim_date(date), 
	symbol varchar REFERENCES dim_symbol(symbol),
	open_price varchar,
	high_price varchar,
	low_price varchar,
	close_price varchar,
	adj_close_price varchar,
	volumn varchar
);
	

INSERT INTO fact_gold_data
SELECT *
FROM temp_gold_data;

select * from fact_gold_data;
select * from dim_date;
select * from dim_symbol;


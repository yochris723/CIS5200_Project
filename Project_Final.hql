--load files into linux files system
scp sephora_reviews.csv clee219@144.24.13.0:~
scp top_world_products.csv clee219@144.24.13.0:~

--connect to hadoop cluster
ssh clee219@144.24.13.0
--password clee219

--check files (linux)
ls

--check folder (hdfs)
hdfs dfs -ls

--check folder (hdfs)
hdfs dfs -ls project

--create a folder in hdfs
hdfs dfs -mkdir project
hdfs dfs -mkdir project/reviews
hdfs dfs -mkdir project/products

--load the file into hdfs
hdfs dfs -put sephora_reviews.csv project/reviews
hdfs dfs -put top_world_products.csv project/products

--check folder (hdfs)
hdfs dfs -ls project/reviews
hdfs dfs -ls project/products

--check file specifications (hdfs)
hdfs dfs -du -h project/reviews/sephora_reviews.csv
hdfs dfs -du -h project/products/top_world_products.csv

--check file contents (hdfs)
hdfs dfs -cat project/reviews/sephora_reviews.csv | tail -3
hdfs dfs -cat project/products/top_world_products.csv | tail -3

--connect to hive
beeline

--Create database (Beeline)
create database IF NOT EXISTS clee219;

--Show database (Beeline)
show databases;

--Use database (Beeline)
use clee219;

--drop table (Beeline)
DROP TABLE IF EXISTS sephora_reviews_source;
--create a table from the csv file sephora_reviews.csv (Beeline)
CREATE EXTERNAL TABLE IF NOT EXISTS sephora_reviews_source (
    product_id string,
    product_name_x string,
    brand_id int,
    brand_name_x string,
    loves_count int,
    rating_x float,
    reviews float,
    size string,
    variation_type string,
    variation_value string,
    variation_desc string,
    ingredients string,
    price_usd_x float,
    value_price_usd float,
    sale_price_usd float,
    limited_edition int,
    new int,
    online_only int,
    out_of_stock int,
    sephora_exclusive int,
    highlights string,
    primary_category string,
    secondary_category string,
    tertiary_category string,
    child_count int,
    child_max_price float,
    child_min_price float,
    Unnamed string,
    author_id string,
    rating_y int,
    is_recommended float,
    helpfulness float,
    total_feedback_count int,
    total_neg_feedback_count int,
    total_pos_feedback_count int,
    submission_time string,
    review_text string,
    review_title string,
    skin_tone string,
    eye_color string,
    skin_type string,
    hair_color string,
    product_name_y string,
    brand_name_y string,
    price_usd_y float)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LOCATION "/user/clee219/project/reviews"
TBLPROPERTIES ('skip.header.line.count' = '1');

-- Display the schema of the merged table
DESCRIBE FORMATTED sephora_reviews_source;

--check table (Beeline)
select * from sephora_reviews_source LIMIT 3;

--drop table (Beeline)
DROP TABLE IF EXISTS sephora_reviews_normalize;
-- Normalize strings for all the string columns in the sephora_reviews table
CREATE TABLE sephora_reviews_normalize AS
SELECT 
    LOWER(TRIM(product_id)) AS product_id,
    LOWER(TRIM(product_name_x)) AS product_name_x,
    LOWER(TRIM(brand_name_x)) AS brand_name_x,
    LOWER(TRIM(size)) AS size,
    LOWER(TRIM(variation_type)) AS variation_type,
    LOWER(TRIM(variation_value)) AS variation_value,
    LOWER(TRIM(variation_desc)) AS variation_desc,
    LOWER(TRIM(ingredients)) AS ingredients,
    LOWER(TRIM(highlights)) AS highlights,
    LOWER(TRIM(primary_category)) AS primary_category,
    LOWER(TRIM(secondary_category)) AS secondary_category,
    LOWER(TRIM(tertiary_category)) AS tertiary_category,
    LOWER(TRIM(submission_time)) AS submission_time,
    LOWER(TRIM(review_text)) AS review_text,
    LOWER(TRIM(review_title)) AS review_title,
    LOWER(TRIM(skin_tone)) AS skin_tone,
    LOWER(TRIM(eye_color)) AS eye_color,
    LOWER(TRIM(skin_type)) AS skin_type,
    LOWER(TRIM(hair_color)) AS hair_color,
    LOWER(TRIM(product_name_y)) AS product_name_y,
    LOWER(TRIM(brand_name_y)) AS brand_name_y,
    brand_id,
    loves_count,
    rating_x,
    price_usd_x,
    is_recommended,
    total_feedback_count,
    total_neg_feedback_count,
    total_pos_feedback_count,
    reviews
FROM sephora_reviews_source;

-- Display the schema of the merged table
DESCRIBE FORMATTED sephora_reviews_normalize;

-- Display the first five rows of the normalized sephora_reviews table
SELECT * FROM sephora_reviews_normalize LIMIT 5;

--drop table (Beeline)
DROP TABLE IF EXISTS top_world_products_source;
--create a table from the csv file top_world_products.csv (Beeline)
CREATE EXTERNAL TABLE IF NOT EXISTS top_world_products_source (
    Product_Name string,
    Brand string,
    Category string,
    Usage_Frequency string,
    Price_USD float,
    Rating float,
    Number_of_Reviews int,
    Product_Size string,
    Skin_Type string,
    Gender_Target string,
    Packaging_Type string,
    Main_Ingredient string,
    Cruelty_Free boolean,
    Country_of_Origin string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LOCATION "/user/clee219/project/products"
TBLPROPERTIES ('skip.header.line.count' = '1');

-- Display the schema of the merged table
DESCRIBE FORMATTED top_world_products_source;

--check table (Beeline)
select * from top_world_products_source LIMIT 3;

--drop table (Beeline)
DROP TABLE IF EXISTS top_world_products_normalize;
-- Normalize strings for all the string columns in the top_world_products table
CREATE TABLE top_world_products_normalize AS
SELECT 
    LOWER(TRIM(Product_Name)) AS Product_Name,
    LOWER(TRIM(Brand)) AS Brand,
    LOWER(TRIM(Category)) AS Category,
    LOWER(TRIM(Usage_Frequency)) AS Usage_Frequency,
    LOWER(TRIM(Product_Size)) AS Product_Size,
    LOWER(TRIM(Skin_Type)) AS Skin_Type,
    LOWER(TRIM(Gender_Target)) AS Gender_Target,
    LOWER(TRIM(Packaging_Type)) AS Packaging_Type,
    LOWER(TRIM(Main_Ingredient)) AS Main_Ingredient,
    LOWER(TRIM(Country_of_Origin)) AS Country_of_Origin,
    Price_USD,
    Rating,
    Number_of_Reviews,
    Cruelty_Free
FROM top_world_products_source;

-- Display the schema of the merged table
DESCRIBE FORMATTED top_world_products_normalize;

-- Display the first five rows of the normalized top_world_products table
SELECT * FROM top_world_products_normalize LIMIT 5;

--drop table (Beeline)
DROP TABLE IF EXISTS sephora_reviews_clean;
-- Create a new table with selected columns from sephora_reviews
CREATE TABLE sephora_reviews_clean AS
SELECT 
    product_id,
    product_name_x,
    brand_id,
    brand_name_x,
    loves_count,
    rating_x,
    reviews,
    ingredients,
    price_usd_x,
    primary_category,
    is_recommended,
    total_feedback_count,
    total_neg_feedback_count,
    total_pos_feedback_count,
    skin_type
FROM sephora_reviews_normalize;

-- Display the first five rows of the cleaned sephora_reviews table
SELECT * FROM sephora_reviews_clean LIMIT 5;

-- Print the schema of the cleaned sephora_reviews table
DESCRIBE FORMATTED sephora_reviews_clean;

--drop table (Beeline)
DROP TABLE IF EXISTS top_world_products_clean;
-- Create a new table with selected columns from top_world_products
CREATE TABLE top_world_products_clean AS
SELECT 
    Product_Name,
    Brand,
    Category,
    Usage_Frequency,
    Product_Size,
    Skin_Type,
    Gender_Target,
    Packaging_Type,
    Country_of_Origin,
    Price_USD,
    Rating,
    Number_of_Reviews,
    Main_Ingredient,
    Cruelty_Free
FROM top_world_products_normalize;

-- Display the first five rows of the cleaned top_world_products table
SELECT * FROM top_world_products_clean LIMIT 5;

-- Print the schema of the cleaned top_world_products table
DESCRIBE FORMATTED top_world_products_clean;

-- Detect the missing values in the sephora_reviews table
SELECT 
    COUNT(*) AS total_rows,
    COUNT(product_id) AS missing_product_id,
    COUNT(product_name_x) AS missing_product_name_x,
    COUNT(brand_id) AS missing_brand_id,
    COUNT(brand_name_x) AS missing_brand_name_x,
    COUNT(loves_count) AS missing_loves_count,
    COUNT(rating_x) AS missing_rating_x,
    COUNT(reviews) AS missing_reviews,
    COUNT(price_usd_x) AS missing_price_usd_x,
    COUNT(primary_category) AS missing_primary_category,
    COUNT(is_recommended) AS missing_is_recommended,
    COUNT(total_feedback_count) AS missing_total_feedback_count,
    COUNT(total_neg_feedback_count) AS missing_total_neg_feedback_count,
    COUNT(total_pos_feedback_count) AS missing_total_pos_feedback_count,
    COUNT(skin_type) AS missing_skin_type
FROM sephora_reviews_clean;

--drop table (Beeline)
DROP TABLE IF EXISTS sephora_reviews_updated;
--Fill missing string values with a placeholder
CREATE TABLE sephora_reviews_updated AS
SELECT 
    product_id,
    product_name_x,
    brand_id,
    brand_name_x,
    loves_count,
    rating_x,
    reviews,
    price_usd_x,
    primary_category,
    COALESCE(ingredients, "Not specified") AS ingredients, -- Replace NULL with "Not specified"
    COALESCE(is_recommended, 0) AS is_recommended,         -- Replace NULL with 0
    total_feedback_count,
    total_neg_feedback_count,
    total_pos_feedback_count,
    COALESCE(skin_type, "unknown") AS skin_type            -- Replace NULL with "unknown"
FROM sephora_reviews_clean;

--Calculate the mode for skin_type
SELECT skin_type
FROM sephora_reviews_updated
GROUP BY skin_type
ORDER BY COUNT(*) DESC
LIMIT 1;

--Drop table
DROP TABLE sephora_reviews_filled;
-- Fill missing skin_type values with the mode
CREATE TABLE sephora_reviews_filled AS
SELECT 
    product_id,
    product_name_x,
    brand_id,
    brand_name_x,
    loves_count,
    rating_x,
    reviews,
    price_usd_x,
    primary_category,
    ingredients,
    COALESCE(skin_type, "normal") AS skin_type, -- Replace NULL with the mode
    is_recommended,
    total_feedback_count,
    total_neg_feedback_count,
    total_pos_feedback_count
FROM sephora_reviews_updated;

--Drop table
DROP TABLE sephora_reviews_filled;
-- Fill missing skin_type values with the mode
CREATE TABLE sephora_reviews_filled AS
SELECT 
    product_id,
    product_name_x,
    brand_id,
    brand_name_x,
    loves_count,
    rating_x,
    reviews,
    price_usd_x,
    primary_category,
    ingredients,
    COALESCE(skin_type, 
        (SELECT skin_type 
         FROM sephora_reviews_updated 
         GROUP BY skin_type 
         ORDER BY COUNT(*) DESC 
         LIMIT 1)) AS skin_type, -- Dynamically replace NULL with the mode
    is_recommended,
    total_feedback_count,
    total_neg_feedback_count,
    total_pos_feedback_count
FROM sephora_reviews_updated;

-- Detect the missing values in the top_world_products table
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN Product_Name IS NULL THEN 1 ELSE 0 END) AS missing_Product_Name,
    SUM(CASE WHEN Brand IS NULL THEN 1 ELSE 0 END) AS missing_Brand,
    SUM(CASE WHEN Category IS NULL THEN 1 ELSE 0 END) AS missing_Category,
    SUM(CASE WHEN Usage_Frequency IS NULL THEN 1 ELSE 0 END) AS missing_Usage_Frequency,
    SUM(CASE WHEN Price_USD IS NULL THEN 1 ELSE 0 END) AS missing_Price_USD,
    SUM(CASE WHEN Rating IS NULL THEN 1 ELSE 0 END) AS missing_Rating,
    SUM(CASE WHEN Number_of_Reviews IS NULL THEN 1 ELSE 0 END) AS missing_Number_of_Reviews,
    SUM(CASE WHEN Product_Size IS NULL THEN 1 ELSE 0 END) AS missing_Product_Size,
    SUM(CASE WHEN Skin_Type IS NULL THEN 1 ELSE 0 END) AS missing_Skin_Type,
    SUM(CASE WHEN Gender_Target IS NULL THEN 1 ELSE 0 END) AS missing_Gender_Target,
    SUM(CASE WHEN Packaging_Type IS NULL THEN 1 ELSE 0 END) AS missing_Packaging_Type,
    SUM(CASE WHEN Main_Ingredient IS NULL THEN 1 ELSE 0 END) AS missing_Main_Ingredient,
    SUM(CASE WHEN Cruelty_Free IS NULL THEN 1 ELSE 0 END) AS missing_Cruelty_Free,
    SUM(CASE WHEN Country_of_Origin IS NULL THEN 1 ELSE 0 END) AS missing_Country_of_Origin
FROM top_world_products_clean;

--drop table (Beeline)
DROP TABLE IF EXISTS sephora_reviews_renamed
-- Rename the column names in the sephora_reviews table
CREATE TABLE sephora_reviews_renamed AS
SELECT 
    product_id AS Product_ID,
    product_name_x AS Product_Name,
    brand_id AS Brand_ID,
    brand_name_x AS Brand_Name,
    loves_count AS Loves,
    rating_x AS Rating,
    ingredients AS Ingredients,
    price_usd_x AS Price_USD,
    primary_category AS Category,
    is_recommended AS Is_Recommended,
    total_feedback_count AS Feedback,
    total_neg_feedback_count AS Neg_Feedback,
    total_pos_feedback_count AS Pos_Feedback,
    skin_type AS Skin_Type,
    reviews AS Reviews
FROM sephora_reviews_filled;

-- Display the first five rows of the renamed sephora_reviews table
SELECT * FROM sephora_reviews_renamed LIMIT 5;

-- Print the schema of the renamed sephora_reviews table
DESCRIBE FORMATTED sephora_reviews_renamed;

--drop table (Beeline)
DROP TABLE IF EXISTS top_world_products_renamed;
-- Rename the column names in the top_world_products table
CREATE TABLE top_world_products_renamed AS
SELECT 
    Product_Name,
    Brand AS Brand_Name,
    Category,
    Price_USD,
    Rating,
    Number_of_Reviews AS Reviews,
    Skin_Type,
    Main_Ingredient AS Ingredients,
    Country_of_Origin
FROM top_world_products_clean;

-- Display the first five rows of the renamed top_world_products table
SELECT * FROM top_world_products_renamed LIMIT 5;

-- Print the schema of the renamed top_world_products table
DESCRIBE FORMATTED top_world_products_renamed;

-- Save and rename the sephora_reviews table to a new CSV file
INSERT OVERWRITE DIRECTORY '/user/clee219/project/sephora_reviews_final'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
SELECT * FROM sephora_reviews_renamed;

-- Save the top_world_products table to an HDFS directory
INSERT OVERWRITE DIRECTORY '/user/clee219/project/top_world_products_final'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
SELECT * FROM top_world_products_renamed;

-- Summary statistics in sephora_reviews table
SELECT 
    MIN(Price_USD) AS min_price,
    MAX(Price_USD) AS max_price,
    AVG(Price_USD) AS avg_price,
    MIN(Rating) AS min_rating,
    MAX(Rating) AS max_rating,
    AVG(Rating) AS avg_rating,
    MIN(Reviews) AS min_reviews,
    MAX(Reviews) AS max_reviews,
    AVG(Reviews) AS avg_reviews
FROM sephora_reviews_renamed;

-- Summary statistics in top_world_products table
SELECT 
    MIN(Price_USD) AS min_price,
    MAX(Price_USD) AS max_price,
    AVG(Price_USD) AS avg_price,
    MIN(Rating) AS min_rating,
    MAX(Rating) AS max_rating,
    AVG(Rating) AS avg_rating,
    MIN(Reviews) AS min_reviews,
    MAX(Reviews) AS max_reviews,
    AVG(Reviews) AS avg_reviews
FROM top_world_products_renamed;

-- Individual stats in sephora_reviews table
SELECT 
    AVG(Price_USD) AS price_mean,
    PERCENTILE_APPROX(CAST(Price_USD AS DOUBLE), 0.5) AS price_median, -- Use PERCENTILE_APPROX
    STDDEV(Price_USD) AS price_std,
    MIN(Rating) AS rating_min,
    MAX(Rating) AS rating_max,
    SUM(Reviews) AS review_sum,
    AVG(Reviews) AS review_mean
FROM sephora_reviews_renamed;

-- Individual stats in top_world_products table
SELECT 
    AVG(Price_USD) AS price_mean,
    PERCENTILE_APPROX(CAST(Price_USD AS DOUBLE), 0.5) AS price_median, -- Use the correct column name
    STDDEV(Price_USD) AS price_std,
    MIN(Rating) AS rating_min,
    MAX(Rating) AS rating_max,
    SUM(Reviews) AS review_sum,
    AVG(Reviews) AS review_mean
FROM top_world_products_renamed;

--check for files and directories (hadoop)
hdfs dfs -ls /user/clee219/project

--copy files to local (hadoop)
hdfs dfs -get /user/clee219/project/sephora_reviews_final/00000*_0 sephora_reviews_final.csv
hdfs dfs -get /user/clee219/project/top_world_products_final/00000*_0 top_world_products_final.csv

--check local files (linux)
ls

--check file (linux)
cat sephora_reviews_final.csv | tail -n 2
cat top_world_products_final.csv | tail -n 2

--if it exists remove the file
rm sephora_reviews_final.csv
rm top_world_products_final.csv

--download file (linux)
scp clee219@144.24.13.0:~/sephora_reviews_final.csv .
scp clee219@144.24.13.0:~/top_world_products_final.csv .
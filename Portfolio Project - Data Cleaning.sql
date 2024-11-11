-- =============================================================================================================
-- SQL Project: Data Cleaning for Netflix Shows Dataset
-- Source: https://www.kaggle.com/datasets/shivamb/netflix-shows
-- =============================================================================================================

SELECT * FROM netflix.netflix_titles;

-- ==============================================================================================================
-- Step 1: Create Staging Tables
-- Create a staging table that mimics the structure of the original table for safe data transformations

CREATE TABLE netflix.netflix_staging 
LIKE netflix.netflix_titles;

-- Insert data into the staging table to avoid any accidental data loss in the original table

INSERT netflix_staging
SELECT * FROM netflix.netflix_titles;

-- ===============================================================================================================
-- Step 2: Data Cleaning Steps
-- Step 2.1: Remove Duplicate Records
-- Identify duplicates by grouping on show_id and checking counts greater than 1

SELECT show_id, COUNT(*)
FROM netflix_staging
GROUP BY show_id
HAVING COUNT(*) > 1;

-- If duplicates are found, consider deleting them or keeping one entry per show_id
-- -----------------------------------------------------------------------------------------------------------------
-- Step 2.2: Identify and Handle Null Values
-- Count null values across all key columns to ensure no missing data in critical fields

SELECT
    SUM(CASE WHEN show_id IS NULL OR show_id = '' THEN 1 ELSE 0 END) AS showid_nulls,
    SUM(CASE WHEN type IS NULL OR type = '' THEN 1 ELSE 0 END) AS type_nulls,
    SUM(CASE WHEN title IS NULL OR title = '' THEN 1 ELSE 0 END) AS title_nulls,
    SUM(CASE WHEN director IS NULL OR director = '' THEN 1 ELSE 0 END) AS director_nulls,
    SUM(CASE WHEN cast IS NULL OR cast = '' THEN 1 ELSE 0 END) AS movie_cast_nulls,
    SUM(CASE WHEN country IS NULL OR country = '' THEN 1 ELSE 0 END) AS country_nulls,
    SUM(CASE WHEN date_added IS NULL OR date_added = '' THEN 1 ELSE 0 END) AS date_added_nulls,
    SUM(CASE WHEN release_year IS NULL OR release_year = '' THEN 1 ELSE 0 END) AS release_year_nulls,
    SUM(CASE WHEN rating IS NULL OR rating = '' THEN 1 ELSE 0 END) AS rating_nulls,
    SUM(CASE WHEN duration IS NULL OR duration = '' THEN 1 ELSE 0 END) AS duration_nulls,
    SUM(CASE WHEN listed_in IS NULL OR listed_in = '' THEN 1 ELSE 0 END) AS listed_in_nulls,
    SUM(CASE WHEN description IS NULL OR description = '' THEN 1 ELSE 0 END) AS description_nulls
FROM 
    netflix_staging;
-- -----------------------------------------------------------------------------------------------------------------
-- Step 2.3: Handle Null Values in Key Columns
-- For director nulls, populate the column by associating specific directors with recurring actors in movie_cast.
-- For unmatched director nulls, set as 'Not Given'.

WITH cte AS
(
SELECT title, CONCAT(director, '---', cast) AS director_cast 
FROM netflix_staging
)

SELECT director_cast, COUNT(*) AS count
FROM cte
GROUP BY director_cast
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;

-- Check for specific actor/director associations to populate director field
-- Populate director for rows with nulls
-- Additional updates for specific directors/casts...

UPDATE netflix_staging 
SET director = 'Alastair Fothergill'
WHERE cast = 'David Attenborough'
AND (director IS NULL or director = '') ;

UPDATE netflix_staging 
SET director = 'Jon Mackey, Joe Guidry'
WHERE cast like '%David Spade%'
AND (director IS NULL or director = '') ;

UPDATE netflix_staging 
SET director = 'Todd Kauffman, Mark Thornton'
WHERE cast like '%Michela Luci%'
AND (director IS NULL or director = '')
AND title like 'True%' ;

UPDATE netflix_staging 
SET director = 'Simon Pike'
where title like 'Oddbods%';

-- Populate any remaining director nulls with 'Not Given'

UPDATE netflix_staging 
SET director = 'Not Given'
WHERE director IS NULL or director = '';

-- -------------------------------------------------------------------------------------------------------------------
-- Step 2.4: Attempt to Populate Country Values Using Associated Directors
-- Using a join on director, update the country column for rows with null values.

SELECT director, MAX(country) AS known_country
    FROM netflix_staging
    WHERE country IS NOT NULL
    GROUP BY director;

-- Update country values for rows with missing or empty country field

UPDATE netflix_staging ns
JOIN (
    SELECT director, MAX(country) AS known_country
    FROM netflix_staging
    WHERE country IS NOT NULL
    GROUP BY director
) director_country
ON ns.director = director_country.director
SET ns.country = director_country.known_country
WHERE ns.country IS NULL or ns.country = '';

-- ---------------------------------------------------------------------------------------------------------------------------
-- Step 2.5: Finding Directors with Multiple Titles but No Country
-- Identify directors with multiple titles but still missing country information

SELECT COUNT(*) AS count, ns.director, director_country.known_country AS new_country
FROM netflix_staging ns
JOIN (
    SELECT director, MAX(country) AS known_country
    FROM netflix_staging
    WHERE country IS NOT NULL
    GROUP BY director
) director_country
ON ns.director = director_country.director
WHERE ns.country IS NULL OR ns.country = ''
GROUP BY ns.director, director_country.known_country
ORDER BY count DESC;

-- --------------------------------------------------------------------------------------------------------------------------
-- Step 2.6: Update Country for Specific Directors with Multiple Films

UPDATE netflix_staging
SET country = 'India'
WHERE director = 'Prakash Satam'
AND (country IS NULL OR country = '');

UPDATE netflix_staging
SET country = 'Canada'
WHERE director = 'Joey So'
AND (country IS NULL OR country = '');

UPDATE netflix_staging
SET country = 'India'
WHERE director = 'Rathindran R Prasad'
AND (country IS NULL OR country = '');

-- Populate 'Not Given' for any remaining missing country values

UPDATE netflix_staging
SET country = 'Not Given'
WHERE country IS NULL or country = '';

-- -----------------------------------------------------------------------------------------------------------------------------------
-- Step 2.7: Handle Other Nulls
-- For columns like date_added, rating, and duration, where the number of nulls is minimal, consider deleting these rows.
-- Delete rows with null or empty values in specific columns

DELETE FROM netflix_staging
WHERE date_added IS NULL OR date_added = ''
   OR rating IS NULL OR rating = ''
   OR duration IS NULL OR duration = '';

-- ======================================================================================================================================
-- Step 3: Drop Unnecessary Columns
-- Remove columns that are not needed for analysis, such as 'movie_cast' and 'description'.

ALTER TABLE netflix_staging
DROP COLUMN cast,
DROP COLUMN description;

-- =======================================================================================================================================
-- Step 4: Final Data Quality Checks
-- Recheck for any remaining null values in critical columns

SELECT
    SUM(CASE WHEN show_id IS NULL OR show_id = '' THEN 1 ELSE 0 END) AS showid_nulls,
    SUM(CASE WHEN type IS NULL OR type = '' THEN 1 ELSE 0 END) AS type_nulls,
    SUM(CASE WHEN title IS NULL OR title = '' THEN 1 ELSE 0 END) AS title_nulls,
    SUM(CASE WHEN director IS NULL OR director = '' THEN 1 ELSE 0 END) AS director_nulls,
    SUM(CASE WHEN country IS NULL OR country = '' THEN 1 ELSE 0 END) AS country_nulls,
    SUM(CASE WHEN date_added IS NULL OR date_added = '' THEN 1 ELSE 0 END) AS date_added_nulls,
    SUM(CASE WHEN release_year IS NULL OR release_year = '' THEN 1 ELSE 0 END) AS release_year_nulls,
    SUM(CASE WHEN rating IS NULL OR rating = '' THEN 1 ELSE 0 END) AS rating_nulls,
    SUM(CASE WHEN duration IS NULL OR duration = '' THEN 1 ELSE 0 END) AS duration_nulls,
    SUM(CASE WHEN listed_in IS NULL OR listed_in = '' THEN 1 ELSE 0 END) AS listed_in_nulls
FROM 
    netflix_staging;

-- Verify country data and counts
select country, count(*) from netflix_staging group by country order by country;

-- ===============================================================================================================================
-- Step 5: Final Clean-Up and Validation
-- Simplify the country field and populate missing or ambiguous entries
-- Split country values and keep only the primary country (first part before the comma).
-- Create a new column 'country1' to store the first country listed.

ALTER TABLE netflix_staging
ADD country1 VARCHAR(500);

-- Populate country1 with the first country listed from 'country'

UPDATE netflix_staging
SET country1 = SUBSTRING_INDEX(country, ',', 1);

-- Verify the updates to the country1 column

select country1, country from netflix_staging order by country1;

-- -------------------------------------------------------------------------------------------------------------------------------
-- Step 5.1: Drop Original Country Column and Rename 'country1'
-- Drop the original 'country' column and rename 'country1' to 'country'

ALTER TABLE netflix_staging
DROP COLUMN country;

ALTER TABLE netflix_staging
RENAME COLUMN country1 TO country;

-- Remove rows where 'country' is NULL or blank

DELETE FROM netflix_staging
WHERE country IS NULL OR country = '';

-- Step 5.2: Convert the string format of 'date_added' column to MySQL DATE format
-- The STR_TO_DATE function is used to convert the 'date_added' string values from format '%M %d, %Y' (e.g., 'September 25, 2021')
-- into a proper MySQL DATE type (YYYY-MM-DD). This helps in standardizing the date format for further operations.

UPDATE netflix_staging
SET date_added = STR_TO_DATE(date_added, '%M %d, %Y');

-- Step 5.3: Alter the 'date_added' column data type to DATE
-- After converting the 'date_added' column values to a valid DATE format, the column's data type is changed 
-- from its original type (likely VARCHAR or TEXT) to the DATE type for better storage and query performance.

ALTER TABLE netflix_staging
MODIFY COLUMN date_added DATE;

-- ========================================================================================================================================
-- Step 6: Verify Cleaned Data
-- Confirm there are no remaining nulls in critical columns
-- Verify Row Count Consistency: Ensure that all columns have the same number of rows after cleaning, with no unintended nulls remaining.

SELECT
    SUM(CASE WHEN show_id IS NULL OR show_id = '' THEN 1 ELSE 0 END) AS showid_nulls,
    SUM(CASE WHEN type IS NULL OR type = '' THEN 1 ELSE 0 END) AS type_nulls,
    SUM(CASE WHEN title IS NULL OR title = '' THEN 1 ELSE 0 END) AS title_nulls,
    SUM(CASE WHEN director IS NULL OR director = '' THEN 1 ELSE 0 END) AS director_nulls,
    SUM(CASE WHEN country IS NULL OR country = '' THEN 1 ELSE 0 END) AS country_nulls,
    SUM(CASE WHEN date_added IS NULL OR date_added = '' THEN 1 ELSE 0 END) AS date_added_nulls,
    SUM(CASE WHEN release_year IS NULL OR release_year = '' THEN 1 ELSE 0 END) AS release_year_nulls,
    SUM(CASE WHEN rating IS NULL OR rating = '' THEN 1 ELSE 0 END) AS rating_nulls,
    SUM(CASE WHEN duration IS NULL OR duration = '' THEN 1 ELSE 0 END) AS duration_nulls,
    SUM(CASE WHEN listed_in IS NULL OR listed_in = '' THEN 1 ELSE 0 END) AS listed_in_nulls
FROM 
    netflix_staging;
    
-- Final table review to ensure data consistency and correctness

SELECT * FROM netflix.netflix_staging;

-- =================================================================================================================================================

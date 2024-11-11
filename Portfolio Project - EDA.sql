-- -------------------------------------------------------------
-- ** Netflix Data Analysis Queries for Portfolio**
-- These queries are designed to analyze the content dataset
-- from the `netflix_staging` table. The goal is to explore 
-- various aspects of the dataset such as the distribution of
-- movie types, genres, release years, ratings, durations, 
-- and directors. Each query is aimed at uncovering valuable 
-- insights into the content catalog of Netflix.
-- -------------------------------------------------------------

-- Fetch all records from the `netflix_staging` table
-- This query retrieves all the data in the table for full inspection.
SELECT * 
FROM netflix.netflix_staging;

-- Count the number of records for each type (e.g., Movie, TV Show)
-- This query provides a breakdown of the total number of movies and TV shows.
SELECT type, COUNT(*) AS "Total" 
FROM netflix.netflix_staging 
GROUP BY type;

-- Count the number of titles by each director, excluding 'Not Given' as the director
-- This query helps identify directors with the highest number of titles in the catalog.
SELECT director, COUNT(*) AS `No of Titles` 
FROM netflix.netflix_staging 
WHERE director != "Not Given"
GROUP BY director 
ORDER BY `No of Titles` DESC;

-- Count the number of titles released in each year
-- This query shows how many titles were released each year, which is useful for trend analysis.
SELECT release_year, COUNT(*) AS `No of Titles`
FROM netflix.netflix_staging
GROUP BY release_year
ORDER BY release_year;

-- Count the number of titles released in each year, ordered by the number of titles (descending)
-- A variation of the previous query, this orders the years by the volume of titles released.
SELECT release_year, COUNT(*) AS `No of Titles`
FROM netflix.netflix_staging
GROUP BY release_year
ORDER BY `No of Titles` DESC;

-- Count the number of titles added in each year
-- This shows the number of titles added to the platform per year, useful for analyzing growth.
SELECT YEAR(date_added) AS `year_added`, COUNT(*) AS `No of Titles`
FROM netflix.netflix_staging
GROUP BY YEAR(date_added)
ORDER BY `year_added`;

-- Count the number of titles added in each year, ordered by the number of titles (descending)
-- Similar to the previous query but sorted by the number of titles added each year.
SELECT YEAR(date_added) AS `year_added`, COUNT(*) AS `No of Titles`
FROM netflix.netflix_staging
GROUP BY YEAR(date_added)
ORDER BY `No of Titles` DESC;

-- Count the number of titles by rating (e.g., PG, R, etc.)
-- This query helps understand the distribution of titles across different ratings.
SELECT rating, COUNT(*) AS `No of Titles`
FROM netflix.netflix_staging
GROUP BY rating
ORDER BY `No of Titles` DESC;

-- Count the number of titles by duration (in minutes)
-- This provides insights into the distribution of movie lengths.
SELECT duration, COUNT(*) AS `No of Titles`
FROM netflix.netflix_staging
GROUP BY duration
ORDER BY `No of Titles` DESC;

-- Calculate the average duration of titles (in minutes)
-- This query calculates the average movie length to understand content time span trends.
SELECT AVG(CAST(SUBSTRING_INDEX(duration, ' ', 1) AS UNSIGNED)) AS `Average Duration`
FROM netflix.netflix_staging
WHERE duration LIKE '%min';

-- Count the number of TV shows by duration (number of seasons)
-- This query helps analyze how TV shows vary by their number of seasons.
SELECT duration, COUNT(*) AS `No of Titles`
FROM netflix.netflix_staging
WHERE duration LIKE '%Seasons%'
GROUP BY duration
ORDER BY `No of Titles` DESC;

-- Calculate the average number of seasons for TV shows
-- This gives an overview of the average number of seasons per TV show.
SELECT AVG(CAST(SUBSTRING_INDEX(duration, ' ', 1) AS UNSIGNED)) AS `Average Seasons`
FROM netflix.netflix_staging
WHERE duration LIKE '%Seasons%';

-- Count the number of titles by category from the `listed_in` column
-- This query helps identify the distribution of titles across genres or categories.
SELECT category, COUNT(*) AS `No of Titles`
FROM (
    SELECT TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(listed_in, ',', n.n), ',', -1)) AS category
    FROM netflix.netflix_staging
    CROSS JOIN (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10) n
    WHERE n.n <= 1 + LENGTH(listed_in) - LENGTH(REPLACE(listed_in, ',', ''))
) AS categories
GROUP BY category
ORDER BY `No of Titles` DESC;

-- Find the movie with the longest duration
-- This identifies the movie with the longest runtime.
SELECT title, duration
FROM netflix.netflix_staging
WHERE type = 'Movie' AND duration LIKE '%min'
ORDER BY CAST(SUBSTRING_INDEX(duration, ' ', 1) AS UNSIGNED) DESC
LIMIT 1;

-- Find the movie with the shortest duration
-- This identifies the movie with the shortest runtime.
SELECT title, duration
FROM netflix.netflix_staging
WHERE type = 'Movie' AND duration LIKE '%min'
ORDER BY CAST(SUBSTRING_INDEX(duration, ' ', 1) AS UNSIGNED) ASC
LIMIT 1;

-- Count the number of titles by country
-- This query shows how many titles are available in each country.
SELECT country, COUNT(*) AS `No of Titles`
FROM netflix.netflix_staging
GROUP BY country
ORDER BY `No of Titles` DESC;

-- Count the number of titles by director in a specific genre (e.g., Comedy)
-- This helps find which directors specialize in specific genres.
SELECT director, COUNT(*) AS `No of Titles`
FROM netflix.netflix_staging
WHERE listed_in LIKE '%Comedy%' AND director != "Not Given"
GROUP BY director
ORDER BY `No of Titles` DESC;

-- Count the number of titles by director in a specific genre (e.g., Drama)
-- Similar to the previous query but for the Drama genre.
SELECT director, COUNT(*) AS `No of Titles`
FROM netflix.netflix_staging
WHERE listed_in LIKE '%Drama%' AND director != "Not Given"
GROUP BY director
ORDER BY `No of Titles` DESC;

-- Count the number of titles by director in a specific genre (e.g., Documentaries)
-- Similar to the previous queries but for Documentaries.
SELECT director, COUNT(*) AS `No of Titles`
FROM netflix.netflix_staging
WHERE listed_in LIKE '%Documentaries%' AND director != "Not Given"
GROUP BY director
ORDER BY `No of Titles` DESC;

-- Count movies by duration ranges
-- This query categorizes movies by their duration into short, medium, and long movies.
SELECT 
    CASE 
        WHEN CAST(SUBSTRING_INDEX(duration, ' ', 1) AS UNSIGNED) <= 60 THEN 'Short (<= 60 min)'
        WHEN CAST(SUBSTRING_INDEX(duration, ' ', 1) AS UNSIGNED) BETWEEN 61 AND 120 THEN 'Medium (61-120 min)'
        WHEN CAST(SUBSTRING_INDEX(duration, ' ', 1) AS UNSIGNED) > 120 THEN 'Long (> 120 min)'
    END AS duration_range,
    COUNT(*) AS `No of Titles`
FROM netflix.netflix_staging
WHERE type = 'Movie' AND duration LIKE '%min%'
GROUP BY duration_range
ORDER BY duration_range;

-- Count of titles by rating over the years
-- This shows how the distribution of ratings has changed over time.
SELECT release_year, rating, COUNT(*) AS `No of Titles`
FROM netflix.netflix_staging
GROUP BY release_year, rating
ORDER BY release_year, rating;

-- Count of Movies vs TV Shows by Country
-- This compares the number of movies and TV shows available in each country.
SELECT country, 
       SUM(CASE WHEN type = 'Movie' THEN 1 ELSE 0 END) AS `Movies`,
       SUM(CASE WHEN type = 'TV Show' THEN 1 ELSE 0 END) AS `TV Shows`
FROM netflix.netflix_staging
GROUP BY country
ORDER BY country;

-- -------------------------------------------------------------
-- ** End of Queries **
-- -------------------------------------------------------------

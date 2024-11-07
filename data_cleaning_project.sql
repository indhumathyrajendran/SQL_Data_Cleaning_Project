-- DATA CLEANING 

SELECT *
FROM layoffs
;

-- Remove Duplicates
-- 



CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging
;

select *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`
) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
select *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'oda';

SELECT *
FROM layoffs_staging
WHERE company = 'casper';

WITH duplicate_cte AS
(
select *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;




SELECT `layoffs_staging`.`company`,
    `layoffs_staging`.`location`,
    `layoffs_staging`.`industry`,
    `layoffs_staging`.`total_laid_off`,
    `layoffs_staging`.`percentage_laid_off`,
    `layoffs_staging`.`date`,
    `layoffs_staging`.`stage`,
    `layoffs_staging`.`country`,
    `layoffs_staging`.`funds_raised_millions`
FROM `world_layoffs`.`layoffs_staging`;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS row_num
FROM layoffs_staging;

select *
from layoffs_staging2;

select *
from layoffs_staging2
where row_num > 1;

SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;


WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
)duplicates
)
DELETE
FROM DELETE_CTE
WHERE 
	row_num > 1
;


WITH DELETE_CTE AS
 (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER 
    (
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs.layoffs_staging2
)
DELETE 
FROM world_layoffs.layoffs_staging2
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
)
AND row_num > 1;

SET SQL_SAFE_UPDATES = 0;
DELETE FROM layoffs_staging2 WHERE row_num > 1;
SET SQL_SAFE_UPDATES = 1;



select *
FROM layoffs_staging2
where row_num > 1;


select *
FROM layoffs_staging2
;

-- standadizing data

select company, TRIM(company)
from layoffs_staging2
;

update layoffs_staging2
SET company = TRIM(company);

SELECT distinct industry
FROM layoffs_staging2
ORDER by 1;

SELECT *
FROM layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
SET industry = 'crypto'
where industry like 'crypto%';

SELECT distinct industry
FROM layoffs_staging2;

SELECT distinct country
FROM layoffs_staging2
;

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;

SELECT distinct country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT * 
FROM layoffs_staging2;

-- DATE

SELECT `date`,
str_to_date(`date` , '%m/%d/%Y')
FROM layoffs_staging2;

update layoffs_staging2
SET `date`= str_to_date(`date` , '%m/%d/%Y');

ALTER table layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT * 
FROM layoffs_staging2;

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;



SELECT distinct industry
from layoffs_staging2
WHERE industry IS NULL;

SELECT *
from layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
from layoffs_staging2
WHERE company='Airbnb' ;

SELECT t1.industry ,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
    WHERE (t1.industry IS NULL OR t1.industry = '')
    AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
 SET t1.industry =	t2.industry
 WHERE (t1.industry IS NULL OR t1.industry = '')
 AND t2.industry IS NOT NULL;

SELECT *
from layoffs_staging2;

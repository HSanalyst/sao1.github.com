-- Data Cleaning

select *
from world_layoffs.layoffs;

-- 1. Remove Dulpicates
-- 2. Standardize the data (spelling check)
-- 3. Null values or blank values
-- 4. Remove any columns (removing dataset from the raw dataset is going to be big problem)

Create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

select *,
row_number () over (partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging;

with duplicate_cte as 
(
select *,
row_number () over (partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;


select *
from layoffs_staging
where company = 'Casper';


with duplicate_cte as 
(
select *,
row_number () over (partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;




CREATE TABLE `layoffs_staging3` (
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


select *
from layoffs_staging3;

insert into layoffs_staging3
select *,
row_number () over (partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

delete
from layoffs_staging3
where row_num > 1;

select * 
from layoffs_staging3;



-- Standardizing data

select company, trim(company)
from layoffs_staging3;

update layoffs_staging3
set company = trim(company);
 

select distinct industry
from layoffs_staging3
order by 1;


select *
from layoffs_staging3
where industry like 'Crypto%';

update layoffs_staging3
set industry = 'crypto'
where industry like 'crypto%';


select distinct industry
from layoffs_staging3;


select distinct country
from layoffs_staging3
order by 1;


select *
from layoffs_staging3
where country like 'United States%';


select distinct country, trim(trailing '.' from country)
from layoffs_staging3
order by 1;

update layoffs_staging3
set country = trim(trailing '.' from country)
where country like 'united States%';

select *
from layoffs_staging3;

Select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging3;


update layoffs_staging3
set date = str_to_date(`date`, '%m/%d/%Y');


select `date`
from layoffs_staging3;

Alter table layoffs_staging3
modify column `date` date;

select *
from layoffs_staging3
where total_laid_off is null
and percentage_laid_off is null;

update layoffs_staging3
set industry = null
where industry = '';

select *
from layoffs_staging3
where industry is null
or industry  = '';

select *
from layoffs_staging3
where company = 'Airbnb';

select st2.industry, st3.industry
from layoffs_staging3 as st2
join layoffs_staging3 as st3
	on st2.company = st3.company
where (st2.industry is null or st2.industry = '')
and st3.industry is not null;


update layoffs_staging3 as st2
 join layoffs_staging3 as st3
	on st2.company = st3.company
set st2.industry = st3.industry
where (st2.industry is null or st2.industry = '')
and st3.industry is not null;

select *
from layoffs_staging3;



select *
from layoffs_staging3
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging3
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging3;

alter table layoffs_staging3
drop column row_num;












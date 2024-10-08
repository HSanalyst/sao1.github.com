-- Exploratory data analysis

select *
from layoffs_staging3;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging3;


select *
from layoffs_staging3
where percentage_laid_off = 1
order by total_laid_off desc;

select company,sum(total_laid_off)
from layoffs_staging3
group by company
order by 2 desc;


select min(`date`), max(`date`)
from layoffs_staging3;

select year(`date`) ,sum(total_laid_off)
from layoffs_staging3
group by year(`date`)
order by 1 desc;

select *
from layoffs_staging3;

select substring(`date`,1,7) as `Month`, sum(total_laid_off)
from layoffs_staging3
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc;


with rolling_total as
(
select substring(`date`,1,7) as `Month`, sum(total_laid_off) as total_off
from layoffs_staging3
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
)

select `month`, total_off, sum(total_off) over(order by `month`) as rolling_total
from rolling_total;


select company,sum(total_laid_off)
from layoffs_staging3
group by company
order by 2 desc;

select company, Year(`date`),sum(total_laid_off)
from layoffs_staging3
group by company, year(`date`)
order by company asc;


select company, Year(`date`),sum(total_laid_off)
from layoffs_staging3
group by company, year(`date`)
order by 3 desc;


with Company_year (company, years, total_laid_off) as
(
select company, Year(`date`),sum(total_laid_off)
from layoffs_staging3
group by company, year(`date`)
)
select *
from company_year;

















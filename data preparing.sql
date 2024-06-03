-- data cleaning
select *
from layoffs;

create table layoffs_copy
like layoffs;

select *
from layoffs_copy;

insert layoffs_copy
select *
from layoffs;

-- 1. remove duplicates 
select *,
row_number() over( partition by company, location, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num -- bec date is a key word
from layoffs_copy;

with duplicate_cte as(
select *,
row_number() over( partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num -- bec date is a key word
from layoffs_copy
)
select *
from duplicate_cte 
where row_num > 1;

CREATE TABLE `layoffs_copy2` (
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
from layoffs_copy2;

insert into layoffs_copy2
select *,
row_number() over( partition by company, location, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num -- bec date is a key word
from layoffs_copy;

SET SQL_SAFE_UPDATES = 0;

delete 
from layoffs_copy2
where row_num > 1;

select *
from layoffs_copy2
where row_num > 1;

-- --------------------------------------------------------------- --

-- standardizing data 
select company, TRIM(company) -- trim takes white space of the end
from layoffs_copy2;

select distinct industry
from layoffs_copy2
order by 1;

update layoffs_copy2
set company = TRIM(company);

update layoffs_copy2
set industry = 'Crypto'
where industry like 'Crypto%';

select *
from layoffs_copy2
where industry like 'Crypto%';










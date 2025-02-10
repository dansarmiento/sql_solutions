USE [ucr_health]
GO

/****** Object:  View [rpt].[vw_appt_kpi_spec_hist]    Script Date: 2/10/2025 11:07:42 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




/*
historical KPI separated from current KPI for data clarity
average volume per month (historical) = month volume / 12
annualized weighted average (historical) = visit volume / fiscal year total
fiscal year variance (historical) = (current FY total - previous FY total) / previous FY total
fiscal period variance (historical) = (current fiscal period total - previous fiscal period total) / previous fiscal period total

separated much for clarity, but ended up inserting the fiscal_year_target as it was simpler here
*/

CREATE view [rpt].[vw_appt_kpi_spec_hist] as 

with visit_data as 
(
-- source data is previous fiscal years for completed appts
select 
	count([csn]) as visits
	,[dept_specialty_name]
	,[fiscal_year]
	,month([fiscal_yearmonth]) as fiscal_period
	,cast([month_start] as date) as month_start
from [ucr_health].[rpt].[appointment]
where fiscal_year >= (select max(fiscal_year) -3 from [ucr_health].[rpt].[appointment])
	and appt_status = 'Completed'
	--and visit_provider_name not like '%UCR%'
	and fiscal_year <> (select max(fiscal_year) from [ucr_health].[rpt].[appointment])
group by 	[dept_specialty_name] ,[fiscal_year], month([fiscal_yearmonth]), 	[month_start]	
),
monthly_kpi as
(
-- each function in this CTE calculates sums at different granularities
-- those sums are used to calculate the avg monthly volume & annualized weighted avg
select distinct
	sum(visits) over( partition by dept_specialty_name, month_start) as specialty_monthly_total
	, sum(visits) over( partition by dept_specialty_name, fiscal_year) as specialty_fy_total
	, (sum(visits) over( partition by dept_specialty_name, fiscal_year) * 1.0) / 12 as avg_monthly_volume
	, cast((sum(visits) over( partition by dept_specialty_name, month_start) * 1.0)
		/ (sum(visits) over( partition by dept_specialty_name, fiscal_year) * 1.0 ) 
		as decimal (6,4))
		as annualized_weighted_avg
	, fiscal_year
	, month_start
	, dept_specialty_name
from visit_data
order by dept_specialty_name, month_start 
offset 0 rows
),
yearly_spec_kpi_source as
(
-- have to get unique rows and sort the results for the yearly calculation table
-- duplicate rows are result of going from monthly to yearly granularity
select distinct 
	specialty_fy_total
	, fiscal_year
	, dept_specialty_name
from monthly_kpi
order by dept_specialty_name, fiscal_year
offset 0 rows
),
yearly_spec_kpi as
(
-- lag function grabs the previous fiscal year total & then variance calculates using that result
select 
	specialty_fy_total 
	, LAG(specialty_fy_total, 1, null) OVER (PARTITION BY dept_specialty_name ORDER BY fiscal_year) AS previous_year_fy_total
	, cast( ( (specialty_fy_total * 1.0) - (LAG(specialty_fy_total, 1, null) OVER (PARTITION BY dept_specialty_name ORDER BY fiscal_year) * 1.0) )
		/ (LAG(specialty_fy_total, 1, null) OVER (PARTITION BY dept_specialty_name ORDER BY fiscal_year) * 1.0)  as decimal(6,4))
		as fiscal_year_variance
	, fiscal_year
	, dept_specialty_name
	
from yearly_spec_kpi_source
)
select 
    m.specialty_monthly_total,
    -- Use the joined table (m2) to get the previous year’s same-month total
    m2.specialty_monthly_total AS previous_year_monthly_total,

    -- Compute the variance using the previous_year_monthly_total
    CAST
    (
        (
            (m.specialty_monthly_total * 1.0) 
            - (m2.specialty_monthly_total * 1.0)
        )
        / NULLIF((m2.specialty_monthly_total * 1.0), 0)
        AS DECIMAL(6,4)
    ) AS fiscal_period_variance,

    m.specialty_fy_total,
    m.avg_monthly_volume,
    y.fiscal_year_variance,
    m.annualized_weighted_avg,
    m.fiscal_year,
    m.month_start,
    m.dept_specialty_name
FROM monthly_kpi AS m
    -- Self-join to get the same month in the previous year
    left outer join monthly_kpi AS m2
        ON  m2.dept_specialty_name = m.dept_specialty_name
        AND m2.month_start        = DATEADD(YEAR, -1, m.month_start)
    -- Join to yearly_kpi to get your fiscal_year_variance
    left outer join yearly_spec_kpi AS y
        ON  m.fiscal_year         = y.fiscal_year
        AND m.dept_specialty_name = y.dept_specialty_name
--order by m.dept_specialty_name, m.month_start
--offset 0 rows
		;
GO



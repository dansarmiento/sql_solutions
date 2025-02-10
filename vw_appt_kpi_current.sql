USE [ucr_health]
GO

/****** Object:  View [rpt].[vw_appt_kpi_spec_curr]    Script Date: 2/10/2025 11:07:20 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/*
current year KPI calculated separately from historical KPI for clarity

target weighted average = average of annualized weighted average for each same period in previous 3 years
target avg monthly volume = fiscal year target * target weighted average
*/

CREATE view [rpt].[vw_appt_kpi_spec_curr] as 

with tgt_weighted_vol_src as 

(
SELECT
    month_start
    ,dept_specialty_name
	, fiscal_year 
	, --fiscal_year_target
	    (
       select MAX(m3.specialty_fy_total) 
       from [ucr_health].[rpt].[vw_appt_kpi_spec_hist] AS m3 
       where m3.dept_specialty_name = m.dept_specialty_name 
    )  * 1.05 AS fiscal_year_target
    -- Window function that computes the average across rows with the same specialty and same month
    ,AVG(annualized_weighted_avg) 
        OVER (
            PARTITION BY dept_specialty_name, MONTH(month_start)
        ) AS target_weighted_average
FROM [ucr_health].[rpt].[vw_appt_kpi_spec_hist] as m
),
tgt_weighted_vol as
(
select
dateadd (yy, 1, month_start) as month_start
, dept_specialty_name
, fiscal_year
, target_weighted_average
, fiscal_year_target
, target_weighted_average * fiscal_year_target as target_average_monthly_volume
from tgt_weighted_vol_src
where fiscal_year = (select max(fiscal_year) from tgt_weighted_vol_src)
),
annual_volume_projection_src as
(
-- source data is current fiscal year for completed appts
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
	and fiscal_year = (select max(fiscal_year) from [ucr_health].[rpt].[appointment])
group by 	[dept_specialty_name], [fiscal_year], month([fiscal_yearmonth]), 	[month_start]	
),
annual_volume_projection as
(
select 
    dept_specialty_name,
    fiscal_year,
    fiscal_period,
    month_start,
    visits,

    -- Calculate the running total of visits from the first month of the fiscal year up to current row
    SUM(visits) 
        OVER (
            PARTITION BY dept_specialty_name, fiscal_year
            ORDER BY month_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS visits_ytd,

    -- Count how many months have occurred so far this fiscal year (up to current month)
    COUNT(*) 
        OVER (
            PARTITION BY dept_specialty_name, fiscal_year
            ORDER BY month_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS months_so_far,

    -- Divide running total by the count of months for the YTD average
    CASE 
        WHEN COUNT(*) OVER (
               PARTITION BY dept_specialty_name, fiscal_year
               ORDER BY month_start
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
             ) = 0
        THEN 0
        ELSE 
            1.0 * SUM(visits) 
                  OVER (
                      PARTITION BY dept_specialty_name, fiscal_year
                      ORDER BY month_start
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                  ) 
            / COUNT(*) 
                  OVER (
                      PARTITION BY dept_specialty_name, fiscal_year
                      ORDER BY month_start
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                  )
    END AS annual_volume_projection
from annual_volume_projection_src 
)
-- just need to add the FY target and average monthly volume
select 
t.month_start
, t.dept_specialty_name 
, t.target_weighted_average
, t.fiscal_year_target
, t.target_average_monthly_volume 
, a.fiscal_year
, a.fiscal_period
, a.visits
, a.visits_ytd
, a.months_so_far
, a.annual_volume_projection
from tgt_weighted_vol as t
left outer join annual_volume_projection as a
on t.month_start = a.month_start and t.dept_specialty_name = a.dept_specialty_name
order by dept_specialty_name, month_start
offset 0 rows;
GO



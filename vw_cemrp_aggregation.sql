USE [ucr_health]
GO

/****** Object:  View [rpt].[vw_cemrp_monthly]    Script Date: 2/10/2025 2:46:16 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




/*
Created By: [daniel.sarmiento@medsch.ucr.edu]
Creation Date: [November 1, 2023]
Last Altered By: [daniel.sarmiento@medsch.ucr.edu]
Alteration Date: [April 15, 2024]
Description of report: [Combined monthly report for CEMRP,
each CTE aggregates different components of the report,
new_pt_days is a pivot table for new patient appointment turnaround time
in ten days or less, new_pt_ratio is a pivot table aggregating patient status 
for each year_month, median uses a function to find the median days to appointment
for new patients
]
Update description: inserted month end column into view as prep to deliver report in Power BI
*/


CREATE view [rpt].[vw_cemrp_monthly] as
with new_pt_days as
(		/* result table select includes columns created below and calculations for ratio */
select
	year_month
	, month_start
	, [10 or less]
	, [over 10]
	, [10 or less] + [over 10] as new_pt_total
	/* multiplying columns by 1.0 to make into float, the casting into decimal for format */
	, cast([10 or less] * 1.0 / ([10 or less] * 1.0 + [over 10] * 1.0) as decimal(5,3))
		as new_pt_within_ten
from
	(	/* pivot table source rows */
	select
		count(pat_enc_csn_id) as pt_count	
		, appt_tat
		, year_month 
		, month_start
	from rpt.vw_cemrp_encounters
	where patient_status = 'new_patient'
	group by appt_tat, year_month, month_start
	) as src
pivot
	(	/* aggregation function required, but agg already completed so using max */
	max(pt_count)
	for appt_tat in ([10 or less],[over 10])
	)
as pivottable
--order by year_month
)
, new_pt_ratio as
(		/* same format pivot table as above */
select
	year_month
	, month_start
	, [new_patient]
	, [follow_up]
	, new_patient + follow_up as pt_total
	, cast(new_patient * 1.0 / (new_patient * 1.0 + follow_up * 1.0) as decimal(5,3))
		as new_pt_ratio
from
	(
	select
		count(pat_enc_csn_id) as pt_count	
		, patient_status
		, year_month 
		, month_start
	from rpt.vw_cemrp_encounters
	group by patient_status, year_month, month_start
	) as src
pivot
	(
	max(pt_count)
	for patient_status in ([new_patient],[follow_up])
	)
as pivottable
--order by year_month
)
, median as
(
select distinct 
	year_month
	, month_start
	/* this function is new to sql server 2012 validated results with python */
	, PERCENTILE_CONT(0.5)
		within group (order by cast([days_to_appt] as int))
		over (partition by year_month) as median_new_pt_days_to_appt
from rpt.vw_cemrp_encounters
where patient_status = 'new_patient'
)
select  
	d.year_month
	, d.month_start as month_end
	, d.[10 or less]
	, d.[over 10]
	, d.new_pt_total
	, d.new_pt_within_ten
	, r.new_patient
	, r.follow_up
	, r.pt_total
	, r.new_pt_ratio
	, m.median_new_pt_days_to_appt
from new_pt_days as d
left outer join new_pt_ratio as r on d.year_month = r.year_month
left outer join median as m on d.year_month = m.year_month
order by d.year_month OFFSET 0 ROWS
/* OFFSET function required so I can use ORDER BY in view */
GO



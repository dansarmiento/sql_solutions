USE [ucr_health]
GO

/****** Object:  View [rpt].[vw_no_pcp_pt_volume]    Script Date: 2/10/2025 2:58:28 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


 /*
Created By: [daniel.sarmiento@medsch.ucr.edu]
Creation Date: [December 9, 2024]
Description of report: [View returning the patient volume ]
*/

CREATE view [rpt].[vw_no_pcp_pt_volume] as 

with no_pcp_date_range as
(
	select
		mrn
		, earliest_no_pcp_appt_date AS start_date
		, case
			when pcp_update_date IS NULL then cast(getdate() as date)		-- if no pcp, then end of date range = today
			when pcp_update_date >= cast(getdate() as date) then cast(getdate() as date)	
				-- if pcp update date is in future, patient counts as no pcp until appt date
            else pcp_update_date
			end as effective_end_date
	from [ucr_health].[rpt].[vw_no_pcp_patients]
),
applicable_dates as
(
	select
		n.mrn
		, d.[date]
	from no_pcp_date_range as n 
	left outer join [ucr_health].[src].[dim_date] as d
	on d.date >= n.start_date and d.date <= effective_end_date
)
select
	d.date
	, a.mrn
from applicable_dates as a
left outer join [ucr_health].[src].[dim_date] as d
on a.date = d.date 
--group by d.date
--order by d.date
GO



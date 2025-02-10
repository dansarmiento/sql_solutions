USE [ucr_health]
GO

/****** Object:  View [rpt].[vw_agg_appointment_year]    Script Date: 2/10/2025 11:03:51 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


/*
Created By: [daniel.sarmiento@medsch.ucr.edu]
Creation Date: [January 25, 2024]
Last Altered By: []
Alteration Date: []
Description of report: [Aggregates completed appointments for UCR Health patients per calendar year]
*/

create view [rpt].[vw_agg_appointment_year] as 

with src as
(
SELECT 
	[mrn]
	,count([csn]) as appt_count
	,year([appt_date]) as calendar_year
FROM [ucr_health].[rpt].[appointment]
where appt_status = 'Completed' and mrn is not null and len(visit_prov_id) < 7 and year([appt_date]) > 2020
group by mrn, visit_prov_id, visit_provider_name, year([appt_date])
)
select mrn, [2021],[2022],[2023],[2024]
from 
	( select mrn, appt_count,  calendar_year from src ) as pvt_src
	pivot
	( max([appt_count]) for [calendar_year] in ([2021],[2022],[2023],[2024]) ) as pivottable 


GO



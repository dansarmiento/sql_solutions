USE [ucr_health]
GO

/****** Object:  View [rpt].[vw_no_pcp_patients]    Script Date: 2/10/2025 2:58:17 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO







 /*
Created By: [daniel.sarmiento@medsch.ucr.edu]
Creation Date: [November 13, 2024]
Last Altered By: [steven.staples@medsch.ucr.edu]
Alteration Date: [December 9, 2024]
Description of report: [View identifying patients with no pcp and finding encounters where they have an updated pcp
sourced from rpt.appointment]
*/
CREATE view [rpt].[vw_no_pcp_patients] as 

with pcp_base_data as
(
select 
                mrn,
                min(appt_date) as earliest_no_pcp_appt_date,
                max(appt_date) as latest_no_pcp_appt_date
from [ucr_health].[rpt].[appointment]
where (pcp_prov_id is null 
                or pcp_prov_id = '55555' 
                or pcp_prov_id = '80355555' 
                or pcp_prov_id = '44444' 
                or pcp_prov_id = '88888')
                and (appt_status = 'Completed')
                and appt_date >= DATEADD(MONTH, DATEDIFF(MONTH, 0, DATEADD(YEAR, -3, GETDATE())), 0)
                and dept_specialty_name in ('FAMILY MEDICINE', 'PEDIATRICS SPECIALTIES', 'INTERNAL MEDICINE', 'PRIMARY CARE')
                --Excludes: cardiolody, endo/met/diabetes, rheumatology, sports medicine
                --Uncertain about primary care inclusion (mostly nurse visits)
group by mrn
),

pcp_change_encounters as
(
select 
                mrn
                , appt_date
                , pcp_prov_id
                , pcp_name
                , row_number() over ( partition by mrn order by appt_time asc) as rn
from [ucr_health].[rpt].[appointment] as a
where pcp_prov_id is not null
                and pcp_prov_id <> '55555' 
                and pcp_prov_id <> '80355555' 
                and pcp_prov_id <> '44444' 
                and pcp_prov_id <> '88888'
and exists (select 1 from pcp_base_data as n 
                where a.mrn = n.mrn and appt_date > latest_no_pcp_appt_date)
                --and appt_status <> 'Scheduled' and appt_status <> 'Canceled'
                --Why limit status on this?
), 
pcp_snapshot as
(
SELECT 
	[pat_mrn_id]
	,[cur_pcp_prov_id]
	,[pcp_name]
	,pcp_report_date
FROM [ucr_health].[rpt].[vw_pcp_snapshot] as p
where pcp_status = 'has pcp' and 
	exists (select 1 from pcp_base_data as b where b.mrn = p.[pat_mrn_id])
)
select 
                bd.mrn
				, bd.earliest_no_pcp_appt_date
				, bd.latest_no_pcp_appt_date
				, case 
					when r.mrn is not null then 'Yes' 
					else 'No' end as only_nurse_encs
                , case 
					when s.cur_pcp_prov_id is null then ce.pcp_prov_id
					else s.cur_pcp_prov_id end as pcp_prov_id
                , case 
					when s.pcp_name is null then ce.pcp_name
					else s.pcp_name end as pcp_name
                , case 
					when ce.appt_date is null then s.pcp_report_date
					else ce.appt_date end as pcp_update_date
                , case
                                when MONTH(GETDATE()) > MONTH(pt.birth_date) 
             OR (MONTH(GETDATE()) = MONTH(pt.birth_date) AND DAY(GETDATE()) >= DAY(pt.birth_date))
        then DATEDIFF(YEAR, pt.birth_date, GETDATE())
        else DATEDIFF(YEAR, pt.birth_date, GETDATE()) - 1
                                end as current_pt_age
								
from pcp_base_data as bd
left outer join pcp_change_encounters as ce
                on bd.mrn = ce.mrn and ce.rn = 1
left outer join pcp_snapshot as s
				on bd.mrn = s.[pat_mrn_id] 
inner join src.patient as pt 
				on bd.mrn = pt.mrn
left outer join rpt.vw_pt_only_nurse_enc as r
				on bd.mrn = r.mrn 



GO



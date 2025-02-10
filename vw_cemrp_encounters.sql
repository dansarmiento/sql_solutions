USE [ucr_health]
GO

/****** Object:  View [rpt].[vw_cemrp_encounters]    Script Date: 2/10/2025 2:45:40 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





/*
Created By: [daniel.sarmiento@medsch.ucr.edu]
Creation Date: [October 29, 2023]
Last Altered By: [daniel.sarmiento@medsch.ucr.edu]
Alteration Date: [April 15, 2024]
Description of report: [View joining productivity and appointments tables to find eligible
	encounters.  Other views will aggregate from these eligible rows.
	Formatted year_quarter and year_month for easy aggregation]
Update description: [Inserted month start as a date field to insert into eligible encounters for prep to create visualization ready date column
7/10/2024 - Changed eligible encounter source to clarity_tdl_tran charges, updated encounter matching to pat_enc import to include outpatient care we are charging for 
]
*/

CREATE view [rpt].[vw_cemrp_encounters] as
with eligible_encounters as
(
select
	pat_enc_csn_id
	, PAT_ID
	, case 
		when cpt_code in 
		(
		'99201', '99202', '99203', '99204', '99205', '99381',
		'99382', '99383', '99384', '99385', '99386', '99387',
		'92002', '92004', '99241', '99242', '99243', '99244',
		'99245'
		)
		then 'new_patient'
		else 'follow_up' 
		end as patient_status
	, cpt_code
	, e.[PROC_NAME] as [procedure]
FROM [ucr_health].[src].[clarity_tdl_tran_charges] as c
left outer join [ucr_health].[src].[clarity_eap] as e on c.PROC_ID = e.PROC_ID
where (
		cpt_code in
		(
		'99201', '99202', '99203', '99204', '99205', '99381',
		'99382', '99383', '99384', '99385', '99386', '99387',
		'92002', '92004', '99241', '99242', '99243', '99244',
		'99245'
		)
		or cpt_code in 
		(
		'99211', '99212', '99213', '99214', '99215', '99391',
		'99392', '99393', '99394', '99395', '99396', '99397',
		'92012', '92014', '99024', '99495', '99496', '99441',
		'99442', '99443', '99421', '99422', '99423', '99024'
		)
	)
	/*
	and pos_type_name in
	(
	'Office', 'On Campus - Outpatient Hospital', 
	'Off Campus - Outpatient Hospital',
	'Telehealth - Provided Other than in Patient"s Home',
	'Telehealth - Provided in Patient"s Home'
	)
	*/
	and year(ORIG_SERVICE_DATE) > 2021
)
--select * from eligible_encounters
, unique_rows as
(
select
	a.csn as pat_enc_csn_id
	, a.mrn
	, e.patient_status
	, e.cpt_code
	, e.[procedure]
	, a.visit_prov_id
	, a.department_name
	, a.activity_name
	, a.subdivision_name
	, a.provider_specialty
	, year(a.appt_time) as calendar_year
	, a.appt_made_date
	, a.appt_time
	, appt_date
	, d.month_start
	, cast(calendar_year as nvarchar) + '_Q' 
		+ cast(d.calendar_quarter_of_year as nvarchar) as year_quarter
	, a.days_to_appt
	, case 
		when days_to_appt > 10
		then 'over 10' 
		else '10 or less'
		end as appt_tat
	, row_number() over( partition by e.pat_enc_csn_id 
		order by patient_status desc) as rn
from rpt.appointment as a
inner join eligible_encounters as e
	on a.csn = e.pat_enc_csn_id
inner join src.dim_date as d
	on a.appt_date = d.[date]
)
select
	pat_enc_csn_id
	, mrn
	, patient_status
	, cpt_code
	, [procedure]
	, department_name
	, activity_name
	, subdivision_name
	, provider_specialty
	, month_start
	, calendar_year
	, year_quarter
	, appt_made_date
	, appt_time
	, appt_date
	, format(month_start, 'yyyy_MM') as year_month
	, days_to_appt
	, appt_tat
from unique_rows
where rn = 1

GO



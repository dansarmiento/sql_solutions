USE [ucr_health]
GO

/****** Object:  View [rpt].[vw_agg_smartset_activations]    Script Date: 2/10/2025 11:05:28 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


/*
Created By: [daniel.sarmiento@medsch.ucr.edu]
Creation Date: [January 19, 2024]
Last Altered By: []
Alteration Date: []
Description of report: [Smartset aggregation report staging to join with provider dimension and date dimension]
*/
create view [rpt].[vw_agg_smartset_activations] as 

with src as
(
SELECT 
	[patient_name]
	,[mrn]
	,[pat_enc_csn_id]
	,[auth_md_name]
	,[auth_prov_id]
	,[subdivision_name]
	,[fiscal_period]
	,[fiscal_year]
	,[calendar_month_and_year]
	,[smartset_id]
	,[protocol_name]
	,[order_date]
	,[order_description]
	,[order_type]
FROM [ucr_health].[stg].[vw_fact_smartset_activations]
),
pvt as
(
select [patient_name],[mrn],[pat_enc_csn_id],[auth_md_name],[auth_prov_id],[protocol_name],[subdivision_name],[fiscal_period],[fiscal_year],[calendar_month_and_year]
	,[Back Office Lab],[Imaging],[Lab],[Medications],[Micro],[Procedures],[Referral]
from 
	(
	SELECT  
		[patient_name],[mrn],[pat_enc_csn_id],[auth_md_name],[auth_prov_id],[protocol_name],[subdivision_name],[fiscal_period],[fiscal_year],[calendar_month_and_year],[order_description],[order_type]
	FROM src
	) as pvt_src
	pivot
		(
		count([order_description]) 
		for [order_type] in ([Back Office Lab],[Imaging],[Lab],[Medications],[Micro],[Procedures],[Referral])
		) as pivottable
)
select * from pvt
GO



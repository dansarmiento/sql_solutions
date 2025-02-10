USE [ucr_health]
GO

/****** Object:  StoredProcedure [dbo].[merge_appointments_rolling]    Script Date: 2/10/2025 10:56:10 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





/*
Created By: [daniel.sarmiento@medsch.ucr.edu]
Creation Date: [December 5, 2023]
Last Altered By: [daniel.sarmiento@medsch.ucr.edu]
Alteration Date: [December 14, 2023]
Description of report: [Stored procedure to merge daily file drops from UCSD into UCR Health database]
Update description: [Changed merge log to write to universal ETL table]
*/

CREATE procedure [dbo].[merge_appointments_rolling] as 

begin

/* create temp table */
IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects WHERE id = OBJECT_ID(N'tempdb..#appointments'))
DROP TABLE #appointments
CREATE TABLE #appointments
(
	[mrn] [nvarchar](200) NULL,
	[csn] [nvarchar](200) NOT NULL,
	[prc_name] [nvarchar](200) NULL,
	[department_name] [nvarchar](200) NULL,
	[dept_specialty_name] [nvarchar](200) NULL,
	[center_name] [nvarchar](200) NULL,
	[enc_type_c] [nvarchar](200) NULL,
	[enc_type_title] [nvarchar](200) NULL,
	[age] [nvarchar](200) NULL,
	[pcp_prov_id] [nvarchar](200) NULL,
	[pcp_name] [nvarchar](200) NULL,
	[pcp_type] [nvarchar](200) NULL,
	[visit_provider_name] [nvarchar](200) NULL,
	[visit_provider_type] [nvarchar](200) NULL,
	[visit_prov_id] [nvarchar](200) NULL,
	[bp_systolic] [nvarchar](200) NULL,
	[bp_diastolic] [nvarchar](200) NULL,
	[temperature] [nvarchar](200) NULL,
	[pulse] [nvarchar](200) NULL,
	[weight] [nvarchar](200) NULL,
	[height] [nvarchar](200) NULL,
	[respirations] [nvarchar](200) NULL,
	[bmi] [nvarchar](200) NULL,
	[appt_status] [nvarchar](200) NULL,
	[days_to_appt] [nvarchar](200) NULL,
	[appt_time] [datetime2](7) NULL,
	[appt_length] [nvarchar](200) NULL,
	[appt_made_date] [date] NULL,
	[cancel_reason] [nvarchar](200) NULL,
	[benefit_plan_id] [nvarchar](200) NULL,
	[benefit_plan_name] [nvarchar](200) NULL,
	[payor_id] [nvarchar](200) NULL,
	[payor_name] [nvarchar](200) NULL,
	[fin_class_c] [nvarchar](200) NULL,
	[fin_class_name] [nvarchar](200) NULL,
	[appt_conf_inst] [datetime2](7) NULL,
	[cancel_reason_cmt] [nvarchar](500) NULL,
	[appt_canc_dttm] [datetime2](7) NULL,
	[data_import_date] [date] NULL,
	[signin_dttm] [datetime2](7) NULL,
	[begin_checkin_dttm] [datetime2](7) NULL,
	[checkin_dttm] [datetime2](7) NULL,
	[roomed_dttm] [datetime2](7) NULL,
	[nurse_leave_dttm] [datetime2](7) NULL,
	[phys_enter_dttm] [datetime2](7) NULL,
	[visit_end_dttm] [datetime2](7) NULL,
	[checkout_dttm] [datetime2](7) NULL,
	[appt_conf_dttm] [datetime2](7) NULL,
	[appt_arrival_dttm] [datetime2](7) NULL,
	[enc_closed_yn] [nvarchar](200) NULL,
	[enc_close_date] [datetime2](7) NULL,
)

/* insert data from vault file */
BULK INSERT #appointments
FROM '\\vault\powerbiflatfiles$\production\APPOINTMENTS_ROLLING_3.txt'
WITH
(
FIRSTROW = 2,
FIELDTERMINATOR = '|',
ROWTERMINATOR = '\n'
)
;

/* create temp table for logging */	-- attempting variable table instead as temp table giving trouble
/*
IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects WHERE id = OBJECT_ID(N'tempdb..#merge_log'))
DROP TABLE #merge_log
CREATE TABLE #merge_log
(
	[action]			[nvarchar](200) NULL,
	[src_csn]			[nvarchar](200) NULL,
	[src_import_date]	[date] NULL,
)
;
*/

DECLARE @merge_log TABLE 
(
	[action]			[nvarchar](200) NULL,
	[src_csn]			[nvarchar](200) NULL,
	[src_import_date]	[date] NULL,
	[file_name]			[nvarchar](200) NULL
)
;
--select * from #appointments

/* merge join process */
MERGE [ucr_health].[src].[appointment] as tgt
USING #appointments as src
ON src.csn = tgt.csn

WHEN MATCHED 
	THEN UPDATE SET
	tgt.[mrn]					=   src.[mrn]
    ,tgt.[csn]					=   src.[csn]
    ,tgt.[prc_name]				=   src.[prc_name]
    ,tgt.[department_name]		=   src.[department_name]
    ,tgt.[dept_specialty_name]  =   src.[dept_specialty_name]
    ,tgt.[center_name]			=   src.[center_name]
    ,tgt.[enc_type_c]			=   src.[enc_type_c]
    ,tgt.[enc_type_title]		=   src.[enc_type_title]
    ,tgt.[age]					=   src.[age]
    ,tgt.[pcp_prov_id]			=   src.[pcp_prov_id]
    ,tgt.[pcp_name]				=   src.[pcp_name]
    ,tgt.[pcp_type]				=   src.[pcp_type]
    ,tgt.[visit_provider_name]  =   src.[visit_provider_name]
    ,tgt.[visit_provider_type]  =   src.[visit_provider_type]
    ,tgt.[visit_prov_id]		=   src.[visit_prov_id]
    ,tgt.[bp_systolic]			=   src.[bp_systolic]
    ,tgt.[bp_diastolic]			=   src.[bp_diastolic]
    ,tgt.[temperature]			=   src.[temperature]
    ,tgt.[pulse]				=   src.[pulse]
    ,tgt.[weight]				=   src.[weight]
    ,tgt.[height]				=   src.[height]
    ,tgt.[respirations]			=   src.[respirations]
    ,tgt.[bmi]					=   src.[bmi]
    ,tgt.[appt_status]			=   src.[appt_status]
    ,tgt.[days_to_appt]			=   src.[days_to_appt]
    ,tgt.[appt_time]			=   src.[appt_time]
    ,tgt.[appt_length]			=   src.[appt_length]
    ,tgt.[appt_made_date]		=   src.[appt_made_date]
    ,tgt.[cancel_reason]		=   src.[cancel_reason]
    ,tgt.[benefit_plan_id]		=   src.[benefit_plan_id]
    ,tgt.[benefit_plan_name]	=   src.[benefit_plan_name]
    ,tgt.[payor_id]				=   src.[payor_id]
    ,tgt.[payor_name]			=   src.[payor_name]
    ,tgt.[fin_class_c]			=   src.[fin_class_c]
    ,tgt.[fin_class_name]		=   src.[fin_class_name]
    ,tgt.[appt_conf_inst]		=   src.[appt_conf_inst]
    ,tgt.[cancel_reason_cmt]	=   src.[cancel_reason_cmt]
    ,tgt.[appt_canc_dttm]		=   src.[appt_canc_dttm]
    ,tgt.[data_import_date]		=   src.[data_import_date]
    ,tgt.[signin_dttm]			=   src.[signin_dttm]
    ,tgt.[begin_checkin_dttm]   =   src.[begin_checkin_dttm]
    ,tgt.[checkin_dttm]			=   src.[checkin_dttm]
    ,tgt.[roomed_dttm]			=   src.[roomed_dttm]
    ,tgt.[nurse_leave_dttm]		=   src.[nurse_leave_dttm]
    ,tgt.[phys_enter_dttm]		=   src.[phys_enter_dttm]
    ,tgt.[visit_end_dttm]		=   src.[visit_end_dttm]
    ,tgt.[checkout_dttm]		=   src.[checkout_dttm]
    ,tgt.[appt_conf_dttm]		=   src.[appt_conf_dttm]
    ,tgt.[appt_arrival_dttm]	=   src.[appt_arrival_dttm]
    ,tgt.[enc_closed_yn]		=   src.[enc_closed_yn]
    ,tgt.[enc_close_date]		=   src.[enc_close_date]

WHEN NOT MATCHED 
	THEN INSERT 
			([mrn]
			,[csn]
			,[prc_name]
			,[department_name]
			,[dept_specialty_name]
			,[center_name]
			,[enc_type_c]
			,[enc_type_title]
			,[age]
			,[pcp_prov_id]
			,[pcp_name]
			,[pcp_type]
			,[visit_provider_name]
			,[visit_provider_type]
			,[visit_prov_id]
			,[bp_systolic]
			,[bp_diastolic]
			,[temperature]
			,[pulse]
			,[weight]
			,[height]
			,[respirations]
			,[bmi]
			,[appt_status]
			,[days_to_appt]
			,[appt_time]
			,[appt_length]
			,[appt_made_date]
			,[cancel_reason]
			,[benefit_plan_id]
			,[benefit_plan_name]
			,[payor_id]
			,[payor_name]
			,[fin_class_c]
			,[fin_class_name]
			,[appt_conf_inst]
			,[cancel_reason_cmt]
			,[appt_canc_dttm]
			,[data_import_date]
			,[signin_dttm]
			,[begin_checkin_dttm]
			,[checkin_dttm]
			,[roomed_dttm]
			,[nurse_leave_dttm]
			,[phys_enter_dttm]
			,[visit_end_dttm]
			,[checkout_dttm]
			,[appt_conf_dttm]
			,[appt_arrival_dttm]
			,[enc_closed_yn]
			,[enc_close_date])
	VALUES (src.[mrn]
			,src.[csn]
			,src.[prc_name]
			,src.[department_name]
			,src.[dept_specialty_name]
			,src.[center_name]
			,src.[enc_type_c]
			,src.[enc_type_title]
			,src.[age]
			,src.[pcp_prov_id]
			,src.[pcp_name]
			,src.[pcp_type]
			,src.[visit_provider_name]
			,src.[visit_provider_type]
			,src.[visit_prov_id]
			,src.[bp_systolic]
			,src.[bp_diastolic]
			,src.[temperature]
			,src.[pulse]
			,src.[weight]
			,src.[height]
			,src.[respirations]
			,src.[bmi]
			,src.[appt_status]
			,src.[days_to_appt]
			,src.[appt_time]
			,src.[appt_length]
			,src.[appt_made_date]
			,src.[cancel_reason]
			,src.[benefit_plan_id]
			,src.[benefit_plan_name]
			,src.[payor_id]
			,src.[payor_name]
			,src.[fin_class_c]
			,src.[fin_class_name]
			,src.[appt_conf_inst]
			,src.[cancel_reason_cmt]
			,src.[appt_canc_dttm]
			,src.[data_import_date]
			,src.[signin_dttm]
			,src.[begin_checkin_dttm]
			,src.[checkin_dttm]
			,src.[roomed_dttm]
			,src.[nurse_leave_dttm]
			,src.[phys_enter_dttm]
			,src.[visit_end_dttm]
			,src.[checkout_dttm]
			,src.[appt_conf_dttm]
			,src.[appt_arrival_dttm]
			,src.[enc_closed_yn]
			,src.[enc_close_date])




/* merge logging */


OUTPUT $action as [action], 
INSERTED.csn AS src_csn,
getdate() as src_import_date,
'APPOINTMENT_ROLLING_3.txt' as [file_name]
INTO @merge_log
;

with src as
(
select [action], src_import_date, count(src_csn) as row_count, [file_name]
from @merge_log
group by [action], src_import_date, [action], [file_name]
),
pvt as 
(
select
src_import_date 
, [insert] 
, [update] 
, [file_name] 
from 
	(
	select [action], src_import_date, row_count, [file_name] from src 
	) as pvtsrc
	pivot
		(
		sum(row_count)
		for [action] in ([insert],[update]) 
		) as pivottable 
)
insert into dbo.etl_file_log
select 
	src_import_date as import_date
	, [insert] as rows_inserted
	, [update] as rows_updated
	, [FILE_NAME]
from pvt
	

update [ucr_health].[src].[appointment]
set [benefit_plan_id] = replace([benefit_plan_id], '.0','')
;

update [ucr_health].[src].[appointment]
set [payor_id] = replace([payor_id], '.0','')
;

update [ucr_health].[src].[appointment] 
set [mrn] = null where [mrn] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [csn] = null where [csn] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [prc_name] = null where [prc_name] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [department_name] = null where [department_name] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [dept_specialty_name] = null where [dept_specialty_name] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [center_name] = null where [center_name] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [enc_type_c] = null where [enc_type_c] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [enc_type_title] = null where [enc_type_title] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [age] = null where [age] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [pcp_prov_id] = null where [pcp_prov_id] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [pcp_name] = null where [pcp_name] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [pcp_type] = null where [pcp_type] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [visit_provider_name] = null where [visit_provider_name] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [visit_provider_type] = null where [visit_provider_type] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [visit_prov_id] = null where [visit_prov_id] = 'NULL'  
;
update [ucr_health].[src].[appointment] 
set [bp_systolic] = null where [bp_systolic] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [bp_diastolic] = null where [bp_diastolic] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [temperature] = null where [temperature] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [pulse] = null where [pulse] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [weight] = null where [weight] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [height] = null where [height] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [respirations] = null where [respirations] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [bmi] = null where [bmi] = 'NULL' 
;
update [ucr_health].[src].[appointment] 
set [appt_status] = null where [appt_status] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [days_to_appt] = null where [days_to_appt] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [appt_length] = null where [appt_length] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [cancel_reason] = null where [cancel_reason] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [benefit_plan_id] = null where [benefit_plan_id] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [benefit_plan_name] = null where [benefit_plan_name] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [payor_id] = null where [payor_id] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [payor_name] = null where [payor_name] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [fin_class_c] = null where [fin_class_c] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [fin_class_name] = null where [fin_class_name] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [cancel_reason_cmt] = null where [cancel_reason_cmt] = 'NULL'
;
update [ucr_health].[src].[appointment] 
set [enc_closed_yn] = null where [enc_closed_yn] = 'NULL'
;

end;
GO



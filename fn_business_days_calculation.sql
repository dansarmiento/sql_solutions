USE [ucr_health]
GO

/****** Object:  UserDefinedFunction [dbo].[busDaysBtwn]    Script Date: 2/10/2025 10:57:52 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/*
Created By: [daniel.sarmiento@medsch.ucr.edu]
Creation Date: [January 20, 2024]
Last Altered By: []
Alteration Date: []
Description of report: [Function to calculate business days taking three parameters:
- start date
- end date
- inc for including last day
- exc for excluding last day ]
Function found on medium.com 
https://medium.com/@nick.pulvino/sql-biz-days-btwn-c4cef47237ce#:~:text=Once%20we%20have%20the%20calendar,off%20of%20our%20calendar%20table
*/

CREATE function [dbo].[busDaysBtwn] 
(	
	@startDate datetime
	,@endDate datetime
	,@pickExcInc nvarchar(3)
)
	returns int
	
begin

declare @defaultPick nvarchar(3) --null handling
set @defaultPick ='inc'
declare @daysBtwn int
set @daysBtwn = 
(
	select
		sum(cast(dd.isbusinessday as int))
		from src.dim_date as dd
	where
	dd.[date] >= @startDate
	and (
	(isnull(@pickExcInc, @defaultPick) ='exc'
	and dd.[date] < @endDate)
	or
	(isnull(@pickExcInc, @defaultPick) ='inc'
	and dd.[date] <= @endDate) 
		)
)
return(@daysBtwn)
end
GO



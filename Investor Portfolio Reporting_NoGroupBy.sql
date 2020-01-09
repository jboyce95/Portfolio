Declare @MEPeriod datetime

set @MEPeriod = DATEADD(MONTH, -1, GETDATE())


--LEFT(convert(varchar, DATEADD(MONTH, -1, GETDATE()), 112),6)

select
ME.LoanId,

Case
	when MM.LoanFolder='Active Modification' then 'Active Mod'
	else LS.LoanSubStatusDesc
end as CurrentLoanStatus,


ME.Currentprincipalbalanceamt as 'Current UPB',

CASE
	WHEN ME.InvestorId = 'D01' THEN 'Mass Mutual'
	WHEN ME.InvestorId = 'D80' THEN 'NexBank'
	ELSE 'UNKNOWN'
END AS 'Investor Name',

ME.InvestorId as 'Investor',

case
	when MM.LoanFolder='Active Modification' then MM.CurrentMilestone 
	else 'NULL'
end as 'Modification Current Milestone',

LS.LoanSubStatusDesc as 'Delinquency Status',

case 
	when MM.LoanFolder='Active Modification' and MM.Verify3rdTrialPaymentReceivedDt>'1/1/2018' then '3'
	when MM.LoanFolder='Active Modification' and MM.Verify2ndTrialPaymentReceivedDt>'1/1/2018' then '2'
	when MM.LoanFolder='Active Modification' and MM.VerifyFirstTrialPaymentReceivedDt>'1/1/2018' then '1'
	when MM.LoanFolder='Active Modification' and MM.CurrentMilestone in ('Trial Payment Tracking') then '0'
	ELSE 'NULL'
end as 'TrialPaymentsMade',

CASE
	WHEN LS.LoanSubStatusDesc in('3rd Party Foreclosure Sale','Paid Off In Full','Short Sale','Foreclosure Sale','Deed in Lieu') THEN 'FC Sale/SS/DIL/PIF'
	WHEN LS.LoanSubStatusDesc in('Liquidation Post Deed In Lieu Activity','Liquidation Post Foreclosure Sale Activity','Liquidation Post REO Sale Activity','Liquidation Post Short Sale Activity','Liquidation Post Unknown Sale Activity') THEN 'Liquidation'
	WHEN LS.LoanSubStatusDesc in('Current', 'Active Mod') THEN LS.LoanSubStatusDesc
	WHEN LS.LoanSubStatusDesc in('Foreclosure Active','Foreclosure In Redemption','Foreclosure On Hold - Bankruptcy','Foreclosure On Hold - Other') THEN 'FC'
	WHEN LS.LoanSubStatusDesc in('REO', 'REO Sale') THEN 'REO'
	WHEN LS.LoanSubStatusDesc in('30 Days Delinquent') THEN '30 DLQ'
	WHEN LS.LoanSubStatusDesc in('60 Days Delinquent','90 Days Delinquent','120+ Days Delinquent') THEN '60+ DLQ'
	ELSE 'UNKNOWN - ' + LS.LoanSubStatusDesc
END AS 'DLQ Status-Short',

CASE
	WHEN LS.LoanSubStatusDesc in('30 Days Delinquent','60 Days Delinquent','90 Days Delinquent','120+ Days Delinquent','Active Mod','Foreclosure Active','Foreclosure In Redemption','Foreclosure On Hold - Bankruptcy','Foreclosure On Hold - Other') THEN 'In Portfolio'
	WHEN LS.LoanSubStatusDesc in('3rd Party Foreclosure Sale','Paid Off In Full','Short Sale','Foreclosure Sale','Deed in Lieu') THEN 'Out of Portfolio'
	WHEN LS.LoanSubStatusDesc in('Current','Liquidation Post Deed In Lieu Activity','Liquidation Post Foreclosure Sale Activity','Liquidation Post REO Sale Activity','Liquidation Post Short Sale Activity','Liquidation Post Unknown Sale Activity','REO', 'REO Sale') THEN 'In-Pending Out of Portfolio'
	ELSE 'UNKNOWN NB Position - ' + LS.LoanSubStatusDesc
END AS 'NB Position Status',
(SELECT LEFT(convert(varchar, @MEPeriod, 112),6)) as MEPeriod_forReport



from SMD..Loan_Master_ME ME

left join SMD..Modification_Master_ME MM
on ME.LoanId=MM.LoanId and MM.MEPeriod=ME.MEPeriod

left join SMD..ref_LoanSubStatus LS
on LS.LoanSubStatusId=ME.LoanSubStatusId

where ME.InvestorId in ('D01','D80') and ME.MEperiod=(SELECT LEFT(convert(varchar, @MEPeriod, 112),6))
-->>>>>>>>CHANGE MEPERIOD-----^^^^^^^^^^^^^^^^^^<<<<<<<<<<<<< !!NO LONGER NEED TO INPUT MEPERIOD...IT'S AUTO-CALCULATED AS THE PREVIOUS MONTH
and ME.LoanStatusId not in ( 'O' , 'S', 'U') 
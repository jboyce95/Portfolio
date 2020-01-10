/*
UPDATES TO THE CODE:
	20200109: Updated Investor Status to incorporate 'Liquidation Unknown' substatus in the 'Out of 'Portfolio' bucket		
	20190821: Revised loan level to produce groupby report for in-out position
		Goal is to create a groupby summary as well as the loan level support
	20190701: Added auto-calculation of MEPeriod for month prior cutoff

SELECT 

BO.BuyoutMonth
,COUNT(BO.BuyoutUPB) as 'Loan Count'
,SUM(BO.BuyoutUPB) as 'Total Buyout UPB'
,SUM(BO.BuyoutPT * BO.BuyoutUPB) / SUM(BO.BuyoutUPB) AS 'WA PT Rate'

FROM spd.ServInvestment.PBOBuyout BO

--WHERE LoanID in ('')


WHERE BO.BuyoutMonth in('201901','201902','201903','201904')

GROUP BY BO.BuyoutMonth
ORDER BY BO.BuyoutMonth

*/

DECLARE @MEPeriod datetime
SET @MEPeriod = DATEADD(MONTH, -1, GETDATE());

--(SELECT LEFT(convert(varchar, DATEADD(MONTH, -1, GETDATE()), 112),6))

With rpt_temp as (


SELECT
ME.LoanId,

Case
when MM.LoanFolder='Active Modification' then 'Active Mod'
else LS.LoanSubStatusDesc
end as CurrentLoanStatus,


ME.Currentprincipalbalanceamt as 'Current UPB',
ME.InvestorId as 'Investor',

CASE
	WHEN MM.LoanFolder='Active Modification' THEN MM.CurrentMilestone 
	ELSE 'NULL'
END AS 'Modification Current Milestone',

LS.LoanSubStatusDesc as 'Delinquency Status',

CASE 
	WHEN MM.LoanFolder='Active Modification' and MM.Verify3rdTrialPaymentReceivedDt>'1/1/2018' THEN '3'
	WHEN MM.LoanFolder='Active Modification' and MM.Verify2ndTrialPaymentReceivedDt>'1/1/2018' THEN '2'
	WHEN MM.LoanFolder='Active Modification' and MM.VerifyFirstTrialPaymentReceivedDt>'1/1/2018' THEN '1'
	WHEN MM.LoanFolder='Active Modification' and MM.CurrentMilestone in ('Trial Payment Tracking') THEN '0'
	ELSE 'NULL'
END AS 'TrialPaymentsMade',

CASE
	WHEN LS.LoanSubStatusDesc in('3rd Party Foreclosure Sale','Paid Off In Full','Short Sale','Foreclosure Sale','Deed in Lieu') THEN 'FC Sale/SS/DIL/PIF'
	WHEN LS.LoanSubStatusDesc in('Liquidation Post Deed In Lieu Activity','Liquidation Post Foreclosure Sale Activity','Liquidation Post REO Sale Activity','Liquidation Post Short Sale Activity','Liquidation Post Unknown Sale Activity', 'Liquidation Unknown') THEN 'Liquidation'
	WHEN LS.LoanSubStatusDesc in('Current', 'Active Mod') THEN LS.LoanSubStatusDesc
	WHEN LS.LoanSubStatusDesc in('Foreclosure Active','Foreclosure In Redemption','Foreclosure On Hold - Bankruptcy','Foreclosure On Hold - Other') THEN 'FC'
	WHEN LS.LoanSubStatusDesc in('REO', 'REO Sale') THEN 'REO'
	WHEN LS.LoanSubStatusDesc in('30 Days Delinquent') THEN '30 DLQ'
	WHEN LS.LoanSubStatusDesc in('60 Days Delinquent','90 Days Delinquent','120+ Days Delinquent') THEN '60+ DLQ'
	ELSE 'UNKNOWN - ' + LS.LoanSubStatusDesc
END AS 'DLQ Status-Short',

CASE
	WHEN LS.LoanSubStatusDesc in('30 Days Delinquent','60 Days Delinquent','90 Days Delinquent','120+ Days Delinquent','Active Mod','Foreclosure Active','Foreclosure In Redemption','Foreclosure On Hold - Bankruptcy','Foreclosure On Hold - Other') THEN 'In Portfolio'
	WHEN LS.LoanSubStatusDesc in('3rd Party Foreclosure Sale','Paid Off In Full','Short Sale','Foreclosure Sale','Deed in Lieu', 'Liquidation Unknown') THEN 'Out of Portfolio'
	WHEN LS.LoanSubStatusDesc in('Current','Liquidation Post Deed In Lieu Activity','Liquidation Post Foreclosure Sale Activity','Liquidation Post REO Sale Activity','Liquidation Post Short Sale Activity','Liquidation Post Unknown Sale Activity','REO', 'REO Sale') THEN 'In-Pending Out of Portfolio'
	ELSE 'UNKNOWN Investor Position - ' + LS.LoanSubStatusDesc
END AS 'Investor Position Status',
(SELECT LEFT(convert(varchar, DATEADD(MONTH, -1, GETDATE()), 112),6)) as MEPeriod_forReport



from SMD..Loan_Master_ME ME

left join SMD..Modification_Master_ME MM
on ME.LoanId=MM.LoanId and MM.MEPeriod=ME.MEPeriod

left join SMD..ref_LoanSubStatus LS
on LS.LoanSubStatusId=ME.LoanSubStatusId

where ME.InvestorId in('D01','D80') and ME.MEperiod=(SELECT LEFT(convert(varchar, @MEPeriod, 112),6))
-->>>>>>>>CHANGE MEPERIOD-----^^^^^^^^^^^^^^^^^^<<<<<<<<<<<<< !!NO LONGER NEED TO INPUT MEPERIOD...IT'S AUTO-CALCULATED AS THE PREVIOUS MONTH
and ME.LoanStatusId not in ( 'O' , 'S', 'U') 
)

-----------------------------
--GROUPBY SECTION
-----------------------------

SELECT 

CASE
	WHEN rt.Investor = 'D01' THEN 'Mass Mutual'
	WHEN rt.Investor = 'D80' THEN 'NexBank'
	ELSE 'UNKNOWN'
END AS 'Investor Name'
,Investor as 'Investor Id'
,[Investor Position Status] as 'Investor Position Status'
,COUNT(LoanId) as 'Count of LoanId'
,REPLACE(CONVERT(VARCHAR,CONVERT(MONEY,SUM([Current UPB])),1), '.00','') AS 'Total UPB ($)' --convert(varchar(25), SUM([Current UPB]), 1) --select convert(varchar(25), @v, 1)
,@MEPeriod as 'MEPeriod for Report'
--,SUM(BO.BuyoutPT * BO.BuyoutUPB) / SUM(BO.BuyoutUPB) AS 'WA PT Rate'

FROM rpt_temp rt

GROUP BY [Investor], [Investor Position Status], MEPeriod_forReport
ORDER BY [Investor]
--WITH ROLLUP --This adds grand totals
--ORDER BY [Investor Position Status]


--need to add formatting to numbers

--UNION ALL

--SELECT * FROM rpt_temp
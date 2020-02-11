--WITH nb_capacity as (

Declare @Report_Cutoff datetime
Declare @Test_Final datetime
Declare @MEPeriod datetime
Declare @Cutoff_NPDD date --for eligibility cutoff first of the current month
Declare @Cutoff_1stofMonth date
Declare @Cutoff_1stofMonth_NxtMonth date
Declare @Cutoff_1stOfMonth_Prev2Months date
SET @Cutoff_1stofMonth_NxtMonth = (select dateadd(MONTH,1,(CONVERT(date,dateadd(d,-(day(getdate())) + 1,getdate()),112))))

SET @Report_Cutoff = DATEADD(MONTH, -1, GETDATE()) --convert(varchar, (DATEADD(MONTH, 1, GETDATE())), 112)
SET @Test_Final = DATEADD(MONTH, -1, GETDATE())
--SET @Test_Final = convert(varchar, DATEADD(MONTH, -1, GETDATE()), 112) --LEFT(convert(varchar, DATEADD(MONTH, -1, GETDATE()), 112),6)
SET @Cutoff_1stofMonth = (select CONVERT(date,dateadd(d,-(day(getdate())) + 1,getdate()),112))
SET @Cutoff_NPDD = DATEADD(MONTH, -3, @Cutoff_1stofMonth)
SET @Cutoff_1stOfMonth_Prev2Months = (select dateadd(MONTH,-2,(CONVERT(date,dateadd(d,-(day(getdate())) + 1,getdate()),112))))

SELECT @Report_Cutoff as SHOW_CUTOFF

SELECT convert(varchar, @Report_Cutoff, 112) as Report_Cutoff_FORMAT_112

SELECT LEFT(convert(varchar, @Report_Cutoff, 112),6) as Report_Cutoff_FORMAT_112_LEFT

SELECT LEFT(convert(varchar, @Report_Cutoff, 112),6) as MEPeriod_Sample

SELECT @Test_Final as SHOW_TEST_FINAL

SELECT @Cutoff_1stofMonth as Cutoff_1stofMonth

SELECT @Cutoff_NPDD as Cutoff_NPDD
SELECT @Cutoff_1stofMonth_NxtMonth as Cutoff_1stofMonth_NxtMonth
SELECT @Cutoff_1stOfMonth_Prev2Months as Cutoff_1stOfMonth_Prev2Months

--SELECT LEFT(@Report_Cutoff,6) AS LEFT_STRING_1

--SELECT DATEADD(MONTH, 1, GETDATE()) AS 'DATEADD_MONTH'

--select convert(varchar, DATEADD(MONTH, -1, GETDATE()),112) AS DATEADD_MONTH_112_DateFormat

--SELECT LEFT('20190407',6) AS LEFT_STRING


--create temp table with Report_Cutoff


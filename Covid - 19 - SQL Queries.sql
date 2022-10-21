CREATE DATABASE COVID_19;
USE COVID_19;

--Checking all the files added as tables:
SELECT * FROM covid_stats_state_wise_totals;
SELECT * FROM covid_stats_state_wise_delta;
SELECT * FROM covid_stats_state_wise_delta7;
SELECT * FROM covid_stats_state_wise_meta;
SELECT * FROM covid_stats_state_wise_timeseries;
SELECT * FROM covid_district_total;

-- Question 1: State-wise testing ratio
-- Testing ratio = total number of tests / total population

SELECT * FROM covid_stats_state_wise_totals;
SELECT * FROM covid_stats_state_wise_meta;

SELECT  c.state as State,c.state_code,A.tested as Statewise_total_number_of_tests, B.population as Statewise_population, ROUND((CAST(A.tested as FLOAT)/B.population),2) as Testing_ratio 
FROM covid_stats_state_wise_totals as A
INNER JOIN covid_stats_state_wise_meta as B ON
A.state_code = B.state_code
inner join state_name as C on C.state_code=b.state_code
ORDER BY Testing_ratio DESC;

-- Question 2: Compare delta7 confirmed cases with respect to vaccination
-- Insight one : Vaccination 1 v/s confirmed cases

SELECT c.state,c.state_code,A.confirmed,A.vaccinated1,A.vaccinated2,B.population,Round((CAST(A.confirmed as FLOAT)/B.population *1000000),0) as 'confirmed_population(Per million)',
Round((CAST(A.vaccinated1 as FLOAT)/B.population *1000000),0) as 'vaccinated_1_population(Per million)',
Round((CAST(A.vaccinated2 as FLOAT)/B.population *1000000),0) as 'vaccinated_2_population(Per million)'
FROM covid_stats_state_wise_delta7 as A
INNER JOIN covid_stats_state_wise_meta as B
ON A.state_code = B.state_code
inner join state_name as c on c.state_code=b.state_code
ORDER BY [vaccinated_1_population(Per million)] desc

-- Most confirmed cases KPI [Statewise total data]
SELECT b.state,b.state_code,confirmed,deceased,recovered,tested,vaccinated1,vaccinated2 
FROM covid_stats_state_wise_totals as a
inner join state_name as b on a.state_code=b.state_code
ORDER BY confirmed DESC;

-- Most confirmed cases KPI [Statewise Delta - one day analysis]
SELECT b.state,b.state_code,confirmed,deceased,recovered,tested,vaccinated1,vaccinated2 FROM covid_stats_state_wise_delta as a
inner join state_name as b on a.state_code=b.state_code
ORDER BY confirmed DESC;

-- Most confirmed cases KPI [Statewise Delta7 - one day analysis]
SELECT b.state,b.state_code,confirmed,deceased,recovered,tested,vaccinated1,vaccinated2 FROM covid_stats_state_wise_delta7 a
inner join state_name as b on a.state_code=b.state_code
ORDER BY confirmed DESC;

--Question 3: Categorize total number of confirmed, deceased, v1, v2,   
--cases in a state by Months and come up with that one month 
--which was worst for India in terms of the number of cases

SELECT * FROM covid_stats_state_wise_timeseries

--FOR TOTAL MONTHLY CONFIRMED CASES

SELECT DATEPART(YEAR,dates) as Year ,DATEPART(month,dates) as Month,SUM(Daily_confirmed) as confirmed_cases FROM
(SELECT *,(confirmed-data) as Daily_confirmed FROM 
(SELECT *, lag(confirmed,1) OVER(PARTITION BY state_code ORDER BY dates) AS data
FROM covid_stats_state_wise_timeseries) a ) b
GROUP BY DATEPART(YEAR,dates),DATEPART(month,dates)
ORDER BY confirmed_cases DESC

--FOR TOTAL MONTHLY DECEASED CASES

SELECT 
DATEPART(YEAR,dates) as Year ,
DATEPART(month,dates) as Month,
SUM(Daily_deceased) as deceased_cases FROM
(SELECT *,(deceased-data) as Daily_deceased FROM 
(SELECT *, lag(deceased,1) OVER(PARTITION BY STATE_code ORDER BY dates) AS data
FROM covid_stats_state_wise_timeseries) a ) b
GROUP BY DATEPART(YEAR,dates),DATEPART(month,dates)
ORDER BY deceased_cases DESC

-- WE CAN SEE THAT IN YEAR 2021 MONTH 5 - THERE HAVE BEEN BOTH THE HIGHEST CONFIRMED AND THE DECEASED CASES.

-- Question - 4: Weekly evolution of the number of confirmed cases, recovered cases, deaths, tests

SELECT a.state_code, b.state,
DATEPART(YEAR,dates) as [Year],
DATEPART(week,dates) as [week number],
SUM(confirmed) as weekly_confirmed,
SUM(recovered) as weekly_recovered,
SUM(tested) as weekly_tested,
SUM(deceased) as weekly_deaths 
FROM covid_stats_state_wise_timeseries as a
inner join state_name as b on a.state_code=b.state_code
GROUP BY DATEPART(YEAR,dates), DATEPART(week,dates),b.state,a.state_code
ORDER BY Year,[week number] 

--Question - 5 : Categorise every district testing ratio wise

CREATE VIEW district_category_tr_data AS
SELECT * FROM(
SELECT *,
CASE WHEN Testing_Ratio <=0.1 THEN 'CATEGORY A'
     WHEN Testing_Ratio>0.1 and Testing_Ratio <=0.3 THEN 'CATEGORY B'
	 WHEN Testing_Ratio>0.3 and Testing_Ratio <=0.5 THEN 'CATEGORY C'
	 WHEN Testing_Ratio>0.5 and Testing_Ratio <=0.75 THEN 'CATEGORY D'
	 WHEN Testing_Ratio>0.75 THEN 'CATEGORY E'
END AS Category_tr
FROM
(SELECT A.state_code,c.state, A.district, A.tested,A.confirmed,A.deceased,A.recovered,A.vaccinated1,A.vaccinated2,
B.population,(A.tested/B.population) As Testing_Ratio 
FROM covid_district_total as A
INNER JOIN covid_district_meta as B ON A.district = B.district and a.state_code=b.state_code
inner join state_name as c on c.state_code=b.state_code
)a) AS X;

select * from covid_district_meta
select * from district_category_tr_data;


select * from covid_district_total;

 --- category_tr wise deaths 
 select a.category_tr,sum(a.deceased) from district_category_tr_data as a
 group by a.Category_tr;

 select * from state_name
--Category wise count of districts

SELECT Category_tr,COUNT(Category_tr) as Category_wise_count FROM district_category_tr_data
GROUP BY Category_tr
ORDER BY Category_wise_count DESC;

--View for daily data from time series (removing cumulative sum)
CREATE VIEW daily_statewise_data AS 
SELECT * FROM(
SELECT *,
(confirmed-confirmed_lag) as daily_confirmed,
(recovered-recovered_lag) as daily_recovered,
(tested-tested_lag) as daily_tested,
(deceased-deceased_lag) as daily_deceased,
(vaccinated1-vaccinated1_lag) as daily_vaccinated1,
(vaccinated2-vaccinated2_lag) as daily_vaccinated2
FROM
(SELECT t.state,s.*,
lag(confirmed,1) OVER(PARTITION BY STATE ORDER BY dates) AS confirmed_lag,
lag(recovered,1) OVER(PARTITION BY STATE ORDER BY dates) AS recovered_lag,
lag(tested,1) OVER(PARTITION BY STATE ORDER BY dates) AS tested_lag,
lag(deceased,1) OVER(PARTITION BY STATE ORDER BY dates) AS deceased_lag,
lag(vaccinated1,1) OVER(PARTITION BY STATE ORDER BY dates) AS vaccinated1_lag,
lag(vaccinated2,1) OVER(PARTITION BY STATE ORDER BY dates) AS vaccinated2_lag
FROM covid_stats_state_wise_timeseries as s 
inner join state_name as t on s.state_code=t.state_code) a) AS Y

select * from daily_statewise_data

--Final weekly data for states
--Weekly evolution of number of confirmed cases, recovered cases, deaths, tests. 
--For instance, your dashboard should be able to compare Week 3 of May with Week 2 of August

SELECT DATEPART(Year,dates) as Year,DATEPART(week,dates) as Week_number,state_code,state,
SUM(daily_confirmed) AS weekly_confirmed,
SUM(daily_deceased)AS weekly_deceased,
SUM(daily_recovered)AS weekly_recovered,
SUM(daily_tested)AS weekly_tested,
SUM(daily_vaccinated1)AS weekly_vaccinated1,
SUM(daily_vaccinated2)AS weekly_vaccinated2
FROM daily_statewise_data
GROUP BY state_code, state,DATEPART(Year,dates),DATEPART(week,dates)
ORDER BY state,Year,Week_number DESC

--TOP 10 districts data 

SELECT TOP 10* FROM covid_district_total
ORDER BY deceased Desc

--Categorize the total number of confirmed cases in a state by Months and 
--come up with that one month which was worst for India in terms of number of cases 

SELECT DATEPART(YEAR,dates) as Year,DATEPART(month,dates)as Months,SUM(daily_confirmed) as monthly_confirmed,SUM(daily_recovered) as monthly_recovered
FROM daily_statewise_data
WHERE state != 'total'
GROUP BY DATEPART(YEAR,dates),DATEPART(month,dates)
HAVING DATEPART(YEAR,dates) = '2021'
ORDER BY Months

SELECT DATEPART(YEAR,dates) as Year,DATEPART(month,dates)as Months,SUM(daily_confirmed) as monthly_confirmed,SUM(daily_recovered) as monthly_recovered
FROM daily_statewise_data
WHERE state != 'total'
GROUP BY DATEPART(YEAR,dates),DATEPART(month,dates)
HAVING DATEPART(YEAR,dates) = '2020'
ORDER BY Months

--Weekly evolution of number of confirmed cases, recovered cases, deaths, tests. 
--For instance, your dashboard should be able to compare Week 3 of May with Week 2 of August

CREATE VIEW weekly_evolution_data AS
SELECT * FROM (
SELECT 
DATEPART(YEAR,dates) as Year,
DATEPART(MONTH,dates) as Month,
DATEPART(week,dates) as week_number,
SUM(daily_tested) as Tested,
SUM(daily_confirmed) as Confirmed,
SUM(daily_recovered) as Recovered,
SUM(daily_deceased) as Deaths
FROM daily_statewise_data
GROUP BY 
DATEPART(YEAR,dates),
DATEPART(MONTH,dates),
DATEPART(week,dates)) as C

SELECT * ,DENSE_RANK() OVER (PARTITION BY YEAR,MONTH ORDER BY week_number) AS week_number_monthwise
FROM weekly_evolution_data
ORDER BY Deaths DESC

-- TOP states wrt. confirmed,deceased

SELECT TOP 1 state,confirmed,
population,ROUND((CAST(confirmed as FLOAT)/population)*100000,0) AS [Confirmed cases per lakh] 
FROM covid_stats_state_wise_totals AS A
INNER JOIN state_name AS B ON A.state_code=B.state_code
INNER JOIN covid_stats_state_wise_meta AS C ON A.state_code=C.state_code
WHERE state != 'total'
ORDER BY [Confirmed cases per lakh] DESC

SELECT TOP 1 state,confirmed,
population,ROUND((CAST(confirmed as FLOAT)/population)*100000,0) AS [Confirmed cases per lakh] 
FROM covid_stats_state_wise_totals AS A
INNER JOIN state_name AS B ON A.state_code=B.state_code
INNER JOIN covid_stats_state_wise_meta AS C ON A.state_code=C.state_code
WHERE state != 'total'
ORDER BY confirmed DESC

-- Deceased same

SELECT TOP 1 state,deceased,
population,ROUND((CAST(deceased as FLOAT)/population)*100000,0) AS [Deceased cases per lakh] 
FROM covid_stats_state_wise_totals AS A
INNER JOIN state_name AS B ON A.state_code=B.state_code
INNER JOIN covid_stats_state_wise_meta AS C ON A.state_code=C.state_code
WHERE state != 'total'
ORDER BY [Deceased cases per lakh] DESC

SELECT TOP 1 state,deceased,
population,ROUND((CAST(deceased as FLOAT)/population)*100000,0) AS [Deceased cases per lakh] 
FROM covid_stats_state_wise_totals AS A
INNER JOIN state_name AS B ON A.state_code=B.state_code
INNER JOIN covid_stats_state_wise_meta AS C ON A.state_code=C.state_code
WHERE state != 'total'
ORDER BY deceased DESC

-- Delta 7 confirmed to vaccination comparison

SELECT TOP 5 state,confirmed FROM covid_stats_state_wise_delta7 AS A
INNER JOIN state_name as B 
ON A.state_code = B.state_code
WHERE state != 'total'
ORDER BY confirmed DESC

SELECT TOP 5 state,vaccinated1 FROM covid_stats_state_wise_delta7 AS A
INNER JOIN state_name as B 
ON A.state_code = B.state_code
WHERE state != 'total'
ORDER BY vaccinated1 DESC

SELECT TOP 5 state,vaccinated2 FROM covid_stats_state_wise_delta7 AS A
INNER JOIN state_name as B 
ON A.state_code = B.state_code
WHERE state != 'total'
ORDER BY vaccinated2 DESC

-- TOP districts wrt. confirmed,deceased

SELECT TOP 1 A.district,state,confirmed,
population,ROUND((CAST(confirmed as FLOAT)/population)*100000,0) AS [Confirmed cases per lakh] 
FROM covid_district_total AS A
INNER JOIN state_name AS B ON A.state_code=B.state_code
INNER JOIN covid_district_meta AS C ON A.district=C.district AND A.state_code = C.state_code
WHERE state != 'total'
ORDER BY [Confirmed cases per lakh] DESC

SELECT TOP 1 A.district,state,confirmed,
population,ROUND((CAST(confirmed as FLOAT)/population)*100000,0) AS [Confirmed cases per lakh] 
FROM covid_district_total AS A
INNER JOIN state_name AS B ON A.state_code=B.state_code
INNER JOIN covid_district_meta AS C ON A.district=C.district AND A.state_code = C.state_code
WHERE state != 'total' and state != 'Delhi' 
-- Not considering Delhi because district-wise bifurcation is not availabe in the data provided.
ORDER BY confirmed DESC

-- Deceased same

SELECT TOP 1 A.district,state,deceased,
population,ROUND((CAST(deceased as FLOAT)/population)*100000,0) AS [Deceased cases per lakh] 
FROM covid_district_total AS A
INNER JOIN state_name AS B ON A.state_code=B.state_code
INNER JOIN covid_district_meta AS C ON A.district=C.district AND A.state_code = C.state_code
WHERE state != 'total'
ORDER BY [Deceased cases per lakh] DESC

SELECT TOP 1 A.district,state,deceased,
population,ROUND((CAST(deceased as FLOAT)/population)*100000,0) AS [Deceased cases per lakh] 
FROM covid_district_total AS A
INNER JOIN state_name AS B ON A.state_code=B.state_code
INNER JOIN covid_district_meta AS C ON A.district=C.district AND A.state_code = C.state_code
WHERE state != 'total' and state != 'Delhi' 
-- Not considering Delhi because district-wise bifurcation is not availabe in the data provided.
ORDER BY deceased DESC

-- TOP 10 Stateswise data for tested, confirmed, recovered, deceased, vaccinated 1 and vaccinated 2

SELECT TOP 10 state,vaccinated2 FROM covid_stats_state_wise_totals AS A
INNER JOIN state_name AS B 
ON A.state_code = B.state_code
WHERE state != 'total'
ORDER BY vaccinated2 DESC
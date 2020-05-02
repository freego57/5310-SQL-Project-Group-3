#Initialization
library(dplyr)
library('RPostgreSQL')
require('RPostgreSQL')

#Connect to DB
drv <- dbDriver('PostgreSQL')
con <- dbConnect(drv, dbname = 'homecredit',
                 host = 'localhost', port = 5432,
                 user = 'postgres', password = '123')
stmt=
'
CREATE TABLE financial_status(
cust_id	varchar(6) PRIMARY KEY,
contract_type	varchar(100),
flag_own_car	varchar(1),
flag_own_reality	varchar(1),
own_car_age	integer,
amt_income_total	float,
amt_credit	float, 
amt_annuity	float,
amt_goods_price	float);

CREATE TABLE time_of_application(
cust_id varchar(6) PRIMARY KEY,
weekday_start	character(20),
hour_start	integer,
FOREIGN KEY (cust_id) REFERENCES financial_status(cust_id));

CREATE TABLE personal_info(
cust_id varchar(6) PRIMARY KEY, 
gender	varchar(1),
education_type	character(50),
age	integer,
FOREIGN KEY (cust_id) REFERENCES financial_status(cust_id));

CREATE TABLE family_info(
cust_id varchar(6) PRIMARY KEY,
cnt_children integer, 
family_status	character(50),
cnt_family_members	integer,
FOREIGN KEY (cust_id) REFERENCES financial_status(cust_id));

CREATE TABLE occupation_info(
cust_id varchar(6) PRIMARY KEY,
name_income_type character(20),
days_employed	integer, 
occupation_type	character(50),
organization_type	character(50),
FOREIGN KEY (cust_id) REFERENCES financial_status(cust_id));

CREATE TABLE information_change(
cust_id varchar(6) PRIMARY KEY,
days_id_publish character(10),
days_last_phone_change character(10),
FOREIGN KEY (cust_id) REFERENCES financial_status(cust_id));

CREATE TABLE region(
cust_id varchar(6) PRIMARY KEY,
region_rating_client integer, 
region_rating_client_city integer,
FOREIGN KEY (cust_id) REFERENCES financial_status(cust_id));

CREATE TABLE external_score(
cust_id varchar(6) PRIMARY KEY,
ext_source_1 numeric, 
ext_source_2 numeric, 
ext_source_3 numeric,
FOREIGN KEY (cust_id) REFERENCES financial_status(cust_id));

CREATE TABLE bureau_personal_info(
cust_id varchar(6),
b_id varchar(10) PRIMARY KEY,
FOREIGN KEY (cust_id) REFERENCES financial_status(cust_id));

CREATE TABLE bureau_credit_info(
b_id varchar(10) PRIMARY KEY,
credit_active varchar, 
credit_currency varchar,
cnt_credit_prolong int,
credit_type varchar,
FOREIGN KEY (b_id) REFERENCES bureau_personal_info(b_id));

CREATE TABLE bureau_balance(
b_id varchar(10),
months_balance numeric, 
status varchar,
PRIMARY KEY (b_id,months_balance),
FOREIGN KEY (b_id) REFERENCES bureau_personal_info(b_id));

CREATE TABLE prev_personal_info(
prev_id varchar(10) PRIMARY KEY,
cust_id varchar(6),
FOREIGN KEY (cust_id) REFERENCES financial_status(cust_id));

CREATE TABLE prev_amount(
prev_id varchar(10) PRIMARY KEY,
amt_annuity	numeric, 
amt_application numeric,
amt_credit numeric,
amt_down_payment numeric,
prev_amt_goods_price numeric,
FOREIGN KEY (prev_id) REFERENCES prev_personal_info(prev_id));

CREATE TABLE credit_balance(
prev_id varchar(10) PRIMARY KEY,
credit_card_balance numeric,
FOREIGN KEY (prev_id) REFERENCES prev_personal_info(prev_id));

CREATE TABLE prev_finance(
prev_id varchar(10) PRIMARY KEY,
weekday_appr_process varchar,
hour_appr_process int,
flag_last_appl_contract varchar,
nflag_last_appl_in_day int,
rate_down_payment float,
rate_interest_primary float,
rate_interest_privileged float,
FOREIGN KEY (prev_id) REFERENCES prev_personal_info(prev_id));'
df<-dbGetQuery(con,stmt)

#read data
setwd('/Users/liyidan/desktop/SQL/group project/home-credit-default-risk')
test <- read.csv('application_test_updated.csv',nrows=10000)
bureau <- read.csv("bureau.csv",nrows=10000)
bureau_balance <- read.csv("bureau_balance.csv",nrows=10000)
credit_card_balance<-read.csv('credit_card_balance.csv',nrows=10000)
previous_application <- read.csv("previous_application.csv",nrows=10000)


#Separate Data into Identified Tables
#occupation_info table
occupation_info <- test[,c('cust_id', 'name_income_type','days_employed', 'occupation_type', 'organization_type')]

#information_change table
information_change <- test[,c('cust_id', 'days_id_publish', 'days_last_phone_change')]

#region table
region<- test[,c('cust_id', 'region_rating_client', 'region_rating_client_city')]

#external_score table
external_score <- test[,c('cust_id', 'ext_source_1', 'ext_source_2', 'ext_source_3')]

#financial_status table
financial_status=test[,c('cust_id','contract_type','flag_own_car','flag_own_reality','own_car_age','amt_income_total','amt_credit','amt_annuity','amt_goods_price')]

#personal_info table
personal_info=test[,c('cust_id','gender','education_type','days_birth')]
#alter days_birth to age
personal_info$age=-round(personal_info$days_birth/365,0)
personal_info$days_birth=NULL

#time_of_application table
time_of_application=test[,c('cust_id','weekday_start','hour_start')]

#family_info table
family_info=test[,c('cust_id','cnt_children','family_status','cnt_family_members')]

#bureau_personal_info table

bureau_personal_info <- bureau[,c('SK_ID_CURR','SK_ID_BUREAU')]
names(bureau_personal_info) <- c("cust_id","b_id")
bureau_personal_info=bureau_personal_info %>% semi_join(financial_status, by = "cust_id")
#bureau_credit_info table
bureau_credit_info <- bureau[,c('SK_ID_BUREAU','CREDIT_ACTIVE','CREDIT_CURRENCY','CNT_CREDIT_PROLONG','CREDIT_TYPE')]
names(bureau_credit_info) <- c('b_id',"credit_active","credit_currency","cnt_credit_prolong","credit_type")
bureau_credit_info=bureau_credit_info %>% semi_join(bureau_personal_info, by = "b_id")
#bureau_balance table
names(bureau_balance) <- c("b_id","months_balance","status")
bureau_balance=bureau_balance %>% semi_join(bureau_personal_info, by = "b_id")
#prev_personal_info table
prev_personal_info<- previous_application[,c('SK_ID_PREV','SK_ID_CURR')]
names(prev_personal_info)<- c('prev_id','cust_id')
prev_personal_info=prev_personal_info %>% semi_join(financial_status, by = "cust_id")

#prev_amount table
prev_amount<- previous_application[,c('SK_ID_PREV','AMT_ANNUITY','AMT_APPLICATION','AMT_CREDIT','AMT_DOWN_PAYMENT','AMT_GOODS_PRICE')]
names(prev_amount) <- c("prev_id","amt_annuity","amt_application","amt_credit","amt_down_payment","prev_amt_goods_price")
prev_amount=prev_amount %>% semi_join(prev_personal_info, by = "prev_id")
#credit_balance table
credit_balance<-credit_card_balance[,c('SK_ID_PREV','AMT_BALANCE')]
names(credit_balance)<-c('prev_id','credit_card_balance')
credit_balance<-credit_balance[!duplicated(credit_balance$prev_id), ]
credit_balance<-credit_balance %>% semi_join(prev_personal_info, by = "prev_id")

#prev_finance table
prev_finance<-previous_application[,c('SK_ID_PREV','WEEKDAY_APPR_PROCESS_START','HOUR_APPR_PROCESS_START','FLAG_LAST_APPL_PER_CONTRACT','NFLAG_LAST_APPL_IN_DAY','RATE_DOWN_PAYMENT','RATE_INTEREST_PRIMARY','RATE_INTEREST_PRIVILEGED')]
names(prev_finance)<-c('prev_id','weekday_appr_process','hour_appr_process','flag_last_appl_contract','nflag_last_appl_in_day','rate_down_payment','rate_interest_primary','rate_interest_privileged')
prev_finance<-prev_finance %>% semi_join(prev_personal_info, by = "prev_id")

#Insert data into SQL
#Data inserted into financial_status table
dbWriteTable(con, "financial_status", financial_status,row.names=FALSE,append=TRUE)
#Data inserted into occupation_info table
dbWriteTable(con, "occupation_info", occupation_info,row.names=FALSE,append=TRUE)
#Data inserted into information_change table
dbWriteTable(con,"information_change", information_change,row.names=FALSE,append=TRUE)
#Data inserted into region table
dbWriteTable(con,"region", region,row.names=FALSE,append=TRUE)
#Data inserted into personal_info table
dbWriteTable(con, "personal_info",personal_info,row.names=FALSE,append=TRUE)
#Data inserted into time_of_application table
dbWriteTable(con, "time_of_application",time_of_application,row.names=FALSE,append=TRUE)
#Data inserted into family_info table
dbWriteTable(con, "family_info",family_info,row.names=FALSE,append=TRUE)
#Data inserted into external_score table
dbWriteTable(con,"external_score", external_score,row.names=FALSE,append=TRUE)
#Data inserted into bureau_personal_info table
dbWriteTable(con,"bureau_personal_info",bureau_personal_info,row.names=FALSE,append=TRUE)
#Data inserted into bureau_credit_info table
dbWriteTable(con,"bureau_credit_info",bureau_credit_info,row.names=FALSE,append=TRUE)
#Data inserted into bureau_balance table
dbWriteTable(con,"bureau_balance",bureau_balance,row.names=FALSE,append=TRUE)
#Data inserted into prev_personal_info table
dbWriteTable(con, "prev_personal_info", prev_personal_info,row.names=FALSE,append=TRUE)
#Data inserted into prev_amount table
dbWriteTable(con,"prev_amount",prev_amount,row.names=FALSE,append=TRUE)
#Data inserted into credit_balance table
dbWriteTable(con, "credit_balance", credit_balance,row.names=FALSE,append=TRUE)
#Data inserted into prev_finance table
dbWriteTable(con, "prev_finance", prev_finance,row.names=FALSE,append=TRUE)

#10 procedures
#1.average portrait of customers
stmt1.1="SELECT AVG(age) AS avg_age,AVG(cnt_children) AS avg_children,
AVG(cnt_family_members) AS avg_family_member,AVG(own_car_age) AS avg_car_age,AVG(amt_credit) AS avg_amt_credit,
AVG(amt_annuity) AS avg_annuity
FROM
(SELECT * FROM
(personal_info AS pi
NATURAL JOIN family_info AS fi
) AS a1
NATURAL JOIN financial_status as a2) AS data;
"
df1.1<-dbGetQuery(con,stmt1.1)
print(df1.1)
stmt1.2="SELECT gender, COUNT(gender) from personal_info
GROUP BY gender
ORDER BY COUNT(gender) DESC;"
df1.2<-dbGetQuery(con,stmt1.2)
print(df1.2)
stmt1.3="SELECT family_status, COUNT(family_status) FROM family_info
GROUP BY family_status
ORDER BY COUNT(family_status) DESC;"
df1.3<-dbGetQuery(con,stmt1.3)
print(df1.3)

#2.Does credit amount differ significantly among different regions (we have 1,2,3 for region score)?
stmt2='SELECT AVG(amt_credit) AS amt_credit, region_rating_client
FROM region AS r
NATURAL JOIN financial_status AS fs
WHERE r.cust_id=fs.cust_id
GROUP BY region_rating_client
ORDER BY amt_credit DESC;'
df2<-dbGetQuery(con,stmt2)
print(df2)

#3.When is the most popular weekday and hour for application?
#the most popular weekday
stmt3.1='SELECT weekday_start, COUNT(weekday_start) AS count_weekday_start
FROM time_of_application
GROUP BY weekday_start
ORDER BY  count_weekday_start DESC;'
df3.1<-dbGetQuery(con,stmt3.1)
print(df3.1)
#the most popular hour
stmt3.2='SELECT hour_start, COUNT(hour_start) AS count_hour_start
FROM time_of_application
GROUP BY hour_start
ORDER BY  count_hour_start DESC;'
df3.2<-dbGetQuery(con,stmt3.2)
print(df3.2)
#the most popular weekday, hour combination
stmt3.3='SELECT weekday_start, hour_start, COUNT(*) AS count_time_start
FROM time_of_application
GROUP BY  weekday_start,hour_start
ORDER BY count_time_start DESC
LIMIT 5;'
df3.3<-dbGetQuery(con,stmt3.3)
print(df3.3)

#4.Do customers with different credit status from the credit bureau get different amounts of credit?
stmt4="
SELECT credit_active, avg(amt_credit) AS avg_amt_credit
FROM (SELECT * FROM bureau_credit_info AS bci
      LEFT JOIN bureau_personal_info AS bpi
      ON bci.b_id=bpi.b_id) AS d1
LEFT JOIN financial_status AS fs
ON d1.cust_id=fs.cust_id
GROUP BY credit_active
ORDER BY avg_amt_credit;"
df4<-dbGetQuery(con,stmt4)
print(df4)

#5. Are customers using home credit to buy something more expensive than their previous purchase?
stmt5="SELECT AVG(amt_goods_price) AS present_goods_price,
AVG(prev_amt_goods_price) AS previous_goods_price
FROM ((SELECT * FROM prev_amount AS pa
     LEFT JOIN prev_personal_info AS ppi
      ON pa.prev_id=ppi.prev_id) AS d1
      LEFT JOIN financial_status AS fs
      ON d1.cust_id=fs.cust_id) AS data;"
df5<-dbGetQuery(con,stmt5)
print(df5)

#6.Do income types associate with a customer’s amount of income?
stmt6='SELECT name_income_type,AVG(amt_income_total) AS avg_amount_income_total
FROM occupation_info AS oi
LEFT JOIN financial_status AS fs
ON oi.cust_id=fs.cust_id
GROUP BY name_income_type
ORDER BY avg_amount_income_total DESC;'
df6<-dbGetQuery(con,stmt6)
print(df6)

#7.Do clients who own a house/car have more family members?
#house
stmt7.1='SELECT flag_own_reality,avg(cnt_family_members) AS avg_family_members
FROM family_info as fi
LEFT JOIN financial_status AS fs
ON fi.cust_id=fs.cust_id
GROUP BY flag_own_reality;'
df7.1<-dbGetQuery(con,stmt7.1)
print(df7.1)
#car
stmt7.2='SELECT flag_own_car,avg(cnt_family_members) AS avg_family_members
FROM family_info as fi
LEFT JOIN financial_status AS fs
ON fi.cust_id=fs.cust_id
GROUP BY flag_own_car;'
df7.2<-dbGetQuery(con,stmt7.2)
print(df7.2)

#8.Do clients with different education levels have different amounts of credit? 
#And if so, what education level has the highest amount of credit?
stmt8='SELECT AVG(amt_credit) AS avg_amt_credit, education_type
FROM financial_status AS fs
LEFT JOIN personal_info AS pi
ON fs.cust_id=pi.cust_id
GROUP BY education_type
ORDER BY avg_amt_credit DESC;'
df8<-dbGetQuery(con,stmt8)
print(df8)

#9.Since clients from region 2 and 3, 
#with lower education tend to have lower credit, how much can they get on average?
stmt9="SELECT AVG(amt_credit) AS avg_credit FROM
((SELECT * FROM
financial_status AS fs
NATURAL JOIN personal_info AS pi
WHERE fs.cust_id=pi.cust_id) AS d1
LEFT JOIN region AS r
ON r.cust_id=d1.cust_id) AS data
WHERE education_type='Lower secondary'
AND region_rating_client BETWEEN 2 AND 3;"
df9<-dbGetQuery(con,stmt9)
print(df9)

#10.Do clients with higher external scores get more credit?
stmt10='SELECT AVG(amt_credit) AS avg_credit
FROM financial_status AS fs
LEFT JOIN external_score AS es
ON fs.cust_id=es.cust_id
WHERE ext_source_1>(SELECT AVG(ext_source_1) FROM external_score) 
AND ext_source_2>(SELECT AVG(ext_source_2) FROM external_score) 
AND ext_source_3>(SELECT AVG(ext_source_3) FROM external_score);'
df10<-dbGetQuery(con,stmt10)
print(df10)

#Close the connection----
dbDisconnect(con)
dbUnloadDriver(drv)
/********************************************
#Autohr: Jinwei Wang
#Date:4/20/2020




SQL Server: 
1. Use import/export wizard 
2. Bulk insert table1 from 'c:\data.csv' with (FORMAT='csv', keepnulls,  firstrow= 2, FIELDTERMINATOR = ',')
********************************************/ 


-- URL is case sensitive
drop stage if exists jwang; 

create or replace stage jwang url='s3://sftp-files-transfer/jwang'
  credentials=(aws_key_id='' aws_secret_key=''); 
  
ls @jwang; 

select * from sandbox.information_schema.tables limit 10 

create or replace file format jw_csv_format
type = csv field_delimiter = ',' skip_header = 1 null_if = ('NULL', 'null') empty_field_as_null = true    FILE_EXTENSION ='csv'; 

-- to load multiple file in a batch: pattern='.*[.]csv';

drop table if exists jwtemp1;
create table jwtemp1 (seq int, scf int, zip int, pene float); 

copy into jwtemp1 from '@jwang/scf_penetration_data.csv' FILE_FORMAT =jw_csv_format; 
copy into jwtemp1 from @jwang pattern='.*[.]txt.gz' FILE_FORMAT =jw_csv_format; 

grant all privileges on jwtemp1 to role public; 
grant all privileges on TDU to role public; 


select * from sandbox.information_schema.load_history limit 10 ; 
select * from sandbox.information_schema.file_formats limit 10 ; 
select * from sandbox.information_schema.stages limit 10 ; 

/** COPY INTO location statements separate table data into a set of output files to take advantage of parallel operations. 
max_file_size default is 16000000 (16MB), the maximize for AWS S3 is 5G
*/ 

copy  into @jwang/ftp_dpm_446341_1551967080.txt.gz 
from mytable file_format=(type=csv compression='gzip')  FIELD_OPTIONALLY_ENCLOSED_BY '"' single=true null_if='null' EMPTY_FIELD_AS_NULL = TRUE  
max_file_size=4900000000





/*********** String Function *******************/ 

select trim('*-&*A&BC-&-*-','-&*')
-- other similar function: rtrim & ltrim
select charindex('n', '324nfakfn', 5) 
select position ('n', '324nfakfn') or select position ('n' in '324nfakfn')
select substr ('abcdef', 3, 100);

select substr( 'jwang@hotmail.com',  position('@', 'jwang@hotmail.com' ) +1 ) 

-- Other string function--
-- replace/lower/upper/like/ilike/length/






/*********** Date Function **********************/ 

select  ('2019-10-21 12:35:36.362'::timestamp), 
        year('2019-10-21 12:35:36.362'::timestamp) as year,  
        quarter('2019-10-21 12:35:36.362'::timestamp) as quarter, 
        month('2019-10-21 12:35:36.362'::timestamp) as month,           
        day('2019-10-21 12:35:36.362'::timestamp) as day, 
        hour('2019-10-21 12:35:36.362'::timestamp) as hour, 
        weekofyear('2019-10-21 12:35:36.362'::timestamp) as weekofyear, 
        weekiso('2019-10-21 12:35:36.362'::timestamp) as weekiso, 
        dateadd(day, 10, '2019-10-21 12:35:36.362'::timestamp ), 
        dateadd(hour, -10, current_timestamp ), 
        datediff(day, '2019-10-21', '2019-11-01'); 
        
select  column1 date_1, column2 date_2,
        datediff(year, column1, column2)  as diff_years,
        datediff(month, column1, column2) as  diff_months,
        datediff(day, column1, column2)  as diff_days
from    values
        ('2015-12-30', '2015-12-31'),
        ('2015-12-31', '2016-01-01'),
        ('2016-01-01', '2017-12-31'),
        ('2016-08-23', '2016-09-07');        
       
select  ('2019-12-31 12:35:36.362'::timestamp)+ interval '2 year, 3 month, 4 day, 5 hours, 6 minute'; 

select date_part(epoch_second, ) 
select dateadd(ms, 1554514368123, '1970-01-01'), dateadd(s,  1554514368, '1970-01-01') ; 



/************************************************/

-- window function
-- a window is a group of related rows.

create or replace sequence seq1; 
drop table jwtemp2; 
create temp table jwtemp2 (id int default seq1.nextval , score int); 
insert into jwtemp2 values ( default, 100),  ( default, 200),  ( default, 300),  ( default, 300),  ( default, 400),  ( default, 500); 

select *, rank () over(order by score ) from jwtemp2; 
select *, dense_rank () over(order by score ) from jwtemp2; 
select *, row_number() over (order by score, id ) from jwtemp2; 

select decile, min(score), max(score), count(*) 
from ( 
select agility_hh_key, score,  ntile (10) over(order by score desc) as decile  from c2g_total_us_s2  ) a 
group by decile order by 1; 

select percentile_cont(0.25) within group (order by score desc )  from c2g_total_us_s2  union all 
select percentile_disc(0.25) within group (order by score desc )  from c2g_total_us_s2 ; 





/************** Deduplication **************************************/ 


select *, random(100), random()  from tdu order by zip, tdu limit 100 ; 

create temp table jwtemp3 as 
select zip, tdu from tdu group by zip, tdu ; 

select count(distinct zip), count(*) from jwtemp3; 



--- choose the zip with more than one tdu;

create temp table jwtemp4 as 
select  a.* 
from    jwtemp3 a inner join (select zip from jwtemp3 group by zip having count (distinct tdu) >1 )b 
on a.zip=b.zip; 




-- dedup by the sorting tdu name alphabetical; 

create temp table jwtemp5 as 
select  * 
From    ( 
        select *, row_number() over (partition by zip order by tdu) as rid 
        from    jwtemp4 
        ) a
where rid=1; 

select count(distinct zip), count(*) from jwtemp5 

/*********************************************************************/ 

set zip='21044'; 
SET table1='jwtemp1';
set scf='scf'; 
show variables; 
select $zip; 
select $table1; 

drop table if exists  identifier ($table1); 
create table identifier ($table1) (seq int, scf int, zip int, pene float); 

copy into identifier ($table1) from '@jwang/scf_penetration_data.csv' FILE_FORMAT =jw_csv_format; 
select * from identifier($table1)   where zip=$zip; 
select * from table($table1) limit 10 ; 

show schemas;
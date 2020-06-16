---Create Epsilon Data for Scoring---
DROP TABLE IF EXISTS SANDBOX.DS.OTA_MODEL2_ALLCENTERS1; 
CREATE temp TABLE  SANDBOX.DS.OTA_MODEL2_ALLCENTERS1 AS 
select AGILITY_ADDR_KEY,AGILITY_HH_KEY,AGILITY_INDIVIDUAL_KEY1,PERSON_SEQ_NO1,GIVEN_NAME1,SURNAME,CONTRACTED_ADDRESS,
POST_OFFICE_NAME,STATE,B.ZIP,B.ZIP4,CARRIER_ROUTE,A.center,A.Center_Zip_code,A.Distance,gender1,invest_insur_investments_all,
propensity_to_buy_luxury_truck_full_size,mt_price_motivated_personal_care_product_users,mt_monitored_home_security_system_owners,
num_sourc_verify_hh,mt_likely_cruiser,mt_online_magazinenewspaper_subscribers,mt_retired_but_still_working,mt_active_on_fb,mt_auto_loan_purchr,
sports_fitness_exercise_all,hobbies_home_improv_diy_all,payment_method_cc,num_one_shot_orders,mt_likely_planned_givers,mt_online_broker_user
from (select ZIP,CENTER,CENTER_ZIP_CODE,DISTANCE from OTA_ALLUS_DISTANCE) A
join C2G_LAKE_DEV.VDS_FTP.EPSILON_TSP_LATEST B 
on A.ZIP = B.ZIP
where  B.FILE_CODE = 'P' and A.distance < 60 AND (TRY_CAST(B.ADV_NUM_ADULTS as INTEGER)  > 1 OR B.INVEST_INSUR_INVESTMENTS_ALL='Y');

select count(*) from OTA_MODEL2_ALLCENTERS1;



DROP TABLE IF EXISTS OTA_MODEL2_ALLCENTERS_UNIQUE_ID;
create table OTA_MODEL2_ALLCENTERS_UNIQUE_ID AS 
select *, 2200000000+ROW_NUMBER() OVER(ORDER BY CENTER DESC) AS UNIQUE_ID 
from SANDBOX.DS.OTA_MODEL2_ALLCENTERS1;




---National model score---


DROP TABLE IF EXISTS SANDBOX.DS.JW_OTA_TEMP_NATIONAL; 
CREATE TEMP TABLE SANDBOX.DS.JW_OTA_TEMP_NATIONAL AS
SELECT  *,
CASE WHEN (gender1 IS NULL OR gender1 = 2) THEN 1 ELSE 0 END AS GENDER11,
CASE WHEN invest_insur_investments_all IS NULL THEN 0 ELSE 1 END AS invest_insur_investments_all1,
SQRT(CASE WHEN (propensity_to_buy_luxury_truck_full_size IS NULL OR propensity_to_buy_luxury_truck_full_size <=0 ) 
     THEN 1 ELSE propensity_to_buy_luxury_truck_full_size END) AS propensity_to_buy_luxury_truck_full_size1,
CASE WHEN mt_price_motivated_personal_care_product_users IS NULL THEN 0
     WHEN mt_price_motivated_personal_care_product_users < 2 THEN 2 
     else mt_price_motivated_personal_care_product_users end as mt_price_motivated_personal_care_product_users1,
(CASE WHEN distance IS NULL THEN 0 
     WHEN distance > 57 THEN 57
     ELSE distance
     END) AS DISTANCE1,
SQRT(CASE WHEN mt_monitored_home_security_system_owners IS NULL THEN 0
          WHEN mt_monitored_home_security_system_owners > 94 THEN 94
          WHEN mt_monitored_home_security_system_owners <= 0 THEN 1
          ELSE mt_monitored_home_security_system_owners
          END) AS mt_monitored_home_security_system_owners1,
POWER((CASE WHEN num_sourc_verify_hh IS NULL THEN 0
           WHEN num_sourc_verify_hh < 2 THEN 2 
           WHEN num_sourc_verify_hh > 26 THEN 26
           ELSE num_sourc_verify_hh
           END),2) AS num_sourc_verify_hh1,
(CASE WHEN mt_likely_cruiser IS NULL THEN 0
     WHEN mt_likely_cruiser > 96 THEN 96 
     WHEN mt_likely_cruiser < 1 THEN 1
     ELSE mt_likely_cruiser 
     END) AS mt_likely_cruiser1,
(CASE WHEN mt_online_magazinenewspaper_subscribers IS NULL THEN 0
     WHEN mt_online_magazinenewspaper_subscribers > 96 THEN 96
     WHEN mt_online_magazinenewspaper_subscribers < 1 THEN 1
     ELSE mt_online_magazinenewspaper_subscribers
     END) AS mt_online_magazinenewspaper_subscribers1,
POWER((CASE WHEN mt_retired_but_still_working IS NULL THEN 0
     ELSE mt_retired_but_still_working
     END),2) AS mt_retired_but_still_working1
FROM OTA_MODEL2_ALLCENTERS_UNIQUE_ID;



DROP TABLE IF EXISTS SANDBOX.DS.OTA_MODEL2CENTERS_SCORE_20200225; 
CREATE temp TABLE  SANDBOX.DS.OTA_MODEL2CENTERS_SCORE_20200225 AS 
SELECT *,  EXP(NATIONAL_LOGIT)/(1+EXP(NATIONAL_LOGIT)) AS NATIONAL_SCORE 
FROM ( 
SELECT  *,  -3.728 +(-0.6282 * GENDER11) +
 0.8334 * invest_insur_investments_all1 +
 0.007593 *  mt_active_on_fb +
 (-0.00002445 * mt_retired_but_still_working1) +
 0.001053 *  num_sourc_verify_hh1 +
 (-0.02264 * distance1) +
 (-0.008458 * mt_likely_cruiser1) +
 (-0.001984 * mt_online_magazinenewspaper_subscribers1) +
 0.01097 *  mt_price_motivated_personal_care_product_users1 +
 (-0.1706 * mt_monitored_home_security_system_owners1) +
 0.1998*  propensity_to_buy_luxury_truck_full_size1
 AS NATIONAL_LOGIT 
FROM    SANDBOX.DS.JW_OTA_TEMP_NATIONAL);




SELECT * FROM OTA_MODEL2CENTERS_SCORE_20200225 LIMIT 100;


DROP TABLE IF EXISTS SANDBOX.DS.OTA_MODEL2CENTERS_SCORE_DECILE_20200225;
CREATE temp TABLE  SANDBOX.DS.OTA_MODEL2CENTERS_SCORE_DECILE_20200225 AS 
select *, case 
     when NATIONAL_SCORE >= 0.160192471385323 then 1
     when NATIONAL_SCORE >= 0.0970910295649512	and NATIONAL_SCORE < 0.160192471385323 then 2
     when NATIONAL_SCORE >= 0.06643137 and	NATIONAL_SCORE < 0.0970910295649512 then 3
     when NATIONAL_SCORE >= 0.04740023 and	NATIONAL_SCORE < 0.06643137 then 4
     when NATIONAL_SCORE >= 0.03438043 and	NATIONAL_SCORE < 0.04740023 then 5
     when NATIONAL_SCORE >= 0.02508624 and	NATIONAL_SCORE < 0.03438043 then 6
     when NATIONAL_SCORE >= 0.0179616 and	NATIONAL_SCORE < 0.02508624 then 7
     when NATIONAL_SCORE >= 0.01211027 and	NATIONAL_SCORE < 0.0179616 then 8
     when NATIONAL_SCORE >= 0.00730428465672837 and	NATIONAL_SCORE < 0.01211027 then 9
     when NATIONAL_SCORE < 0.00730428465672837 then 10
end as NATIONAL_decile
from SANDBOX.DS.OTA_MODEL2CENTERS_SCORE_20200225;



select center,NATIONAL_decile,count(*)
from OTA_MODEL2CENTERS_SCORE_DECILE_20200225
group by center,NATIONAL_decile
order by center, NATIONAL_decile;

select NATIONAL_decile,count(*)
from OTA_MODEL2CENTERS_SCORE_DECILE_20200225
group by NATIONAL_decile
order by NATIONAL_decile;



DROP TABLE IF EXISTS OTA_MODEL2_TESTING_NATIONAL;
CREATE TEMP TABLE OTA_MODEL2_TESTING_NATIONAL AS
select A.*,B.NATIONAL_SCORE
from OTA_MODEL2_ALLCENTERS_UNIQUE_ID A
JOIN (SELECT UNIQUE_ID,NATIONAL_SCORE,NATIONAL_decile FROM OTA_MODEL2CENTERS_SCORE_DECILE_20200225) B
ON A.UNIQUE_ID = B.UNIQUE_ID;





---Upward Model---



DROP TABLE IF EXISTS SANDBOX.DS.JW_OTA_TEMP_UPWARD; 
CREATE TEMP TABLE SANDBOX.DS.JW_OTA_TEMP_UPWARD AS
SELECT  *,
CASE WHEN invest_insur_investments_all IS NULL THEN 0 ELSE 1 END AS invest_insur_investments_all1,
CASE WHEN payment_method_cc IS NULL THEN 0 ELSE 1 END AS payment_method_cc1,
(CASE WHEN distance IS NULL THEN 0 
     WHEN distance >57 THEN 57
     ELSE distance
     END) AS DISTANCE1,
CASE WHEN mt_monitored_home_security_system_owners IS NULL THEN 0
          WHEN mt_monitored_home_security_system_owners > 96 THEN 96
          WHEN mt_monitored_home_security_system_owners < 1 THEN 1
          ELSE mt_monitored_home_security_system_owners
          END AS mt_monitored_home_security_system_owners1,
(CASE WHEN mt_price_motivated_personal_care_product_users IS NULL THEN 0
     WHEN mt_price_motivated_personal_care_product_users < 2 THEN 2 
     ELSE mt_price_motivated_personal_care_product_users
     END) AS mt_price_motivated_personal_care_product_users1,
CASE WHEN CAST(num_one_shot_orders AS INTEGER) IS NULL THEN 0 else CAST(num_one_shot_orders AS INTEGER) end as num_one_shot_orders1,
CASE WHEN num_one_shot_orders1 = 0 THEN 1
     WHEN num_one_shot_orders1 > 88.3799999999992 THEN 88.3799999999992 ELSE num_one_shot_orders1 end as num_one_shot_orders11,
CASE WHEN num_one_shot_orders11 BETWEEN 0 AND 88.3799999999992 THEN 1/num_one_shot_orders11
     END AS num_one_shot_orders111,
(CASE WHEN mt_auto_loan_purchr IS NULL THEN 0
     WHEN mt_auto_loan_purchr < 2 THEN 2 
     ELSE mt_auto_loan_purchr
     END) AS mt_auto_loan_purchr1, 
SQRT(CASE WHEN (propensity_to_buy_luxury_truck_full_size IS NULL OR propensity_to_buy_luxury_truck_full_size <=0 ) 
     THEN 1 ELSE propensity_to_buy_luxury_truck_full_size END) AS propensity_to_buy_luxury_truck_full_size1,
(CASE WHEN num_sourc_verify_hh IS NULL THEN 0
           WHEN num_sourc_verify_hh < 2 THEN 2 
           WHEN num_sourc_verify_hh > 27 THEN 27
           ELSE num_sourc_verify_hh
           END) AS num_sourc_verify_hh1,
(CASE WHEN mt_likely_planned_givers IS NULL THEN 0 
     WHEN mt_likely_planned_givers > 97 THEN 97 
     ELSE mt_likely_planned_givers
     END) AS mt_likely_planned_givers1,
POWER((CASE WHEN mt_active_on_fb IS NULL THEN 0
     WHEN mt_active_on_fb < 3.53 THEN 3.53
     ELSE mt_active_on_fb
     END),2) AS mt_active_on_fb1,
POWER((CASE WHEN mt_likely_cruiser IS NULL THEN 0
     WHEN mt_likely_cruiser > 97 THEN 97
     ELSE mt_likely_cruiser
     END),2) AS mt_likely_cruiser1
FROM OTA_MODEL2_TESTING_NATIONAL;




DROP TABLE IF EXISTS SANDBOX.DS.OTA_MODEL2CENTERS_SCORE_20200225; 
CREATE temp TABLE  SANDBOX.DS.OTA_MODEL2CENTERS_SCORE_20200225 AS 
SELECT *,  EXP(UPWARD_LOGIT)/(1+EXP(UPWARD_LOGIT)) AS UPWARD_SCORE 
FROM ( 
SELECT  *,  -3.613 + -0.01871 * mt_monitored_home_security_system_owners1 + 
- 0.0269 * distance1 + 
0.8196 * invest_insur_investments_all1 +
 0.01125 * mt_auto_loan_purchr1 +
 0.1193 * propensity_to_buy_luxury_truck_full_size1 +
 0.009352 * mt_price_motivated_personal_care_product_users1 +
 0.04802 * num_sourc_verify_hh1 +
-1.03 * num_one_shot_orders111 +
-0.0000563 * mt_likely_cruiser1 +
0.00003681 * mt_active_on_fb1 +
-0.003914 * mt_likely_planned_givers1 +
 0.1956 * payment_method_cc1
 AS UPWARD_LOGIT 
FROM    SANDBOX.DS.JW_OTA_TEMP_UPWARD);




SELECT * FROM OTA_MODEL2CENTERS_SCORE_20200225 LIMIT 100;


DROP TABLE IF EXISTS SANDBOX.DS.OTA_MODEL2CENTERS_SCORE_DECILE_20200225;
CREATE temp TABLE  SANDBOX.DS.OTA_MODEL2CENTERS_SCORE_DECILE_20200225 AS 
select *, case 
     when UPWARD_SCORE >= 0.158273956604178 then 1
     when UPWARD_SCORE >= 0.0942912230607925	and UPWARD_SCORE < 0.158273956604178 then 2
     when UPWARD_SCORE >= 0.0633593 and	UPWARD_SCORE < 0.0942912230607925 then 3
     when UPWARD_SCORE >= 0.04481848 and	UPWARD_SCORE < 0.0633593 then 4
     when UPWARD_SCORE >= 0.03238278 and	UPWARD_SCORE < 0.04481848 then 5
     when UPWARD_SCORE >= 0.0235558724608303 and	UPWARD_SCORE < 0.03238278 then 6
     when UPWARD_SCORE >= 0.01660561 and	UPWARD_SCORE < 0.0235558724608303 then 7
     when UPWARD_SCORE >= 0.0111104893411734 and	UPWARD_SCORE < 0.01660561 then 8
     when UPWARD_SCORE >= 0.0065800621091472 and	UPWARD_SCORE < 0.0111104893411734 then 9
     when UPWARD_SCORE < 0.0065800621091472 then 10
end as UPWARD_decile
from SANDBOX.DS.OTA_MODEL2CENTERS_SCORE_20200225;



select UPWARD_decile,count(*)
from OTA_MODEL2CENTERS_SCORE_DECILE_20200225
group by UPWARD_decile
order by UPWARD_decile;





DROP TABLE IF EXISTS OTA_MODEL2_TESTING_NATIONAL_UPWARD;
CREATE TEMP TABLE OTA_MODEL2_TESTING_NATIONAL_UPWARD AS
select A.*,B.UPWARD_SCORE
from OTA_MODEL2_TESTING_NATIONAL A
JOIN (SELECT UNIQUE_ID,UPWARD_SCORE,UPWARD_decile FROM OTA_MODEL2CENTERS_SCORE_DECILE_20200225) B
ON A.UNIQUE_ID = B.UNIQUE_ID;

SELECT * FROM OTA_MODEL2_TESTING_NATIONAL_UPWARD;





---Downward Model---


sample_train$distance[is.na(sample_train$distance)] <- 0; 
sample_train$SHT_Z_distance <- pmin(pmax(sample_train$distance,0),55); 
sample_train$num_one_shot_orders[is.na(sample_train$num_one_shot_orders)] <- 0; 
sample_train$INV_Z_num_one_shot_orders <- pmin(pmax(sample_train$num_one_shot_orders,0),95.8800000000001); 
sample_train$INV_Z_num_one_shot_orders[sample_train$INV_Z_num_one_shot_orders==0] <- 1; 
sample_train$INV_Z_num_one_shot_orders <- 1/sample_train$INV_Z_num_one_shot_orders; 
sample_train$mt_likely_planned_givers[is.na(sample_train$mt_likely_planned_givers)] <- 0; 
sample_train$SHT_Z_mt_likely_planned_givers <- pmin(pmax(sample_train$mt_likely_planned_givers,1),97); 
sample_train$num_sourc_verify_hh[is.na(sample_train$num_sourc_verify_hh)] <- 0; 
sample_train$PW2_Z_num_sourc_verify_hh <- pmin(pmax(sample_train$num_sourc_verify_hh,2),27); 
sample_train$PW2_Z_num_sourc_verify_hh <- (sample_train$PW2_Z_num_sourc_verify_hh)^2; 
sample_train$mt_price_motivated_personal_care_product_users[is.na(sample_train$mt_price_motivated_personal_care_product_users)] <- 0;
sample_train$SHT_Z_mt_price_motivated_personal_care_product_users <- pmin(pmax(sample_train$mt_price_motivated_personal_care_product_users,3),99); 
sample_train$propensity_to_buy_luxury_truck_full_size[is.na(sample_train$propensity_to_buy_luxury_truck_full_size)] <- 0; 
sample_train$SQT_Z_propensity_to_buy_luxury_truck_full_size <- pmin(pmax(sample_train$propensity_to_buy_luxury_truck_full_size,2),99) - (1); 
sample_train$SQT_Z_propensity_to_buy_luxury_truck_full_size[sample_train$SQT_Z_propensity_to_buy_luxury_truck_full_size <= 0] <- 1; 
sample_train$SQT_Z_propensity_to_buy_luxury_truck_full_size <- sqrt(sample_train$SQT_Z_propensity_to_buy_luxury_truck_full_size); 
sample_train$mt_monitored_home_security_system_owners[is.na(sample_train$mt_monitored_home_security_system_owners)] <- 0; 
sample_train$SQT_Z_mt_monitored_home_security_system_owners <- pmin(pmax(sample_train$mt_monitored_home_security_system_owners,1),95) - (0); 
sample_train$SQT_Z_mt_monitored_home_security_system_owners[sample_train$SQT_Z_mt_monitored_home_security_system_owners <= 0] <- 1; 
sample_train$SQT_Z_mt_monitored_home_security_system_owners <- sqrt(sample_train$SQT_Z_mt_monitored_home_security_system_owners); 
sample_train$mt_likely_cruiser[is.na(sample_train$mt_likely_cruiser)] <- 0; 
sample_train$SHT_Z_mt_likely_cruiser <- pmin(pmax(sample_train$mt_likely_cruiser,1),96); 
sample_train$mt_online_broker_user[is.na(sample_train$mt_online_broker_user)] <- 0;
sample_train$SHT_Z_mt_online_broker_user <- pmin(pmax(sample_train$mt_online_broker_user,1),96.8100000000004);


DROP TABLE IF EXISTS SANDBOX.DS.JW_OTA_TEMP_DOWNWARD; 
CREATE TEMP TABLE SANDBOX.DS.JW_OTA_TEMP_DOWNWARD AS
SELECT  *,
CASE WHEN invest_insur_investments_all IS NULL THEN 0 ELSE 1 END AS invest_insur_investments_all1,
CASE WHEN sports_fitness_exercise_all IS NULL THEN 0 ELSE 1 END AS sports_fitness_exercise_all1,
CASE WHEN hobbies_home_improv_diy_all IS NULL THEN 0 ELSE 1 END AS hobbies_home_improv_diy_all1,
CASE WHEN payment_method_cc IS NULL THEN 0 ELSE 1 END AS payment_method_cc1,
(CASE WHEN distance IS NULL THEN 0  
     WHEN distance > 55 THEN 55
     ELSE DISTANCE
     END) AS DISTANCE1,
CASE WHEN CAST(num_one_shot_orders AS INTEGER) IS NULL THEN 0 else CAST(num_one_shot_orders AS INTEGER) end as num_one_shot_orders1,
CASE WHEN num_one_shot_orders1 = 0 THEN 1
     WHEN num_one_shot_orders1 > 95.8800000000001 THEN 95.8800000000001 ELSE num_one_shot_orders1 end as num_one_shot_orders11,
CASE WHEN num_one_shot_orders11 BETWEEN 0 AND 95.8800000000001 THEN (1/num_one_shot_orders11)
     END AS num_one_shot_orders111,
(CASE WHEN mt_likely_planned_givers IS NULL THEN 0
     WHEN mt_likely_planned_givers > 97 THEN 97
     ELSE mt_likely_planned_givers
     END) AS mt_likely_planned_givers1,
POWER((CASE WHEN num_sourc_verify_hh IS NULL THEN 0
           WHEN num_sourc_verify_hh < 2 THEN 2 
           WHEN num_sourc_verify_hh > 27 THEN 27
           ELSE num_sourc_verify_hh
           END),2) AS num_sourc_verify_hh1,
(CASE WHEN mt_price_motivated_personal_care_product_users IS NULL THEN 0
     WHEN mt_price_motivated_personal_care_product_users < 3 THEN 3
     ELSE mt_price_motivated_personal_care_product_users
     END) AS mt_price_motivated_personal_care_product_users1,
SQRT(CASE WHEN (propensity_to_buy_luxury_truck_full_size IS NULL OR propensity_to_buy_luxury_truck_full_size <=0 ) 
     THEN 1 ELSE propensity_to_buy_luxury_truck_full_size-1 END) AS propensity_to_buy_luxury_truck_full_size1,
SQRT(CASE WHEN mt_monitored_home_security_system_owners IS NULL THEN 0
          WHEN mt_monitored_home_security_system_owners > 95 THEN 95
          WHEN mt_monitored_home_security_system_owners <= 0 THEN 1
          ELSE mt_monitored_home_security_system_owners
          END) AS mt_monitored_home_security_system_owners1,
(CASE WHEN mt_likely_cruiser IS NULL THEN 0
     WHEN mt_likely_cruiser > 96 THEN 96 
     WHEN mt_likely_cruiser < 1 THEN 1
     ELSE mt_likely_cruiser
     END) AS mt_likely_cruiser1,
(CASE WHEN mt_online_broker_user IS NULL THEN 0
     WHEN mt_online_broker_user > 96.8100000000004 THEN 96.8100000000004 
     WHEN mt_online_broker_user < 1 THEN 1
     ELSE mt_online_broker_user
     END) AS mt_online_broker_user1
FROM OTA_MODEL2_TESTING_NATIONAL_UPWARD;





DROP TABLE IF EXISTS SANDBOX.DS.OTA_MODEL2CENTERS_SCORE_20200225; 
CREATE temp TABLE  SANDBOX.DS.OTA_MODEL2CENTERS_SCORE_20200225 AS 
SELECT *,  EXP(DOWNWARD_LOGIT)/(1+EXP(DOWNWARD_LOGIT)) AS DOWNWARD_SCORE 
FROM ( 
SELECT  *,  -2.6635564 + 0.1784599 * propensity_to_buy_luxury_truck_full_size1 +
-0.1687960 * mt_monitored_home_security_system_owners1 + 
-0.0215127 * distance1 +
0.0094267 * mt_price_motivated_personal_care_product_users1 +
-1.095568 * num_one_shot_orders111 +
0.5313444 * sports_fitness_exercise_all1 +
0.5348834 * invest_insur_investments_all1 +
-0.0064388 * mt_likely_planned_givers1 +
0.3141735 * hobbies_home_improv_diy_all1 +
0.3113718 * payment_method_cc1 +
0.0007459 * num_sourc_verify_hh1 +
-0.0042373 * mt_likely_cruiser1 +
-0.0025648 * mt_online_broker_user1
 AS DOWNWARD_LOGIT 
FROM    SANDBOX.DS.JW_OTA_TEMP_DOWNWARD);




SELECT * FROM OTA_MODEL2CENTERS_SCORE_20200225 where downward_score is null LIMIT 100;



DROP TABLE IF EXISTS SANDBOX.DS.OTA_MODEL2CENTERS_SCORE_DECILE_20200225;
CREATE temp TABLE  SANDBOX.DS.OTA_MODEL2CENTERS_SCORE_DECILE_20200225 AS 
select *, case 
     when DOWNWARD_SCORE >= 0.156273003314925 then 1
     when DOWNWARD_SCORE >= 0.0918378725096444	and DOWNWARD_SCORE < 0.156273003314925 then 2
     when DOWNWARD_SCORE >= 0.0636557055773944 and	DOWNWARD_SCORE < 0.0918378725096444 then 3
     when DOWNWARD_SCORE >= 0.04554712 and	DOWNWARD_SCORE < 0.0636557055773944 then 4
     when DOWNWARD_SCORE >= 0.0335650176661567 and	DOWNWARD_SCORE < 0.04554712 then 5
     when DOWNWARD_SCORE >= 0.0247404797938108 and	DOWNWARD_SCORE < 0.0335650176661567 then 6
     when DOWNWARD_SCORE >= 0.0177237225159412 and	DOWNWARD_SCORE < 0.0247404797938108 then 7
     when DOWNWARD_SCORE >= 0.0121975246998621 and	DOWNWARD_SCORE < 0.0177237225159412 then 8
     when DOWNWARD_SCORE >= 0.00752966346539754 and	DOWNWARD_SCORE < 0.0121975246998621 then 9
     when DOWNWARD_SCORE < 0.00752966346539754 then 10
end as DOWNWARD_DECILE
from SANDBOX.DS.OTA_MODEL2CENTERS_SCORE_20200225;




select DOWNWARD_decile,count(*)
from OTA_MODEL2CENTERS_SCORE_DECILE_20200225
group by DOWNWARD_decile
order by DOWNWARD_decile;





DROP TABLE IF EXISTS OTA_MODEL2_TESTING_NATIONAL_UPWARD_DOWNWARD;
CREATE TABLE OTA_MODEL2_TESTING_NATIONAL_UPWARD_DOWNWARD AS
select A.*,B.DOWNWARD_SCORE
from OTA_MODEL2_TESTING_NATIONAL_UPWARD A
JOIN (SELECT UNIQUE_ID,DOWNWARD_SCORE,DOWNWARD_DECILE FROM OTA_MODEL2CENTERS_SCORE_DECILE_20200225) B
ON A.UNIQUE_ID = B.UNIQUE_ID;





create or replace stage jinwei url='s3://sftp-files-transfer/Jinwei/'
credentials=(aws_key_id='AKIAXXPFZTE76MXUHH3A' aws_secret_key='B4tQOOKwqzxcQmNVCWW8wAN3UjUJH09bNVKXnXES') ;

create or replace file format jw_csv_format type=csv field_delimiter =','  file_extension='csv'  SKIP_HEADER=1;

copy into @jinwei/OTA_MODELDATA_SCORE_FINAL.txt.gz
from OTA_MODELCENTERS_SCORE_DECILE_OUTPUT file_format=jw_csv_format single=true 
max_file_size=5368709120; 

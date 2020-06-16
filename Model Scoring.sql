---Create Epsilon Data for Scoring---
DROP TABLE IF EXISTS SANDBOX.DS.OOOO_MODEL2_ALLCENTERS1; 
CREATE temp TABLE  SANDBOX.DS.OOOO_MODEL2_ALLCENTERS1 AS 
select AGILITY_ADDR_KEY,AGILITY_HH_KEY,AGILITY_INDIVIDUAL_KEY1,PERSON_SEQ_NO1,GIVEN_NAME1,SURNAME,CONTRACTED_ADDRESS,
POST_OFFICE_NAME,STATE,B.ZIP,B.ZIP4,CARRIER_ROUTE,A.center,A.Center_Zip_code,A.Distance,gender1,invest_insur_investments_all,
propensity_to_buy_luxury_truck_full_size,mt_price_motivated_personal_care_product_users,mt_monitored_home_security_system_owners,
num_sourc_verify_hh,mt_likely_cruiser,mt_online_magazinenewspaper_subscribers,mt_retired_but_still_working,mt_active_on_fb,mt_auto_loan_purchr,
sports_fitness_exercise_all,hobbies_home_improv_diy_all,payment_method_cc,num_one_shot_orders,mt_likely_planned_givers,mt_online_broker_user
from (select ZIP,CENTER,CENTER_ZIP_CODE,DISTANCE from OOOO_ALLUS_DISTANCE) A
join EPSILON_TSP_LATEST B 
on A.ZIP = B.ZIP
where  B.FILE_CODE = 'P' and A.distance < 1 AND (TRY_CAST(B.ADV_NUM_ADULTS as INTEGER)  > 1 OR B.INVEST_INSUR_INVESTMENTS_ALL='Y');

select count(*) from OOOO_MODEL2_ALLCENTERS1;



DROP TABLE IF EXISTS OOOO_MODEL2_ALLCENTERS_UNIQUE_ID;
create table OOOO_MODEL2_ALLCENTERS_UNIQUE_ID AS 
select *, 1000000000+ROW_NUMBER() OVER(ORDER BY CENTER DESC) AS UNIQUE_ID 
from SANDBOX.DS.OOOO_MODEL2_ALLCENTERS1;




---National model score---


DROP TABLE IF EXISTS SANDBOX.DS.JW_OOOO_TEMP_NATIONAL; 
CREATE TEMP TABLE SANDBOX.DS.JW_OOOO_TEMP_NATIONAL AS
SELECT  *,
CASE WHEN (gender1 IS NULL OR gender1 = 1) THEN 1 ELSE 0 END AS GENDER11,
CASE WHEN invest_insur_investments_all IS NULL THEN 0 ELSE 1 END AS invest_insur_investments_all1,
SQRT(CASE WHEN (propensity_to_buy_luxury_truck_full_size IS NULL OR propensity_to_buy_luxury_truck_full_size <=0 ) 
     THEN 1 ELSE propensity_to_buy_luxury_truck_full_size END) AS propensity_to_buy_luxury_truck_full_size1,
CASE WHEN mt_price_motivated_personal_care_product_users IS NULL THEN 0
     WHEN mt_price_motivated_personal_care_product_users < 1 THEN 1 
     else mt_price_motivated_personal_care_product_users end as mt_price_motivated_personal_care_product_users1,
(CASE WHEN distance IS NULL THEN 0 
     WHEN distance > 1 THEN 1
     ELSE distance
     END) AS DISTANCE1,
SQRT(CASE WHEN mt_monitored_home_security_system_owners IS NULL THEN 0
          WHEN mt_monitored_home_security_system_owners > 1 THEN 1
          WHEN mt_monitored_home_security_system_owners <= 0 THEN 1
          ELSE mt_monitored_home_security_system_owners
          END) AS mt_monitored_home_security_system_owners1,
POWER((CASE WHEN num_sourc_verify_hh IS NULL THEN 0
           WHEN num_sourc_verify_hh < 1 THEN 1 
           WHEN num_sourc_verify_hh > 1 THEN 1
           ELSE num_sourc_verify_hh
           END),2) AS num_sourc_verify_hh1,
(CASE WHEN mt_likely_cruiser IS NULL THEN 0
     WHEN mt_likely_cruiser > 1 THEN 1 
     WHEN mt_likely_cruiser < 1 THEN 1
     ELSE mt_likely_cruiser 
     END) AS mt_likely_cruiser1,
(CASE WHEN mt_online_magazinenewspaper_subscribers IS NULL THEN 0
     WHEN mt_online_magazinenewspaper_subscribers > 1 THEN 1
     WHEN mt_online_magazinenewspaper_subscribers < 1 THEN 1
     ELSE mt_online_magazinenewspaper_subscribers
     END) AS mt_online_magazinenewspaper_subscribers1,
POWER((CASE WHEN mt_retired_but_still_working IS NULL THEN 0
     ELSE mt_retired_but_still_working
     END),1) AS mt_retired_but_still_working1
FROM OOOO_MODEL2_ALLCENTERS_UNIQUE_ID;



DROP TABLE IF EXISTS SANDBOX.DS.OOOO_MODEL2CENTERS_SCORE_20200225; 
CREATE temp TABLE  SANDBOX.DS.OOOO_MODEL2CENTERS_SCORE_20200225 AS 
SELECT *,  EXP(NATIONAL_LOGIT)/(1+EXP(NATIONAL_LOGIT)) AS NATIONAL_SCORE 
FROM ( 
SELECT  *,  1 +(1 * GENDER11) +
 1 * invest_insur_investments_all1 +
 1 *  mt_active_on_fb +
 (-1 * mt_retired_but_still_working1) +
 1 *  num_sourc_verify_hh1 +
 (-1 * distance1) +
 (-1 * mt_likely_cruiser1) +
 (-1 * mt_online_magazinenewspaper_subscribers1) +
 1 *  mt_price_motivated_personal_care_product_users1 +
 (-1 * mt_monitored_home_security_system_owners1) +
 1*  propensity_to_buy_luxury_truck_full_size1
 AS NATIONAL_LOGIT 
FROM    SANDBOX.DS.JW_OOOO_TEMP_NATIONAL);




SELECT * FROM OOOO_MODEL2CENTERS_SCORE_20200225 LIMIT 100;


DROP TABLE IF EXISTS SANDBOX.DS.OOOO_MODEL2CENTERS_SCORE_DECILE_20200225;
CREATE temp TABLE  SANDBOX.DS.OOOO_MODEL2CENTERS_SCORE_DECILE_20200225 AS 
select *, case 
     when NATIONAL_SCORE >= 1 then 1
     when NATIONAL_SCORE >= 1	and NATIONAL_SCORE < 1 then 2
     when NATIONAL_SCORE >= 1 and	NATIONAL_SCORE < 1 then 3
     when NATIONAL_SCORE >= 1 and	NATIONAL_SCORE < 1 then 4
     when NATIONAL_SCORE >= 1 and	NATIONAL_SCORE < 1 then 5
     when NATIONAL_SCORE >= 1 and	NATIONAL_SCORE < 1 then 6
     when NATIONAL_SCORE >= 1 and	NATIONAL_SCORE < 1 then 7
     when NATIONAL_SCORE >= 1 and	NATIONAL_SCORE < 1 then 8
     when NATIONAL_SCORE >= 1 and	NATIONAL_SCORE < 1 then 9
     when NATIONAL_SCORE < 1 then 10
end as NATIONAL_decile
from SANDBOX.DS.OOOO_MODEL2CENTERS_SCORE_20200225;



select NATIONAL_decile,count(*)
from OOOO_MODEL2CENTERS_SCORE_DECILE_20200225
group by NATIONAL_decile
order by NATIONAL_decile;



DROP TABLE IF EXISTS OOOO_MODEL2_TESTING_NATIONAL;
CREATE TEMP TABLE OOOO_MODEL2_TESTING_NATIONAL AS
select A.*,B.NATIONAL_SCORE
from OOOO_MODEL2_ALLCENTERS_UNIQUE_ID A
JOIN (SELECT UNIQUE_ID,NATIONAL_SCORE,NATIONAL_decile FROM OOOO_MODEL2CENTERS_SCORE_DECILE_20200225) B
ON A.UNIQUE_ID = B.UNIQUE_ID;



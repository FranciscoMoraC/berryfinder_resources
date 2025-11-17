-- Intermediate query views for the "STAPH AUREUS COAG +" organism resistance query, organism id: 80023
-- This queries result in a materialized view that contains the data of the patients with the organism id
-- The view contains additional data that should be removed before its use (e.g. ids and used drugs)

-- Drops any existing intermediate and materialized views to ensure a clean slate for the query.
drop materialized view if exists inter_query_7;
drop view if exists last_drug_date_view;
drop view if exists inter_query_6;
drop view if exists last_drug_date_view;
drop view if exists inter_query_5;
drop view if exists curr_unit_view;

-- View to determine the current care unit of a patient when the culture was taken
-- Joins transfer data to associate patients with their ICU status based on culture event time
create view curr_unit_view as
select iq.event_id, careunit as icu_when_culture 
from 
    (select * from inter_query_4) iq
left outer join
    (select subject_id, hadm_id, careunit, INTIME, OUTTIME from mimiciv_hosp.transfers) t
on iq.subject_id = t.subject_id and iq.hadm_id = t.hadm_id
where iq.date > t.intime and iq.date < t.outtime;


-- View that adds ICU status to the dataset from the culture data
create view inter_query_5 as
select iq.*, v.icu_when_culture 
from 
    (select * from inter_query_4) iq
left outer join
    (select * from curr_unit_view) v
on iq.event_id = v.event_id;

-- Exclude patients who were in more than one ICU during the culture collection period
create view inter_query_6 as
SELECT *
FROM inter_query_5 iq 
WHERE event_id NOT IN (
    SELECT event_id
    FROM inter_query_5 iq2
    GROUP BY event_id
    HAVING COUNT(*) > 1
);

-- View to obtain the most recent treatment data within the last 180 days for each patient
-- Includes the last treatment date, the number of treatments, and the total days with treatment
create view last_drug_date_view as 
select iq.event_id, 
    max(pr.stoptime) as last_treatment_date, 
    count(pr.stoptime) as number_of_treatments_last_180_days,
    sum(days_with_treatment_last_180_days) as days_with_treatment_last_180_days
from
    (select * from inter_query_6) iq
left outer join
    (select subject_id, hadm_id, stoptime, drug, extract(day from stoptime - starttime) as days_with_treatment_last_180_days 
     from mimiciv_hosp.prescriptions) pr
on iq.subject_id = pr.subject_id and iq.hadm_id = pr.hadm_id 
   and to_tsvector('english', upper(pr.drug)) @@ to_tsquery('english', replace(split_part(iq.ab_name, '/', 1), ' ', ' & '))
where pr.stoptime < iq.date and pr.stoptime > iq.date - interval '180' day
group by iq.event_id;

-- Final materialized view that calculates the time since the last treatment
-- It also aggregates the number of treatments and treatment duration within the last 180 days
create materialized view inter_query_7 as
select iq.*, 
    coalesce(extract(day from iq.date - v.last_treatment_date), 360) as days_since_last_treatment,
    coalesce(v.number_of_treatments_last_180_days, 0) as number_of_treatments_last_180_days,
    coalesce(v.days_with_treatment_last_180_days, 0) as days_with_treatment_last_180_days
from
    (select * from inter_query_6 iq) iq
left outer join
    (select * from last_drug_date_view) v
on iq.event_id = v.event_id;
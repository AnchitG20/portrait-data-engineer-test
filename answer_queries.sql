select * from  appointments where days_since_last is not null order by patient_id, appointment_date ;
select * from patients;
select * from providers;
select * from prescriptions;

-- ANALYSIS
-- What is the distribution of patients across age groups?

SELECT age_group, round(count(patient_id)*100/(select count(*) from patients),2) as number_of_patients from patients 
group by age_group
order by age_group;

-- How does the appointment frequency vary by patient type?

SELECT p.registration_type, COUNT(a.appointment_id) AS appointment_count
FROM appointments a
JOIN patients p ON a.patient_id = p.patient_id
GROUP BY p.registration_type
ORDER BY p.registration_type;


-- What are the most common appointment types by age group?
with cte as (
select p.age_group, a.appointment_type, count(a.appointment_type) as count, 
dense_rank() over (partition by age_group order by count(a.appointment_type) desc) as rnk from appointments a join patients p 
on a.patient_id = p.patient_id
group by p.age_group, a.appointment_type
order by p.age_group)

select age_group, appointment_type, count from cte where rnk = 1;

select * from appointments;


-- Are there specific days of the week with higher emergency visits?

select day_of_week, count(patient_id) as number_of_visits from appointments
where appointment_type = 'Emergency'
group by day_of_week
order by count(patient_id) desc;

-- What are the most prescribed medication categories by age group?

with cte as (
select pt.age_group, p.med_category, count(p.patient_id) as count_of_patients, dense_rank() over (partition by age_group order by count(p.patient_id) desc) as rnk
from prescriptions p join patients pt on p.patient_id = pt.patient_id
group by pt.age_group, p.med_category
order by pt.age_group, count_of_patients desc)

select age_group, med_category, count_of_patients from cte where rnk=1
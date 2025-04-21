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


DOCUMENTATION

# Transformation Approach

The transformation phase was designed to enrich raw data from four core tables—patients, appointments, prescriptions, and providers—to enable meaningful analysis and insights. The transformations were executed using Python (Pandas) after ingestion into a MySQL database.

Importing necessary libraries required in this code base.
 
import pandas as pd
from sqlalchemy import create_engine
import os
from urllib.parse import quote_plus   -- This is used to encode teh password in mysql containing and special characters to a proper string to avoid errors


password = getpass.getpass("Enter your Password!")

#As my password has @ to encode it into connection
encoded_pass = quote_plus(password)

#create Database Connection

engine = create_engine(f"mysql+pymysql://root:{encoded_pass}@localhost:3306/ehr")

#Testing the connection
try:
    with engine.connect() as connection:
        print("Successfully connected to mysql")
except Exception as e:
    print(f"Error connecting to mysql : {e}")

# Mapping of CSV file paths to table names
csv_to_table = {
    "patients.csv": "patients",
    "providers.csv": "providers",
    "appointments.csv": "appointments",
    "prescriptions.csv": "prescriptions"
}


#Function to add data in mysql database

def load_data():
    """Loading all the csv files into MySQL Database"""

    for csv_path, table_name in csv_to_table.items():
        print(f"Importing {csv_path} into `{table_name}`...")

        # Load CSV
        df = pd.read_csv(csv_path)

        # Upload to MySQL
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    
        print(f"Successfully imported {len(df)} rows into `{table_name}`")
    
    print("All CSVs have been imported into MySQL")


# Patient-Level Transformations
# 1.	Age Group Classification
Patients were segmented into 5 age groups for demographic analysis:
o	0-18, 19-30, 31-50, 51-70, and 71+
The transformation was applied using a custom function mapping the patient’s age into these ranges.

from datetime import datetime
#Finding the age group of patients

def find_age_groups():
    
    df = pd.read_sql("Select * from patients",con = engine, parse_dates = ['registration_date'])
    
    def age_group(age):
        if age <=18: return '0-18'
        elif age <=30: return '19-30'
        elif age <=50: return '31-50'
        elif age <= 70: return '51-70'
        else: return '71+'
        
    df['age_group'] = df['age'].apply(age_group)
    
    #Saving back to Table
    
    df.to_sql("patients", con=engine, if_exists='replace', index=False)
 
# Patient Type
Based on the time since registration:
               New: < 6 months
               Regular: 6–24 months
               Long-term: > 24 months
         This was calculated by subtracting registration_date from the current date and converting to months.
         The result was stored back into the table.
#Finding the patient type

def find_patient_type():
    
    df = pd.read_sql("Select * from patients",con = engine, parse_dates = ['registration_date'])
    
    # Months since registration
    df['months_registered'] = (
        (pd.Timestamp.now() - df['registration_date']) / pd.Timedelta(days=30)).astype(int)
    
    def reg_type(months):
        
        if months<6: return 'New'
        elif months <=24: return 'Regular'
        else: return 'Long Term'
        
    df['registration_type'] = df['months_registered'].apply(reg_type)
    
    #Saving back to Patients table
    
    df.to_sql("patients", con=engine, if_exists='replace', index=False)
 
# Appointment-Level Transformations
# 1.	Day of the Week
Extracted from appointment_date using dt.day_name() to analyze visit patterns across the week.

#Day of the week

def day_of_week():
    
    df = pd.read_sql("Select * from appointments",con = engine, parse_dates = ['appointment_date'])
    
    #Converting dates to day name
    df['day_of_week'] = df['appointment_date'].dt.day_name()
    
    #Saving the new column in the table
    
    df.to_sql("appointments", con=engine, if_exists='replace', index=False)
    
    print('New column added Successfully!')
 
# 2.Time Since Last Appointment
Calculated per patient by sorting appointments chronologically and subtracting the days from the previous appointment date to get the number of days.

 #Calculating days since last appointment

def days_since_last_appointment():
    
    df = pd.read_sql("Select * from appointments",con = engine, parse_dates = ['appointment_date'])
    
    # Sort by patient and appointment date
    df.sort_values(by=['patient_id', 'appointment_date'], inplace=True)
    
    # Shift appointment dates to get the "previous appointment"
    df['prev_appointment'] = df.groupby('patient_id')['appointment_date'].shift(1)

    # Calculate time since last appointment in days
    df['days_since_last'] = (df['appointment_date'] - df['prev_appointment']).dt.days
    
    df.to_sql("appointments", con=engine, if_exists='replace', index=False)
    print('New column added Successfully!')

# Prescription-Level Transformations
# 1.	Medication Category
Mapped common medication names to categories like Pain Relief, Diabetes, Heart, etc., using a dictionary.

#Defining medication category

def medication_category():
    
    df = pd.read_sql("Select * from prescriptions",con = engine, parse_dates = ['prescription_date'])
    
    medication_map = {'Paracetamol': 'Pain Relief',
    'Ibuprofen': 'Pain Relief',
    'Aspirin': 'Pain Relief',
    'Atorvastatin': 'Heart',
    'Metformin': 'Diabetes',
    'Insulin': 'Diabetes',
    'Atenolol': 'Heart',
    'Lisinopril': 'Heart',
    'Amoxicillin' : 'Infection'}
    
    df['med_category'] = df['medication_name'].map(medication_map)# Sort by patient + medication + date
    
    # Determine if it's the first time or repeat
    df['prescription_frequency'] = df.duplicated(subset=['patient_id'], keep='first')
    df['prescription_frequency'] = df['prescription_frequency'].map({False: 'First-time', True: 'Repeat'})
    
    # Sort by patient + medication + date
    df.sort_values(by=['patient_id', 'medication_name', 'prescription_date'], inplace=True)
    
   
    df.to_sql("prescriptions", con=engine, if_exists='replace', index=False)
    print('New column added Successfully!')
 
# 3.	Prescription Frequency
Classified as First-time or Repeat based on whether a patient had received the same medication before. Used duplicated() to identify repeat prescriptions.


Analysis
# •	What is the distribution of patients across age groups?

SELECT age_group, round(count(patient_id)*100/(select count(*) from patients),2) as number_of_patients from patients 
group by age_group
order by age_group;

# RESULT
Age Group	   Number of Patients (%)
  0-18	           1.82%
  19-30	          14.55%
  31-50	          25.45%
  51-70	          21.82%
  71+	            36.36%
# Patients above 50 make approx 50% of the patients
        
# •	How does the appointment frequency vary by patient type?

SELECT p.registration_type, COUNT(a.appointment_id) AS appointment_count
FROM appointments a
JOIN patients p ON a.patient_id = p.patient_id
GROUP BY p.registration_type
ORDER BY p.registration_type;

# RESULT

Registration Type	Appointment Count
      Long Term	39
      New	      4
      Regular	  62
	
# 62 Patients were registered between 6-24 months from current date

# •	What are the most common appointment types by age group?

with cte as (
select p.age_group, a.appointment_type, count(a.appointment_type) as count, 
dense_rank() over (partition by age_group order by count(a.appointment_type) desc) as rnk from appointments a join patients p 
on a.patient_id = p.patient_id
group by p.age_group, a.appointment_type
order by p.age_group)

select age_group, appointment_type, count from cte where rnk = 1;


# •	Are there specific days of the week with higher emergency visits?

select day_of_week, count(patient_id) as number_of_visits from appointments
where appointment_type = 'Emergency'
group by day_of_week
order by count(patient_id) desc;

day_of_week           number_of_visits
    Friday                    9
    Saturday                  6
    Monday                    6
    Thursday                  4
    Tuesday                   3
    Sunday                    3
    Wednesday                 2

# Friday records the highest Emergrncy appointments followed by Saturday. Sunday with low availability due no doctors present.

# •	What are the most prescribed medication categories by age group?

with cte as (
select pt.age_group, p.med_category, count(p.patient_id) as count_of_patients, dense_rank() over (partition by age_group order by count(p.patient_id) desc) as rnk
from prescriptions p join patients pt on p.patient_id = pt.patient_id
group by pt.age_group, p.med_category
order by pt.age_group, count_of_patients desc)

select age_group, med_category, count_of_patients from cte where rnk=1

RESULT

Age_group             Med_category             number_of_patients
0-18                    Infection                      1
0-18                    Heart                          1
0-18                    Diabetes                       1
19-30                   Pain Relief                    5
31-50                   Heart                          16
51-70                   Pain Relief                    15
71+                     Pain Relief                    15

# People above age group 50 tend to take more Painkiller medicines and people of age group 31-50 are more likely to suffer from heart related issues.
            	
	
# Key Findings to Improve Healthcare Operations
# 1.	High Engagement from Long-Term Patients
o	Long-term patients account for a significant portion of appointment volume.
o	Implication: These patients are more likely to follow through on care plans, making them ideal candidates for chronic condition management programs and long-term wellness initiatives.

# 2.	Underutilization by New Patients
o	New patients show very low appointment frequency.
o	Implication: There may be barriers to engagement early in the care journey (e.g., poor onboarding, limited access). Targeted welcome programs and follow-up reminders could help increase their engagement and retention.
# 3.	Age-Driven Healthcare Demand
o	The 71+ age group makes up the largest proportion of the patient population and likely drives demand for prescriptions and appointments.
o	Implication: Resource planning (staffing, appointment slots, specialized care) should account for the needs of this growing demographic.	

# Suggestions for Further Analysis
•	Explore missed appointments and no-show rates by patient type and weekday.
•	Analyze provider efficiency based on appointment load and prescription volume.
•	Study cost implications of prescription patterns across different demographics.




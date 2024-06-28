import os
from datetime import datetime
import pymysql as mysql
import pandas as pd
import numpy as np
def row_to_payload(row,file_tag):
    return {
        "company": row["company"],
        "company_description": row["company_description"],
        "company_employee_count": row["company_employee_count"],
        "company_employee_range": row["company_employee_range"],
        "company_industry": row["company_industry"],
        "company_linkedin": row["company_linkedin"],
        "company_location": row["company_location"],
        "company_logo": row["company_logo"],
        "company_specialties": row["company_specialties"].split(', ') if isinstance(row["company_specialties"], str) else [""],
        "company_type": row["company_type"],
        "company_website": row["company_website"],
        "company_year_founded": row["company_year_founded"],
        "connected_at": "test",  # Placeholder, replace as needed
        "connections": row["connections"],
        "degree": "test",  # Placeholder, replace as needed
        "email": row["email"],
        "firstname": row["firstname"],
        "headline": row["headline"],
        "job": row["job"],
        "job_description": row["job_description"],
        "lastname": row["lastname"],
        "linkedin_id": row["linkedin_public_id"],
        "linkedin_profile_url": row["linkedin_profile_url"],
        "location": row["location"],
        "month_company": row["month_company"],
        "month_position": row["month_position"],
        "phone": row["phone"],
        "picture": row["picture"],
        "pipeline": "test",  # Placeholder, replace as needed
        "skills": row["all_skills"].split(', ') if isinstance(row["all_skills"], str) else [""],
        "spotlight": "test",  # Placeholder, replace as needed
        "step": "test",  # Placeholder, replace as needed
        "summary": row["summary"],
        "uploaded_email": row["email_enrich"],
        "file_tag" : file_tag,
        "user": {
            "email": row["email"],
            "id": "test",  # Placeholder, replace as needed
            "firstname": row["firstname"],
            "lastname": row["lastname"],
            "linkedin_id": row["linkedin_public_id"],
            "linkedin_public_id": row["linkedin_public_id"]
        }
        
    }

def save_to_database(data, connection):
    try:
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO stg_linkedin (
            company, company_description, company_employee_count, company_employee_range,
            company_industry, company_linkedin, company_location, company_logo,
            company_specialties, company_type, company_website, company_year_founded,
            connected_at, connections, degree, email,file_tag, firstname, headline,
            job, job_description, lastname, linkedin_id, linkedin_profile_url,
            location, month_company, month_position, phone, picture, pipeline,
            skills, spotlight, step, summary, uploaded_email, user_email,
            user_firstname, user_id, user_lastname, user_linkedin_id, user_linkedin_public_id,
            timestamp_column_name
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON DUPLICATE KEY UPDATE
        company_description = VALUES(company_description),
        company_employee_count = VALUES(company_employee_count),
        company_employee_range = VALUES(company_employee_range),
        company_industry = VALUES(company_industry),
        company_linkedin = VALUES(company_linkedin),
        company_location = VALUES(company_location),
        company_logo = VALUES(company_logo),
        company_specialties = VALUES(company_specialties),
        company_type = VALUES(company_type),
        company_website = VALUES(company_website),
        company_year_founded = VALUES(company_year_founded),
        connected_at = VALUES(connected_at),
        connections = VALUES(connections),
        degree = VALUES(degree),
        email = VALUES(email),
        file_tag = VALUES(file_tag),
        firstname = VALUES(firstname),
        headline = VALUES(headline),
        job = VALUES(job),
        job_description = VALUES(job_description),
        lastname = VALUES(lastname),
        linkedin_id = VALUES(linkedin_id),
        linkedin_profile_url = VALUES(linkedin_profile_url),
        location = VALUES(location),
        month_company = VALUES(month_company),
        month_position = VALUES(month_position),
        phone = VALUES(phone),
        picture = VALUES(picture),
        pipeline = VALUES(pipeline),
        skills = VALUES(skills),
        spotlight = VALUES(spotlight),
        step = VALUES(step),
        summary = VALUES(summary),
        uploaded_email = VALUES(uploaded_email),
        user_email = VALUES(user_email),
        user_firstname = VALUES(user_firstname),
        user_id = VALUES(user_id),
        user_lastname = VALUES(user_lastname),
        user_linkedin_id = VALUES(user_linkedin_id),
        user_linkedin_public_id = VALUES(user_linkedin_public_id),
        timestamp_column_name = CURRENT_TIMESTAMP
        """
            data_tuple = (
                data.get('company'),
                data.get('company_description'),
                data.get('company_employee_count'),
                data.get('company_employee_range'),
                data.get('company_industry'),
                data.get('company_linkedin'),
                data.get('company_location'),
                data.get('company_logo'),
                '; '.join(data.get('company_specialties', [])) if data.get('company_specialties') is not None else '',
                data.get('company_type'),
                data.get('company_website'),
                data.get('company_year_founded'),
                data.get('connected_at'),
                data.get('connections'),
                data.get('degree'),
                data.get('email'),
                data.get('file_tag'),
                data.get('firstname'),
                data.get('headline'),
                data.get('job'),
                data.get('job_description'),
                data.get('lastname'),
                data.get('linkedin_id'),
                data.get('linkedin_profile_url'),
                data.get('location'),
                data.get('month_company'),
                data.get('month_position'),
                data.get('phone'),
                data.get('picture'),
                data.get('pipeline'),
                '; '.join(data.get('skills', [])) if data.get('skills') is not None else '',
                data.get('spotlight'),
                data.get('step'),
                data.get('summary'),
                data.get('uploaded_email'),
                data.get('user', {}).get('email'),
                data.get('user', {}).get('firstname'),
                data.get('user', {}).get('id'),
                data.get('user', {}).get('lastname'),
                data.get('user', {}).get('linkedin_id'),
                data.get('user', {}).get('linkedin_public_id')
            )
            cursor.execute(sql, data_tuple)
        connection.commit()
        print(data)
   
        return data
    except Exception as e:
        return str(e)

def connect_to_database():
    try:
        connection = mysql.connect(
            host='vzleadgen.cma1wzfub9dh.us-west-2.rds.amazonaws.com',
            port = 3306,
            database='vizleads',
            user='tlubben',
            password='VizualLead21!',
        )
        print("Connected to database successfully")
        return connection
    except :
        print("Error while connecting to MySQL")
        return None


connection = connect_to_database()
today = datetime.today()
day = today.day
month = today.month
year = today.year

date_string = f"{month}_{day}_{year}"

directory = os.path.join(os.path.expanduser("~"), "Downloads")

filename1 = ""
for filename in os.listdir(directory):
    if filename.startswith(date_string):
        file_path = os.path.join(directory, filename)
        filename1 += filename

        print(f"Found file: {file_path}")
        df = pd.read_csv(file_path)
        df = df.replace({np.nan:None})
        for i in range(len(df)):
            payload = row_to_payload(df.iloc[i],file_tag=filename1)
            print(payload)
            save_to_database(payload,connection=connection)
        break
else:
    print(f"No file found with today's date ({date_string}) in the filename.")

counter = 0 
print(len(df))





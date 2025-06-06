from faker import Faker
import pandas as pd
import random
from datetime import timedelta, datetime

fake = Faker()
Faker.seed(0)
random.seed(0)

num_employees = 50
start_date = datetime(2025, 6, 1)
end_date = datetime(2025, 6, 7)
date_range = pd.date_range(start=start_date, end=end_date)

# Helper
def rand_date(start, end):
    return fake.date_between(start, end)

def round_hours(dt):
    return round(dt.total_seconds() / 3600, 2)

# Employee Master
employees = []
for i in range(1, num_employees + 1):
    emp_code = f"EMP{i:04d}"
    doj = rand_date('-10y', '-1y')
    dob = rand_date('-50y', '-25y')
    employees.append({
        "Employee Code": emp_code,
        "Employee Name": fake.name(),
        "Email": fake.email(),
        "Additional Email": fake.email(),
        "Mobile Number": fake.phone_number(),
        "Secondary Mobile Number": fake.phone_number(),
        "Gender": random.choice(["Male", "Female", "Other"]),
        "Date Of Joining": doj,
        "Date Of Birth": dob,
        "Fax": fake.phone_number(),
        "Marital Status": random.choice(["Single", "Married"]),
        "Self Service": random.choice(["Yes", "No"]),
        "Employee Type": random.choice(["Full Time", "Part Time", "Intern"]),
        "Office Location": fake.city(),
        "Business Unit": random.choice(["Tech", "HR", "Marketing"]),
        "Designation": random.choice(["Manager", "Engineer", "Analyst"]),
        "Department": random.choice(["IT", "Sales", "Support"]),
        "Grade": random.choice(["A", "B", "C"]),
        "Parent Department": random.choice(["Corporate", "Engineering"]),
        "Primary Manager": fake.name(),
        "Primary Manager Email": fake.email(),
        "Bank Name": fake.company(),
        "Branch Name": fake.city(),
        "Account Holder Name": fake.name(),
        "Account Number": fake.iban(),
        "Account Type": random.choice(["Savings", "Current"]),
        "IFSC Code": f"IFSC{random.randint(1000,9999)}",
        "Swift Code": f"SW{random.randint(100000,999999)}",
        "PAN Number": fake.bothify(text='?????####?'),
        "Aadhaar Enrollment Number": fake.bothify(text='############'),
        "Aadhaar Number": fake.bothify(text='############'),
        "Present Address": fake.address(),
        "Present State": fake.state(),
        "Present City": fake.city(),
        "Present Pincode": fake.postcode(),
        "Present Country": "India",
        "Permanent Address": fake.address(),
        "Permanent State": fake.state(),
        "Permanent City": fake.city(),
        "Permanent Pincode": fake.postcode(),
        "Permanent Country": "India",
        "Status": random.choice(["Active", "Inactive"])
    })

df_emp = pd.DataFrame(employees)
df_emp.to_csv("employee_master.csv", index=False)

# Exit Report
exit_report = []
for emp in employees[:10]:  # 10 employees resigned
    doj = pd.to_datetime(emp["Date Of Joining"])
    exit_date = doj + timedelta(days=random.randint(500, 3000))
    exit_report.append({
        "Employee Code": emp["Employee Code"],
        "Employee Name": emp["Employee Name"],
        "Business Unit": emp["Business Unit"],
        "Designation": emp["Designation"],
        "Date Of Joining": doj.date(),
        "Exit Date": exit_date.date(),
        "Expected Resignation Date": (exit_date - timedelta(days=30)).date()
    })
pd.DataFrame(exit_report).to_csv("employee_exit_report.csv", index=False)

# Work Profile
work_profiles = [{
    "Employee Code": emp["Employee Code"],
    "Employee Name": emp["Employee Name"],
    "Business Unit": emp["Business Unit"],
    "Parent Designation": "Senior " + emp["Designation"],
    "Assigned Department": emp["Department"],
    "Designation": emp["Designation"],
    "Office Location Name": emp["Office Location"]
} for emp in employees]
pd.DataFrame(work_profiles).to_csv("employee_work_profile.csv", index=False)

# Experience Report
experience_reports = []
for emp in employees:
    current_exp = round(random.uniform(1.0, 10.0), 2)
    past_exp = round(random.uniform(0.0, 5.0), 2)
    total_exp = round(current_exp + past_exp, 2)
    experience_reports.append({
        "Employee Code": emp["Employee Code"],
        "Employee Name": emp["Employee Name"],
        "Business Unit": emp["Business Unit"],
        "Department": emp["Department"],
        "Designation": emp["Designation"],
        "Date Of Joining": emp["Date Of Joining"],
        "Current Experience": current_exp,
        "Past Experience": past_exp,
        "Total Experience": total_exp
    })
pd.DataFrame(experience_reports).to_csv("experience_report.csv", index=False)

# Daily Attendance Report
attendance = []
for emp in employees:
    for day in date_range:
        clock_in = datetime.combine(day, datetime.min.time()) + timedelta(hours=random.randint(8, 10), minutes=random.randint(0, 59))
        clock_out = clock_in + timedelta(hours=random.uniform(7.0, 9.0))
        total_hours = round_hours(clock_out - clock_in)
        attendance.append({
            "Date": day.date(),
            "Employee Code": emp["Employee Code"],
            "Employee Name": emp["Employee Name"],
            "Clock-In Time": clock_in.strftime("%H:%M:%S"),
            "Clock-Out Time": clock_out.strftime("%H:%M:%S"),
            "Total Hours": total_hours
        })
pd.DataFrame(attendance).to_csv("attendance_report_daily.csv", index=False)

# Timesheet Report (Daily Project-wise)
projects = [{"Project ID": f"PRJ{pid:03d}", "Project Name": fake.bs().title()} for pid in range(1, 6)]
timesheets = []
for emp in employees:
    for day in date_range:
        hours_left = 8.0
        while hours_left > 0:
            proj = random.choice(projects)
            hrs = round(random.uniform(1.0, min(4.0, hours_left)), 2)
            timesheets.append({
                "Date": day.date(),
                "Employee Code": emp["Employee Code"],
                "Project ID": proj["Project ID"],
                "Project Name": proj["Project Name"],
                "Hours Worked": hrs
            })
            hours_left -= hrs
pd.DataFrame(timesheets).to_csv("timesheet_report.csv", index=False)
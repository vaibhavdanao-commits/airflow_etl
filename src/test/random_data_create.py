import csv
import random
from datetime import date, timedelta
from pathlib import Path

output_path = Path("/mnt/data/employees_100.csv")
random.seed(42)

first_names = ["Aarav","Aditi","Akash","Amit","Ananya","Arjun","Bhavna","Chetan","Deepak","Diya","Gaurav","Isha","Karan","Kavya","Manish","Meera","Neha","Nikhil","Pooja","Pranav"]
last_names = ["Sharma","Patel","Kumar","Singh","Verma","Gupta","Joshi","Mehta","Shah","Reddy"]
departments = ["Data Engineering","Software Engineering","HR","Finance","Sales","Marketing","Operations","IT","Business Analytics","Quality Assurance"]
statuses = ["Active","Active","Active","On Leave","Inactive"]
locations = ["Hyderabad","Bangalore","Pune","Mumbai","Delhi","Chennai","Gurgaon","Noida","Ahmedabad","Kolkata"]

start_date = date(2019, 1, 1)
end_date = date(2026, 8, 31)
days = (end_date - start_date).days

with output_path.open("w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["employee_id","name","department","salary","status","location","joining_date"])
    for i in range(1, 101):
        writer.writerow([
            f"EMP{i:05d}",
            f"{random.choice(first_names)} {random.choice(last_names)}",
            random.choice(departments),
            random.randrange(30000, 150001, 5000),
            random.choice(statuses),
            random.choice(locations),
            (start_date + timedelta(days=random.randint(0, days))).isoformat()
        ])

print(f"Created: {output_path}")
print("Records: 100")
print("Columns: employee_id, name, department, salary, status, location, joining_date")

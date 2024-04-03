import requests

url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
file_name = "exchange_rate.csv"

r = requests.get(url)

if r.status_code == 200:
    with open(file_name, 'wb') as f:
        f.write(r.content)

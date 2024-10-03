import pysolr
import json
import pandas as pd
solr = pysolr.Solr('http://localhost:8983/solr/ganesh_core', always_commit=True)


try:
    response = solr.ping()
    response_dict = json.loads(response)
    
    if response_dict['responseHeader']['status'] == 0:
        print('Connection to Solr server successful!')
    else:
        print('Connection to Solr server failed.')
except Exception as e:
    print(f'Connection to Solr server failed: {e}')


df=pd.read_csv("C:\\Users\\poovi\\Desktop\\hashagile\\bot\\Employee.csv",encoding="latin")
k=0
for index,row in df.iterrows():
   
   doc = {
    "id": row["Employee ID"],
    "Full Name": row["Full Name"],
    "Job Title":row["Job Title"],
    "Department":row["Department"],
    "Business Unit":row["Business Unit"],
    "Gender":row["Gender"],
    "Ethnicity":row["Ethnicity"],
    "Age":row["Age"],
    "Hire Date":row["Hire Date"],
    "Annual Salary":row["Annual Salary"],
    "Bonus":row["Bonus %"],
    "Country":row["Country"],
    "City":row["City"],
    "Exit Date":row["Exit Date"]

}


try:
    solr.add([doc])
    print("Document added successfully!")
except Exception as e:
    print(f"Failed to add document: {e}")

solr.commit()
try:
    response = solr.ping()
    print('Connection to Solr server successful!')
except Exception as e:
    print(f'Connection to Solr server failed: {e}')
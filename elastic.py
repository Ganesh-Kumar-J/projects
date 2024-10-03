import pysolr
import pandas as pd
import requests

# Solr connection setup
def get_solr_connection(collection_name):
    return pysolr.Solr(f'http://localhost:8985/solr/{collection_name}', always_commit=True)

# Create a new Solr collection
def createCollection(p_collection_name):
    solr_url = 'http://localhost:8985/solr/admin/cores'
    params = {
        'action': 'CREATE',
        'name': p_collection_name,
        'instanceDir': p_collection_name,
        'configSet': '_default'  # Use the _default config set
    }
    try:
        response = requests.get(solr_url, params=params)
        if response.status_code == 200:
            print(f"Collection '{p_collection_name}' created successfully!")
        else:
            print(f"Failed to create collection '{p_collection_name}'. Status Code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"An error occurred while creating the collection: {e}")

# Index data into the specified collection, excluding the specified column
def indexData(p_collection_name, p_exclude_column):
    solr = get_solr_connection(p_collection_name)
    # Example CSV file; replace this with the actual file path or input
    file = 'employee_data.csv'  # Ensure you have this CSV file in the same directory
    df = pd.read_csv(file, encoding="latin")
    docs = []
    
    for index, row in df.iterrows():
        doc = {
            "id": row["Employee ID"],
            "Full Name": row["Full Name"],
            "Job Title": row["Job Title"],
            "Department": row["Department"],
            "Business Unit": row["Business Unit"],
            "Gender": row["Gender"],
            "Ethnicity": row["Ethnicity"],
            "Age": row["Age"],
            "Hire Date": row["Hire Date"],
            "Annual Salary": row["Annual Salary"],
            "Bonus": row["Bonus %"],
            "Country": row["Country"],
            "City": row["City"]
        }

        # Exclude the specified column
        if p_exclude_column in doc:
            del doc[p_exclude_column]

        # Handle the Exit Date
        if pd.notnull(row["Exit Date"]):
            try:
                exit_date = pd.to_datetime(row["Exit Date"], format="%m/%d/%Y")
                doc["Exit Date"] = exit_date.strftime('%Y-%m-%d')
            except ValueError:
                pass
        
        docs.append(doc)

    try:
        solr.add(docs)
        solr.commit()
        print(f"Indexed {len(docs)} documents into '{p_collection_name}'.")
    except Exception as e:
        print(f"Error adding documents: {e}")

# Search for records in the specified collection
def searchByColumn(p_collection_name, p_column_name, p_column_value):
    solr = get_solr_connection(p_collection_name)
    query = f"{p_column_name}:{p_column_value}"
    results = solr.search(query)
    return results

# Get the employee count for the specified collection
def getEmpCount(p_collection_name):
    solr = get_solr_connection(p_collection_name)
    results = solr.search("*:*", rows=0)  # rows=0 returns only the count
    return results.hits

# Delete an employee by ID
def delEmpById(p_collection_name, p_employee_id):
    solr = get_solr_connection(p_collection_name)
    try:
        solr.delete(id=str(p_employee_id))
        solr.commit()
        print(f"Employee with ID '{p_employee_id}' deleted successfully from '{p_collection_name}'.")
    except Exception as e:
        print(f"Error deleting employee with ID '{p_employee_id}': {e}")

# Get department facets for the specified collection
def getDepFacet(p_collection_name):
    solr = get_solr_connection(p_collection_name)
    results = solr.search("*:*", **{
        'facet': 'true',
        'facet.field': 'Department',
        'facet.mincount': 1,
        'rows': 0
    })
    facets = results.facets['facet_fields']['Department']
    return dict(zip(facets[::2], facets[1::2]))

# Main execution
if __name__ == "__main__":
    v_nameCollection = 'Hash_Gane'
    v_phoneCollection = 'Hash_1234'

    # Create collections
    createCollection(v_nameCollection)
    createCollection(v_phoneCollection)

    # Get employee counts
    emp_count_name = getEmpCount(v_nameCollection)
    print(f"Total Employees in '{v_nameCollection}': {emp_count_name}")

    # Index data into collections
    indexData(v_nameCollection, 'Department')
    indexData(v_phoneCollection, 'Gender')

    # Delete an employee by ID
    delEmpById(v_nameCollection, 'E02003')

    # Get employee count again
    emp_count_name_after_delete = getEmpCount(v_nameCollection)
    print(f"Total Employees in '{v_nameCollection}' after deletion: {emp_count_name_after_delete}")

    # Search by columns
    search_result_department = searchByColumn(v_nameCollection, 'Department', 'IT')
    print(f"Search results for Department='IT' in '{v_nameCollection}': {[result for result in search_result_department]}")

    search_result_gender = searchByColumn(v_nameCollection, 'Gender', 'Male')
    print(f"Search results for Gender='Male' in '{v_nameCollection}': {[result for result in search_result_gender]}")

    search_result_department_phone = searchByColumn(v_phoneCollection, 'Department', 'IT')
    print(f"Search results for Department='IT' in '{v_phoneCollection}': {[result for result in search_result_department_phone]}")

    # Get department facets
    facets_name = getDepFacet(v_nameCollection)
    print(f"Department facets for '{v_nameCollection}': {facets_name}")

    facets_phone = getDepFacet(v_phoneCollection)
    print(f"Department facets for '{v_phoneCollection}': {facets_phone}")

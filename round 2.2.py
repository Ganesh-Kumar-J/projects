import streamlit as st
import pysolr
import pandas as pd


solr = pysolr.Solr('http://localhost:8983/solr/ganesh_core', always_commit=True)

def search_employee_data(query):
    """Search employee data in Solr based on the provided query."""
    results = solr.search(query)
    return results

def fetch_all_data():
    """Fetch all employee data from Solr."""
    results = solr.search('*:*')
    return results

def upload_employee_data(file):
    """Upload employee data from CSV file to Solr."""
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
        return len(docs)
    except Exception as e:
        st.error(f"Error adding documents: {e}")
        return 0

st.title("Employee Management System")


st.header("Employee Data")
results = fetch_all_data()
if results:
    data = [result for result in results]
    df_results = pd.DataFrame(data)
    df_results.drop(columns=['_version_', 'root'], errors='ignore', inplace=True)
    st.dataframe(df_results)
else:
    st.warning("No employee data found.")


st.header("Upload Employee Data")
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
if uploaded_file is not None:
    num_docs_added = upload_employee_data(uploaded_file)
    if num_docs_added > 0:
        st.success(f"Successfully added {num_docs_added} documents to Solr.")


st.header("Search Employee Data")
columns = df_results.columns.tolist() if not df_results.empty else []
selected_column = st.selectbox("Select Column", options=columns)
search_term = st.text_input("Enter search term")

if st.button("Search"):
    if selected_column and search_term:

        search_query = f"{selected_column}:{search_term}*"
        results = search_employee_data(search_query)
        if results:
            data = [result for result in results]
            df_results = pd.DataFrame(data)
            df_results.drop(columns=['_version_', 'root'], errors='ignore', inplace=True)
            st.dataframe(df_results)
        else:
            st.warning("No results found.")

if __name__ == "__main__":
    st.write("Streamlit app is running.")

import streamlit as st
import pandas as pd
import duckdb
import os
from openai import OpenAI
from dotenv import load_dotenv
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError

# --- 1. SETUP ---
# Load environment variables (for OPENAI_API_KEY)
load_dotenv()

# Initialize OpenAI Client
try:
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
except Exception as e:
    st.error(f"Failed to initialize OpenAI client. Is OPENAI_API_KEY set? Error: {e}")
    st.stop()

# Initialize BigQuery Client
try:
    # This requires the 'service-account.json' file in your project directory
    if not os.path.exists("service-account.json"):
        st.error("`service-account.json` not found. Please add it to your project directory.")
        st.stop()
    bq_client = bigquery.Client.from_service_account_json("service-account.json")
except Exception as e:
    st.error(f"Failed to initialize BigQuery client. Error: {e}")
    st.stop()


# --- 2. DATA LOADING (with Streamlit caching) ---
@st.cache_data(ttl=3600) # Cache data for 1 hour
def load_data_from_bigquery(table_id, limit=1000):
    """Loads data from a BigQuery table into a pandas DataFrame."""
    try:
        query = f"SELECT * FROM `{table_id}` LIMIT {limit}"
        # Use bqstorage_client for faster downloads
        df = bq_client.query(query).to_dataframe(create_bqstorage_client=True)
        return df
    except GoogleAPICallError as e:
        st.error(f"Error querying BigQuery: {e}")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred while loading data: {e}")
        return None

@st.cache_data
def load_field_definitions(filepath="field_definitions_farmers.txt"):
    """Loads field definitions from the text file."""
    try:
        with open(filepath, "r") as f:
            return f.read()
    except FileNotFoundError:
        st.warning(f"'{filepath}' not found. Context will be limited to column names.")
        return ""

# --- 3. MAIN APPLICATION ---
st.title("💬 Ask your BigQuery data")

# -- App Inputs & Data Loading --
# Using a fixed table from your notebook, but this could be a st.text_input
bq_table_id = "dev-project-431208.cic_india_data.farmers"
st.info(f"Querying table: `{bq_table_id}`")

df = load_data_from_bigquery(bq_table_id)
field_definitions = load_field_definitions()

if df is None or df.empty:
    st.warning("Could not load data from BigQuery. Please check the table ID and your credentials.")
    st.stop()

st.success(f"Successfully loaded {len(df)} rows from BigQuery.")
user_question = st.text_input("Enter your question:")

if user_question:
    # --- Step 1: Generate SQL Query from Natural Language ---
    with st.spinner("Generating SQL query..."):
        system_prompt_sql = f"""You are an expert data analyst. Convert the natural language question into a SQL query.
Use DuckDB-compatible SQL on a DataFrame named `df`.
Only return the SQL query, with no other text.

Available columns and their definitions:
{field_definitions}

The table `df` also has these columns: {', '.join(df.columns)}

Response:
```sql
"""
        try:
            response_1 = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt_sql},
                    {"role": "user", "content": user_question}
                ],
                temperature=0.0
            )
            sql_query = response_1.choices[0].message.content
            sql_query = sql_query.strip().replace("```sql", "").replace("```", "").strip()

            st.subheader("Generated SQL Query")
            st.code(sql_query, language="sql")

        except Exception as e:
            st.error(f"Error generating SQL with OpenAI: {e}")
            st.stop()

    # --- Step 2: Execute SQL Query on the DataFrame ---
    with st.spinner("Executing query..."):
        try:
            query_result_df = duckdb.query(sql_query).to_df()
            st.subheader("Query Result")
            st.dataframe(query_result_df)
        except Exception as e:
            st.error(f"DuckDB SQL Error: {e}")
            st.stop()

    # --- Step 3: Generate Natural Language Answer from the result ---
    with st.spinner("Generating answer..."):
        context = query_result_df.to_csv(index=False)
        system_prompt_answer = "You are a helpful analyst. Answer the user's question based on the provided data table. Be concise."
        
        try:
            response_2 = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt_answer},
                    {"role": "user", "content": f"Question: {user_question}\n\nData:\n{context}"}
                ],
                temperature=0.3
            )
            answer = response_2.choices[0].message.content
            st.subheader("Answer")
            st.write(answer)
        except Exception as e:
            st.error(f"Error generating answer with OpenAI: {e}")
            st.stop()
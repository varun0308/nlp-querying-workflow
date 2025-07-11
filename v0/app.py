from openai import OpenAI
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
import duckdb
import os
from google.cloud import bigquery
import random

# Load API key
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Set up BigQuery client
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service-account.json"
bq_client = bigquery.Client()

@st.cache_data
def load_data():
    """Load data from BigQuery instead of local CSV"""
    table_ref = "dev-project-431208.cic_india_data.farmers"
    query = f"SELECT * FROM `{table_ref}` LIMIT 1000"  # Adjust limit as needed
    df = bq_client.query(query).to_dataframe(create_bqstorage_client=False)
    return df

@st.cache_data
def load_field_definitions():
    with open("field_definitions_farmers.txt", "r", encoding="utf-8") as f:
        field_definitions = f.read()
    return field_definitions

df = load_data()
field_definitions = load_field_definitions()

st.title("💬 Ask your data in Natural Language")
user_question = st.text_input("Enter your question")

if user_question:
    system_sql = f"""You are an expert data analyst. Convert the following natural language question into a SQL query. 
Use DuckDB-compatible SQL syntax on the table called `df`. 

Available columns and their definitions are: {field_definitions}

Return only the SQL query, no other text, as this response will directly try to execute the query.

Respose:
```sql"""

    response_1 = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_sql},
            {"role": "user", "content": user_question}
        ],
        temperature=0.0
    )
    sql_query = response_1.choices[0].message.content
    sql_query = sql_query.replace("```sql", "").replace("```", "")
    st.subheader("Generated SQL Query")
    st.code(sql_query, language="sql")

    try:
        query_result = duckdb.query(sql_query).to_df()
        st.dataframe(query_result)
    except Exception as e:
        st.error(f"SQL error: {e}")
        st.stop()

    # context = query_result.to_dict(orient='records')
    # if len(context) > 100:
    #     context = str(random.sample(context, 20)) + '\n' + '... and so on. Total of {} such rows.'.format(len(context))
    #     st.error("The query result is too large to be sent to the LLM. Sending a sample of 20 rows.")

    # response_2 = client.chat.completions.create(
    #     model="gpt-4o-mini",
    #     messages=[
    #         {"role": "system", "content": "You are a helpful analyst. Answer the user's question based on the table after querying it. Answer it naturally and directly."},
    #         {"role": "user", "content": f"Question: {user_question}\n\nTable converted to dict:\n{context}"}
    #     ],
    #     temperature=0.1
    # )

    # st.subheader("Answer")
    # st.write(response_2.choices[0].message.content)

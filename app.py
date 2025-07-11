import streamlit as st
from google.cloud import bigquery
import json
import pandas as pd
from carbon_query_system import CarbonFootprintQuerySystem
from openai import OpenAI
from dotenv import load_dotenv
import os
from google.cloud import bigquery

# Load API key
load_dotenv()
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Set up BigQuery client
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service-account.json"

# Set up BQ client
bq_client = bigquery.Client()
query_system = CarbonFootprintQuerySystem(bq_client, openai_client)

# Load documents 
domain_doc = open("domain_knowledge.md").read()
use_case_doc = open("use_case_water_filter.md").read()
table_docs = [open("table_schemas/farmers.txt", encoding="utf-8").read(), 
              open("table_schemas/fpo.txt", encoding="utf-8").read(),
              open("table_schemas/users.txt", encoding="utf-8").read(),
              open("table_schemas/activity_devices.txt", encoding="utf-8").read(),
              open("table_schemas/activity_land_registered.txt", encoding="utf-8").read(),
              open("table_schemas/activity_ds_bottles.txt", encoding="utf-8").read(),
              open("table_schemas/activity_monitoring.txt", encoding="utf-8").read()]

st.title("'Intervention' Query System")

with st.form("query_form"):
    user_query = st.text_input("Enter your question:", placeholder="e.g. How many households have broken filters?")
    debug = st.toggle("Debug Mode")
    submit_button = st.form_submit_button("Submit")

if submit_button:
    with st.spinner("Processing..."):
        # Step-by-step query process
        tables = query_system.identify_relevant_tables_and_columns(user_query, table_docs)
        
        ir = query_system.generate_ir_and_sql(user_query, table_docs, domain_doc, use_case_doc, tables)
        sql = ir["query"]
        
        data, sample_data, status = query_system.run_bigquery(sql)
        if status == "Failed":
            st.error("Error running BigQuery. Please try again.")
            st.stop()
        # summary, code = query_system.summarize_or_return(data, user_query)
        
        response, df_to_show = query_system.rephrase_result(user_query, ir, data)
        
        st.header("AI Response")
        st.write(response)
        if len(df_to_show) > 10:
            with st.container(height=300):
                st.caption(f"Here is the table (20/{len(df_to_show)} rows)")
                st.table(df_to_show[:20])
                
        tab1, tab2, tab3 = st.tabs(["📄 Query & Response", "📊 Retrieved Data", "Debug"])

        with tab1:
            st.subheader("AI Response")
            if len(df_to_show) > 20:
                st.caption("Here is the complete table that answers your question")
                st.table(df_to_show[:20])
            else:
                pass
            st.write(response)

        with tab2:
            st.subheader("Data Preview")
            st.dataframe(pd.DataFrame(data))
            
        if debug:
            with tab3:
                st.subheader("Relevant tables")
                st.json(tables)
                st.subheader("SQL Query")
                st.code(sql, language="sql")
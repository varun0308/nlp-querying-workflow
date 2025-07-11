from typing import Dict, Any, List
import openai
from google.cloud import bigquery
import json
import openai
import re
import streamlit as st
import pandas as pd
from prompts import get_relevant_tables_prompt, generate_ir_and_sql_prompt, summarize_or_return_prompt, rephrase_result_prompt
from datetime import datetime

class CarbonFootprintQuerySystem:
    def __init__(self, bq_client, openai_client):
        self.bq_client = bq_client
        self.openai_client = openai_client
        
    def _call_llm(self, prompt: str) -> str:
        if len(prompt.split()) > 10000:
            raise ValueError("Prompt is too long")
        
        response = self.openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0.0
        )
        return response.choices[0].message.content

    def _get_table_context(self, schema_docs: List[str]):
        # Parse schema_docs into a dict: {table_name: set(columns)}
        table_columns = {}
        for schema_doc in schema_docs:
            lines = schema_doc.splitlines()
            table_name = None
            columns = set()
            for line in lines:
                if line.lower().startswith("table name:"):
                    table_name = line.split(":", 1)[1].strip()
                elif line.strip().startswith("- "):
                    col = line.strip()[2:].split(":", 1)[0].strip()
                    columns.add(col)
            if table_name:
                table_columns[table_name] = columns

        # Compose schema_docs string for prompt
        schema_docs_str = "\n".join(schema_docs)
        return schema_docs_str, table_columns
        
    def _get_full_table_names(self, tables: List[Dict[str, Any]]):
        return [{"table": "dev-project-431208.cic_india_data."+name['table'], "relevant_columns": name['relevant_columns']} for name in tables]
        
    def _get_only_relevant_columns_data(self, tables: List[Dict[str, Any]], schema_docs: List[str]) -> List[str]:                
        column_specific_doc_list = []
        # Get the relevant columns for each table from the schema docs
        for schema_doc in schema_docs:
            # Check if this table is there in selected tables
            table_info = schema_doc.split("\n")
            table_name = table_info[0].split(":")[1].strip()
            
            
            if any(t['table'] == table_name for t in tables):
                table_description = table_info[1].strip()
                table_column_definitions = table_info[3:]
                
                try:
                    table_column_definitions = {re.search(r'\- (.*?)\:', x).group(1): x for x in table_column_definitions}
                    
                    relevant_columns = [x for x in tables if x['table'] == table_name][0]['relevant_columns']
                    # Use regex match to find the columns in column definitions
                    column_definitions = []
                    for column in relevant_columns:
                        for column_name, column_definition in table_column_definitions.items():
                            if column == column_name:
                                column_definitions.append(column_definition)

                    column_specific_doc_list.append({
                        'table': table_name,
                        'description': table_description,
                        'column_definitions': column_definitions
                    })    
                except Exception as e:
                    print(e)
        
        column_specific_docs = "\n".join(
            f"- {doc['table']}: {doc['description']}\n{'\n'.join(doc['column_definitions'])}" for doc in column_specific_doc_list
        )
        return column_specific_docs
    
    def _get_tables_metadata_str(self, full_table_names: List[Dict[str, Any]]):
        tables_metadata_str = "\n".join(
            f"- {t['table']}: columns {t['relevant_columns']}" for t in full_table_names
        )
        return tables_metadata_str
            
    def identify_relevant_tables_and_columns(self, user_query: str, schema_docs: List[str]) -> List[dict]:
        schema_docs_str, table_columns = self._get_table_context(schema_docs)

        # Loop until all columns are valid
        max_attempts = 3
        attempt = 0
        feedback = ""
        while attempt < max_attempts:
            prompt = get_relevant_tables_prompt.format(
                schema_docs_str=schema_docs_str, 
                user_query=user_query, 
                feedback=feedback
            )
        
            content = self._call_llm(prompt)
            content = content.replace('```python', '').replace('```', '')
            content = eval(content)
            print(">>>", content)
            # If column is not present in the table schema, reprompt with feedback
            feedback = ""
            for t in content:
                for c in t['relevant_columns']:
                    if c not in table_columns[t['table']]:
                        feedback = f"Column {c} is not present in the table {t['table']}. Please change the column name."
                        attempt += 1
                        break
            print("Feedback >>>", feedback)
            if feedback == "":
                return content
            
            if attempt == max_attempts:
                raise ValueError("Failed to identify relevant tables and columns")
            
        return content

    def generate_ir_and_sql(self, user_query: str, schema_docs: List[str], domain_doc: str, use_case_doc: str, tables: List[Dict[str, Any]]) -> Dict[str, Any]:
        
        full_table_names = self._get_full_table_names(tables)
        column_specific_info = self._get_only_relevant_columns_data(tables, schema_docs)
        tables_metadata_str = self._get_tables_metadata_str(full_table_names)
        
        prompt = generate_ir_and_sql_prompt.format(
            domain_doc=domain_doc,
            use_case_doc=use_case_doc,
            column_specific_docs=column_specific_info,
            tables_str=tables_metadata_str,
            user_query=user_query,
            today_date=datetime.now().strftime("%Y-%m-%d")
        )
        response = self._call_llm(prompt)
        print("Response>>>", response)
        response = response.replace('```json', '').replace('```', '')
        return json.loads(response)

    def run_bigquery(self, query: str) -> List[Dict[str, Any]]:
        try:
            df = self.bq_client.query(query).to_dataframe(create_bqstorage_client=False)
        except Exception as e:
            print(f"Error running BigQuery: {e}")
            return [], [], "Failed"
        sample = df.head(5) if len(df) > 25 else df
        return df, sample.to_dict(orient="records"), "Success"

    def summarize_or_return(self, df: pd.DataFrame, user_query: str) -> Any:
        """
        If df has < 20 rows, return as is.
        If >= 20 rows, ask OpenAI for a Python code snippet to summarize the table for the user query,
        execute it, and return the result.
        """
        if len(df) < 20:
            return df, ""

        # Prepare sample data (first 3 rows)
        sample_data = df.head(3).to_dict(orient="records")
        metadata = f"Number of rows: {len(df)}\nNumber of columns: {len(df.columns)}"
        prompt = summarize_or_return_prompt.format(
            user_query=user_query,
            sample_data=json.dumps(sample_data, indent=2),
            columns=", ".join(df.columns),
            table_meta_data=metadata
        )

        response = self._call_llm(prompt)
        code = response.replace('```python', '').replace('```', '').strip()

        # Evaluate the code in a restricted namespace
        local_vars = {'df': df}
        try:
            exec(code, {}, local_vars)
            result = local_vars['_summarize'](df)
            return result, code
        except Exception as e:
            result = f"Error executing summary code: {e}\nCode was:\n{code}"
        return df, code
       
    def rephrase_result(self, user_query: str, ir: Dict[str, Any], df: List[Dict[str, Any]]) -> str:
        sample_data = df[:10]
        metadata = f"Number of rows: {len(df)}\nNumber of columns: {len(df.columns)}"
        if len(df) > 10:
            directions  = "You have been provided with a sample of data. The user will be shown the complete table separately. You may prompt the user to refer the complete table to understand better."
        else:
            directions = ""
        prompt = rephrase_result_prompt.format(
            user_query=user_query,
            sample_data=json.dumps(sample_data, default=str, indent=2),
            table_meta_data=metadata,
            directions=directions
        )
        response = self._call_llm(prompt)
        
        if len(df) > 10:
            return response, df
        else:
            return response, sample_data
    
    def handle_user_query(self, user_query: str, schema_docs: List[str], domain_doc: str, use_case_doc: str) -> str:
        # Step 1: Decide relevant tables
        tables = self.identify_relevant_tables_and_columns(user_query, schema_docs)

        # Step 2: Generate IR and SQL query
        ir = self.generate_ir_and_sql(user_query, schema_docs, domain_doc, use_case_doc, tables)
        sql_query = ir["query"]

        # Step 3: Execute SQL query
        raw_data = self.run_bigquery(sql_query)

        # Step 4: Generate natural language response
        final_output = self.rephrase_result(user_query, ir, raw_data)

        return final_output
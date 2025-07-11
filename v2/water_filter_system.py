# water_filter_system.py

from google.cloud import bigquery
from generic_ai_system import AbstractInterventionAISystem
import json
import openai
import duckdb

class WaterFilterAISystem(AbstractInterventionAISystem):
    def __init__(self, bq_project: str, bq_dataset: str, openai_client):
        self.bq_project = bq_project
        self.client = bigquery.Client(project=bq_project)
        self.dataset = bq_dataset
        self.openai_client = openai_client

    def generate_ir_from_query(self, user_query: str, domain_doc: str, use_case_doc: str, table_doc: str, db_type: str) -> dict:
        prompt = f"""
        You are an assistant that converts user questions about a carbon intervention program into structured queries using domain-specific terminology.

        You are always provided two documents:

        Domain Document — defines terms like 'agency', 'household', 'intervention', 'activity'

        Use Case Document — maps phrases like 'filter', 'broken', or 'replacement' to internal filters and attributes

        Return your answer in JSON format only using the following structure:

        {{
        "intent": "...", # e.g., "count", "retrieve"
        "entity": "...", # e.g., "household", "activity",
        "filters": {{ # domain-based filtering conditions
        ...
        }},
        "query": "...", # The {db_type} query.
        "output_template": "..." # natural language output description
        }}

        DOMAIN DOCUMENT:
        {domain_doc}

        USE CASE DOCUMENT:
        {use_case_doc}

        TABLE SCHEMA:
        {table_doc}
        
        USER QUERY:
        {user_query}
        
        BigQuery Path of table (Use full path in FROM):
        {self.bq_project}.{self.dataset}
        """
        
        response = self.openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
        {"role": "system", "content": "You are a domain-aware assistant that converts user queries into structured intermediate representations."},
        {"role": "user", "content": prompt}
        ],
        temperature=0.0
        )

        try:
            content = response.choices[0].message.content
            content = content.replace('```json', '').replace('```', '')
            print(">>> LLM RESPONSE: ", json.loads(content))
            print("-"*25)
            return json.loads(content)
        except Exception as e:
            raise ValueError(f"Failed to parse LLM response: {e}\nRaw content:\n{content}")

    def convert_ir_to_backend_query(self, ir: dict) -> str:
        return ir['query']
        # where_clauses = []
        # if filters := ir.get("filters"):
        #     if "intervention_type" in filters:
        #         where_clauses.append(f"intervention_type = '{filters['intervention_type']}'")
        #     if "activity_type" in filters:
        #         where_clauses.append(f"activity_type = '{filters['activity_type']}'")
        #     if "date_range" in filters:
        #         # Placeholder translation, ideally should use dateutil or similar
        #         where_clauses.append("activity_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)")

        # where_str = " AND ".join(where_clauses)
        # query = f"SELECT COUNT(*) as count FROM `{self.dataset}.activities` WHERE {where_str} LIMIT 100"
        
        # return query

    def run_backend_query(self, query: str) -> int:
        print(">>> QUERY: ", query)
        print("-"*25)
        # query_job = self.client.query(query)
        # results = query_job.result()
        results = self.client.query(query).to_dataframe(create_bqstorage_client=False)
        print(">>> QUERY RESULT: ", results)
        print("-"*25)
        return results

    # def rephrase_output(self, user_query: str, ir: dict, raw_result: Any) -> str:
    #     return f"{raw_result} {ir.get('output_template', 'interventions')} distributed last month."

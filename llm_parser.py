import json
import openai


class LLMParser:
    def __init__(self, llm_client):
        self.llm = llm_client

    def parse_query(self, domain_doc: str, use_case_doc: str, user_query: str) -> dict:
        prompt = self._build_prompt(domain_doc, use_case_doc, user_query)
        response = self.llm.chat_completion(prompt)
        try:
            return json.loads(response.strip())
        except json.JSONDecodeError:
            raise ValueError(f"LLM returned invalid JSON: {response}")

    def _build_prompt(self, domain_doc, use_case_doc, user_query):
        return f"""
You are an AI system that maps natural language queries to structured domain-level instructions.
Use the following reference documents.

--- DOMAIN DOCUMENT ---
{domain_doc}

--- USE CASE DOCUMENT ---
{use_case_doc}

--- USER QUERY ---
{user_query}

--- OUTPUT FORMAT (strict JSON using only domain terms) ---
{{
  "intent": "count | retrieve | aggregate",
  "entity": "household | activity | intervention",
  "filters": {{ 
    "intervention_type": "...",
    "activity_type": "...",
    "usage_status": "...",
    "duration_days": 14,
    "date_range": "last_month"
  }},
  "output_template": "water_filters | stoves | interventions"
}}
"""

    def generate_sql_query(self, domain_doc, use_case_doc, schema_doc, user_query, structured_json=None):
        prompt = self._build_sql_prompt(domain_doc, use_case_doc, schema_doc, user_query, structured_json)
        response = self.llm.chat_completion(prompt)
        return self._extract_sql(response)
    
    def _build_sql_prompt(self, domain_doc, use_case_doc, schema_doc, user_query, structured_json):
        return f"""
    You are a SQL assistant for a BigQuery dataset about carbon interventions (like water filters, stoves, etc.).

You are given:

A domain document that defines system terms

A use-case document that maps terms like "filters" to domain fields

A BigQuery table schema (column names and types)

A user query in natural language

An optional structured JSON (parsed filters, intent)

Use these to generate a correct BigQuery SQL query that answers the user query using the table.

DO NOT provide any commentary — only return the SQL query.

--- DOMAIN DOCUMENT ---
{domain_doc}

--- USE CASE DOCUMENT ---
{use_case_doc}

--- TABLE SCHEMA ---
{schema_doc}

--- USER QUERY ---
{user_query}

--- STRUCTURED JSON ---
{json.dumps(structured_json, indent=2) if structured_json else "None"}

--- SQL QUERY ---
"""
    
    def _extract_sql(self, response: str) -> str:
        if "```sql" in response:
            return response.split("```sql")[1].split("```")[0].strip()
        return response.strip()
    
class OpenAIWrapper:
    def __init__(self, model="gpt-4o", temperature=0, api_key=None):
        self.model = model
        self.temperature = temperature
        self.api_key = api_key

    def chat_completion(self, prompt: str) -> str:
        client = openai.OpenAI(api_key=self.api_key) if self.api_key else openai
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You convert user queries into structured domain schema using only the domain and use-case documents."},
                {"role": "user", "content": prompt}
            ],
            temperature=self.temperature
        )
        return response.choices[0].message.content

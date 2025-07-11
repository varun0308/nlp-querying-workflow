get_relevant_tables_prompt = """
Given the following BigQuery table schemas:

{schema_docs_str}

Which tables and their columns are relevant to answer the following question?
"{user_query}"

Ensure that the columns are present in the table schema.
{feedback}

Return a python list of dicts as follows:
[
    {{
        'table': 'farmers',
        'relevant_columns': ['first_name', 'area_code' ...]
    }},
    {{
        'table': 'distribution',
        'relevant_columns': ['distribution_date' ...]
    }}
]

and nothing else

```python
"""

generate_ir_and_sql_prompt = """
You are an expert in writing SQL queries. Given the following inputs:

DOMAIN DOCUMENT:
{domain_doc}

USE CASE DOCUMENT:
{use_case_doc}

TABLES TO USE with relevant columns: 
{tables_str}

TABLE SCHEMA (with definitions of relevant columns):
{column_specific_docs}

USER QUERY:
{user_query}

Note: 
1. TIMESTAMP_SUB with YEAR as the interval unit is not allowed when working with TIMESTAMP type values.
BigQuery only supports certain units (like MICROSECOND, MILLISECOND, SECOND, MINUTE, HOUR, DAY) for TIMESTAMP_SUB. YEAR and MONTH are only supported with DATE or DATETIME types—not TIMESTAMP
Hence use it like this if applicable: SELECT * FROM table
WHERE created_at >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR))
2. When combining multiple SELECT DISTINCT queries, use UNION ALL instead of just UNION, as BigQuery requires an explicit keyword (ALL or DISTINCT) after UNION. Avoid syntax errors.
3. If needed, use this info. Today's date is(YYYY-MM-DD): {today_date}.

Return a JSON object with the following keys:
- intent: e.g. "count", "retrieve"
- entity: the primary domain entity
- filters: dictionary of structured filter conditions
- query: SQL query (use the complete table names and relevant columns)
Ensure that the response can be parsed by json.loads()

```json
"""

summarize_or_return_prompt = """
You are a Python data analyst. A user asked the following question about a table:

"{user_query}"

Here are the first 3 rows of the table already queried (as a list of dicts, each dict is a row):

{sample_data}

Here is a meta data of the table:
{table_meta_data}

Write a Python function snippet (do not include imports) that takes a pandas DataFrame called 'df' that is similar to the one above
and returns a summary (DataFrame) that best answers the user's question based on the table.
If the code is multiline, then split it with a newline. Only output the function, nothing else.

The available columns in the dataframe are: {columns}

Example:
```python
def _summarize(df):
    return df['cost'].sum()
```

```python
"""

rephrase_result_prompt = """
You are a helpful assistant. A user asked the following question:

"{user_query}"

Here is a sample of the data retrieved:
{sample_data}

Here is a meta data of the table:
{table_meta_data}

{directions}

Generate a helpful answer to the user's question using this data. Keep it concise.
Do not include statements like "Feel free to ask me anything else", just answer the question.
"""

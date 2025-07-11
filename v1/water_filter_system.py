from llm_parser import LLMParser, OpenAIWrapper
from ai_query_system import BigQueryQuerySystem
from google.cloud import bigquery
import os
from dotenv import load_dotenv
from openai import OpenAI

# Load API key
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def get_table_schema_str(client, full_table_id):
    table = client.get_table(full_table_id)
    schema = "\n".join([f"{field.name} {field.field_type}" for field in table.schema])
    return f"Table {full_table_id}:\n{schema}"

if __name__ == "__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service-account.json"

    domain_doc = open("domain_knowledge.md").read()
    use_case_doc = open("use_case_water_filter.md").read()
    user_query = "How many filters were given out last month?"

    bq_client = bigquery.Client()
    full_table_id = "dev-project-431208.cic_india_data.farmers"

    schema_doc = get_table_schema_str(bq_client, full_table_id)

    config = {
        "llm_parser": LLMParser(OpenAIWrapper(model="gpt-4o-mini", temperature=0)),
        "bq_client": bq_client,
        "collection_map": {
            "activity": "activity_logs",
            "household": "households"
        },
        "bq_table_map": {
            "activity_logs": full_table_id,
            "households": "dev-project-431208.cic_india_data.farmers"
        }
    }

    system = BigQueryQuerySystem(config)
    result = system.handle_user_query(user_query, domain_doc, use_case_doc, schema_doc)
    print("Final Output:", result)
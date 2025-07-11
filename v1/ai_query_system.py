from abc import ABC, abstractmethod

class BaseInterventionQuerySystem(ABC):
    def __init__(self, config):
        self.config = config
        self.llm_parser = config["llm_parser"]
        self.db_client = config["bq_client"]
        self.collection_map = config["collection_map"]
        self.bq_table_map = config["bq_table_map"]

    @abstractmethod
    def handle_user_query(self, user_query: str, domain_doc: str, use_case_doc: str) -> str:
        structured = self.llm_parser.parse_query(domain_doc, use_case_doc, user_query)
        db_query = self.build_db_query(structured)
        raw_result = self.execute_query(db_query)
        return self.format_output(structured, raw_result)

    def build_db_query(self, structured):
        filters = structured["filters"]
        collection = self.collection_map.get(structured["entity"], structured["entity"])
        return {
            "collection": collection,
            "operation": "count_documents" if structured["intent"] == "count" else "find",
            "query": self.transform_filters(filters)
        }

    def transform_filters(self, filters):
        # Converts domain filters to DB filters
        query = {}
        if "intervention_type" in filters:
            query["intervention.type"] = filters["intervention_type"]
        if "activity_type" in filters:
            query["activity.type"] = filters["activity_type"]
        if "usage_status" in filters:
            query["usage.status"] = filters["usage_status"]
        if "duration_days" in filters:
            query["usage.last_used"] = {"$lte": f"NOW - {filters['duration_days']}d"}
        return query

    # @abstractmethod
    # def execute_query(self, db_query: dict):
    #     collection = db_query["collection"]
    #     query_filters = db_query["query"]
    #     operation = db_query["operation"]
    #     table = self.config["bq_table_map"].get(collection, collection)

    #     where_clauses = []
    #     for field, value in query_filters.items():
    #         if isinstance(value, dict) and "$lte" in value:
    #             # Handle NOW - duration_days logic
    #             where_clauses.append(f"{field} <= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {value['$lte'].split('-')[-1]} DAY)")
    #         else:
    #             where_clauses.append(f"{field} = '{value}'")

    #     where_clause = " AND ".join(where_clauses)

    #     if operation == "count_documents":
    #         query = f"SELECT COUNT(*) as count FROM `{table}` WHERE {where_clause}"
    #         result = self.config["bq_client"].query(query).result()
    #         return list(result)[0].count

    #     raise NotImplementedError(f"Operation '{operation}' not supported")

    def format_output(self, structured, raw_result):
        output_template = structured.get("output_template", "interventions")
        intent = structured["intent"]
        entity = structured["entity"]

        if intent == "count":
            return f"{raw_result} {output_template} {entity}s match the criteria."
        return str(raw_result)

class BigQueryQuerySystem(BaseInterventionQuerySystem):
    def handle_user_query(self, user_query: str, domain_doc: str, use_case_doc: str, schema_doc: str) -> str:
        # Step 1: LLM parses the user query into structured format
        structured = self.llm_parser.parse_query(domain_doc, use_case_doc, user_query)

        # Step 2: LLM generates SQL query based on schema + structure
        sql_query = self.llm_parser.generate_sql_query(
            domain_doc, use_case_doc, schema_doc, user_query, structured
        )

        # Step 3: Execute SQL
        raw_result = self.run_sql(sql_query)

        # Step 4: Format result
        return self.format_output(structured, raw_result)

    def run_sql(self, query: str):
        print(">>>", query)
        result = self.db_client.query(query).to_dataframe(create_bqstorage_client=False)
        # rows = list(result)
        # if not rows:
        #     return "No results"
        return result
        # return rows[0][0] if len(rows[0]) == 1 else dict(rows[0])

    def format_output(self, structured, raw_result):
        output_template = structured.get("output_template", "interventions")
        intent = structured["intent"]
        entity = structured["entity"]

        if intent == "count":
            return f"{raw_result} {output_template} {entity}s match the criteria."
        return str(raw_result)
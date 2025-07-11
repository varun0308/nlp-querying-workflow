from abc import ABC, abstractmethod
from typing import Dict, Any

class AbstractInterventionAISystem(ABC):
    """
    Base AI system for handling queries related to interventions.
    Contains only one implemented method: handle_user_query.
    All use-case specifics are abstracted out.
    """

    def handle_user_query(self, user_query: str, domain_doc: str, use_case_doc: str, table_doc: str, db_type: str) -> str:
        # Step 1: Get domain-native intermediate representation from LLM
        ir = self.generate_ir_from_query(user_query, domain_doc, use_case_doc, table_doc, db_type)

        # Step 2: Convert IR to actual query (SQL/BigQuery/Pandas/Mongo)
        query = self.convert_ir_to_backend_query(ir)

        # Step 3: Run the query
        raw_result = self.run_backend_query(query)

        return raw_result

    @abstractmethod
    def generate_ir_from_query(self, user_query: str, domain_doc: str, use_case_doc: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def convert_ir_to_backend_query(self, ir: Dict[str, Any]) -> str:
        pass

    @abstractmethod
    def run_backend_query(self, query: str) -> Any:
        pass


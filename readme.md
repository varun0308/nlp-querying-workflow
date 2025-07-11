# NLP Table Query System for Carbon Footprint Interventions

## Overview

This project provides an **NLP-powered query system** for agencies distributing biomass-reducing interventions (such as water filters or LPG stoves) to rural households and farmers. The system enables users to ask natural language questions about intervention data—such as monitoring, usage, and distribution—and receive answers directly from a BigQuery database.

The goal is to help agencies track the impact of their interventions, monitor usage, identify issues (like filter replacements), and ultimately quantify carbon credits earned by reducing biomass usage.

---

## Key Concepts

- **Agency**: The organization responsible for deploying interventions (e.g., water filters) to households/farmers.
- **Intervention**: Any product or tool distributed to reduce biomass usage (e.g., water filter, LPG stove).
- **Farmers/Households**: The beneficiaries who receive interventions.
- **Activity**: Events in the lifecycle of an intervention, such as onboarding, distribution, monitoring, replacement, and usage tracking.
- **Monitoring**: Ongoing checks to ensure interventions are used and maintained properly.

---

## Example Use Cases

- How many farmers have KYC
- How many farmers were onboarded last week / month
- How many farmers don’t have a mobile number
- How many farmers were distributed waterfilters in last three months. Give me a weekly summary

---

## How It Works

1. **Natural Language Query Input**:  
   Users enter their questions in plain English via a web interface (Streamlit app).

2. **Table & Column Identification**:  
   The system uses an LLM (OpenAI) to analyze the question and identify relevant tables and columns from the database schema.

3. **SQL Generation**:  
   The LLM generates an intermediate representation (IR) and a corresponding SQL query tailored to the user's question.

4. **BigQuery Execution**:  
   The generated SQL is executed on the agency's BigQuery database.

5. **Result Summarization & Rephrasing**:  
   The system summarizes and rephrases the results into a user-friendly response, optionally showing a preview of the data.

---

## Project Structure

- `app.py`: Streamlit web app for user interaction.
- `carbon_query_system.py`: Core logic for LLM prompting, SQL generation, and result handling.
- `prompts.py`: Prompt templates for LLM interactions.
- `table_schemas/`: Text files describing the schema of each table in the database.
- `domain_knowledge.md`: Domain-specific definitions and concepts.
- `use_case_water_filter.md`: Context and rationale for the water filter intervention.

---

## Getting Started

1. **Install Requirements**  
   Ensure you have Python 3.8+ and install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. **Set Up Credentials**  
   - Place your Google Cloud BigQuery service account JSON as `service-account.json`.
   - Set your OpenAI API key in a `.env` file:
     ```
     OPENAI_API_KEY=your_openai_key_here
     ```

3. **Run the App**  
   ```bash
   streamlit run app.py
   ```

4. **Ask Questions!**  
   Use the web interface to ask questions about your intervention data.

---

## Customization

- **Schema Adaptation**:  
  Update the files in `table_schemas/` to match your actual database tables and columns.
- **Domain Knowledge**:  
  Edit `domain_knowledge.md` and `use_case_water_filter.md` to provide more context for the LLM.

---

## Why This Matters

By enabling agencies to query their intervention data in natural language, this system:
- Lowers the barrier for data-driven decision making.
- Helps track and maximize the impact of interventions.
- Supports accurate carbon credit calculations.
- Improves transparency and accountability in rural development projects.
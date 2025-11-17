import os
import json
import argparse

# LangChain imports
from langchain_anthropic import ChatAnthropic
from langchain.prompts import ChatPromptTemplate
from langchain.chains import LLMChain
from langchain_core.output_parsers.json import JsonOutputParser

from pprint import pprint


def create_chains(llm):
    template = """
    Act as a data scientist. Here is a DBT model:
    {sql_content}

    Document this model in terms of business requirements.
    Document transformations for the source models used in this model.
    Document each column in this model.
    IMPORTANT: don't include any text in your responce other then JSON format
    IMPORTANT: make sure to escape all special characters in your JSON responce including newlines
    
    {metadata_context}

    Response should be in JSON format:
    {{
        "description": "Your model description here",
        "business_logic": "Transformations for the model",
        "columns": [
            {{"name": "column 1", "description": "column description", "business_logic": "business logic"}},
            {{"name": "column 2", "description": "column description", "business_logic": "business logic"}} 
        ]
    }}
    """

    table_description_prompt = ChatPromptTemplate.from_template(template)


    # Create the chains with output parsers
    table_description_chain = LLMChain(
        llm=llm,
        prompt=table_description_prompt,
        output_parser=JsonOutputParser()
    )

    return table_description_chain


def process_dbt_model(table_chain, model_path):
    with open(model_path, 'r') as f:
        sql_content = f.read()

    model_name = os.path.splitext(os.path.basename(model_path))[0]

    table_info = table_chain.invoke({
        "model_name": model_name,
        "sql_content": sql_content,
        "metadata_context": None
    })

    return table_info


MODEL = "claude-3-7-sonnet-20250219"
TEMPERATURE = 0.2
API_KEY = os.environ.get('ANTHROPIC_API_KEY')
if not API_KEY:
    raise ValueError("ANTHROPIC_API_KEY environment variable is not set")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', '-m', default='./dbt/models/intermediate/ga/ga_hit_step3.sql')
    args = parser.parse_args()

    llm = ChatAnthropic(
        model=MODEL,
        anthropic_api_key=API_KEY,
        temperature=TEMPERATURE
    )

    table_chain = create_chains(llm)
    table_info = process_dbt_model(table_chain, args.model)

    # Create output directory if it doesn't exist
    os.makedirs('out', exist_ok=True)

    model_name = os.path.splitext(os.path.basename(args.model))[0]
    with open(f'out/{model_name}.json', 'w') as f:
        json.dump(table_info, f, indent=2)
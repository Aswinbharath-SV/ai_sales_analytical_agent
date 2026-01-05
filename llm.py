import openai
import json

# SET YOUR OPENAI API KEY HERE
openai.api_key = "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"

def extract_query_details(question):
    prompt = f"""
Extract intent and region from the question.
Return JSON only.
Question: "{question}"
Example output:
{{"intent":"REVENUE_DROP","region":"South India"}}
"""
    response = openai.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role":"user", "content": prompt}]
    )
    return json.loads(response.choices[0].message.content)

def explain_result(data):
    prompt = f"""
Explain this sales summary in simple business language:
{data}
"""
    response = openai.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role":"user", "content": prompt}]
    )
    return response.choices[0].message.content

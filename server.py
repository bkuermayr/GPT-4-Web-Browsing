from flask import Flask, request, jsonify
import time
import json
from fetch_web_content import WebContentFetcher
from retrieval import EmbeddingRetriever
from llm_answer import GPTAnswer
from locate_reference import ReferenceLocator

app = Flask(__name__)

@app.route('/api/query', methods=['POST'])
def process_query():
    data = request.get_json()
    query = data.get('query')
    output_format = data.get('output_format', "")
    profile = data.get('profile', "")

    # Fetch web content based on the query
    web_contents_fetcher = WebContentFetcher(query)
    web_contents, serper_response = web_contents_fetcher.fetch()

    # Retrieve relevant documents using embeddings
    retriever = EmbeddingRetriever()
    relevant_docs_list = retriever.retrieve_embeddings(web_contents, serper_response['links'], query)
    content_processor = GPTAnswer()
    formatted_relevant_docs = content_processor._format_reference(relevant_docs_list, serper_response['links'])

    # Measure the time taken to get an answer from the GPT model
    start = time.time()

    # Generate answer from ChatOpenAI
    ai_message_obj = content_processor.get_answer(query, formatted_relevant_docs, serper_response['language'], output_format, profile)
    answer = ai_message_obj.content + '\n'
    end = time.time()

    response = {
        'answer': answer,
        'gpt_answer_time': end - start,
    }

    return jsonify(response)

if __name__ == "__main__":
    app.run(debug=True)
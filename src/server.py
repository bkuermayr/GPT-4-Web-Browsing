from flask import Flask, request, jsonify
import time
import json
from fetch_web_content import WebContentFetcher
from retrieval import EmbeddingRetriever
from llm_answer import GPTAnswer
from locate_reference import ReferenceLocator
import logging


app = Flask(__name__)


@app.route('/api/query', methods=['POST'])
def process_query():
    logging.basicConfig(level=logging.INFO)
    data = request.get_json()
    query = data.get('search_query', '')
    prompt = data.get('prompt', '')
    output_format = data.get('output_format', "")
    profile = data.get('profile', "")

    logging.info(f'Received query: {query}')
    logging.info(f'Received prompt: {prompt}')

    # Fetch web content based on the query
    print("Fetching web content: " + query + "\n")
    web_contents_fetcher = WebContentFetcher(query=query)
    web_contents, serper_response = web_contents_fetcher.fetch()

    # Retrieve relevant documents using embeddings
    retriever = EmbeddingRetriever()
    relevant_docs_list = retriever.retrieve_embeddings(web_contents, serper_response['links'], query)
    content_processor = GPTAnswer()
    formatted_relevant_docs = content_processor._format_reference(relevant_docs_list, serper_response['links'])

    # Measure the time taken to get an answer from the GPT model
    start = time.time()

    # Generate answer from ChatOpenAI
    output_language = data.get('output_language', serper_response['language'])
    ai_message_obj = content_processor.get_answer(prompt, formatted_relevant_docs, output_language, output_format, profile)
    # Generate answer from ChatOpenAI
    ai_message_obj = content_processor.get_answer(prompt, formatted_relevant_docs, output_language, output_format, profile)
    full_answer = ai_message_obj.content + '\n'
    answer, _, _ = full_answer.rpartition('\n\n')

    end = time.time()

    logging.info(f'Generated answer in {end - start} seconds')

    # Optional Part: display the reference sources of the quoted sentences in LLM's answer
    locator = ReferenceLocator(full_answer, serper_response)
    reference_cards = locator.locate_source()

    response = {
        'answer': answer,
        'gpt_answer_time': end - start,
        'output_language': output_language,
        'reference_cards': reference_cards,
    }
    return jsonify(response)


@app.route('/')
def hello():
    return 'Hello, World!'

if __name__ == "__main__":
    app.run(debug=True)
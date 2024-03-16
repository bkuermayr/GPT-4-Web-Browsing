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
    search_location = data.get('search_location', "")
    search_language = data.get('search_language', "")
    output_language = data.get('output_language', "")
    use_web_search = data.get('use_web_search', True)

    logging.info(f'Received query: {query}, search_location: {search_location}, search_language: {search_language}, output_language: {output_language}')
    logging.info(f'Received prompt: {prompt}')

    max_retries = 3
    for attempt in range(max_retries):
        try:
            if(use_web_search):
                # Fetch web content based on the query
                print("Fetching web content: " + query + "\n")
                web_contents_fetcher = WebContentFetcher(query=query, search_location=search_location, search_language=search_language, output_language=output_language)
                web_contents, serper_response = web_contents_fetcher.fetch()

                # Retrieve relevant documents using embeddings
                retriever = EmbeddingRetriever()
                relevant_docs_list = retriever.retrieve_embeddings(web_contents, serper_response['links'], query)
                content_processor = GPTAnswer()
                formatted_relevant_docs = content_processor._format_reference(relevant_docs_list, serper_response['links'])
            else:
                formatted_relevant_docs = None
                serper_response = None

            # Measure the time taken to get an answer from the GPT model
            start = time.time()

            # Generate answer from ChatOpenAI
            output_language = data.get('output_language', serper_response['language'])
            ai_message_obj = content_processor.get_answer(prompt, formatted_relevant_docs, output_language, output_format, profile)
            answer = ai_message_obj.content + '\n'
            end = time.time()

            logging.info(f'Generated answer in {end - start} seconds')

            # Optional Part: display the reference sources of the quoted sentences in LLM's answer
            locator = ReferenceLocator(answer, serper_response)
            reference_cards = locator.locate_source()

            response = {
                'answer': answer,
                'gpt_answer_time': end - start,
                'output_language': output_language,
                'reference_cards': reference_cards,
            }

            return jsonify(response)

        except Exception as e:
            logging.error(e)
            if attempt < max_retries - 1:  # i.e. not the last attempt
                time.sleep(2)  # wait for 2 seconds before trying again
            else:
                # after the last attempt, return an error
                return jsonify({'error': str(e)})


@app.route('/')
def hello():
    return 'Hello, World!'

if __name__ == "__main__":
    app.run(debug=True)
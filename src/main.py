from fetch_web_content import WebContentFetcher
from retrieval import EmbeddingRetriever
from llm_answer import GPTAnswer
from locate_reference import ReferenceLocator
import time
import json

if __name__ == "__main__":
    query = "TaylorMade - Hybrid - Qi10"
    prompt = "Was sind die Produkthighlights vom  TaylorMade - Hybrid - Qi10? Schreibe daraufhin eine Produktbeschreibung"
    output_format = "json" # User can specify output format
    profile = "expert for product data and product description copywriter for a renowned ecommerce company. " # User can define the role for LLM
    output_language = "de-de"

    # Fetch web content based on the query
    web_contents_fetcher = WebContentFetcher(query)
    web_contents, serper_response = web_contents_fetcher.fetch()

    # Retrieve relevant documents using embeddings
    retriever = EmbeddingRetriever()
    relevant_docs_list = retriever.retrieve_embeddings(web_contents, serper_response['links'], query)
    content_processor = GPTAnswer()
    formatted_relevant_docs = content_processor._format_reference(relevant_docs_list, serper_response['links'])
    print(formatted_relevant_docs)

    # Measure the time taken to get an answer from the GPT model
    start = time.time()

    # Generate answer from ChatOpenAI
    ai_message_obj = content_processor.get_answer(query, formatted_relevant_docs, output_language, output_format, profile)
    answer = ai_message_obj.content + '\n'
    end = time.time()
    print("\n\nGPT Answer time:", end - start, "s")

    # Optional Part: display the reference sources of the quoted sentences in LLM's answer
    # 
    # print("\n\n", "="*30, "Refernece Cards: ", "="*30, "\n")
    # locator = ReferenceLocator(answer, serper_response)
    # reference_cards = locator.locate_source()
    # json_formatted_cards = json.dumps(reference_cards, indent=4)
    # print(json_formatted_cards)
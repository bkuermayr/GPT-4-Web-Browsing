import time
import os
import yaml
from fetch_web_content import WebContentFetcher
from retrieval import EmbeddingRetriever
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain.schema import HumanMessage
from langchain.callbacks.streaming_stdout import StreamingStdOutCallbackHandler
#from langchain_community.callbacks import get_openai_callback
from dotenv import load_dotenv
import validators
import logging


class GPTAnswer:
    TOP_K = 12  # Top K documents to retrieve

    def __init__(self):
        # Load configuration from a YAML file
        # Load configuration from .env file
        load_dotenv()
        config_path = os.path.join(os.path.dirname(__file__), 'config', 'config.yaml')
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        self.model_name = os.getenv("MODEL_NAME")
        self.api_key = os.getenv("OPENAI_API_KEY")

    def _format_reference(self, relevant_docs_list, link_list):
        link_list = []
        for temp in relevant_docs_list:
            url = temp.metadata["url"]
            if(url not in link_list):
                link_list.append(url)
        try:
            num_docs = min(len(relevant_docs_list), self.TOP_K)
            reference_url_list = [(relevant_docs_list[i].metadata)['url'] for i in range(num_docs)]
            reference_content_list = [relevant_docs_list[i].page_content for i in range(num_docs)]
            reference_index_list = [link_list.index(link)+1 for link in reference_url_list if link in link_list]      
            rearranged_index_list = self._rearrange_index(reference_index_list)

            formatted_reference = "\n"
            for i in range(num_docs):
                formatted_reference += ('Webpage[' + str(rearranged_index_list[i]) + '], url: ' + reference_url_list[i] + ':\n' + reference_content_list[i] + '\n\n\n')
            return formatted_reference

        except Exception as e:
            print("Exception while formatting reference: ", e)
            return None


    def _rearrange_index(self, original_index_list):
        # Rearrange indices to ensure they are unique and sequential
        index_dict = {}
        rearranged_index_list = []
        for index in original_index_list:
            if index not in index_dict:
                index_dict.update({index: len(index_dict)+1})
                rearranged_index_list.append(len(index_dict))
            else:
                rearranged_index_list.append(index_dict[index])
        return rearranged_index_list

    def get_answer(self, query, relevant_docs, language, profile, image_url=None, attributes = "", product_name = ""):
        # Create an instance of ChatOpenAI and generate an answer
        llm = ChatOpenAI(model_name=self.model_name, openai_api_key=self.api_key, temperature=0.0, streaming=False, callbacks=[StreamingStdOutCallbackHandler()], model_kwargs={"response_format": {"type": "json_object"}})
        
        template = self.config["template"]
        prompt_template = PromptTemplate(
            input_variables=["profile", "context_str", "language", "query","context_attributes", "product_name"],
            template=template
        )

        profile = "You are a helpful data extraction assistant." if not profile else profile
        summary_prompt = prompt_template.format(context_str=relevant_docs, language=language, query=query, profile=profile,context_attributes=attributes,product_name=product_name)
        # print("\n\nThe message sent to LLM:\n", summary_prompt)
        # print("\n\n", "="*30, "GPT's Answer: ", "="*30, "\n")
        #Variant with Base64:
        #image_data = base64.b64encode(httpx.get(image_url).content).decode("utf-8")
        message = [{"type": "text", "text": summary_prompt}]
        imageMessage = [{"type": "text", "text": summary_prompt}]
        if image_url and validators.url(image_url):
            imageMessage.append({
                "type": "image_url",
                "image_url": {"url":image_url}
                #"image_url":  {"url": f"data:image/png;base64,{image_data}"}
            })
        '''
        f = open(f"demofile{2}.txt", "w")
        f.write(f'{imageMessage.__str__()}')
        f.close()
        '''
        '''
        with get_openai_callback() as cb:
            gpt_answer = llm.invoke([HumanMessage(content = message)])
            print(cb)'''
        #print(message.__str__())
        try:
            gpt_answer = llm.invoke([HumanMessage(content=imageMessage)])
            return gpt_answer
        except Exception as e:
            logging.info(f"Image not accessible for AI: {image_url}")
            gpt_answer = llm.invoke([HumanMessage(content=message)])
            return gpt_answer

# Example usage
if __name__ == "__main__":
    content_processor = GPTAnswer()
    query = "Sunbrella Schirm"
    attributeList = "Material:96% Polyamid, 4% Elasthan \n Grössen:34, 36, 38, 40, 42, 44 \n Farbe: Gestreift (gemustert)\n Passform: Regular Fit"
    prompt = '''Schreibe eine Beschreibung, dabei soll diese aus drei Teilen bestehen: 
Attribute: Dieser Punkt darf nur mit dir übergebenen attributes aus der attributes list befüllt werden (wenn du keine bekommen hast, dann gib einen leeren Text für diesen Punkt zurück), gelistet als key-value pairs
Merkmale: Minimal drei Merkmale, maximal acht. Diese sollen wichtige Merkmale des Produktes sein, also Punkte die ihm speziell machen. 
Fliesstext: Circa 100-200 Wörter. Inkludiere die wichtigsten Feature und Benefits, sowie
eine kurze Erläuterung für wen das Produkt geeignet ist. Ende den Produktbeschreib immer mit einem “Call to Action”. Achte bei der gesamter Produktbeschreibung auf die Benutzung von relevanten Keywords, um SEO zu vereinfachen.'''
    output_format = "" # User can specify output format
    profile = "" # User can define the role for LLM

    # Fetch web content based on the query
    web_contents_fetcher = WebContentFetcher(query)
    web_contents, serper_response = web_contents_fetcher.fetch()

    # Retrieve relevant documents using embeddings
    retriever = EmbeddingRetriever()

    try:
        relevant_docs_list = retriever.retrieve_embeddings(web_contents, serper_response['links'], prompt, 7952244, 7 )
    except Exception as e:
        print("Exception while retrieving embeddings: ", e)
    formatted_relevant_docs = content_processor._format_reference(relevant_docs_list, serper_response['links'])
    # print(formatted_relevant_docs)

    # Measure the time taken to get an answer from the GPT model
    start = time.time()

    # Generate answer from ChatOpenAI
    ai_message_obj = content_processor.get_answer(prompt, formatted_relevant_docs, 'german', profile, None, attributeList, query)
    answer = ai_message_obj.content + '\n'
    print(answer)
    end = time.time()
    print("\n\nGPT Answer time:", end - start, "s")
    #print(ai_message_obj)
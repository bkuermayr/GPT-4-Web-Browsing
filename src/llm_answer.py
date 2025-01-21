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


class GPTAnswer:
    TOP_K = 15  # Top K documents to retrieve

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

    def get_answer(self, query, relevant_docs, language, output_format, profile, image_url=None, attributes = "", product_name = ""):
        # Create an instance of ChatOpenAI and generate an answer
        llm = ChatOpenAI(model_name=self.model_name, openai_api_key=self.api_key, temperature=0.0, streaming=False, callbacks=[StreamingStdOutCallbackHandler()])
        
        template = self.config["template"]
        prompt_template = PromptTemplate(
            input_variables=["profile", "context_str", "language", "query", "format","context_attributes", "product_name"],
            template=template
        )

        profile = "conscientious researcher" if not profile else profile
        summary_prompt = prompt_template.format(context_str=relevant_docs, language=language, query=query, format=output_format, profile=profile,context_attributes=attributes,product_name=product_name)
        # print("\n\nThe message sent to LLM:\n", summary_prompt)
        # print("\n\n", "="*30, "GPT's Answer: ", "="*30, "\n")
        #Variant with Base64:
        #image_data = base64.b64encode(httpx.get(image_url).content).decode("utf-8")
        message = [{"type": "text", "text": summary_prompt}]
        if image_url and validators.url(image_url):
            message.append({
                "type": "image_url",
                "image_url": {"url":image_url}
                #"image_url":  {"url": f"data:image/png;base64,{image_data}"}
            })
        '''    
        f = open(f"demofile{2}.txt", "w")
        f.write(f'{message.__str__()}')
        f.close()
        '''
        '''
        with get_openai_callback() as cb:
            gpt_answer = llm.invoke([HumanMessage(content = message)])
            print(cb)'''
        #print(message.__str__())
        gpt_answer = llm.invoke([HumanMessage(content=message)])
        return gpt_answer

# Example usage
if __name__ == "__main__":
    content_processor = GPTAnswer()
    query = "Der Spider GT X Black Putter von Taylormade"
    output_format = "" # User can specify output format
    profile = "" # User can define the role for LLM

    # Fetch web content based on the query
    web_contents_fetcher = WebContentFetcher(query)
    web_contents, serper_response = web_contents_fetcher.fetch()

    # Retrieve relevant documents using embeddings
    retriever = EmbeddingRetriever()

    try:
        relevant_docs_list = retriever.retrieve_embeddings(web_contents, serper_response['links'], query, 2854905, 80 )
    except Exception as e:
        print("Exception while retrieving embeddings: ", e)
    formatted_relevant_docs = content_processor._format_reference(relevant_docs_list, serper_response['links'])
    # print(formatted_relevant_docs)

    # Measure the time taken to get an answer from the GPT model
    start = time.time()

    # Generate answer from ChatOpenAI
    ai_message_obj = content_processor.get_answer(query, formatted_relevant_docs, 'german', output_format, profile, None, "", "")
    answer = ai_message_obj.content + '\n'
    print(answer)
    end = time.time()
    print("\n\nGPT Answer time:", end - start, "s")
    print(ai_message_obj)
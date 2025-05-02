import re
import time
import os
import unicodedata
import yaml
from DatabaseUtil import getCategoryStructure, client
from fetch_web_content import WebContentFetcher
from output_classes import GenerativeTextOutput, CategoryOutput, AttributeParentOutput, AttributeVariantOutput, TranslationOutput
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

    def get_answer(self, prompt, structure, image_url=None, use_web_search=False):
        # Create an instance of ChatOpenAI and generate an answer
        llm = ChatOpenAI(model_name=self.model_name, openai_api_key=self.api_key, streaming=False, temperature=0.2)
        llm = llm.with_structured_output(schema=structure)
        message = [{"type": "text", "text": prompt}]
        imageMessage = [{"type": "text", "text": prompt}]
        if image_url and validators.url(image_url):
            imageMessage.append({
                "type": "image_url",
                "image_url": {"url":image_url}
            })
        tools = []
        #if use_web_search:
        #   tools.append({"type": "web_search_preview"})
        try:
            gpt_answer = llm.invoke([HumanMessage(content=imageMessage)])
            return gpt_answer
        except Exception as e:
            logging.info(f"Image not accessible for AI: {image_url}")
            gpt_answer = llm.invoke([HumanMessage(content=message)])
            return gpt_answer

    def get_template_category(self, product_name, categories, attributes):
        template = self.config["template_category"]
        prompt_template = PromptTemplate(
            input_variables=["product_name", "categories", "attributes"],
            template=template
        )
        summary_prompt = prompt_template.format(product_name=product_name, categories=categories, attributes=attributes)
        return summary_prompt, CategoryOutput
    
    def get_template_generativeText(self, query, web_sources, language, profile, attributes = "", product_name = ""):
        template = self.config["template"]
        prompt_template = PromptTemplate(
            input_variables=["profile", "context_str", "language", "query","context_attributes", "product_name"],
            template=template
        )

        profile = "You are a helpful data extraction assistant." if not profile else profile
        summary_prompt = prompt_template.format(language=language, context_str=web_sources, query=query, profile=profile,context_attributes=attributes,product_name=product_name)
        return summary_prompt, GenerativeTextOutput
    
    def get_template_generativeText_single(self, query, web_sources, language, profile, attributes = "", product_name = ""):
        template = self.config["template_single"]
        prompt_template = PromptTemplate(
            input_variables=["profile", "context_str", "language", "query","context_attributes", "product_name"],
            template=template
        )

        profile = "You are a helpful data extraction assistant." if not profile else profile
        summary_prompt = prompt_template.format(language=language, context_str=web_sources, query=query, profile=profile,context_attributes=attributes,product_name=product_name)
        return summary_prompt, GenerativeTextOutput
    
    def get_template_attribute_parent(self, product_name, input_attributes, output_attributes):
        template = self.config["template_attribute_parent"]
        prompt_template = PromptTemplate(
            input_variables=["product_name", "input_attributes", "output_attributes"],
            template=template
        )
        summary_prompt = prompt_template.format(product_name=product_name, input_attributes=input_attributes, output_attributes=output_attributes)
        return summary_prompt, AttributeParentOutput
    
    def get_template_attribute_variants(self, input, output):
        template = self.config["template_attribute_variants"]
        prompt_template = PromptTemplate(
            input_variables=["input_attributes", "output_attributes"],
            template=template
        )
        summary_prompt = prompt_template.format(input_attributes=input, output_attributes=output)
        return summary_prompt, AttributeVariantOutput
    
    def get_template_translation(self, input, context, language):
        template = self.config["template_translations"]
        prompt_template = PromptTemplate(
            input_variables=["input_attributes", "context", "language"],
            template=template
        )
        summary_prompt = prompt_template.format(input_attributes=input, context=context, language=language)
        return summary_prompt, TranslationOutput
        

def clean_text(text):
    """Cleans a single text string by removing annotations, fixing Unicode errors, and normalizing spaces."""
    text = text.encode('utf-8', errors='ignore').decode('utf-8', errors="ignore")
    text = unicodedata.normalize("NFC", text)
    # Decode any incorrectly encoded Unicode escape sequences

    # Remove URLs and text inside parentheses/brackets (annotations)
    text = re.sub(r'\(.*?\)|\[.*?\]', '', text)

    # Remove non-printable control characters
    text = re.sub(r'[\u0000-\u001F\u007F-\u009F]', ' ', text)

    # Remove excessive whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    return text

def clean_data(data):
    """Cleans text input whether it's a string or a dictionary with string values."""
    if isinstance(data, str):
        return clean_text(data)
    elif isinstance(data, dict):
        return {key: clean_text(value) for key, value in data.items() if isinstance(value, str)}
    else:
        return {}

# Example usage
if __name__ == "__main__":
    content_processor = GPTAnswer()
    query = "Sunbrella Schirm"
    attributeList = "Material:96% Polyamid, 4% Elasthan \n Grössen:34, 36, 38, 40, 42, 44 \n Farbe: Gestreift (gemustert)\n Passform: Regular Fit"
    prompt = '''
    Schreibe eine Beschreibung, dabei soll diese aus drei Teilen bestehen: 
Attribute: Dieser Punkt darf nur mit dir übergebenen attributes aus der attributes list befüllt werden (wenn du keine bekommen hast, dann gib einen leeren Text für diesen Punkt zurück), gelistet als key-value pairs
Merkmale: Minimal drei Merkmale, maximal acht. Diese sollen wichtige Merkmale des Produktes sein, also Punkte die ihm speziell machen. 
Fliesstext: Circa 100-200 Wörter. Inkludiere die wichtigsten Feature und Benefits, sowie
eine kurze Erläuterung für wen das Produkt geeignet ist. Ende den Produktbeschreib immer mit einem “Call to Action”. Achte bei der gesamter Produktbeschreibung auf die Benutzung von relevanten Keywords, um SEO zu vereinfachen.Wenn die Antworten formatiert sind, benutzte HTML Formatierung'''

    # Measure the time taken to get an answer from the GPT model
    start = time.time()

    # Generate answer from ChatOpenAI
    categories = client.table('categories').select('id, title, parent_id').eq('org_id',41).execute().data
    cs = getCategoryStructure(categories)
    summary, scheme = content_processor.get_template_category(query,cs, attributeList)
    #print(summary)
    #print(scheme)
    ai_message_obj = content_processor.get_answer(summary, scheme)
    answer = ai_message_obj
    print(answer)
    end = time.time()
    print("\n\nGPT Answer time:", end - start, "s")
    #print(ai_message_obj)
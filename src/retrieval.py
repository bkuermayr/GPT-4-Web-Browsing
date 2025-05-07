import os
from fetch_web_content import WebContentFetcher
from langchain.text_splitter import RecursiveCharacterTextSplitter
#from langchain_chroma import Chroma
from langchain_community.vectorstores import SupabaseVectorStore
from langchain_openai.embeddings import OpenAIEmbeddings
from dotenv import load_dotenv
from DatabaseUtil import client as supabaseClient

class EmbeddingRetriever:
    TOP_K = 12  # Number of top K documents to retrieve

    def __init__(self):
        # Load configuration from .env file
        load_dotenv()

        # Initialize the text splitter
        self.text_splitter = RecursiveCharacterTextSplitter(separators=["\n\n"],chunk_size=1500, chunk_overlap=0)

    def retrieve_embeddings(self, contents_list: list, link_list: list, query: str, id:str, job_id: str, rule=0):
        if len(contents_list) != len(link_list):
            raise ValueError("contents_list and link_list must have the same length")

        # Pre-process contents to ensure they are suitable for splitting and embedding
        processed_contents = []
        l = []
        for i in range(0,len(contents_list)):
            if contents_list[i]:
                processed_contents.append(contents_list[i])
                l.append(link_list[i])
        '''f = open(f"demofile{id}.txt", "w")
        f.write(f'{processed_contents.__str__()} \n')
        f.close() '''
        if len(processed_contents) <= 3 and rule == 0:
            return []
        #Create metadata and prepare documents for Chroma
        metadatas = [{'url': link, 'product_id':id, 'job_id': job_id} for link in l]
        texts = self.text_splitter.create_documents(processed_contents, metadatas=metadatas)

        for document in texts:
            contLen = len(document.page_content)
            if(contLen < 150):
                texts.remove(document)
                #print(document)

        # Safely initialize and populate Chroma database
        try:
            db = SupabaseVectorStore.from_documents(
                documents=texts,
                embedding=OpenAIEmbeddings(model='text-embedding-ada-002', openai_api_key=os.getenv("OPENAI_API_KEY")),
                client=supabaseClient,
                table_name="documents",
                product_id=id
            )
            retriever = db.as_retriever(search_kwargs={"k": self.TOP_K, "filter": {"job_id" : job_id, "product_id" : id}})

            # What are key-features and usages of the product
            # What are the features and details that should be highlighted in a product description?
            result = retriever.invoke(f'What are the main features and advantages of the product?')
            #db.delete_collection()
            return result
        except Exception as e:
            print(f"An error occurred while creating or querying the PGVector database: {e}")
            return []

    def retrieveExisting(self, id:str):
        embedding = OpenAIEmbeddings(
            model='text-embedding-ada-002',
            openai_api_key=os.getenv("OPENAI_API_KEY")
        )
        try:
            # This assumes the vector store already exists in Supabase
            db = SupabaseVectorStore(
                embedding=embedding,
                client=supabaseClient,
                table_name="documents",
            )

            retriever = db.as_retriever(
                search_kwargs={"k": self.TOP_K, "filter": {"product_id": id}}
            )

            result = retriever.invoke(f'What are the main features and advantages of the product?')
            return result
        except Exception as e:
            print(f"An error occurred while creating or querying the PGVector database: {e}")
            return []

# Example usage
if __name__ == "__main__":
    query = "CHERVÒ Allista Damen-Poloshirt"
    #query = "Approach S62 GPS-Golfuhr"

    # Create a WebContentFetcher instance and fetch web contents
    web_contents_fetcher = WebContentFetcher(query)
    web_contents, serper_response = web_contents_fetcher.fetch()

    # Create an EmbeddingRetriever instance and retrieve relevant documents
    retriever = EmbeddingRetriever()
    relevant_docs_list = retriever.retrieve_embeddings(web_contents, serper_response['links'], query, 2854931, 80)
    print(f"\n\nRelevant Documents from VectorDB: {relevant_docs_list} \n")    
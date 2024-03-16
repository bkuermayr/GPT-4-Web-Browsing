import os
from fetch_web_content import WebContentFetcher
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import Chroma
from langchain_openai.embeddings import OpenAIEmbeddings
from dotenv import load_dotenv

class EmbeddingRetriever:
    TOP_K = 10  # Number of top K documents to retrieve

    def __init__(self):
        # Load configuration from .env file
        load_dotenv()

        # Initialize the text splitter
        self.text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=0)

    def retrieve_embeddings(self, contents_list: list, link_list: list, query: str):
        if len(contents_list) != len(link_list):
            raise ValueError("contents_list and link_list must have the same length")

        # Pre-process contents to ensure they are suitable for splitting and embedding
        processed_contents = [content if content and len(content) >= 50 else "No content available." for content in contents_list]

        # Create metadata and prepare documents for Chroma
        metadatas = [{'url': link} for link in link_list]
        texts = self.text_splitter.create_documents(processed_contents, metadatas=metadatas)

        # Safely initialize and populate Chroma database
        try:
            db = Chroma.from_documents(
                texts,
                OpenAIEmbeddings(model='text-embedding-ada-002', openai_api_key=os.getenv("OPENAI_API_KEY"))
            )
            retriever = db.as_retriever(search_kwargs={"k": self.TOP_K})
            return retriever.get_relevant_documents(query)
        except Exception as e:
            print(f"An error occurred while creating or querying the Chroma database: {e}")
            return []

# Example usage
if __name__ == "__main__":
    query = "What happened to Silicon Valley Bank"

    # Create a WebContentFetcher instance and fetch web contents
    web_contents_fetcher = WebContentFetcher(query)
    web_contents, serper_response = web_contents_fetcher.fetch()

    # Create an EmbeddingRetriever instance and retrieve relevant documents
    retriever = EmbeddingRetriever()
    relevant_docs_list = retriever.retrieve_embeddings(web_contents, serper_response['links'], query)

    print("\n\nRelevant Documents from VectorDB:\n", relevant_docs_list)
    
from pydantic import BaseModel, Field

class References(BaseModel):
    url: str = Field(description="The url of which the extracted_text originates from")
    extracted_text: str = Field(description="The extracted text that was used to formulate the answer from the web results.")

class GenerativeTextFlags(BaseModel):
    different: bool = Field(description="Do the web results and image depitic different products")
    usedImage: bool = Field(description="Was the image used for creating a answer")
    emptyWebResults: bool = Field(description="Were the provided web results empty.")

class GenerativeTextOutput(BaseModel):
    answer: dict = Field(description="Here come all the generated text with the answer keys")
    flags: GenerativeTextFlags = Field("Boolean Flags for determining answer trustworthiness")
    references: list[References] = Field(description="References that were used to generate the answers")

class CategoryOutput(BaseModel):
    name: str = Field(description="Name of the category you assigned to it, dont include the id in the name, use only the most subcategory, dont include the previos path")
    answer_id: int = Field(description="ID of the category you assigned to it")
    answer_path: str = Field(description="The path to the subcategory, ex: category 1 > sub 1 > subsub 2, only use names of the categories and dont include the id")

  

class AttributeParentOutput(BaseModel):
    answer: dict = Field("Here comes a json object of the filled out output attributes. The key is the attribute_name and the value, your assigned value")

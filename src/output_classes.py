from pydantic import BaseModel, Field
from typing import Annotated, Dict, Any, List, TypedDict

class References(TypedDict):
    url: str 
    extracted_text: str

class GenerativeTextFlags(TypedDict):
    different: bool = Field(False, description="Do the web results and image depitic different products")
    usedImage: bool = Field(False, description="Was the image used for creating a answer")
    emptyWebResults: bool = Field(True, description="Were you not able to use your web search capabilities.")

class GenerativeTextOutput(TypedDict):
    answer: Dict[str, Any] = Field(description="Here come all the generated text with the answer keys, they should not contain references")
    flags: GenerativeTextFlags = Field(description="Boolean Flags for determining answer trustworthiness")
    references: List[References] = Field(description="References that were used to generate the answers")

class CategoryOutput(TypedDict):
    name: str = Field(description="Name of the category you assigned to it, dont include the id in the name, use only the most subcategory, dont include the previos path")
    answer_id: int = Field(description="ID of the category you assigned to it")
    answer_path: str = Field(description="The path to the subcategory, ex: category 1 > sub 1 > subsub 2, only use names of the categories and dont include the id")

class AttributeVariantOutput(TypedDict):
    answer: Dict[str, Any] = Field(description="Test")

class AttributeParentOutput(TypedDict):
    answer: Dict[str, Any] = Field(description="Here comes a json object of the filled out output attributes. The key is the attribute_name and the value, your assigned value")

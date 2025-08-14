from pydantic import BaseModel, Field
from typing import List, Optional, Generic, TypeVar
from datetime import date, datetime

T = TypeVar('T')

class TopProduct(BaseModel):
    product_keyword: str = Field(..., description="The detected product keyword.")
    mention_count: int = Field(..., description="The number of times the product keyword was mentioned.")

class ChannelActivity(BaseModel):
    message_date: date = Field(..., description="The date of the activity.")
    message_count: int = Field(..., description="The number of messages posted on that date.")

class MessageSearchResult(BaseModel):
    message_id: int = Field(..., description="Unique ID of the Telegram message.")
    message_text: Optional[str] = Field(None, description="The full text of the message.")
    message_date: date = Field(..., description="The date the message was posted.")
    channel_name: Optional[str] = Field(None, description="The name of the Telegram channel.")

class APIResponse(BaseModel, Generic[T]):
    status: str = Field("success", description="Status of the API request.")
    message: Optional[str] = Field(None, description="A descriptive message for the response.")
    data: Optional[T] = Field(None, description="The main data payload of the response.")
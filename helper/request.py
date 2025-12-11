from pydantic import BaseModel, Field


class AccountPool(BaseModel):
    username: str = Field(...)
    password: str = Field(...)
    email: str = Field(...)
    email_pass: str = Field(...)

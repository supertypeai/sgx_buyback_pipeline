from pydantic import BaseModel, Field


class TitleBodyGeneration(BaseModel):
    title: str = Field(
        description='News title for the filing transaction'
    )
    body: str = Field(
        description='One or two paragraph news body summarizing the filing with context'
    )

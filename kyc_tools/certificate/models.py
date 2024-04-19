from pydantic import BaseModel

class certificateResponse(BaseModel):
    title: str
    corporateName: str
    documentId: str
    recordId: str
    currentOperator: str

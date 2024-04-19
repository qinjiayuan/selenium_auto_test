from pydantic import BaseModel


class PublicInfoResponse(BaseModel):
    title : str
    corporateName : str
    documentId : str
    recordId:str
    currentOperator : str

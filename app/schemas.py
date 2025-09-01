from typing import List
from pydantic import BaseModel
import datetime

class AnalysisBase(BaseModel):
    analysis_json: dict
    
class AnalysisCreate(AnalysisBase):
    pass 

class GenerateAnalysesRequest(BaseModel):
    documents: List[str]

class Analysis(AnalysisBase):
    id: int
    document_id: int
    analysis_date: datetime.datetime
    comparison_resume: str | None = None
    
    class Config:
         from_attributes = True
        
class MonitoredDocumentBase(BaseModel):
    document: str
    is_active: bool = True
    
class MonitoredDocumentCreate(MonitoredDocumentBase):
    pass

class MonitoredDocument(MonitoredDocumentBase):
    id: int
    created_at: datetime.datetime
    analyses: list[Analysis] = []

    class Config:
         from_attributes = True
         
         


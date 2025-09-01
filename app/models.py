from sqlalchemy import (Column, Integer, String, DateTime, ForeignKey, Boolean, Text, JSON)
from sqlalchemy.orm import relationship
from .database import Base
import datetime
import pytz

br_tz = pytz.timezone('America/Sao_Paulo')

class MonitoredDocument(Base):
    __tablename__ = "monitored_documents"
    
    id = Column(Integer, primary_key=True, index=True)
    document = Column(String(18), nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=lambda: datetime.datetime.now(br_tz))
    
    analyses = relationship("Analysis", back_populates="document_owner")
    
class Analysis(Base):
    __tablename__ = "analyses"
    
    id = Column(Integer, primary_key=True, index=True)
    document_id = Column(Integer, ForeignKey("monitored_documents.id"))
    analysis_date = Column(DateTime, default=lambda: datetime.datetime.now(br_tz))
    analysis_json = Column(JSON, nullable=False)
    comparison_resume = Column(JSON, nullable=True)
    prompt_tokens = Column(Integer, nullable=True)
    completion_tokens = Column(Integer, nullable=True)
    total_tokens = Column(Integer, nullable=True)
    unique_id = Column(Integer, nullable=True)
    analytics_id = Column(Integer, nullable=True)
    
    document_owner = relationship("MonitoredDocument", back_populates="analyses")
    
from sqlalchemy.orm import Session
from . import models, schemas
from sqlalchemy import desc

def get_active_documents(db: Session) -> list[models.MonitoredDocument]:
    # BUSCA DOCUMENTOS ATIVOS PARA MONITORAMENTO
    return db.query(models.MonitoredDocument).filter(models.MonitoredDocument.is_active == True).all()


def get_active_documents_from_id(db: Session, start_id: int) -> list[models.MonitoredDocument]:
    return (
        db.query(models.MonitoredDocument)
        .filter(
            models.MonitoredDocument.is_active == True,
            models.MonitoredDocument.id >= start_id  
        )
        .order_by(models.MonitoredDocument.id.asc()) 
        .all()
    )

def create_analysis(db: Session, document_id: int, analysis_data: dict, unique_id: int, analytics_id: int) -> models.Analysis:
    db_analysis = models.Analysis(
        document_id=document_id,
        analysis_json=analysis_data,
        unique_id=unique_id,
        analytics_id=analytics_id
    )
    db.add(db_analysis)
    db.commit()
    db.refresh(db_analysis)
    return db_analysis

def get_last_two_analyses(db: Session, document_id: int) -> list[models.Analysis]:
    # BUSCA AS DUAS ÚLTIMAS ANÁLISES
    return (
        db.query(models.Analysis)
        .filter(models.Analysis.document_id == document_id)
        .order_by(desc(models.Analysis.analysis_date))
        .limit(2)
        .all()
    )
    
def update_analysis_with_summary(db: Session, analysis_id: int, summary: str, usage: dict):
    # ADICIONA OS DADOS DE USO DE TOKENS
    db_analysis = db.query(models.Analysis).filter(models.Analysis.id == analysis_id).first()
    if db_analysis:
        db_analysis.comparison_resume = summary
        db_analysis.prompt_tokens = usage.get('prompt_tokens')
        db_analysis.completion_tokens = usage.get('completion_tokens')
        db_analysis.total_tokens = usage.get('total_tokens')
        db.commit()
        db.refresh(db_analysis)
    return db_analysis

def create_monitored_document(db: Session, doc: schemas.MonitoredDocumentCreate) -> models.MonitoredDocument:
    # CRIA UM NOVO DOCUMENTO PARA SER MONITORADO
    db_doc = models.MonitoredDocument(document=doc.document, is_active=doc.is_active)
    db.add(db_doc)
    db.commit()
    db.refresh(db_doc)
    return db_doc

def get_document_by_number(db: Session, document_number: str) -> models.MonitoredDocument | None:
    return db.query(models.MonitoredDocument).filter(models.MonitoredDocument.document == str(document_number)).first()

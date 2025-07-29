import asyncio
import datetime
from fastapi import FastAPI, Depends
from contextlib import asynccontextmanager
from sqlalchemy import text
from sqlalchemy.orm import Session
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from . import crud, models, schemas, services
from .database import engine, get_db, SessionLocal

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("INICIANDO A APLICAÇÃO")
    await tratum_service.get_token()
    
    scheduler = AsyncIOScheduler()
    scheduler.add_job(analysis_job, 'interval', minutes = 1)
    scheduler.start()
    print("SCHEDULER INICIADO")
    yield
    print("APLICAÇÃO ENCERRADA")
    

app = FastAPI(lifespan=lifespan)

tratum_service = services.TratumService()
comparison_service = services.ComparisonService()

# app/main.py

async def analysis_job():
    print("="*50)
    print(f"AGENDAMENTO INICIADO: {datetime.datetime.now()}")
    db = SessionLocal()
    try:
        documents = crud.get_active_documents(db)
        print(f"ENCONTRADOS {len(documents)} DOCUMENTOS PARA MONITORAR")
        
        for doc in documents:
            print(f"\n--- Processando: {doc.document} (ID: {doc.id}) ---")
            analysis_data = await tratum_service.generate_analysis(doc.document)
            if not analysis_data:
                print(f"Não foi possível gerar dados para o documento {doc.document}. Pulando.")
                continue
            new_analysis = crud.create_analysis(db, document_id=doc.id, analysis_data=analysis_data)
            last_two = crud.get_last_two_analyses(db, document_id=doc.id)
            
            if len(last_two) == 2:
                print("COMPARANDO ANALISES COM O GPT")
                new_data = last_two[0].analysis_json
                old_data = last_two[1].analysis_json
                summary, usage_info = await comparison_service.gpt_comparer(doc.document, old_data, new_data)
                print("==== RESUMO DA COMPARAÇÃO ====")
                print(summary)
                if usage_info:
                    print("--- USO DE TOKENS (OpenAI) ---")
                    print(f"   Tokens TOTAIS: {usage_info.get('total_tokens')}")
                    print("------------------------------")
                    crud.update_analysis_with_summary(db, analysis_id=new_analysis.id, summary=summary, usage=usage_info)
                else:
                    crud.update_analysis_with_summary(db, analysis_id=new_analysis.id, summary=summary, usage={})
            else:
                print("Análise inicial salva. Comparação ocorrerá no próximo ciclo.")
    finally:
        db.close()
        print("="*50)
        
@app.get("/health-check", summary="Verificação de Saúde", tags=["Status"])
def health_check(db: Session = Depends(get_db)):
    try:
        db.execute(text('SELECT 1'))
        return {"status": "OK", "database_connection": "Sucedida"}
    except Exception as e:
        return {
            "status": "Error",
            "database_connection": "Falhou",
            "detail": str(e)
        }        
    
#ADIÇÃO DE UM NOVO DOCUMENTO PRA SER MONITORADO PELO SISTEMA        
@app.post("/documents/", response_model=schemas.MonitoredDocument, status_code=201)
def add_monitored_document(doc: schemas.MonitoredDocumentCreate, db: Session = Depends(get_db)):
   return crud.create_monitored_document(db=db, doc=doc)

#LISTAGEM DE TODOS OS DOCUMENTOS MONITORADOS
@app.get("/documents/", response_model=list[schemas.MonitoredDocument]) 
def read_all_documents(db: Session = Depends(get_db)):
    return crud.get_active_documents(db)

#DISPARO DA TAREFA MANUAL PRA TESTE
@app.post("/trigger_analysis/", status_code=202)
async def trigger_analysis_now():
    asyncio.create_task(analysis_job())
    return {
        "message": "Tarefa de análise disparada em segundo plano"
    }

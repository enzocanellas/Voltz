import asyncio
import datetime
import pandas as pd
from typing import Any
from fastapi import FastAPI, Depends, HTTPException
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
    scheduler.add_job(analysis_job, 'date')
    scheduler.start()
    print("SCHEDULER INICIADO")
    yield
    print("APLICAÇÃO ENCERRADA")
    

app = FastAPI(lifespan=lifespan)

tratum_service = services.TratumService()
comparison_service = services.ComparisonService()


missing_records = [
    {'analytics_id': 42876, 'consume_unique_id': 45495, 'document': '03995458000116'},
    {'analytics_id': 42883, 'consume_unique_id': 45502, 'document': '01440847000150'},
    {'analytics_id': 42884, 'consume_unique_id': 45503, 'document': '07079993000188'},
    {'analytics_id': 42886, 'consume_unique_id': 45505, 'document': '02485314000157'},
    {'analytics_id': 42887, 'consume_unique_id': 45506, 'document': '03063198000140'},
    {'analytics_id': 42900, 'consume_unique_id': 45519, 'document': '03693847000197'},
    {'analytics_id': 42901, 'consume_unique_id': 45520, 'document': '01159435000146'},
    {'analytics_id': 42903, 'consume_unique_id': 45522, 'document': '06119918000130'},
    {'analytics_id': 42908, 'consume_unique_id': 45527, 'document': '00942557000141'},
    {'analytics_id': 42911, 'consume_unique_id': 45530, 'document': '06097792000140'},
    {'analytics_id': 42912, 'consume_unique_id': 45531, 'document': '01136104000190'},
    {'analytics_id': 42916, 'consume_unique_id': 45535, 'document': '02744470000195'}
]

example_documents = [
    {'document': '26362390000133'},
    {'document': '03693847000197'},
    {'document': '01159435000146'},
    {'document': '06119918000130'},
    {'document': '11307426000109'},
    {'document': '32290147000150'}
]


async def resync_all_analyses_task():
    print("="*50)
    print(f"TAREFA DE RESSINCRONIZAÇÃO INICIADA: {datetime.datetime.now()}")
    
    db = SessionLocal()
    try:
        all_documents = crud.get_active_documents(db)
        total_docs = len(all_documents)
        print(f"ENCONTRADOS {total_docs} DOCUMENTOS PARA RESSINCRONIZAR.")
        
        for i, doc in enumerate(all_documents):
            print(f"\n--- Processando {i + 1}/{total_docs}: {doc.document} (ID: {doc.id}) ---")
            analysis_data = await tratum_service.generate_analysis(doc.document)
            if not analysis_data:
                print(f"Não foi possível gerar dados para o documento {doc.document}. Pulando.")
                continue
            crud.create_analysis(db, document_id=doc.id, analysis_data=analysis_data)
            print(f"Análise para {doc.document} salva com sucesso no banco de dados.")

    finally:
        db.close()
        print(f"TAREFA DE RESSINCRONIZAÇÃO FINALIZADA: {datetime.datetime.now()}")
        print("="*50)


async def analysis_job():
    print("="*50)
    print(f"AGENDAMENTO INICIADO: {datetime.datetime.now()}")
    db = SessionLocal()
    try:
        documents = crud.get_active_documents(db)
        # documents = crud.get_active_documents_from_id(db, start_id=11) # Linha para testes
        print(f"ENCONTRADOS {len(documents)} DOCUMENTOS PARA MONITORAR")
        
        for doc in documents:
            print(f"\n--- Processando: {doc.document} (ID: {doc.id}) ---")
            result_data = await tratum_service.generate_analysis(doc.document)
            if not result_data or not result_data.get("analysis_json"):
                print(f"Não foi possível gerar dados para o documento {doc.document}. Pulando.")
                continue
            analysis_json = result_data["analysis_json"]
            unique_id = result_data["unique_id"]
            analytics_id = result_data["analytics_id"]
            new_analysis = crud.create_analysis(
                db, 
                document_id=doc.id, 
                analysis_data=analysis_json,
                unique_id=unique_id,
                analytics_id=analytics_id
            )
            
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
        
async def backfill_csv_task():
    print("="*50)
    print(f"TAREFA DE BACKFILL INICIADA: {datetime.datetime.now()}")
    db = SessionLocal()
    
    try:
        df_ids = pd.read_csv('ids_analytics_and_consume.csv')
        total_rows= len(df_ids)
        print(f"Encontrei {total_rows} registros no CSV para processar.")
        
        for index, row in df_ids.iterrows():
            analytics_id = int(row['analytics_id'])
            consume_unique_id = int(row['consume_unique_id'])
            document_number = str(row['document'])  
            print(f"\n--- Processando linha {index + 1}/{total_rows}: Documento {document_number} ---")
            
            monitored_doc = crud.get_document_by_number(db, document_number=document_number)
            if not monitored_doc:
                print(f"AVISO: Documento {document_number} não encontrado na tabela 'monitored_documents'. Pulando.")
                continue
            
            result_data = await tratum_service.fetch_existing_analysis(
                analytics_id=analytics_id,
                consume_unique_id=consume_unique_id,
                document_number=document_number
            )
            
            if not result_data or not result_data.get("analysis_json"):
                print(f"Não foi possível buscar os dados para a análise ID {analytics_id}. Pulando.")
                continue
            
            crud.create_analysis(
                db, 
                document_id=monitored_doc.id,
                analysis_data=result_data["analysis_json"],
                unique_id=result_data["unique_id"],
                analytics_id=result_data["analytics_id"]
            )
            print(f"Análise para o documento {document_number} (ID: {analytics_id}) salva com sucesso.")
            
    finally:
        db.close()
        print(f"TAREFA DE BACKFILL FINALIZADA: {datetime.datetime.now()}")
        print("="*50)


async def generate_specific_analyses_task(documents_to_process: list[str]):
    print("="*50)
    print(f"TAREFA DE ANÁLISE ESPECÍFICA INICIADA: {datetime.datetime.now()}")
    db = SessionLocal()
    
    try:
        total_docs = len(documents_to_process)
        print(f"Processando lista com {total_docs} documentos específicos.")
        for i, doc_number in enumerate(documents_to_process):
            print(f"\n--- Processando item {i + 1}/{total_docs}: {doc_number} ---")
            monitored_doc = crud.get_document_by_number(db, document_number=doc_number)
            if not monitored_doc:
                print(f"AVISO: Documento {doc_number} não está na lista de monitorados. Pulando.")
                continue
            result_data = await tratum_service.generate_analysis(doc_number)  
            if not result_data or not result_data.get("analysis_json"):
                print(f"Não foi possível gerar dados para o documento {doc_number}. Pulando.")
                continue
            new_analysis = crud.create_analysis(
                db,
                document_id=monitored_doc.id,
                analysis_data=result_data["analysis_json"],
                unique_id=result_data["unique_id"],
                analytics_id=result_data["analytics_id"]
            )
            print(f"Nova análise para o documento {doc_number} salva com sucesso.")
            last_two = crud.get_last_two_analyses(db, document_id=monitored_doc.id)
            if len(last_two) == 2:
                print("COMPARANDO NOVA ANÁLISE COM A ANTERIOR VIA GPT")
                new_data = last_two[0].analysis_json
                old_data = last_two[1].analysis_json
                summary, usage_info = await comparison_service.gpt_comparer(monitored_doc.document, old_data, new_data)
                
                print("==== NOVO RESUMO DA COMPARAÇÃO ====")
                print(summary)
                if usage_info:
                    print("--- USO DE TOKENS (OpenAI) ---")
                    print(f"   Tokens TOTAIS: {usage_info.get('total_tokens')}")
                    print("------------------------------")
                    crud.update_analysis_with_summary(db, analysis_id=new_analysis.id, summary=summary, usage=usage_info)
            else:
                print("Esta é a primeira análise salva para este documento. A comparação ocorrerá no próximo ciclo.")
    finally:
        db.close()
        print(f"TAREFA DE ANÁLISE ESPECÍFICA FINALIZADA: {datetime.datetime.now()}")
        print("="*50)
        
async def backfill_examples_tasks():
    print("="*50)
    print(f"TAREFA DE EXEMPLOS CRIADA: {datetime.datetime.now()}")
    db = SessionLocal()
    
    try: 
        total_rows = len(example_documents)
        print(f"Processando os {total_rows} registros selecionados")
        for example in example_documents:
            analysis_job(example)
    finally:
        db.close()
        print(f"Tarefa finalizada")
   
async def backfill_missing_task():
    print("="*50)
    print(f"TAREFA DE BACKFILL DOS FALTANTES INICIADA: {datetime.datetime.now()}")
    db = SessionLocal()
    
    try:
        total_rows = len(missing_records)
        print(f"Processando os {total_rows} registros faltantes.")
        for index, record in enumerate(missing_records):
            analytics_id = record['analytics_id']
            consume_unique_id = record['consume_unique_id']
            document_number = record['document']
            print(f"\n--- Processando item {index + 1}/{total_rows}: Documento {document_number} ---")
            monitored_doc = crud.get_document_by_number(db, document_number=document_number)
            if not monitored_doc:
                print(f"AVISO: Documento {document_number} não encontrado na tabela 'monitored_documents'. Pulando.")
                continue
            result_data = await tratum_service.fetch_existing_analysis(
                analytics_id=analytics_id,
                consume_unique_id=consume_unique_id,
                document_number=document_number
            )
            if not result_data or not result_data.get("analysis_json"):
                print(f"Não foi possível buscar os dados para a análise ID {analytics_id}. Pulando.")
                continue
            crud.create_analysis(
                db,
                document_id=monitored_doc.id,
                analysis_data=result_data["analysis_json"],
                unique_id=result_data["unique_id"],
                analytics_id=result_data["analytics_id"]
            )
            print(f"Análise para o documento {document_number} (ID: {analytics_id}) salva com sucesso.")
    finally:
        db.close()
        print(f"TAREFA DE BACKFILL DOS FALTANTES FINALIZADA: {datetime.datetime.now()}")
        print("="*50)     
        
@app.post("/maintenance/backfill-from-csv", status_code=202, tags=["Manutenção"])
async def trigger_backfill():
     print(">>> Rota de Backfill acionada.")
     asyncio.create_task(backfill_csv_task())
     return {"message": "Tarefa de backfill iniciada. Olhando os logs do servidor"}  

@app.post("examples", status_code=202, tags=["Analise"])
def trigger_example():
    print(">>> Rota de exemplos acionada.")
    asyncio.create_task(backfill_examples_tasks())
    return {"message": "Tarefa de exemplos iniciada, Olhando os logs do servidor"}
        
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
@app.post("/maintenance/resync-all", status_code=202, tags=["Manutenção"])
async def trigger_resync():
    print(">>> Rota de ressincronização acionada.")
    asyncio.create_task(resync_all_analyses_task())
    return {"message": "Tarefa de ressincronização iniciada em segundo plano."}
 
 
@app.post("/analyses/generate-specific", status_code=202, tags=["Análises"])
async def trigger_specific_analyses(request_body: schemas.GenerateAnalysesRequest):
    print(">>> Rota de geração específica acionada...")
    asyncio.create_task(generate_specific_analyses_task(request_body.documents))
    return {"message": f"Tarefa iniciada para gerar análises para {len(request_body.documents)} documentos. Monitore os logs do servidor."}

   
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

@app.get("/analysis-summary/{analytics_id}", response_model=Any, tags=["Análises"])
async def get_analysis_summary(analytics_id: int, consume_unique_id: int):

    summary_data = await tratum_service.fetch_summary_by_id(
        analytics_id=analytics_id,
        consume_unique_id=consume_unique_id
    )
    
    if not summary_data:
        raise HTTPException(
            status_code=404, 
            detail=f"Não foi possível encontrar ou buscar a análise com ID {analytics_id}. Verifique os logs para mais detalhes."
        )
        
    return summary_data

@app.post("/maintenance/backfill-missing", status_code=202, tags=["Manutenção"])
async def trigger_missing_backfill():
    print(">>> Rota de backfill dos faltantes acionada...")
    asyncio.create_task(backfill_missing_task())
    return {"message": "Tarefa de backfill para os registros faltantes iniciada."}
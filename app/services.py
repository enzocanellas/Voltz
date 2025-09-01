import httpx
import asyncio
import datetime
from openai import OpenAI
import json
from .config import settings

class TratumService:
    def __init__(self):
        self._token: str | None = None
        self._expires_at: datetime.datetime | None = None
        self._lock = asyncio.Lock()
        self.base_url = "https://search.tratum.com.br"
        self.settings = settings
        
    async def _fetch_new_token(self):
        print("=======BUSCANDO NOVO TOKEN========")
        login_url = f"{self.base_url}/v1/login"
        payload = {
            "email": self.settings.TRATUM_EMAIL,
            "password": self.settings.TRATUM_PASSWORD
        }
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(login_url, json=payload)
                response.raise_for_status()
                data = response.json()
                self._token = data.get('token')
                self._expires_at = datetime.datetime.now() + datetime.timedelta(hours=23, minutes=50)
                print(f">>> TOKEN RENOVADO. VÁLIDO ATÉ: {self._expires_at}")
            except Exception as e: 
                print(f"ERRO AO BUSCAR TOKEN: {e}")
                self._token = None
        
    async def get_token(self) -> str | None:
            async with self._lock:
                if self._token is None or datetime.datetime.now() >= self._expires_at:
                    await self._fetch_new_token()
                return self._token 
            
    async def _initiate_analysis(self, token:str, document_number: str, document_type:str) -> dict | None:
            url = f"{self.base_url}/v2/analytics"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "holderId": self.settings.TRATUM_HOLDER_ID,
                "organizationId": self.settings.TRATUM_ORGANIZATION_ID,
                "planId": self.settings.TRATUM_PLAN_ID,
                "documentType": document_type,
                "documentNumber": document_number
            }
            
            async with httpx.AsyncClient() as client:
                try:
                    print(f"[ETAPA 1/3] INICIANDO ANÁLISE PARA: {document_number}")
                    response = await client.post(url, headers=headers, json=payload, timeout=300.0)
                    response.raise_for_status()
                    data = response.json()
                    return data.get("result")
                except Exception as e:
                    print(f"ERRO AO INICIAR ANALISE PARA: {document_number} -> {e}")
                    return None
        
    async def _fetch_generic_detail(self, client: httpx.AsyncClient, url: str, headers: dict, data_key: str = "register", method: str = "GET", payload: dict | None = None) -> dict | None:
        try:
            if method.upper() == "POST":
                response = await client.post(url, headers=headers, json=payload, timeout=120.0)
            else:
                response = await client.get(url, headers=headers, timeout=120.0)
            response.raise_for_status()
            return response.json().get(data_key)
        except Exception as e:
            print(f"ERRO: A busca de dados ({method}) na {url}: {e} falhou")
            return None
        
    async def _fetch_analysis_details(self, token: str, ids: dict) -> dict | None:
            consume_unique_id = ids.get("userPlanConsumeUniqueId") 
            analytics_id = ids.get("userPlanConsumeAnalyticsId")
            
            if not all([consume_unique_id, analytics_id]):
                print("ERRO: IDS de consumo nao encontrados na resposta")
                return None
            
            url = (
                f"{self.base_url}/v2/analytics/{analytics_id}"
            )
            
            payload = {
                "holderId": self.settings.TRATUM_HOLDER_ID,
                "organizationId": self.settings.TRATUM_ORGANIZATION_ID,
                "userPlanConsumeUniqueId": consume_unique_id
                }
            
            headers = {
                "Authorization": f"Bearer {token}"
            }
            async with httpx.AsyncClient() as client:
                try:
                    print(f">>> [ETAPA 3/3] Buscando detalhes da análise ID {analytics_id}...")
                    response = await client.post(url, headers=headers, json=payload, timeout=120.0)
                    response.raise_for_status()
                    data = response.json()
                    return data.get("register")
                except Exception as e:
                    print(f"!!! ERRO ao buscar detalhes da análise: {e}")
                    return None
     

    async def fetch_summary_by_id(self, analytics_id: int, consume_unique_id: int) -> dict | None:
        token = await self.get_token()
        if not token:
            print("ERRO: Não foi possível obter o token de autenticação.")
            return None

        url = f"{self.base_url}/v2/analytics/{analytics_id}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        payload = {
            "holderId": self.settings.TRATUM_HOLDER_ID,
            "organizationId": self.settings.TRATUM_ORGANIZATION_ID,
            "userPlanConsumeUniqueId": consume_unique_id
        }

        async with httpx.AsyncClient() as client:
            try:
                print(f">>> Buscando sumário específico da análise ID {analytics_id}...")
                response = await client.post(url, headers=headers, json=payload, timeout=120.0)
                response.raise_for_status()
                data = response.json()
                return data.get("register")
            except Exception as e:
                print(f"!!! ERRO ao buscar o sumário da análise ID {analytics_id}: {e}")
                return None 
    
    async def fetch_existing_analysis(self, analytics_id: int, consume_unique_id: int, document_number: str) -> dict | None:
        token = await self.get_token()
        if not token: return None
        
        print(f">>> Buscando detalhes agregados para a análise ID {analytics_id}...")
        headers = {"Authorization": f"Bearer {token}"}
        holder_id = self.settings.TRATUM_HOLDER_ID
        org_id = self.settings.TRATUM_ORGANIZATION_ID
        
        summary_payload = {
            "holderId": holder_id,
            "organizationId": org_id,
            "userPlanConsumeUniqueId": consume_unique_id
        }
        main_summary = await self._fetch_generic_detail(
            httpx.AsyncClient(),
            url=f"{self.base_url}/v2/analytics/{analytics_id}",
            headers=headers,
            method="POST",
            payload=summary_payload
        )
        if not main_summary:
            print(f"ERRO: Falha ao buscar o sumário principal da análise {analytics_id}")
        debtor_id = main_summary.get("userPlanConsumeGovernmentDebtorSummaryId")
        
        async with httpx.AsyncClient() as client:
            tasks = {
                        "history_rfb": self._fetch_generic_detail(client, f"{self.base_url}/v1/holder/{holder_id}/organization/{org_id}/consumeunique/{consume_unique_id}/analytics/{analytics_id}/history-rfb/detail?page=1", headers),
                        "faturamento": self._fetch_generic_detail(client, f"{self.base_url}/v1/holder/{holder_id}/organization/{org_id}/consumeunique/{consume_unique_id}/analytics/{analytics_id}/activity-indicator/detail", headers),
                        "certidoes": self._fetch_generic_detail(client, f"{self.base_url}/v1/certification/fake/{analytics_id}", headers, data_key="list"),
                        "qsa": self._fetch_generic_detail(client, f"{self.base_url}/v1/holder/{holder_id}/organization/{org_id}/consumeunique/{consume_unique_id}/analytics/{analytics_id}/qsa?level=1st-LEVEL", headers, data_key="result"),
                        "relacionamentos": self._fetch_generic_detail(client, f"{self.base_url}/v1/organizations/{org_id}/qsa/{document_number}", headers=headers, method="POST")
            }
            if debtor_id: 
                tasks["divida_ativa"] = self._fetch_generic_detail(client, f"{self.base_url}/v1/holder/{holder_id}/organization/{org_id}/consumeunique/{consume_unique_id}/analytics/{analytics_id}/debtor/{debtor_id}/detail?size=100", headers)
            results = await asyncio.gather(*tasks.values())
            final_analysis = dict(zip(tasks.keys(), results))
            final_analysis["summary"] = main_summary
        print(f">>> Detalhes para análise ID {analytics_id} agregados com sucesso.")
        return {
            "analysis_json": final_analysis,
            "unique_id": consume_unique_id,
            "analytics_id": analytics_id
        }
               
    async def generate_analysis(self, document_number: str) -> dict | None:
        token = await self.get_token()
        if not token: return None

        doc_type = "CNPJ" if len(document_number) == 14 else "CPF"
        initiation_result = await self._initiate_analysis(token, document_number, doc_type)
        if not initiation_result: return None
        
        analytics_id = initiation_result.get("userPlanConsumeAnalyticsId")
        consume_unique_id = initiation_result.get("userPlanConsumeUniqueId")
        
        if not all([analytics_id, consume_unique_id]):
            print("ERRO: IDs de consumo ou de análise não encontrados.")
            return None
        
        polling_interval = 30  
        total_timeout = 360 
        start_time = datetime.datetime.now()
        main_certification = None
        main_summary = None
        
        print(f">>> [ETAPA 2/3] Iniciando polling do status da análise ID {analytics_id}...")
        
        headers = {"Authorization": f"Bearer {token}"}
        holder_id = self.settings.TRATUM_HOLDER_ID
        org_id = self.settings.TRATUM_ORGANIZATION_ID
        summary_payload = {"holderId": holder_id, "organizationId": org_id, "userPlanConsumeUniqueId": consume_unique_id}

        while (datetime.datetime.now() - start_time).total_seconds() < total_timeout:
            async with httpx.AsyncClient() as client:
                current_summary = await self._fetch_generic_detail(
                    client,
                    url=f"{self.base_url}/v2/analytics/{analytics_id}",
                    headers=headers,
                    method="POST",
                    payload=summary_payload
                )
            if current_summary:
                status = current_summary.get("status")
                elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
                print(f"--- Status atual: {status} (Tempo decorrido: {elapsed_time:.0f}s)")
                if status == "DONE":
                    print(f">>> Análise concluída com status {status}. Prosseguindo para agregação.")
                    main_summary = current_summary
                    break
            await asyncio.sleep(polling_interval)
        if not main_summary:
            print(f"ERRO: Timeout. A análise não foi concluída com 'DONE' em {total_timeout} segundos.")
            return None
        print(f">>> [ETAPA 3/3] Buscando todos os detalhes para a análise ID {analytics_id} em paralelo...")
        
        debtor_id = main_summary.get("userPlanConsumeGovernmentDebtorSummaryId")
        
        async with httpx.AsyncClient() as client:
            tasks = {
                "history_rfb": self._fetch_generic_detail(client, f"{self.base_url}/v1/holder/{holder_id}/organization/{org_id}/consumeunique/{consume_unique_id}/analytics/{analytics_id}/history-rfb/detail?page=1", headers),
                "faturamento": self._fetch_generic_detail(client, f"{self.base_url}/v1/holder/{holder_id}/organization/{org_id}/consumeunique/{consume_unique_id}/analytics/{analytics_id}/activity-indicator/detail", headers),
                "certidoes": self._fetch_generic_detail(client, f"{self.base_url}/v1/certification/fake/{analytics_id}", headers, data_key="list"),
                "qsa": self._fetch_generic_detail(client, f"{self.base_url}/v1/holder/{holder_id}/organization/{org_id}/consumeunique/{consume_unique_id}/analytics/{analytics_id}/qsa?level=1st-LEVEL", headers, data_key="result"),
                "relacionamentos": self._fetch_generic_detail(client, f"{self.base_url}/v1/organizations/{org_id}/qsa/{document_number}", headers=headers, method="POST"),
                "gerar_certidao": self._fetch_generic_detail(client, f"{self.base_url}/v1/certification/{analytics_id}", headers=headers, data_key="list")
            }
            
            if debtor_id:
                tasks["divida_ativa"] = self._fetch_generic_detail(client, f"{self.base_url}/v1/holder/{holder_id}/organization/{org_id}/consumeunique/{consume_unique_id}/analytics/{analytics_id}/debtor/{debtor_id}/detail?size=100", headers)
            
            results = await asyncio.gather(*tasks.values())
            
            final_analysis = dict(zip(tasks.keys(), results))
            final_analysis["summary"] = main_summary
        
        print(">>> Detalhes agregados com sucesso.")
        return {
            "analysis_json": final_analysis,
            "unique_id": consume_unique_id,
            "analytics_id": analytics_id
        }

class ComparisonService:
    def __init__(self):
        self.client = OpenAI(api_key=settings.OPENAI_API_KEY)
        
    def _generate_prompt(self, document: str, old_data: dict, new_data: dict) -> str:
        old_data_str = json.dumps(old_data, indent=2, ensure_ascii=False)
        new_data_str = json.dumps(new_data, indent=2, ensure_ascii=False)
        
        return f"""
        Sua tarefa é atuar como uma API. Compare os dois relatórios JSON (antigo e novo) para o documento {document} e retorne um único e válido objeto JSON.
        NÃO inclua nenhum texto explicativo antes ou depois do JSON. Sua resposta DEVE começar com `{{` e terminar com `}}`.

        O formato de saída JSON OBRIGATÓRIO é o seguinte, com as chaves (nomes dos campos) em inglês:
        {{
          "resume_description": "Um resumo em PORTUGUÊS em texto corrido, com no máximo 5 linhas, analisando as principais mudanças e o perfil de risco.",
          "general_analysis": {{
            "situation": "estável | melhora | agravamento",
            "lawsuits": {{ "count": <int>, "total_value": <float> }},
            "active_debt": {{ "count": <int>, "total_value": <float> }},
            "protests": {{ "count": <int>, "total_value": <float> }},
            "corporate_structure": {{ "has_changed": <boolean> }},
            "certificates": {{ "issues": ["<string em português>", "..."], "observation": "<string em português>" }}
          }},
          "comparison_table": [
            {{ "metric": "Total de Processos", "previous_analysis": <int|string>, "current_analysis": <int|string>, "percentage_change": "<string>" }},
            {{ "metric": "Valor Total de Processos", "previous_analysis": <float|string>, "current_analysis": <float|string>, "percentage_change": "<string>" }},
            {{ "metric": "Dívida Ativa", "previous_analysis": <int|string>, "current_analysis": <int|string>, "percentage_change": "<string>" }},
            {{ "metric": "Número de Protestos", "previous_analysis": <int|string>, "current_analysis": <int|string>, "percentage_change": "<string>" }},
            {{ "metric": "Mudança no Quadro Societário", "previous_analysis": "Não", "current_analysis": "Não", "percentage_change": "<string>" }}
          ]
        }}

        --- DADOS PARA ANÁLISE ---

        RELATÓRIO ANTIGO:
        ```json
        {old_data_str}
        ```

        RELATÓRIO NOVO:
        ```json
        {new_data_str}
        ```

        Gere o objeto JSON de resposta agora.
        """
        
    async def gpt_comparer(self, document:str, old_data:dict, new_data:dict) -> tuple[dict, dict | None]:
        prompt = self._generate_prompt(document, old_data, new_data)
        try:
            loop = asyncio.get_running_loop()
            response_object = await loop.run_in_executor(
                None,
                lambda: self.client.chat.completions.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2,
                    response_format={"type": "json_object"}
                )
            )
            
            summary_json_string = response_object.choices[0].message.content
            summary_dict = json.loads(summary_json_string)
            
            usage = response_object.usage
            usage_dict = None
            if usage:
                print("--- USO DE TOKENS (OpenAI) ---")
                print(f"   Tokens do Prompt..: {usage.prompt_tokens}")
                print(f"   Tokens da Resposta: {usage.completion_tokens}")
                print(f"   Tokens TOTAIS....: {usage.total_tokens}")
                print("------------------------------")
                usage_dict = {"prompt_tokens": usage.prompt_tokens, "completion_tokens": usage.completion_tokens, "total_tokens": usage.total_tokens}
                
            return summary_dict, usage_dict
            
        except Exception as e:
            print(f"ERRO AO CONECTAR COM A OPENAI OU PARSEAR JSON -> {e}")
            error_response = {"error": "Could not generate JSON summary.", "detail": str(e)}
            return error_response, None
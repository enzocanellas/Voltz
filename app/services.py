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
                    response = await client.post(url, headers=headers, json=payload, timeout=180.0)
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

        wait_time = 30
        print(f">>> [ETAPA 2/3] Análise iniciada. Aguardando {wait_time}s para processamento...")
        await asyncio.sleep(wait_time)
        
        print(f">>> [ETAPA 3/3] Buscando todos os detalhes para a análise ID {analytics_id} em paralelo...")
        
        headers = {"Authorization": f"Bearer {token}"}
        holder_id = self.settings.TRATUM_HOLDER_ID
        org_id = self.settings.TRATUM_ORGANIZATION_ID
        
        # <<< Criando o payload específico para a chamada do sumário >>>
        summary_payload = {
            "holderId": holder_id,
            "organizationId": org_id,
            "userPlanConsumeUniqueId": consume_unique_id
        }
        
        async with httpx.AsyncClient() as client:
            tasks = {
                "summary": self._fetch_generic_detail(
                    client,
                    url=f"{self.base_url}/v2/analytics/{analytics_id}",
                    headers=headers,
                    method="POST",
                    payload=summary_payload
                ),
                "history_rfb": self._fetch_generic_detail(client, f"{self.base_url}/v1/holder/{holder_id}/organization/{org_id}/consumeunique/{consume_unique_id}/analytics/{analytics_id}/history-rfb/detail?page=1", headers),
                "faturamento": self._fetch_generic_detail(client, f"{self.base_url}/v1/holder/{holder_id}/organization/{org_id}/consumeunique/{consume_unique_id}/analytics/{analytics_id}/activity-indicator/detail", headers),
                "certidoes": self._fetch_generic_detail(client, f"{self.base_url}/v1/certification/fake/{analytics_id}", headers, data_key="list"),
                "qsa": self._fetch_generic_detail(client, f"{self.base_url}/v1/holder/{holder_id}/organization/{org_id}/consumeunique/{consume_unique_id}/analytics/{analytics_id}/qsa?level=1st-LEVEL", headers, data_key="result"),
            }
            
            results = await asyncio.gather(*tasks.values())
            final_analysis = dict(zip(tasks.keys(), results))
        
        print(">>> Detalhes agregados com sucesso.")
        return final_analysis
class ComparisonService:
    def __init__(self):
        self.client = OpenAI(api_key=settings.OPENAI_API_KEY)
        
    def _generate_prompt(self, document:str, old_data:dict, new_data:dict) -> str:
        old_summary_str = json.dumps(old_data.get('summary', {}), indent=2, ensure_ascii=False)
        new_summary_str = json.dumps(new_data.get('summary', {}), indent=2, ensure_ascii=False)
        old_rfb_str = json.dumps(old_data.get('history_rfb', {}), indent=2, ensure_ascii=False)
        new_rfb_str = json.dumps(new_data.get('history_rfb', {}), indent=2, ensure_ascii=False)
        old_faturamento_str = json.dumps(old_data.get('faturamento', {}), indent=2, ensure_ascii=False)
        new_faturamento_str = json.dumps(new_data.get('faturamento', {}), indent=2, ensure_ascii=False)
        old_qsa_str = json.dumps(old_data.get('qsa', {}), indent=2, ensure_ascii=False)
        new_qsa_str = json.dumps(new_data.get('qsa', {}), indent=2, ensure_ascii=False)


        return f"""
         # Você é um consultor sênior de riscos da Tratum. Sua missão é transformar dados jurídicos e financeiros complexos em uma análise executiva curta, clara e estratégica, voltada a diretores que não possuem formação técnica.

 Sua tarefa é comparar dois relatórios (um anterior e um mais recente) e redigir uma análise resumida sobre os principais riscos e mudanças percebidas no período sobre o documento: {document}.

 **Regras da resposta:**
 - Utilize **Markdown** para formatar a resposta.
 - Use linguagem objetiva, madura e sem termos técnicos.
 - Faça menção a mudança de valores e quantidades nos processos
 - Não utilize estrutura JSON, bullets, listas ou jargões.
 - A análise final deve ter no máximo **5 linhas de texto corrido** (sem parágrafos separados).
 - Enfoque nos riscos jurídicos, financeiros e sinais de estabilidade ou agravamento.
 - Faça uma análise no Quadro Societário (QSA) para checar se também houve mudança
 - Ao final, - Crie uma seção `#### Quadro Comparativo de Mudanças` e apresente os dados em uma **tabela Markdown**. A tabela deve incluir colunas para "Métrica", "Valor Anterior", "Valor Atual" e "Variação %".
 ---

### RELATÓRIOS PARA ANÁLISE:

    --- RELATÓRIO ANTIGO ---
        Sumário de Processos: ```{old_summary_str}```
        Histórico da Receita Federal: ```{old_rfb_str}```
        Indicador de Faturamento: ```{old_faturamento_str}```
        Quadro Societário: ```{old_qsa_str}```
        
        --- RELATÓRIO NOVO ---
        Sumário de Processos: ```{new_summary_str}```
        Histórico da Receita Federal: ```{new_rfb_str}```
        Indicador de Faturamento: ```{new_faturamento_str}```
        Quadro Societário: ```{new_qsa_str}

    Elabore o resumo executivo agora.
    """
        
    async def gpt_comparer(self, document:str, old_data:dict, new_data:dict) -> tuple[str, dict | None]:
        prompt = self._generate_prompt(document, old_data, new_data)
        try:
            loop = asyncio.get_running_loop()
            response_object = await loop.run_in_executor(
                None,
                lambda: self.client.chat.completions.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2
                )
            )
            summary = response_object.choices[0].message.content.strip()
            usage = response_object.usage
            usage_dict = None
            if usage:
                print("--- USO DE TOKENS (OpenAI) ---")
                print(f"   Tokens do Prompt..: {usage.prompt_tokens}")
                print(f"   Tokens da Resposta: {usage.completion_tokens}")
                print(f"   Tokens TOTAIS....: {usage.total_tokens}")
                print("------------------------------")
                
                usage_dict = {
                    "prompt_tokens": usage.prompt_tokens,
                    "completion_tokens": usage.completion_tokens,
                    "total_tokens": usage.total_tokens
                }
            return summary, usage_dict
        except Exception as e:
            print(f"ERRO AO CONECTAR COM A OPENAI -> {e}")
            return "Não foi possível gerar o resumo devido a um erro.", None
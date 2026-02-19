from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import uvicorn
import os
import zipfile
from database import get_engine

app = FastAPI(title="Intuitive Care API")

# Habilitar CORS para o frontend Vue.js
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Carregar dados (prioridade MySQL)
try:
    engine = get_engine()
    print("🔌 Carregando dados do MySQL...")
    df_despesas = pd.read_sql("SELECT * FROM despesas_consolidadas", engine)
    df_ops = pd.read_sql("SELECT CNPJ, RegistroANS, Modalidade, UF FROM operadoras_ativas", engine)
    df_agregado = pd.read_sql("SELECT * FROM despesas_agregadas", engine)
    
    # Merge para garantir metadados (RazaoSocial, UF) no df_despesas se estiverem faltando
    if 'UF' not in df_despesas.columns:
         df_despesas = pd.merge(df_despesas, df_ops[['CNPJ', 'UF']], on='CNPJ', how='left')
    # Se agregados estiver vazio no banco, tentar CSV local
    if df_agregado.empty and os.path.exists('despesas_agregadas.csv'):
        try:
            df_agregado = pd.read_csv('despesas_agregadas.csv')
        except Exception:
            pass

except Exception as e:
    print(f"⚠️ Erro ao conectar MySQL: {e}. Usando arquivos locais...")
    try:
        # Tentar ler despesas (CSV ou ZIP)
        if os.path.exists('consolidado_despesas.csv'):
             df_despesas = pd.read_csv('consolidado_despesas.csv')
        elif os.path.exists('consolidado_despesas.zip'):
             with zipfile.ZipFile('consolidado_despesas.zip') as z:
                 csv_name = [n for n in z.namelist() if n.endswith('.csv')][0]
                 df_despesas = pd.read_csv(z.open(csv_name))
        else:
             df_despesas = pd.DataFrame()

        # Tentar ler agregados
        if os.path.exists('despesas_agregadas.csv'):
            df_agregado = pd.read_csv('despesas_agregadas.csv')
        else:
            df_agregado = pd.DataFrame()
            
    except Exception as e2:
        print(f"❌ Erro ao carregar arquivos locais: {e2}")
        df_despesas = pd.DataFrame()
        df_agregado = pd.DataFrame()

# Enriquecer RazaoSocial via Cadop quando estiver ausente
def _preencher_razao_social(df):
    try:
        import requests, io
        url_ativas = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
        r = requests.get(url_ativas, timeout=15)
        r.raise_for_status()
        cadop = pd.read_csv(io.BytesIO(r.content), sep=';', encoding='latin1', on_bad_lines='skip')
        cadop.columns = [c.strip() for c in cadop.columns]
        # Normalizar CNPJ
        if 'CNPJ' in cadop.columns:
            cadop['CNPJ'] = cadop['CNPJ'].astype(str).str.replace(r'\D', '', regex=True)
        # Detectar coluna de razão social
        col_razao = next((c for c in cadop.columns if 'RAZAO' in c.upper()), None)
        if not col_razao:
            col_razao = next((c for c in cadop.columns if 'NOME' in c.upper()), None)
        if 'CNPJ' in df.columns and col_razao and 'CNPJ' in cadop.columns:
            df['CNPJ'] = df['CNPJ'].astype(str).str.replace(r'\D', '', regex=True)
            df = pd.merge(df, cadop[['CNPJ', col_razao]], on='CNPJ', how='left')
            if 'RazaoSocial' not in df.columns:
                df['RazaoSocial'] = None
            df['RazaoSocial'] = df['RazaoSocial'].fillna(df[col_razao])
            df = df.drop(columns=[col_razao], errors='ignore')
    except Exception:
        pass
    # Garantir pelo menos string não nula
    if 'RazaoSocial' in df.columns:
        df['RazaoSocial'] = df['RazaoSocial'].fillna('DESCONHECIDO')
    else:
        df['RazaoSocial'] = 'DESCONHECIDO'
    return df

# Aplicar enriquecimento se necessário
if 'RazaoSocial' not in df_despesas.columns or df_despesas['RazaoSocial'].isna().any():
    df_despesas = _preencher_razao_social(df_despesas)

@app.get("/api/operadoras")
def get_operadoras(page: int = 1, limit: int = 10, search: str = None):
    df = df_despesas[['CNPJ', 'RazaoSocial']].drop_duplicates()
    
    if search:
        df = df[df['RazaoSocial'].str.contains(search, case=False) | df['CNPJ'].str.contains(search)]
    
    total = len(df)
    start = (page - 1) * limit
    end = start + limit
    
    data = df.iloc[start:end].to_dict(orient='records')
    
    return {
        "data": data,
        "total": total,
        "page": page,
        "limit": limit
    }

@app.get("/api/operadoras/{cnpj}")
def get_operadora_detail(cnpj: str):
    operadora = df_despesas[df_despesas['CNPJ'].astype(str) == cnpj].iloc[0:1]
    if operadora.empty:
        return {"error": "Operadora não encontrada"}
    return operadora.to_dict(orient='records')[0]

@app.get("/api/operadoras/{cnpj}/despesas")
def get_operadora_despesas(cnpj: str):
    despesas = df_despesas[df_despesas['CNPJ'].astype(str) == cnpj]
    return despesas.to_dict(orient='records')

@app.get("/api/estatisticas")
def get_estatisticas():
    if df_despesas.empty:
        return {}
    
    return {
        "total_despesas": float(df_despesas['ValorDespesas'].sum()),
        "media_despesas": float(df_despesas['ValorDespesas'].mean()),
        "top_5_operadoras": df_agregado.head(5).to_dict(orient='records'),
        "distribuicao_uf": df_agregado.groupby('UF')['TotalDespesas'].sum().to_dict()
    }

@app.get("/", response_class=HTMLResponse)
def serve_index():
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return "<h1>Frontend não encontrado</h1>"

@app.get("/api/debug/agregado_count")
def agregado_count():
    try:
        return {"count": int(len(df_agregado))}
    except Exception:
        return {"count": 0}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

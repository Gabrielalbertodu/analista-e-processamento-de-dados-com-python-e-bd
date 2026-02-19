import requests
import zipfile
import io
import pandas as pd
import re
from bs4 import BeautifulSoup
import os
from database import get_engine
from sqlalchemy import text

url_base = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"

# 
# OBTER DADOS DE OPERADORAS ATIVAS (PARA RAZÃO SOCIAL)
# 
def obter_cadop():
    print("Baixando dados cadastrais de operadoras (Cadop)...")
    url_ativas = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
    try:
        r = requests.get(url_ativas)
        r.raise_for_status()
        df = pd.read_csv(io.BytesIO(r.content), sep=';', encoding='latin1', on_bad_lines='skip')
        
        # Normalizar colunas
        df.columns = [c.strip().upper() for c in df.columns]
        
        # Encontrar coluna de Razão Social
        col_razao = next((c for c in df.columns if 'RAZAO' in c or 'NOME' in c), None)
        col_cnpj = next((c for c in df.columns if 'CNPJ' in c), None)
        col_reg = next((c for c in df.columns if 'REGISTRO' in c), None)
        
        if col_cnpj and col_razao:
            cols = [col_cnpj, col_razao]
            rename_map = {col_cnpj: 'CNPJ', col_razao: 'RazaoSocial_Cadop'}
            
            if col_reg:
                cols.append(col_reg)
                rename_map[col_reg] = 'Registro_ANS_Cadop'
                
            df = df[cols].copy()
            df[col_cnpj] = df[col_cnpj].astype(str).str.replace(r'\D', '', regex=True)
            df = df.rename(columns=rename_map)
            return df
        return None
    except Exception as e:
        print(f"Erro ao baixar Cadop: {e}")
        return None

# 
# LEITURA DE ARQUIVOS DENTRO DO ZIP
# 
def ler_arquivo_do_zip(meu_zip, nome_arquivo):
    try:
        with meu_zip.open(nome_arquivo) as f:
            content = f.read()
            name = nome_arquivo.lower()

            if name.endswith(".xlsx"):
                return pd.read_excel(io.BytesIO(content), engine="openpyxl")
            else:
                try:
                    return pd.read_csv(io.BytesIO(content), sep=";", encoding="utf-8", on_bad_lines="skip")
                except Exception:
                    return pd.read_csv(io.BytesIO(content), sep=";", encoding="latin1", on_bad_lines="skip")
    except Exception as e:
        print(f"    ❌ Erro ao ler {nome_arquivo}: {e}")
        return None

# 
# NORMALIZAÇÃO (VERSÃO REAL DA ANS)
# 
def normalizar(df, ano, trimestre):
    colunas = {c.upper(): c for c in df.columns}

    # CNPJ real ou REG_ANS
    col_cnpj = next((colunas[c] for c in colunas if c.startswith("CNPJ")), None)
    if not col_cnpj:
        col_cnpj = next((colunas[c] for c in colunas if c.startswith("REG_ANS")), None)

    # Qualquer coluna financeira real da ANS
    # Preferência por VL_SALDO_FINAL
    col_valor = next((colunas[c] for c in colunas if c == "VL_SALDO_FINAL"), None)
    if not col_valor:
        col_valor = next(
            (colunas[c] for c in colunas if c.startswith("VL_")),
            None
        )

    # Identificar coluna de Conta Contábil ou Descrição para filtrar Despesas
    col_conta = next((colunas[c] for c in colunas if c.startswith("CD_CONTA")), None)
    col_desc = next((colunas[c] for c in colunas if "DESC" in c), None)

    if not col_cnpj or not col_valor:
        return None

    # Filtrar apenas Despesas (Classe 4 ou contendo termos chave)
    # Se não conseguirmos identificar, assumimos que é um arquivo financeiro mas não filtramos (risco de sujeira, mas evita perda)
    # Mas o requisito pede explícitamente "Despesas com Eventos/Sinistros"
    
    if col_conta:
        # Filtrar classe 4 (Despesas)
        # Converter para string e pegar os que começam com 4
        mask_despesa = df[col_conta].astype(str).str.startswith('4')
        # Também podemos incluir 3 se for reversão, mas focaremos em 4
        df = df[mask_despesa].copy()
    elif col_desc:
        # Tentar filtrar por texto
        termos = ['DESPESA', 'EVENTO', 'SINISTRO']
        mask_despesa = df[col_desc].astype(str).str.upper().str.contains('|'.join(termos))
        df = df[mask_despesa].copy()

    if df.empty:
        return None

    df = df[[col_cnpj, col_valor]].copy()
    df.columns = ["CNPJ", "Valor"]

    df["CNPJ"] = df["CNPJ"].astype(str).str.replace(r"\D", "", regex=True)

    df["Valor"] = (
        df["Valor"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.replace("-", "", regex=False)
        .str.strip()
    )

    df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce")

    if df["Valor"].notna().sum() == 0:
        return None

    df["Ano"] = int(ano)
    df["Trimestre"] = trimestre

    return df[df["Valor"].notna()]

# 
# SALVAR RESULTADO
# 
def salvar_consolidado(df, csv_path="consolidado_despesas.csv", zip_path="consolidado_despesas.zip"):
    # Padronizar nomes de colunas para CSV e Banco
    if "Valor" in df.columns:
        df = df.rename(columns={"Valor": "ValorDespesas"})

    df.to_csv(csv_path, index=False, encoding="utf-8")

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(csv_path, arcname=os.path.basename(csv_path))

    print(f"\n✅ Arquivo final salvo em: {zip_path}")

    # Salvar no MySQL
    try:
        print("🔌 Conectando ao MySQL...")
        engine = get_engine()
        
        # Preparar DataFrame para o banco
        df_db = df.copy() # Já está renomeado
        
        # Garantir colunas
        cols_db = ["CNPJ", "RazaoSocial", "Trimestre", "Ano", "ValorDespesas"]
        for col in cols_db:
             if col not in df_db.columns:
                 df_db[col] = None
                 
        df_db = df_db[cols_db]
        
        print("💾 Inserindo dados no MySQL (tabela despesas_consolidadas)...")
        
        with engine.connect() as conn:
            try:
                if engine.dialect.name == "sqlite":
                    conn.execute(text("DELETE FROM despesas_consolidadas"))
                else:
                    conn.execute(text("TRUNCATE TABLE despesas_consolidadas"))
                conn.commit()
            except Exception:
                pass
            
        df_db.to_sql("despesas_consolidadas", engine, if_exists="append", index=False)
        print("✅ Dados salvos no MySQL com sucesso!")
                
    except Exception as e:
        print(f"❌ Erro ao salvar no MySQL: {e}")

# 
# FUNÇÃO PRINCIPAL
# 
def baixar_e_processar():
    print("Coletando dados da ANS...")

    soup = BeautifulSoup(requests.get(url_base).text, "html.parser")

    anos = sorted(
        [a["href"].strip("/") for a in soup.find_all("a", href=True)
         if re.fullmatch(r"\d{4}/?", a["href"])],
        reverse=True
    )

    dados = []
    trimestres_usados = set()

    for ano in anos:
        if len(trimestres_usados) >= 3:
            break

        url_ano = f"{url_base}{ano}/"
        soup_ano = BeautifulSoup(requests.get(url_ano).text, "html.parser")

        zips = sorted(
            [a["href"] for a in soup_ano.find_all("a", href=True)
             if a["href"].lower().endswith(".zip")],
            reverse=True
        )

        for zip_name in zips:
            if len(trimestres_usados) >= 3:
                break

            m = re.search(r"([1-4])T\d{4}", zip_name, re.I)
            if not m:
                continue

            tri = f"{m.group(1)}T"
            chave = f"{ano}-{tri}"
            if chave in trimestres_usados:
                continue

            print(f"\n📦 {zip_name}")
            r_zip = requests.get(f"{url_ano}{zip_name}")

            with zipfile.ZipFile(io.BytesIO(r_zip.content)) as z:
                for arq in z.namelist():
                    df_raw = ler_arquivo_do_zip(z, arq)
                    if df_raw is None:
                        continue

                    df_norm = normalizar(df_raw, ano, tri)
                    if df_norm is not None:
                        dados.append(df_norm)
                        print(f"   ✅ {arq} aceito (contém despesas)")
                    else:
                        print(f"   ⚠️ {arq} ignorado (sem dados de Despesas com Eventos/Sinistros)")

            trimestres_usados.add(chave)

    if not dados:
        print("❌ Nenhum dado compatível encontrado.")
        return None

    df_final = pd.concat(dados, ignore_index=True)

    # --- Lógica de Correção de ID ---
    print("Verificando consistência de identificadores (CNPJ/Registro ANS)...")
    df_cadop = obter_cadop()
    
    # 2. Se tiver CNPJ mas parecer curto (RegistroANS disfarçado), renomear
    if 'CNPJ' in df_final.columns:
        # Verifica mediana do comprimento
        sample = df_final['CNPJ'].dropna().astype(str)
        if not sample.empty and sample.map(len).median() < 10:
             print("CNPJ parece ser Registro ANS (comprimento < 10). Renomeando para Registro_ANS.")
             df_final = df_final.rename(columns={'CNPJ': 'Registro_ANS'})
    
    # 3. Se tiver Registro_ANS e tiver Cadop, mapear para CNPJ
    if 'Registro_ANS' in df_final.columns and df_cadop is not None and 'Registro_ANS_Cadop' in df_cadop.columns:
        print("Mapeando Registro_ANS para CNPJ usando Cadop...")
        # Garantir tipos compatíveis
        df_final['Registro_ANS'] = pd.to_numeric(df_final['Registro_ANS'], errors='coerce').astype('Int64').astype(str)
        df_cadop['Registro_ANS_Cadop'] = pd.to_numeric(df_cadop['Registro_ANS_Cadop'], errors='coerce').astype('Int64').astype(str)
        
        # Merge
        # Se CNPJ já existe (parcialmente), mantermos. Se não, criamos.
        if 'CNPJ' not in df_final.columns:
             df_final['CNPJ'] = None
             
        df_merged = pd.merge(df_final, df_cadop[['Registro_ANS_Cadop', 'CNPJ']], 
                             left_on='Registro_ANS', right_on='Registro_ANS_Cadop', how='left')
        
        # Preencher CNPJ onde possível
        # CNPJ_x é o original (vazio ou incompleto), CNPJ_y é o do Cadop
        if 'CNPJ_y' in df_merged.columns:
            df_merged['CNPJ_x'] = df_merged['CNPJ_x'].fillna(df_merged['CNPJ_y'])
            df_merged = df_merged.rename(columns={'CNPJ_x': 'CNPJ'})
            df_merged = df_merged.drop(columns=['CNPJ_y', 'Registro_ANS_Cadop'])
        
        df_final = df_merged

    salvar_consolidado(df_final)

    print("\n🎉 PROCESSO FINALIZADO COM SUCESSO")
    return df_final

# 
# EXECUÇÃO
# 
if __name__ == "__main__":
    baixar_e_processar()

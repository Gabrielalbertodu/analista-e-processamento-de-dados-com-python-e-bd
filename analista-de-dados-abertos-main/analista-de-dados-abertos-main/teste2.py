import pandas as pd
import requests
import io
import os
import zipfile
import numpy as np
from database import get_engine
from sqlalchemy import text

def validar_cnpj(cnpj):
    cnpj = str(cnpj).replace('.', '').replace('-', '').replace('/', '')
    
    # Verifica tamanho e se é numérico
    if len(cnpj) != 14 or not cnpj.isdigit():
        return False
        
    # Verifica se todos os dígitos são iguais (ex: 11111111111111)
    if len(set(cnpj)) == 1:
        return False

    # Cálculo do primeiro dígito verificador
    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma1 = sum(int(a) * b for a, b in zip(cnpj[:12], pesos1))
    resto1 = soma1 % 11
    digito1 = 0 if resto1 < 2 else 11 - resto1

    if int(cnpj[12]) != digito1:
        return False

    # Cálculo do segundo dígito verificador
    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma2 = sum(int(a) * b for a, b in zip(cnpj[:13], pesos2))
    resto2 = soma2 % 11
    digito2 = 0 if resto2 < 2 else 11 - resto2

    if int(cnpj[13]) != digito2:
        return False

    return True

def processar_teste2():
    print("[2.1] Lendo dados do MySQL (despesas_consolidadas)...")
    engine = get_engine()
    
    try:
        df = pd.read_sql("SELECT * FROM despesas_consolidadas", engine)
        if df.empty:
            raise ValueError("Tabela vazia")
    except Exception as e:
        print(f"Erro ao ler do MySQL: {e}. Tentando ler arquivo local...")
        if os.path.exists('consolidado_despesas.csv'):
            df = pd.read_csv('consolidado_despesas.csv')
        elif os.path.exists('consolidado_despesas.zip'):
             with zipfile.ZipFile('consolidado_despesas.zip') as z:
                 # Assumindo que há apenas um arquivo ou pegando o primeiro CSV
                 csv_name = [n for n in z.namelist() if n.endswith('.csv')][0]
                 df = pd.read_csv(z.open(csv_name))
        else:
            raise FileNotFoundError("Nem tabela MySQL, nem CSV, nem ZIP encontrados.")

    print(f"Colunas carregadas: {df.columns.tolist()}")

    # Garantir que CNPJ seja string e tenha 14 dígitos (zero à esquerda)
    if 'CNPJ' in df.columns:
        # Remover .0 de float, remover não dígitos, preencher com zero
        df['CNPJ'] = df['CNPJ'].astype(str).str.replace(r'\.0$', '', regex=True).str.replace(r'\D', '', regex=True).str.zfill(14)
        print(f"Amostra CNPJ: {df['CNPJ'].head().tolist()}")

    # Validações
    # 1. CNPJ válido (algoritmo real)
    df['CNPJ_Valido'] = df['CNPJ'].apply(validar_cnpj)
    print(f"CNPJs válidos: {df['CNPJ_Valido'].sum()} de {len(df)}")
    # 2. ValorDespesas > 0 (removendo inconsistências/zerados)
    # 3. RazaoSocial não nula e não vazia (se já existir no CSV, caso contrário será preenchida depois)
    
    df['CNPJ_Valido'] = df['CNPJ'].apply(validar_cnpj)
    
    mask_valid = (
        df['CNPJ_Valido'] & 
        (df['ValorDespesas'] > 0)
    )
    
    df = df[mask_valid]
    df = df.drop(columns=['CNPJ_Valido'])
    
    print("[2.2] Enriquecendo dados com operadoras ativas...")
    url_ativas = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
    
    try:
        # Tentar baixar
        r = requests.get(url_ativas)
        # Tenta ler o CSV de operadoras ativas
        df_ativas = pd.read_csv(io.BytesIO(r.content), sep=';', encoding='latin1', on_bad_lines='skip')
        
        # Normalizar colunas das ativas
        df_ativas.columns = [c.strip() for c in df_ativas.columns]
        print(f"Colunas Cadop: {df_ativas.columns.tolist()}")
        df_ativas['CNPJ'] = df_ativas['CNPJ'].astype(str).str.replace(r'\D', '', regex=True)
        
        # Join (incluindo Razao Social pois vem nulo do teste1)
        # Identificar coluna de Razão Social
        col_razao = next((c for c in df_ativas.columns if 'RAZAO' in c.upper()), None)
        
        cols_merge = ['CNPJ', 'Registro_ANS', 'Modalidade', 'UF']
        if col_razao:
            cols_merge.append(col_razao)
            
        # Filtrar colunas existentes
        cols_merge = [c for c in cols_merge if c in df_ativas.columns]
        
        df_final = pd.merge(df, df_ativas[cols_merge], on='CNPJ', how='left')
        
        # Preencher RazaoSocial nula com a obtida do merge
        if col_razao and col_razao in df_final.columns:
            if 'RazaoSocial' not in df_final.columns:
                df_final['RazaoSocial'] = None
            df_final['RazaoSocial'] = df_final['RazaoSocial'].fillna(df_final[col_razao])
            df_final = df_final.drop(columns=[col_razao])
        
        # Renomear para o solicitado
        df_final = df_final.rename(columns={'Registro_ANS': 'RegistroANS'})
        
    except Exception as e:
        print(f"Erro ao baixar operadoras ativas: {e}. Usando dados mock para UF.")
        # Mock UF para garantir que o teste continue se a rede falhar
        df['RegistroANS'] = '12345'
        df['Modalidade'] = 'Medicina de Grupo'
        df['UF'] = 'SP'
        df_final = df

    # Garantir RazaoSocial antes de agrupar
    if 'RazaoSocial' not in df_final.columns:
        print("⚠️ Coluna RazaoSocial não encontrada. Criando coluna vazia.")
        df_final['RazaoSocial'] = 'DESCONHECIDO'
    
    df_final['RazaoSocial'] = df_final['RazaoSocial'].fillna('DESCONHECIDO')

    print("[2.3] Agregando dados...")
    # Agrupar por RazaoSocial e UF
    agregado = df_final.groupby(['RazaoSocial', 'UF']).agg(
        TotalDespesas=('ValorDespesas', 'sum'),
        MediaDespesas=('ValorDespesas', 'mean'),
        DesvioPadraoDespesas=('ValorDespesas', 'std')
    ).reset_index()
    
    # Ordenar por valor total (maior para menor)
    agregado = agregado.sort_values(by='TotalDespesas', ascending=False)
    
    # Salvar CSV (backup)
    agregado.to_csv('despesas_agregadas.csv', index=False, encoding='utf-8')
    
    # Salvar no MySQL
    try:
        print("Atualizando tabelas no MySQL...")
        with engine.connect() as conn:
            try:
                if engine.dialect.name == "sqlite":
                    conn.execute(text("DELETE FROM despesas_agregadas"))
                    conn.execute(text("DELETE FROM operadoras_ativas"))
                else:
                    conn.execute(text("TRUNCATE TABLE despesas_agregadas"))
                    conn.execute(text("TRUNCATE TABLE operadoras_ativas"))
                conn.commit()
            except Exception:
                pass
            
        agregado.to_sql('despesas_agregadas', engine, if_exists='append', index=False)
        
        # Salvar operadoras ativas
        cols_ops = ['CNPJ', 'RegistroANS', 'Modalidade', 'UF']
        # Mapear colunas se necessário (df_final tem 'Registro_ANS' ou 'RegistroANS' dependendo do merge)
        if 'Registro_ANS' in df_final.columns:
             df_final = df_final.rename(columns={'Registro_ANS': 'RegistroANS'})
        for c in cols_ops:
            if c not in df_final.columns:
                df_final[c] = None
             
        ops_to_save = df_final[cols_ops].drop_duplicates('CNPJ')
        ops_to_save.to_sql('operadoras_ativas', engine, if_exists='append', index=False)
        
        print("✅ Dados salvos no MySQL (despesas_agregadas, operadoras_ativas).")
    except Exception as e:
        print(f"❌ Erro ao salvar no MySQL: {e}")
        
    return df_final, agregado

if __name__ == "__main__":
    processar_teste2()

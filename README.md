# 🏥 Health Data Analytics — Análise de Dados de Saúde

> **Pipeline de Dados + API + Dashboard** para análise de despesas de operadoras de planos de saúde, com foco em **KISS**, **performance** e **robustez**.

**Stack:** Python • MySQL/SQLite • FastAPI • Vue.js 3

---

## 📌 Visão Geral

Este projeto entrega uma solução **end-to-end** de **engenharia de dados** e **desenvolvimento web** para coletar, tratar e analisar dados públicos da **ANS**. Foi pensado para ser **simples de executar**, **eficiente** no processamento e **claro** para avaliação técnica.



## 🚀 Como Executar

### Pré-requisitos

* **Python** 3.10+
* **MySQL Server** *(opcional — padrão: SQLite)*
* **Dependências**:

  ```bash
  pip install pandas requests beautifulsoup4 fastapi uvicorn sqlalchemy pymysql openpyxl
  ```

### Passo a Passo

#### 1) Configuração do Ambiente

* Para **MySQL**, defina as variáveis de ambiente:

  * `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_NAME`
* Caso contrário, o projeto usa **SQLite** local (`health_data.db`).
* Execute o setup inicial:

  ```bash
  python setup_mysql.py
  ```

#### 2) Pipeline de Dados (ETL)

Execute **em ordem** para baixar, processar e popular o banco:

```bash
python teste1.py  # Extração e Carga
python teste2.py  # Transformação e Agregação
```

#### 3) API e Interface

Inicie a API:

```bash
python app.py
```

* API: `http://localhost:8000`
* Dashboard: abra `index.html` no navegador

---

## 🛠 Decisões Técnicas

### 1) Integração com a ANS (Scraping)

* **BeautifulSoup** para navegar diretórios HTML.
* Descoberta **dinâmica** de anos e trimestres (resiliente a novos dados).
* Processamento **incremental** (streaming via `zipfile`) para reduzir uso de memória.

### 2) Transformação & Validação

* **CNPJ**: validação com **módulo 11**.
* **Join**: realizado em memória com **pandas** (volume < 1M linhas) para eficiência.

### 3) Banco de Dados

* **Modelagem híbrida**:

  * *Star Schema* simplificado para agregações rápidas.
  * **3FN** para dados cadastrais.
* Tipos otimizados (`DECIMAL`) para valores monetários.

### 4) API & Frontend

* **FastAPI**: ASGI, alta performance e docs automáticas.
* **Vue.js 3 (CDN)**: simplicidade, sem *build steps* complexos.

---



## 📝 Relato Pessoal

O projeto aplica **boas práticas** de engenharia de software e análise de dados, equilibrando **performance** e **facilidade de execução**. A integração entre **Python** (processamento) e **Vue.js** (visualização) demonstra capacidade **Full Stack orientada a dados

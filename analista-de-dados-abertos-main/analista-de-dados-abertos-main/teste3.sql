-- 3.2 DDL para estruturar as tabelas
-- Usando SQLite para simplicidade no ambiente de teste, mas compatível com lógica SQL padrão

-- Tabela para dados consolidados de despesas
CREATE TABLE IF NOT EXISTS despesas_consolidadas (
    CNPJ VARCHAR(14),
    RazaoSocial VARCHAR(255),
    Trimestre VARCHAR(2),
    Ano INTEGER,
    ValorDespesas DECIMAL(18, 2),
    PRIMARY KEY (CNPJ, Trimestre, Ano)
);

-- Tabela para dados cadastrais das operadoras
CREATE TABLE IF NOT EXISTS operadoras_ativas (
    CNPJ VARCHAR(14) PRIMARY KEY,
    RegistroANS VARCHAR(20),
    Modalidade VARCHAR(100),
    UF CHAR(2)
);

-- Tabela para dados agregados
CREATE TABLE IF NOT EXISTS despesas_agregadas (
    RazaoSocial VARCHAR(255),
    UF CHAR(2),
    TotalDespesas DECIMAL(18, 2),
    MediaDespesas DECIMAL(18, 2),
    DesvioPadraoDespesas DECIMAL(18, 2)
);

-- 3.4 Queries Analíticas

-- 3.5 Queries de Importação e Tratamento de Inconsistências
-- 
-- DOCUMENTAÇÃO DE COMPATIBILIDADE:
-- 1. Sintaxe: Este arquivo usa sintaxe SQL padrão (ANSI) compatível com MySQL 8.0+ e PostgreSQL.
--    - DECIMAL(18,2) e VARCHAR são suportados em ambos.
--    - CONCAT() é suportado em ambos (PostgreSQL também aceita operador ||).
--    - CTEs (WITH) são suportadas em ambos.
-- 2. Importação (LOAD DATA vs COPY):
--    - O comando `LOAD DATA LOCAL INFILE` abaixo é específico do MySQL.
--    - Para PostgreSQL, deve-se usar o comando `COPY`.
--
-- TRATAMENTO DE INCONSISTÊNCIAS DE IMPORTAÇÃO:
-- Ao optar pelo carregamento via SQL direto (LOAD DATA), perdemos a flexibilidade de tratamento
-- linha-a-linha que o Python oferece. As inconsistências são tratadas da seguinte forma:
-- 1. Erros de Tipo: O MySQL converte valores inválidos para o padrão do tipo (0 para números) ou
--    gera avisos (warnings) dependendo do sql_mode.
-- 2. Linhas Inválidas: Podem ser descartadas usando `IGNORE 1 ROWS` para cabeçalhos ou
--    configurando tratamento de erros.
-- 3. Duplicatas: `REPLACE` ou `IGNORE` podem ser usados para gerenciar chaves primárias duplicadas.
-- 4. Justificativa: Para este projeto, optou-se pelo tratamento prévio via Python (teste1.py)
--    para garantir a integridade dos dados (validação de CNPJ mod 11, limpeza de caracteres),
--    deixando o banco apenas com a responsabilidade de armazenamento e consulta (ELT vs ETL).
--
/*
-- Importando despesas consolidadas (Exemplo MySQL)
LOAD DATA LOCAL INFILE '/path/to/consolidado_despesas.csv'
INTO TABLE despesas_consolidadas
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(CNPJ, RazaoSocial, Trimestre, Ano, @ValorDespesas)
SET ValorDespesas = CAST(REPLACE(@ValorDespesas, ',', '.') AS DECIMAL(18,2));

-- Importando operadoras ativas
LOAD DATA LOCAL INFILE '/path/to/operadoras.csv'
INTO TABLE operadoras_ativas
FIELDS TERMINATED BY ';'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(CNPJ, RegistroANS, Modalidade, UF);
*/

-- Query 1: 5 operadoras com maior crescimento percentual entre primeiro e último trimestre
-- Nota: Usando subqueries para identificar o primeiro e último valor
WITH TrimestresExtremos AS (
    SELECT 
        CNPJ,
        RazaoSocial,
        MIN(CONCAT(Ano, Trimestre)) as PrimeiroTri,
        MAX(CONCAT(Ano, Trimestre)) as UltimoTri
    FROM despesas_consolidadas
    GROUP BY CNPJ, RazaoSocial
),
ValoresExtremos AS (
    SELECT 
        te.CNPJ,
        te.RazaoSocial,
        d1.ValorDespesas as ValorInicial,
        d2.ValorDespesas as ValorFinal
    FROM TrimestresExtremos te
    JOIN despesas_consolidadas d1 ON te.CNPJ = d1.CNPJ AND te.PrimeiroTri = CONCAT(d1.Ano, d1.Trimestre)
    JOIN despesas_consolidadas d2 ON te.CNPJ = d2.CNPJ AND te.UltimoTri = CONCAT(d2.Ano, d2.Trimestre)
)
SELECT 
    CNPJ,
    RazaoSocial,
    ValorInicial,
    ValorFinal,
    ((ValorFinal - ValorInicial) / ValorInicial) * 100 as CrescimentoPercentual
FROM ValoresExtremos
WHERE ValorInicial > 0
ORDER BY CrescimentoPercentual DESC
LIMIT 5;

-- Query 2: Distribuição de despesas por UF (Top 5 estados)
SELECT 
    o.UF,
    SUM(d.ValorDespesas) as TotalDespesas,
    AVG(d.ValorDespesas) as MediaPorOperadora
FROM despesas_consolidadas d
JOIN operadoras_ativas o ON d.CNPJ = o.CNPJ
GROUP BY o.UF
ORDER BY TotalDespesas DESC
LIMIT 5;

-- Query 3: Operadoras com despesas acima da média geral em pelo menos 2 dos 3 trimestres
WITH MediaGeral AS (
    SELECT AVG(ValorDespesas) as Media FROM despesas_consolidadas
),
AcimaDaMedia AS (
    SELECT 
        d.CNPJ,
        d.Trimestre,
        d.Ano
    FROM despesas_consolidadas d, MediaGeral m
    WHERE d.ValorDespesas > m.Media
)
SELECT CNPJ, COUNT(*) as TrimestresAcima
FROM AcimaDaMedia
GROUP BY CNPJ
HAVING COUNT(*) >= 2;
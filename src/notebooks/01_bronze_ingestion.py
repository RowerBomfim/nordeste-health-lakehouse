# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Nordeste Health Lakehouse
# MAGIC **Autor:** Rower Bomfim      
# MAGIC **Recorte:** Nordeste
# MAGIC
# MAGIC Este projeto integra cinco fontes Parquet em tabelas Delta nas camadas
# MAGIC Bronze, Silver e Gold, com rastreabilidade, qualidade, modelo dimensional
# MAGIC e indicadores cadastrais de infraestrutura e força de trabalho.
# MAGIC
# MAGIC **Competência configurada:** 202607. A competência da base de estabelecimentos
# MAGIC é atribuída por configuração e precisa ser compatível com as demais fontes.
# MAGIC
# MAGIC **Medidas:** leitos existentes (QT_EXIST), equipamentos em uso (QT_USO)
# MAGIC e horas hospitalares semanais (HORAHOSP).
# MAGIC
# MAGIC **Limites:** os resultados representam cadastros. A carga horária hospitalar
# MAGIC não identifica dedicação exclusiva à UTI. Os cenários quantitativos são
# MAGIC hipóteses explícitas de análise e não certificam segurança assistencial.
# MAGIC
# MAGIC **Execução:** Camada_Bronze → Tratamento_Silver → Modelo_Gold → Analytics_Validação.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparação 1 — Catálogo e schemas
# MAGIC
# MAGIC Esta célula organiza o projeto no Unity Catalog. O catálogo reúne os objetos, e cada schema separa uma responsabilidade do pipeline.
# MAGIC
# MAGIC | Schema | Finalidade |
# MAGIC | --- | --- |
# MAGIC | `landing` | Organização dos volumes de arquivos de entrada |
# MAGIC | `bronze` | Tabelas Delta brutas, com rastreabilidade |
# MAGIC | `silver` | Tabelas tratadas, filtradas e validadas |
# MAGIC | `gold` | Modelo dimensional, KPIs e views de consumo |
# MAGIC
# MAGIC O nome do catálogo contém hífens e deve ser protegido com crases em SQL. `IF NOT EXISTS` evita erro de criação quando o objeto já existe; não recria nem redefine um objeto existente.
# MAGIC
# MAGIC **Resultado esperado:** catálogo e quatro schemas disponíveis para as etapas seguintes. A execução depende das permissões de criação e uso no ambiente.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS `nordeste-health-lakehouse`;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS `nordeste-health-lakehouse`.landing;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS `nordeste-health-lakehouse`.bronze;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS `nordeste-health-lakehouse`.silver;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS `nordeste-health-lakehouse`.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparação 2 — Volume da Landing
# MAGIC
# MAGIC O volume `files` fica no schema `landing` do catálogo `nordeste-health-lakehouse`. Ele armazena os arquivos Parquet usados na carga inicial.
# MAGIC
# MAGIC **Nome SQL do volume:** `nordeste-health-lakehouse.landing.files`  
# MAGIC **Caminho de acesso:** `/Volumes/nordeste-health-lakehouse/landing/files`
# MAGIC
# MAGIC Após criar o volume, os cinco Parquet devem ser enviados para esse caminho. A listagem executada na Bronze conferirá os nomes.
# MAGIC
# MAGIC Volume, schema e tabela desempenham funções distintas: os arquivos ficam no volume; as tabelas de dados brutos serão registradas no schema `bronze`.
# MAGIC
# MAGIC **Resultado esperado:** volume disponível e arquivos acessíveis pela computação do notebook.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS `nordeste-health-lakehouse`.landing.files
# MAGIC COMMENT 'Arquivos do dataset e checkpoints de ingestão do projeto';

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 — Ingestão Bronze
# MAGIC
# MAGIC A Bronze recebe os cinco Parquet e grava cinco tabelas Delta. Nesta camada, o conteúdo e os tipos de origem são preservados, com inclusão de `data_hora_carga` e `arquivo_origem`.
# MAGIC
# MAGIC O processamento inicial utiliza leitura em lote. Cada arquivo representa uma fonte diferente e é lido separadamente. O filtro Nordeste, as conversões e a remoção de duplicatas pertencem à Silver.
# MAGIC
# MAGIC **Entrada:** cinco arquivos no volume `landing.files`.  
# MAGIC **Saída:** cinco tabelas no schema `bronze`.  
# MAGIC **Critérios de conferência:** contagens, tipos, rastreabilidade, conteúdo e formato Delta.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração dos caminhos e nomes
# MAGIC
# MAGIC Esta célula define o catálogo, o caminho da Landing e a lista das cinco bases. A função `tb(camada, nome)` monta nomes completos de tabela para impedir que a gravação dependa do schema selecionado na interface.
# MAGIC
# MAGIC | Configuração | Valor ou função |
# MAGIC | --- | --- |
# MAGIC | `CATALOGO` | `nordeste-health-lakehouse` |
# MAGIC | `LANDING` | `/Volumes/nordeste-health-lakehouse/landing/files` |
# MAGIC | `BASES` | Cinco nomes de arquivo sem a extensão `.parquet` |
# MAGIC | `tb()` | Montar `catálogo.schema.tabela` com proteção dos identificadores |
# MAGIC | Fuso da sessão | `America/Sao_Paulo` |
# MAGIC
# MAGIC A célula configura variáveis e não ingere arquivos. O fuso define como os horários são apresentados na sessão.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = "nordeste-health-lakehouse"

LANDING = f"/Volumes/{CATALOGO}/landing/files"

BASES = [
    "estabelecimentos_de_saude",
    "habilitacoes",
    "leitos",
    "profissionais",
    "equipamentos",
]

def tb(camada, nome):
    return f"`{CATALOGO}`.`{camada}`.`{nome}`"

spark.sql("SET TIME ZONE 'America/Sao_Paulo'")

print("Local dos arquivos:", LANDING)
print("Exemplo de destino:", tb("bronze", "leitos"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Conferência dos arquivos de entrada
# MAGIC
# MAGIC A listagem `dbutils.fs.ls(LANDING)` permite conferir o conteúdo do volume. O código compara os nomes encontrados com os cinco arquivos esperados e interrompe a execução se algum deles estiver ausente.
# MAGIC
# MAGIC **O que observar:** nome exato, extensão `.parquet` e diretório correto. A existência de outros arquivos no volume não substitui a presença das cinco fontes do projeto.
# MAGIC
# MAGIC **Resultado esperado:** mensagem de confirmação dos cinco arquivos. Se houver erro de acesso, conferir a computação e as permissões de leitura do volume. Essa etapa valida disponibilidade dos arquivos, ainda não seu conteúdo.

# COMMAND ----------

arquivos = dbutils.fs.ls(LANDING)

display(arquivos)

nomes_encontrados = {arquivo.name for arquivo in arquivos}

faltantes = [
    f"{base}.parquet"
    for base in BASES
    if f"{base}.parquet" not in nomes_encontrados
]

if faltantes:
    raise ValueError(
        f"Arquivos ausentes em {LANDING}: {faltantes}"
    )

print("Os cinco arquivos Parquet foram encontrados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inspeção dos schemas reais
# MAGIC
# MAGIC Cada Parquet é lido individualmente e seu schema é apresentado por `printSchema()`. A saída contém nomes de colunas, tipos e indicação de nulabilidade.
# MAGIC
# MAGIC Essa inspeção orienta o contrato da Silver. O PDF descreve o conteúdo das bases, mas não informa o nome exato de cada coluna. Campos canônicos como `cnes`, `quantidade` e `horas_semanais` serão alimentados por colunas reais escolhidas posteriormente.
# MAGIC
# MAGIC | Informação a localizar | Uso na etapa seguinte |
# MAGIC | --- | --- |
# MAGIC | Identificação do estabelecimento | Relacionar as cinco fontes |
# MAGIC | UF e município do estabelecimento | Definir o recorte geográfico |
# MAGIC | Códigos de leito, equipamento, habilitação e CBO | Modelar dimensões e classificações |
# MAGIC | Quantidades e horas | Alimentar as medidas analíticas |
# MAGIC | Competência e atributos de vínculo | Definir recorte temporal e chaves |
# MAGIC
# MAGIC **Resultado esperado:** cinco schemas disponíveis para consulta, sem exposição de valores cadastrais nessa saída.

# COMMAND ----------

for base in BASES:
    origem = spark.read.parquet(
        f"{LANDING}/{base}.parquet"
    )

    print(f"\nBASE: {base}")

    origem.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Função de ingestão do snapshot
# MAGIC
# MAGIC A função `ingestar_snapshot` reúne a leitura do Parquet, a inclusão da rastreabilidade, a gravação Delta e verificações iniciais.
# MAGIC
# MAGIC | Operação | Finalidade |
# MAGIC | --- | --- |
# MAGIC | Leitura isolada do Parquet | Preservar a estrutura da fonte correspondente |
# MAGIC | `_metadata.file_path` | Registrar o arquivo de origem |
# MAGIC | `current_timestamp()` | Registrar o horário da carga |
# MAGIC | `format('delta')` | Gravar a tabela em Delta |
# MAGIC | `saveAsTable()` | Registrar a tabela no Unity Catalog |
# MAGIC | Conferência de tipos e contagens | Detectar alterações inesperadas na ingestão |
# MAGIC
# MAGIC A estratégia `overwrite` substitui o snapshot completo da tabela de destino. Ela atende à carga inicial e ao reprocessamento dessa versão. O column mapping por nome permite preservar nomes de coluna da origem que exijam esse recurso.
# MAGIC
# MAGIC **Esta célula define a função.** As cinco cargas serão executadas na próxima célula.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Execução das cinco cargas Bronze
# MAGIC
# MAGIC A função de ingestão é chamada para cada base. A saída de auditoria apresenta o nome da fonte e as contagens de origem e destino.
# MAGIC
# MAGIC | Base | Tabela de destino |
# MAGIC | --- | --- |
# MAGIC | Estabelecimentos | `bronze.estabelecimentos_de_saude` |
# MAGIC | Habilitações | `bronze.habilitacoes` |
# MAGIC | Leitos | `bronze.leitos` |
# MAGIC | Profissionais | `bronze.profissionais` |
# MAGIC | Equipamentos | `bronze.equipamentos` |
# MAGIC
# MAGIC Os nomes acima pertencem ao catálogo `nordeste-health-lakehouse`. Não é aplicado filtro regional nessa etapa.
# MAGIC
# MAGIC **Critério de aceitação:** para cada fonte, `linhas_origem` e `linhas_bronze` devem coincidir. O projeto não estabelece uma contagem fixa de registros; a quantidade depende dos arquivos reais.

# COMMAND ----------

def ingestar_snapshot(base):
    caminho = f"{LANDING}/{base}.parquet"

    origem = spark.read.parquet(caminho)

    reservadas = {
        "data_hora_carga",
        "arquivo_origem",
    }

    nomes_origem = {
        coluna.casefold()
        for coluna in origem.columns
    }

    if reservadas.intersection(nomes_origem):
        raise ValueError(
            f"{base}: a origem já contém colunas "
            "de rastreabilidade reservadas."
        )

    if "_metadata" in nomes_origem:
        raise ValueError(
            f"{base}: a coluna _metadata da origem "
            "conflita com o metadado do leitor."
        )

    bronze = (
        origem
        .select(
            "*",
            F.col("_metadata.file_path")
            .alias("arquivo_origem")
        )
        .withColumn(
            "data_hora_carga",
            F.current_timestamp()
        )
    )

    (
        bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.columnMapping.mode", "name")
        .saveAsTable(tb("bronze", base))
    )

    destino = spark.table(
        tb("bronze", base)
    )

    tipos_origem = [
        (campo.name, campo.dataType.simpleString())
        for campo in origem.schema
    ]

    tipos_destino = [
        (
            destino.schema[nome].name,
            destino.schema[nome].dataType.simpleString()
        )
        for nome in origem.columns
    ]

    assert tipos_origem == tipos_destino, (
        f"Estrutura alterada em {base}"
    )

    qtd_origem = origem.count()
    qtd_bronze = destino.count()

    assert qtd_origem == qtd_bronze, (
        f"Contagem divergente em {base}"
    )

    rastreabilidade_incompleta = destino.filter(
        F.col("data_hora_carga").isNull()
        | F.col("arquivo_origem").isNull()
    )

    assert (
        rastreabilidade_incompleta.limit(1).count() == 0
    ), f"Rastreabilidade incompleta em {base}"

    return (
        base,
        qtd_origem,
        qtd_bronze,
    )

# COMMAND ----------

auditoria = [
    ingestar_snapshot(base)
    for base in BASES
]

resultado = spark.createDataFrame(
    auditoria,
    """
    base string,
    linhas_origem long,
    linhas_bronze long
    """
)

display(resultado)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Conferência das tabelas em Spark SQL
# MAGIC
# MAGIC `SHOW TABLES` verifica o registro dos objetos no schema `bronze`. As consultas de contagem apresentam uma linha por base e ajudam a conferir os destinos.
# MAGIC
# MAGIC `UNION ALL` reúne os cinco resultados de contagem em uma saída. Ele não une nem mistura os registros brutos das cinco bases.
# MAGIC
# MAGIC **Resultado esperado:** cinco tabelas de origem registradas na Bronze e contagens compatíveis com a auditoria da carga. Tabelas adicionais de outros trabalhos não devem ser usadas para substituir as fontes desse projeto.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES IN `nordeste-health-lakehouse`.bronze;
# MAGIC
# MAGIC SELECT
# MAGIC     'estabelecimentos_de_saude' AS base,
# MAGIC     COUNT(*) AS quantidade
# MAGIC FROM `nordeste-health-lakehouse`.bronze.estabelecimentos_de_saude
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'habilitacoes',
# MAGIC     COUNT(*)
# MAGIC FROM `nordeste-health-lakehouse`.bronze.habilitacoes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'leitos',
# MAGIC     COUNT(*)
# MAGIC FROM `nordeste-health-lakehouse`.bronze.leitos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'profissionais',
# MAGIC     COUNT(*)
# MAGIC FROM `nordeste-health-lakehouse`.bronze.profissionais
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'equipamentos',
# MAGIC     COUNT(*)
# MAGIC FROM `nordeste-health-lakehouse`.bronze.equipamentos;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validar o conteúdo completo da Bronze
# MAGIC Contagens iguais não detectam uma alteração de valores com o mesmo número de linhas.
# MAGIC
# MAGIC Conferir a saída e as validações desta etapa antes de seguir.

# COMMAND ----------

def validar_conteudo_bronze(base):
    fonte = spark.read.parquet(f"{LANDING}/{base}.parquet")
    destino = spark.table(tb("bronze", base))
    nomes = fonte.columns
    # Proteger os nomes preservados da origem, inclusive nomes especiais.
    selecao = [F.col("`" + c.replace("`", "``") + "`") for c in nomes]
    bruto = destino.select(*selecao)

    tipos_fonte = [(c.name, c.dataType.simpleString()) for c in fonte.schema]
    tipos_bruto = [(c.name, c.dataType.simpleString()) for c in bruto.schema]
    assert tipos_fonte == tipos_bruto, f"{base}: tipos ou ordem divergentes"
    assert fonte.exceptAll(bruto).limit(1).count() == 0, f"{base}: dado da fonte ausente"
    assert bruto.exceptAll(fonte).limit(1).count() == 0, f"{base}: dado extra na Bronze"
    return (base, "CONTEUDO_E_TIPOS_PRESERVADOS")

verificacoes = [validar_conteudo_bronze(base) for base in BASES]
evidencia = spark.createDataFrame(verificacoes, "base string, resultado string")
(evidencia.withColumn("executado_em", F.current_timestamp())
 .write.format("delta").mode("overwrite")
 .saveAsTable(tb("bronze", "auditoria_conteudo")))
display(evidencia)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testar reprocessamento com versão Delta fixada
# MAGIC O arquivo avaliado não contém uma comparação de conteúdo antes e depois da segunda carga.
# MAGIC
# MAGIC Conferir a saída e as validações desta etapa antes de seguir.

# COMMAND ----------

testes_reprocessamento = []
for base in BASES:
    historico = spark.sql(f"DESCRIBE HISTORY {tb('bronze', base)}")
    versao = int(historico.orderBy(F.desc("version")).first()["version"])
    antes = spark.sql(f"SELECT * FROM {tb('bronze', base)} VERSION AS OF {versao}")
    cols = [c for c in antes.columns if c not in {"data_hora_carga", "arquivo_origem"}]
    selecionadas = [F.col("`" + c.replace("`", "``") + "`") for c in cols]
    dados_antes = antes.select(*selecionadas)
    qtd_antes = dados_antes.count()

    # Reexecuta sua função de snapshot; a segunda escrita usa overwrite.
    _, qtd_fonte, qtd_depois = ingestar_snapshot(base)
    dados_depois = spark.table(tb("bronze", base)).select(*selecionadas)
    assert qtd_antes == qtd_fonte == qtd_depois, f"{base}: quantidade mudou"
    assert dados_antes.exceptAll(dados_depois).limit(1).count() == 0, base
    assert dados_depois.exceptAll(dados_antes).limit(1).count() == 0, base
    testes_reprocessamento.append((base, versao, qtd_antes, qtd_depois, "PASSOU"))

evidencia = spark.createDataFrame(testes_reprocessamento,
    "base string, versao_antes long, linhas_antes long, linhas_depois long, resultado string")
(evidencia.withColumn("executado_em", F.current_timestamp())
 .write.format("delta").mode("overwrite")
 .saveAsTable(tb("bronze", "auditoria_reprocessamento")))
display(evidencia)

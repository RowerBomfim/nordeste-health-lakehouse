# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ##Carregar referências e definir a avaliação das regras
# MAGIC O cálculo precisa distinguir hipótese analítica, ausência de referência e referências inválidas

# COMMAND ----------

from pyspark.sql import functions as F
from decimal import Decimal
from statistics import median
from time import perf_counter
CATALOGO = "nordeste-health-lakehouse"
VERSAO_REFERENCIAS = "2026-09-29_v1"
spark.sql("SET TIME ZONE 'America/Sao_Paulo'")
def tb(camada, nome):
    return f"`{CATALOGO}`.`{camada}`.`{nome}`"
def gravar(df, nome):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(tb("gold", nome)))
def ler_ref(nome):
    df = spark.table(tb("gold", nome)).filter(F.col("versao_referencia") == VERSAO_REFERENCIAS)
    assert df.limit(1).count() > 0, f"Versão ausente: {nome}"
    return df
ref_cbo = ler_ref("ref_classificacao_cbo")
ref_leito = ler_ref("ref_leitos_uti")
ref_equipamento = ler_ref("ref_equipamentos_analiticos")
ref_habilitacao = ler_ref("ref_habilitacoes_uti")
contrato = spark.table(tb("silver", "contrato_medidas"))
assert contrato.count() == 1, "Contrato precisa de exatamente uma linha"
assert contrato.first()["campo_horas_origem"] == "HORAHOSP"

def avaliar_regra_equipamento(df):
    minimo = (F.when(F.col("base_calculo") == "fixo", F.col("quantidade_minima_fixa"))
        .when((F.col("base_calculo") == "por_leito") & (F.col("leitos_base") > 0),
              F.col("coeficiente_por_leito") * F.col("leitos_base")))
    df = df.withColumn("quantidade_minima", minimo.cast("decimal(24,2)"))
    minimo_valido = F.col("quantidade_minima").isNotNull() & (F.col("quantidade_minima") > 0)
    df = df.withColumn("deficit_cadastral", F.when(minimo_valido,
        F.greatest(F.col("quantidade_minima") - F.col("quantidade_cadastrada"),
                   F.lit(0).cast("decimal(24,2)"))).cast("decimal(24,2)"))
    df = df.withColumn("razao_inventario_meta", F.when(minimo_valido,
        F.round(F.col("quantidade_cadastrada") / F.col("quantidade_minima"), 4))
        .cast("decimal(18,4)"))
    df = df.withColumn("status_analitico",
        F.when((F.col("base_calculo") == "por_leito") &
               (F.col("leitos_base").isNull() | (F.col("leitos_base") <= 0)),
               "SEM_LEITOS_BASE_CADASTRADOS")
        .when(F.col("quantidade_minima").isNull(), "SEM_REGRA_DE_REFERENCIA")
        .when(~minimo_valido, "REGRA_QUANTITATIVA_INVALIDA")
        .when((F.col("quantidade_cadastrada") < F.col("quantidade_minima")) &
              (F.col("natureza_regra") == "cenario_analitico"), "POTENCIAL_DEFICIT_NO_CENARIO")
        .when(F.col("natureza_regra") == "cenario_analitico", "ATINGE_META_DO_CENARIO")
        .when(F.col("quantidade_cadastrada") < F.col("quantidade_minima"), "POTENCIAL_DEFICIT_CADASTRAL")
        .otherwise("ATINGE_CRITERIO_CADASTRAL_DA_REGRA"))
    return df

# Inspecionar referências sem identificadores pessoais.
display(ref_cbo.select("cbo", "descricao_oficial", "grupo_kpi1", "eh_especialista"))
display(ref_leito.select("codigo_tipo_leito", "descricao_oficial", "eh_uti_escopo", "modalidade_uti"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Configurar o cenário quantitativo por recurso e modalidade
# MAGIC Substituir habilitações CAPS, tipos de equipamentos incorretos e mínimos ausentes por um cenário explícito e calculável

# COMMAND ----------

VERSAO_REGRAS = "cenario_uti_v1"
META_MEDICO_HORAS_SEMANAIS_POR_LEITO = None
META_ENFERMEIRO_HORAS_SEMANAIS_POR_LEITO = None
FONTE_META_UTI = "Sem referência quantitativa definida para horas hospitalares por leito"
NATUREZA_META_UTI = "sem_referencia"

param_uti = ref_leito.filter("eh_uti_escopo").select("codigo_tipo_leito")
param_cbo = ref_cbo.select("cbo", "grupo_kpi1", "eh_medico", "eh_especialista",
                         "classificacao_kpi1_validada", "classificacao_kpi3_validada")
# Cenário do portfólio: um equipamento cadastrado por leito da modalidade.
# Esses coeficientes NÃO são apresentados como exigências normativas.
regras_linhas = []
for h in ref_habilitacao.select("codigo_habilitacao", "modalidade_uti").collect():
    recursos = ["64", "60"]
    if h.modalidade_uti == "NEONATAL": recursos += ["58"]
    for recurso in recursos:
        regras_linhas.append((h.codigo_habilitacao, recurso, h.modalidade_uti,
            "por_leito", None, Decimal("1.0000"),
            "Hipotese do projeto: 1 unidade cadastrada do recurso por leito da modalidade; inventario total da unidade",
            "cenario_analitico", VERSAO_REGRAS))
param_complex = spark.createDataFrame(regras_linhas, """
    codigo_habilitacao string, codigo_equipamento string, modalidade_uti string,
    base_calculo string, quantidade_minima_fixa decimal(18,2),
    coeficiente_por_leito decimal(18,4), fonte_regra string,
    natureza_regra string, versao_regra string
""")
def decimal_opcional(valor):
    return None if valor is None else Decimal(str(valor))
param_meta = spark.createDataFrame([(
    decimal_opcional(META_MEDICO_HORAS_SEMANAIS_POR_LEITO),
    decimal_opcional(META_ENFERMEIRO_HORAS_SEMANAIS_POR_LEITO),
    FONTE_META_UTI, NATUREZA_META_UTI,
)], "meta_medico decimal(18,4), meta_enfermeiro decimal(18,4), fonte_meta string, natureza_meta string")
display(param_complex)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validar referências, granularidade e parâmetros
# MAGIC A existência do código no arquivo não comprova que sua classificação está correta; separar validade do dicionário e presença no mês

# COMMAND ----------

def validar_chave(df, campos, nome):
    nulo = F.lit(False)
    for c in campos: nulo = nulo | F.col(c).isNull()
    assert df.filter(nulo).limit(1).count() == 0, f"{nome}: chave nula"
    assert df.groupBy(*campos).count().filter("count > 1").limit(1).count() == 0, f"{nome}: duplicata"
for df, campos, nome in [
    (param_uti, ["codigo_tipo_leito"], "UTI"),
    (param_cbo, ["cbo"], "CBO"),
    (param_complex, ["codigo_habilitacao", "codigo_equipamento"], "complexidade"),
]:
    validar_chave(df, campos, nome)
for campo, tabela in [("codigo_habilitacao", ref_habilitacao), ("codigo_equipamento", ref_equipamento)]:
    ausentes = param_complex.select(campo).distinct().join(tabela.select(campo), campo, "left_anti")
    assert ausentes.limit(1).count() == 0, f"Código sem referência: {campo}"
assert param_complex.filter("base_calculo NOT IN ('fixo','por_leito') OR base_calculo IS NULL").limit(1).count() == 0
assert param_complex.filter("natureza_regra NOT IN ('cenario_analitico','normativa_validada') OR natureza_regra IS NULL").limit(1).count() == 0
assert param_complex.filter("fonte_regra IS NULL OR trim(fonte_regra) = ''").limit(1).count() == 0
assert param_complex.filter("base_calculo='por_leito' AND (coeficiente_por_leito IS NULL OR coeficiente_por_leito<=0)").limit(1).count() == 0
assert param_complex.filter("base_calculo='fixo' AND quantidade_minima_fixa<=0").limit(1).count() == 0
for meta in [META_MEDICO_HORAS_SEMANAIS_POR_LEITO, META_ENFERMEIRO_HORAS_SEMANAIS_POR_LEITO]:
    assert meta is None or Decimal(str(meta)) > 0
assert NATUREZA_META_UTI in {"sem_referencia", "cenario_analitico", "normativa_validada"}
if NATUREZA_META_UTI == "sem_referencia":
    assert META_MEDICO_HORAS_SEMANAIS_POR_LEITO is None and META_ENFERMEIRO_HORAS_SEMANAIS_POR_LEITO is None
else:
    assert META_MEDICO_HORAS_SEMANAIS_POR_LEITO is not None and META_ENFERMEIRO_HORAS_SEMANAIS_POR_LEITO is not None
    assert FONTE_META_UTI.strip() and not FONTE_META_UTI.startswith("Sem referência")

# Conferir que as dimensões foram reconstruídas com a mesma versão.
for nome in ["dim_profissional", "dim_tipo_leito", "dim_equipamento", "ponte_estabelecimento_habilitacao"]:
    df = spark.table(tb("gold", nome))
    assert df.filter(F.col("versao_classificacao").isNull() |
                     (F.col("versao_classificacao") != VERSAO_REFERENCIAS)).limit(1).count() == 0, nome
for df, nome in [(param_uti, "param_leitos_uti"), (param_cbo, "param_classificacao_cbo"),
                 (param_complex, "param_complexidade"), (param_meta, "param_meta_uti")]:
    gravar(df, nome)
print("Parâmetros validados e persistidos. Códigos válidos ausentes no mês são permitidos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Executar testes de negócio com respostas conhecidas
# MAGIC Esses testes detectam os erros semânticos mesmo quando PySpark e SQL reproduzem a mesma lógica

# COMMAND ----------

cbo_teste = spark.createDataFrame([
    ("225150", Decimal("12.00")), ("225125", Decimal("40.00")),
    ("223505", Decimal("24.00")), ("223570", Decimal("50.00")),
], "cbo string, horas_hospitalares decimal(18,2)").join(ref_cbo, "cbo", "left")
somas = cbo_teste.agg(
    F.sum(F.when(F.col("grupo_kpi1") == "medico_intensivista", F.col("horas_hospitalares")).otherwise(0)).alias("medicos"),
    F.sum(F.when(F.col("grupo_kpi1") == "enfermeiro", F.col("horas_hospitalares")).otherwise(0)).alias("enfermeiros"),
).first()
assert somas["medicos"] == Decimal("12.00"), "Médico clínico entrou como intensivista"
assert somas["enfermeiros"] == Decimal("24.00"), "Perfusionista entrou como enfermeiro"
leitos_teste = spark.createDataFrame([("10", 5), ("43", 6), ("75", 3), ("81", 2)],
    "codigo_tipo_leito string, quantidade long").join(ref_leito, "codigo_tipo_leito", "left")
total_uti = leitos_teste.filter("eh_uti_escopo").agg(F.sum("quantidade")).first()[0]
assert total_uti == 5, "Leito obstétrico entrou como UTI"

D = lambda x: None if x is None else Decimal(str(x))
casos = [
    ("sem_regra", "fixo", D(None), D(None), D(0), D(7), "cenario_analitico", "SEM_REGRA_DE_REFERENCIA", D(None), D(None)),
    ("deficit_fixo", "fixo", D(10), D(None), D(0), D(7), "cenario_analitico", "POTENCIAL_DEFICIT_NO_CENARIO", D(10), D(3)),
    ("atinge_fixo", "fixo", D(10), D(None), D(0), D(10), "cenario_analitico", "ATINGE_META_DO_CENARIO", D(10), D(0)),
    ("por_leito", "por_leito", D(None), D(1), D(2), D(1), "cenario_analitico", "POTENCIAL_DEFICIT_NO_CENARIO", D(2), D(1)),
    ("sem_leitos", "por_leito", D(None), D(1), D(0), D(4), "cenario_analitico", "SEM_LEITOS_BASE_CADASTRADOS", D(None), D(None)),
]
testes = spark.createDataFrame(casos, """
    caso string, base_calculo string, quantidade_minima_fixa decimal(18,2),
    coeficiente_por_leito decimal(18,4), leitos_base decimal(24,2),
    quantidade_cadastrada decimal(24,2), natureza_regra string,
    status_esperado string, minimo_esperado decimal(24,2), deficit_esperado decimal(24,2)
""")
testes = avaliar_regra_equipamento(testes)
erro = testes.filter(
    ~F.col("status_analitico").eqNullSafe(F.col("status_esperado")) |
    ~F.col("quantidade_minima").eqNullSafe(F.col("minimo_esperado")) |
    ~F.col("deficit_cadastral").eqNullSafe(F.col("deficit_esperado")))
assert erro.limit(1).count() == 0, "Regressão no tratamento de regras e déficits"
evidencias = spark.createDataFrame([
    ("intensivista", "PASSOU"), ("enfermeiro", "PASSOU"), ("leito_uti", "PASSOU"),
    ("referencia_e_deficit", "PASSOU"),
], "teste string, resultado string").withColumn("executado_em", F.current_timestamp())
evidencias = evidencias.withColumn("versao_classificacao", F.lit(VERSAO_REFERENCIAS))
gravar(evidencias, "auditoria_testes_negocio")
display(testes.select("caso", "quantidade_minima", "deficit_cadastral", "status_analitico"))
print("Testes de negócio passaram. Os dados sintéticos não alimentam as fatos reais.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Calcular o KPI 1 em PySpark com classificações corrigidas
# MAGIC As mesmas fórmulas precisam utilizar leitos de UTI, intensivistas e horas hospitalares realmente selecionados

# COMMAND ----------

est = spark.table(tb("gold", "dim_estabelecimento"))
cap = spark.table(tb("gold", "fato_capacidade_hospitalar"))
aloc = spark.table(tb("gold", "fato_alocacao_profissionais"))
dlei = spark.table(tb("gold", "dim_tipo_leito"))
dpro = spark.table(tb("gold", "dim_profissional"))
zero = F.lit(0).cast("decimal(24,2)")
leitos_uti = (cap.filter("tipo_recurso = 'LEITO'")
    .join(dlei.select("sk_tipo_leito", "codigo_tipo_leito"), "sk_tipo_leito", "inner")
    .join(param_uti, "codigo_tipo_leito", "inner")
    .groupBy("sk_estabelecimento")
    .agg(F.sum("quantidade_leitos").cast("decimal(24,2)").alias("leitos_uti")))
prof = aloc.join(dpro.select("sk_profissional", "id_profissional_analitico", "grupo_kpi1",
                            "classificacao_kpi1_validada"), "sk_profissional", "inner")
horas = prof.groupBy("sk_estabelecimento").agg(
    F.sum(F.when(F.col("grupo_kpi1") == "medico_intensivista", F.col("horas_semanais")).otherwise(zero))
        .cast("decimal(24,2)").alias("horas_medicos"),
    F.sum(F.when(F.col("grupo_kpi1") == "enfermeiro", F.col("horas_semanais")).otherwise(zero))
        .cast("decimal(24,2)").alias("horas_enfermeiros"),
    F.countDistinct(F.when(F.col("grupo_kpi1") == "medico_intensivista", F.col("id_profissional_analitico")))
        .alias("qtd_medicos_distintos"),
    F.countDistinct(F.when(F.col("grupo_kpi1") == "enfermeiro", F.col("id_profissional_analitico")))
        .alias("qtd_enfermeiros_distintos"),
    F.sum(F.when(~F.col("classificacao_kpi1_validada"), 1).otherwise(0))
        .cast("long").alias("registros_cbo_sem_classificacao"))
kpi1 = est.join(leitos_uti, "sk_estabelecimento", "left").join(horas, "sk_estabelecimento", "left")
for c in ["leitos_uti", "horas_medicos", "horas_enfermeiros"]:
    kpi1 = kpi1.withColumn(c, F.coalesce(F.col(c), zero).cast("decimal(24,2)"))
for c in ["qtd_medicos_distintos", "qtd_enfermeiros_distintos", "registros_cbo_sem_classificacao"]:
    kpi1 = kpi1.withColumn(c, F.coalesce(F.col(c), F.lit(0)).cast("long"))
for campo, razao in [("horas_medicos", "horas_medicos_por_leito"),
                     ("horas_enfermeiros", "horas_enfermeiros_por_leito")]:
    kpi1 = kpi1.withColumn(razao, F.when(F.col("leitos_uti") > 0,
        F.round(F.col(campo) / F.col("leitos_uti"), 4)).cast("decimal(18,4)"))
kpi1 = kpi1.crossJoin(param_meta).crossJoin(contrato.select(
    "medida_leitos", "medida_horas", "escopo_rede", "alerta_orfaos", "competencia_estabelecimentos_atribuida"))
kpi1 = kpi1.withColumn("versao_classificacao", F.lit(VERSAO_REFERENCIAS))
kpi1 = kpi1.withColumn("status_analitico",
    F.when(F.col("leitos_uti") == 0, "SEM_LEITOS_UTI_CADASTRADOS")
    .when(F.col("registros_cbo_sem_classificacao") > 0, "CLASSIFICACAO_INCOMPLETA")
    .when(F.col("meta_medico").isNull() | F.col("meta_enfermeiro").isNull(), "SEM_META_DE_REFERENCIA")
    .when((F.col("horas_medicos") < F.col("meta_medico") * F.col("leitos_uti")) |
          (F.col("horas_enfermeiros") < F.col("meta_enfermeiro") * F.col("leitos_uti")), "ABAIXO_DA_META_ANALITICA")
    .otherwise("ATINGE_META_ANALITICA"))
COLUNAS_KPI1 = ["sk_estabelecimento", "cnes", "competencia", "nome_estabelecimento", "uf", "id_municipio",
    "nome_municipio", "leitos_uti", "horas_medicos", "horas_enfermeiros", "qtd_medicos_distintos",
    "qtd_enfermeiros_distintos", "registros_cbo_sem_classificacao", "horas_medicos_por_leito",
    "horas_enfermeiros_por_leito", "meta_medico", "meta_enfermeiro", "fonte_meta", "natureza_meta",
    "medida_leitos", "medida_horas", "escopo_rede", "alerta_orfaos",
    "competencia_estabelecimentos_atribuida", "versao_classificacao", "status_analitico"]
kpi1 = kpi1.select(*COLUNAS_KPI1)
gravar(kpi1, "kpi1_suporte_intensivo_pyspark")
display(kpi1.orderBy(F.desc("leitos_uti")).limit(30))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reproduzir o KPI 1 em Spark SQL puro
# MAGIC Corrigir somente o PySpark quebraria o requisito de duas implementações equivalentes.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `nordeste-health-lakehouse`.gold.kpi1_suporte_intensivo_sql
# MAGIC USING DELTA AS
# MAGIC WITH leitos AS (
# MAGIC   SELECT c.sk_estabelecimento, CAST(SUM(c.quantidade_leitos) AS DECIMAL(24,2)) AS leitos_uti
# MAGIC   FROM `nordeste-health-lakehouse`.gold.fato_capacidade_hospitalar c
# MAGIC   JOIN `nordeste-health-lakehouse`.gold.dim_tipo_leito l USING (sk_tipo_leito)
# MAGIC   JOIN `nordeste-health-lakehouse`.gold.param_leitos_uti u USING (codigo_tipo_leito)
# MAGIC   WHERE c.tipo_recurso = 'LEITO'
# MAGIC   GROUP BY c.sk_estabelecimento
# MAGIC ), horas AS (
# MAGIC   SELECT a.sk_estabelecimento,
# MAGIC     CAST(SUM(CASE WHEN d.grupo_kpi1 = 'medico_intensivista' THEN a.horas_semanais ELSE 0 END) AS DECIMAL(24,2)) AS horas_medicos,
# MAGIC     CAST(SUM(CASE WHEN d.grupo_kpi1 = 'enfermeiro' THEN a.horas_semanais ELSE 0 END) AS DECIMAL(24,2)) AS horas_enfermeiros,
# MAGIC     COUNT(DISTINCT CASE WHEN d.grupo_kpi1 = 'medico_intensivista' THEN d.id_profissional_analitico END) AS qtd_medicos_distintos,
# MAGIC     COUNT(DISTINCT CASE WHEN d.grupo_kpi1 = 'enfermeiro' THEN d.id_profissional_analitico END) AS qtd_enfermeiros_distintos,
# MAGIC     CAST(SUM(CASE WHEN d.classificacao_kpi1_validada = false THEN 1 ELSE 0 END) AS BIGINT) AS registros_cbo_sem_classificacao
# MAGIC   FROM `nordeste-health-lakehouse`.gold.fato_alocacao_profissionais a
# MAGIC   JOIN `nordeste-health-lakehouse`.gold.dim_profissional d USING (sk_profissional)
# MAGIC   GROUP BY a.sk_estabelecimento
# MAGIC ), base AS (
# MAGIC   SELECT e.sk_estabelecimento, e.cnes, e.competencia, e.nome_estabelecimento, e.uf, e.id_municipio, e.nome_municipio,
# MAGIC     CAST(COALESCE(l.leitos_uti, 0) AS DECIMAL(24,2)) AS leitos_uti,
# MAGIC     CAST(COALESCE(h.horas_medicos, 0) AS DECIMAL(24,2)) AS horas_medicos,
# MAGIC     CAST(COALESCE(h.horas_enfermeiros, 0) AS DECIMAL(24,2)) AS horas_enfermeiros,
# MAGIC     CAST(COALESCE(h.qtd_medicos_distintos, 0) AS BIGINT) AS qtd_medicos_distintos,
# MAGIC     CAST(COALESCE(h.qtd_enfermeiros_distintos, 0) AS BIGINT) AS qtd_enfermeiros_distintos,
# MAGIC     CAST(COALESCE(h.registros_cbo_sem_classificacao, 0) AS BIGINT) AS registros_cbo_sem_classificacao
# MAGIC   FROM `nordeste-health-lakehouse`.gold.dim_estabelecimento e
# MAGIC   LEFT JOIN leitos l USING (sk_estabelecimento)
# MAGIC   LEFT JOIN horas h USING (sk_estabelecimento)
# MAGIC )
# MAGIC SELECT b.*,
# MAGIC   CAST(ROUND(b.horas_medicos / NULLIF(b.leitos_uti, 0), 4) AS DECIMAL(18,4)) AS horas_medicos_por_leito,
# MAGIC   CAST(ROUND(b.horas_enfermeiros / NULLIF(b.leitos_uti, 0), 4) AS DECIMAL(18,4)) AS horas_enfermeiros_por_leito,
# MAGIC   m.meta_medico, m.meta_enfermeiro, m.fonte_meta, m.natureza_meta,
# MAGIC   r.medida_leitos, r.medida_horas, r.escopo_rede, r.alerta_orfaos, r.competencia_estabelecimentos_atribuida,
# MAGIC   '2026-09-29_v1' AS versao_classificacao,
# MAGIC   CASE WHEN b.leitos_uti = 0 THEN 'SEM_LEITOS_UTI_CADASTRADOS'
# MAGIC        WHEN b.registros_cbo_sem_classificacao > 0 THEN 'CLASSIFICACAO_INCOMPLETA'
# MAGIC        WHEN m.meta_medico IS NULL OR m.meta_enfermeiro IS NULL THEN 'SEM_META_DE_REFERENCIA'
# MAGIC        WHEN b.horas_medicos < m.meta_medico * b.leitos_uti
# MAGIC          OR b.horas_enfermeiros < m.meta_enfermeiro * b.leitos_uti THEN 'ABAIXO_DA_META_ANALITICA'
# MAGIC        ELSE 'ATINGE_META_ANALITICA' END AS status_analitico
# MAGIC FROM base b
# MAGIC CROSS JOIN `nordeste-health-lakehouse`.gold.param_meta_uti m
# MAGIC CROSS JOIN `nordeste-health-lakehouse`.silver.contrato_medidas r;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validar equivalência integral depois das correções
# MAGIC Os testes de negócio e a equivalência entre APIs verificam propriedades complementares

# COMMAND ----------

a = spark.table(tb("gold", "kpi1_suporte_intensivo_pyspark")).select(*COLUNAS_KPI1)
b = spark.table(tb("gold", "kpi1_suporte_intensivo_sql")).select(*COLUNAS_KPI1)
tipos_a = [(c.name, c.dataType.simpleString()) for c in a.schema]
tipos_b = [(c.name, c.dataType.simpleString()) for c in b.schema]
assert tipos_a == tipos_b, f"Tipos divergentes: {tipos_a} x {tipos_b}"
assert a.exceptAll(b).limit(1).count() == 0, "Há linhas diferentes na versão PySpark"
assert b.exceptAll(a).limit(1).count() == 0, "Há linhas diferentes na versão SQL"
assert a.count() == spark.table(tb("gold", "dim_estabelecimento")).count()
print("KPI 1: PySpark e SQL produzem exatamente os mesmos valores e tipos.")
validar_chave(a, ["cnes", "competencia"], "KPI 1 PySpark")
validar_chave(b, ["cnes", "competencia"], "KPI 1 SQL")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Calcular o KPI 2 por equipamento e modalidade de UTI
# MAGIC Aplicar a mesma função testada em A04 a quantidades cadastrais reais, usando a modalidade correta de leito

# COMMAND ----------

cap = spark.table(tb("gold", "fato_capacidade_hospitalar"))
est = spark.table(tb("gold", "dim_estabelecimento"))
dlei = spark.table(tb("gold", "dim_tipo_leito"))
dequ = spark.table(tb("gold", "dim_equipamento"))
hab = spark.table(tb("gold", "ponte_estabelecimento_habilitacao"))
inventario = (cap.filter("tipo_recurso = 'EQUIPAMENTO'")
    .join(dequ.select("sk_equipamento", "codigo_equipamento"), "sk_equipamento", "inner")
    .groupBy("sk_estabelecimento", "codigo_equipamento")
    .agg(F.sum("quantidade_equipamentos").cast("decimal(24,2)").alias("quantidade_cadastrada")))
leitos_base = (cap.filter("tipo_recurso = 'LEITO'")
    .join(dlei.select("sk_tipo_leito", "eh_uti_escopo", "modalidade_uti"), "sk_tipo_leito", "inner")
    .filter("eh_uti_escopo")
    .groupBy("sk_estabelecimento", "modalidade_uti")
    .agg(F.sum("quantidade_leitos").cast("decimal(24,2)").alias("leitos_base")))
base = (hab.select("sk_estabelecimento", "codigo_habilitacao", "descricao_habilitacao")
    .join(param_complex, "codigo_habilitacao", "inner")
    .join(inventario, ["sk_estabelecimento", "codigo_equipamento"], "left")
    .join(leitos_base, ["sk_estabelecimento", "modalidade_uti"], "left")
    .withColumn("sem_registro_equipamento", F.col("quantidade_cadastrada").isNull())
    .withColumn("quantidade_cadastrada", F.coalesce(F.col("quantidade_cadastrada"),
        F.lit(0).cast("decimal(24,2)")))
    .withColumn("leitos_base", F.coalesce(F.col("leitos_base"), F.lit(0).cast("decimal(24,2)"))))
kpi2 = avaliar_regra_equipamento(base)
nomes_recursos = ref_equipamento.select("codigo_equipamento",
    F.col("descricao_oficial").alias("descricao_equipamento"))
kpi2 = (kpi2.join(nomes_recursos, "codigo_equipamento", "left")
    .join(est.select("sk_estabelecimento", "cnes", "competencia", "nome_estabelecimento", "uf",
                     "id_municipio", "nome_municipio"), "sk_estabelecimento", "inner")
    .crossJoin(contrato.select("medida_equipamentos", "medida_leitos", "escopo_rede", "alerta_orfaos",
                              "competencia_estabelecimentos_atribuida"))
    .withColumn("versao_classificacao", F.lit(VERSAO_REFERENCIAS))
    .select("cnes", "competencia", "nome_estabelecimento", "uf", "id_municipio", "nome_municipio",
        "codigo_habilitacao", "descricao_habilitacao", "modalidade_uti", "codigo_equipamento",
        "descricao_equipamento", "leitos_base", "base_calculo", "coeficiente_por_leito",
        "quantidade_minima_fixa", "quantidade_minima", "quantidade_cadastrada", "sem_registro_equipamento",
        "deficit_cadastral", "razao_inventario_meta", "fonte_regra", "natureza_regra", "versao_regra",
        "versao_classificacao", "medida_equipamentos", "medida_leitos", "escopo_rede", "alerta_orfaos",
        "competencia_estabelecimentos_atribuida", "status_analitico"))
validar_chave(kpi2, ["cnes", "competencia", "codigo_habilitacao", "codigo_equipamento"], "KPI 2")
gravar(kpi2, "kpi2_densidade_tecnologica")
display(kpi2.orderBy(F.desc("deficit_cadastral")).limit(30))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Calcular concentração de ocupações médicas especializadas
# MAGIC O KPI anterior considerava apenas um CBO e declarava os demais profissionais não especialistas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `nordeste-health-lakehouse`.gold.kpi3_concentracao_especialistas
# MAGIC USING DELTA AS
# MAGIC WITH municipios AS (
# MAGIC   SELECT uf, id_municipio, MIN(nome_municipio) AS nome_municipio, competencia,
# MAGIC          COUNT(DISTINCT sk_estabelecimento) AS estabelecimentos_cadastrados
# MAGIC   FROM `nordeste-health-lakehouse`.gold.dim_estabelecimento
# MAGIC   GROUP BY uf, id_municipio, competencia
# MAGIC ), profissionais AS (
# MAGIC   SELECT a.uf, a.id_municipio, a.competencia,
# MAGIC     COUNT(DISTINCT d.id_profissional_analitico) AS profissionais_distintos,
# MAGIC     COUNT(DISTINCT CASE WHEN d.eh_medico THEN d.id_profissional_analitico END) AS medicos_distintos,
# MAGIC     COUNT(DISTINCT CASE WHEN d.eh_medico AND d.classificacao_kpi3_validada THEN d.id_profissional_analitico END) AS medicos_classificados_distintos,
# MAGIC     COUNT(DISTINCT CASE WHEN d.eh_especialista THEN d.id_profissional_analitico END) AS especialistas_distintos,
# MAGIC     CAST(SUM(CASE WHEN d.eh_especialista THEN a.horas_semanais ELSE 0 END) AS DECIMAL(24,2)) AS horas_especialistas,
# MAGIC     COUNT(DISTINCT CASE WHEN d.eh_medico AND NOT d.classificacao_kpi3_validada THEN d.id_profissional_analitico END) AS medicos_sem_classificacao
# MAGIC   FROM `nordeste-health-lakehouse`.gold.fato_alocacao_profissionais a
# MAGIC   JOIN `nordeste-health-lakehouse`.gold.dim_profissional d USING (sk_profissional)
# MAGIC   GROUP BY a.uf, a.id_municipio, a.competencia
# MAGIC ), base AS (
# MAGIC   SELECT m.*, COALESCE(p.profissionais_distintos, 0) AS profissionais_distintos,
# MAGIC     COALESCE(p.medicos_distintos, 0) AS medicos_distintos,
# MAGIC     COALESCE(p.medicos_classificados_distintos, 0) AS medicos_classificados_distintos,
# MAGIC     COALESCE(p.especialistas_distintos, 0) AS especialistas_distintos,
# MAGIC     CAST(COALESCE(p.horas_especialistas, 0) AS DECIMAL(24,2)) AS horas_especialistas,
# MAGIC     COALESCE(p.medicos_sem_classificacao, 0) AS medicos_sem_classificacao
# MAGIC   FROM municipios m LEFT JOIN profissionais p USING (uf, id_municipio, competencia)
# MAGIC )
# MAGIC SELECT b.*,
# MAGIC   CAST(ROUND(100.0 * especialistas_distintos / NULLIF(profissionais_distintos, 0), 4) AS DECIMAL(18,4)) AS percentual_especialistas_no_municipio,
# MAGIC   CAST(ROUND(100.0 * especialistas_distintos / NULLIF(medicos_classificados_distintos, 0), 4) AS DECIMAL(18,4)) AS percentual_especialistas_entre_medicos_classificados,
# MAGIC   CAST(ROUND(100.0 * horas_especialistas / NULLIF(SUM(horas_especialistas) OVER (PARTITION BY b.competencia), 0), 4) AS DECIMAL(18,4)) AS percentual_horas_especialistas_regiao,
# MAGIC   'ocupacoes medicas diferenciadas da referencia; CBO nao comprova titulacao' AS criterio_especialista,
# MAGIC   '2026-09-29_v1' AS versao_classificacao,
# MAGIC   r.medida_horas, r.escopo_rede, r.alerta_orfaos, r.competencia_estabelecimentos_atribuida,
# MAGIC   CASE WHEN medicos_sem_classificacao > 0 THEN 'CLASSIFICACAO_INCOMPLETA'
# MAGIC        WHEN especialistas_distintos = 0 THEN 'SEM_ESPECIALISTAS_MEDICOS_CADASTRADOS'
# MAGIC        ELSE 'COM_ESPECIALISTAS_MEDICOS_CADASTRADOS' END AS status_analitico
# MAGIC FROM base b CROSS JOIN `nordeste-health-lakehouse`.silver.contrato_medidas r;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Publicar e comentar as views depois das validações
# MAGIC O consumo precisa explicar cenário, referência ausente, ocupação cadastrada e limitações de cobertura

# COMMAND ----------

testes = spark.table(tb("gold", "auditoria_testes_negocio"))
assert testes.count() == 4
assert testes.filter("resultado <> 'PASSOU' OR resultado IS NULL").limit(1).count() == 0
assert testes.filter(F.col("versao_classificacao").isNull() |
                     (F.col("versao_classificacao") != VERSAO_REFERENCIAS)).limit(1).count() == 0
a = spark.table(tb("gold", "kpi1_suporte_intensivo_pyspark")).select(*COLUNAS_KPI1)
b = spark.table(tb("gold", "kpi1_suporte_intensivo_sql")).select(*COLUNAS_KPI1)
assert a.exceptAll(b).limit(1).count() == 0 and b.exceptAll(a).limit(1).count() == 0
for nome, campos in [
    ("kpi1_suporte_intensivo_sql", ["cnes", "competencia"]),
    ("kpi2_densidade_tecnologica", ["cnes", "competencia", "codigo_habilitacao", "codigo_equipamento"]),
    ("kpi3_concentracao_especialistas", ["uf", "id_municipio", "competencia"]),
]:
    validar_chave(spark.table(tb("gold", nome)), campos, nome)
views = [
    ("vw_suporte_intensivo", "kpi1_suporte_intensivo_sql",
     "Proporcionalidade cadastral: HORAHOSP por leito UTI do recorte; sem dedicacao exclusiva a UTI ou certificacao de seguranca."),
    ("vw_densidade_tecnologica", "kpi2_densidade_tecnologica",
     "Inventario por recurso e habilitacao UTI; cenarios explicitos; deficit nao comprova indisponibilidade fisica ou descumprimento normativo."),
    ("vw_concentracao_especialistas", "kpi3_concentracao_especialistas",
     "Ocupacoes medicas diferenciadas por municipio; CBO e ocupacao; zeros e classificacao incompleta nao comprovam ausencia assistencial."),
]
for view, tabela, comentario in views:
    spark.sql(f"CREATE OR REPLACE VIEW {tb('gold', view)} AS SELECT * FROM {tb('gold', tabela)}")
    texto_sql = comentario.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {tb('gold', view)} IS '{texto_sql}'")
print("Views de indicadores reconstruídas após validação de negócio e equivalência.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Conferir estados e consistência numérica dos indicadores
# MAGIC Além de chaves únicas, medir os estados analíticos e impedir percentuais/razões inconsistentes

# COMMAND ----------

for nome in ["kpi1_suporte_intensivo_sql", "kpi2_densidade_tecnologica", "kpi3_concentracao_especialistas"]:
    df = spark.table(tb("gold", nome))
    print(nome, "linhas:", df.count())
    display(df.groupBy("status_analitico", "alerta_orfaos").count().orderBy("status_analitico"))
k2 = spark.table(tb("gold", "kpi2_densidade_tecnologica"))
assert k2.filter("quantidade_minima IS NULL AND (deficit_cadastral IS NOT NULL OR status_analitico LIKE 'ATINGE%')").limit(1).count() == 0
assert k2.filter("natureza_regra='cenario_analitico' AND status_analitico='ATINGE_CRITERIO_CADASTRAL_DA_REGRA'").limit(1).count() == 0
assert k2.filter("quantidade_cadastrada < 0 OR deficit_cadastral < 0").limit(1).count() == 0
k3 = spark.table(tb("gold", "kpi3_concentracao_especialistas"))
assert k3.filter("especialistas_distintos > profissionais_distintos OR especialistas_distintos > medicos_classificados_distintos").limit(1).count() == 0
assert k3.filter("percentual_especialistas_no_municipio < 0 OR percentual_especialistas_no_municipio > 100").limit(1).count() == 0
assert k3.filter("percentual_especialistas_entre_medicos_classificados < 0 OR percentual_especialistas_entre_medicos_classificados > 100").limit(1).count() == 0
display(k2.orderBy(F.desc("deficit_cadastral")).limit(30))
display(k3.orderBy("especialistas_distintos", "uf", "id_municipio").limit(30))
print("Estados analíticos e limites numéricos conferidos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Conferir ZORDER e fixar a versão anterior para comparação
# MAGIC A otimização precisa ser compatível com o layout da tabela e sua evidência deve preservar o antes/depois

# COMMAND ----------

TABELAS_OTIMIZAR = ["dim_estabelecimento", "fato_capacidade_hospitalar", "fato_alocacao_profissionais"]
for nome in TABELAS_OTIMIZAR:
    detalhe = spark.sql(f"DESCRIBE DETAIL {tb('gold', nome)}").first().asDict()
    assert detalhe["format"] == "delta", nome
    assert not detalhe.get("clusteringColumns"), f"{nome}: liquid clustering exige outro layout"
    print(nome, "Delta e sem liquid clustering")
TABELA_BENCHMARK = tb("gold", "fato_capacidade_hospitalar")
VERSAO_ANTES_OTIMIZACAO = int(spark.sql(f"DESCRIBE HISTORY {TABELA_BENCHMARK}")
    .orderBy(F.desc("version")).first()["version"])
print("Versão fixada antes do OPTIMIZE:", VERSAO_ANTES_OTIMIZACAO)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Executar estatísticas e OPTIMIZE ZORDER BY
# MAGIC A otimização existente atende ao requisito; sua execução precisa ocorrer depois da reconstrução das tabelas

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE `nordeste-health-lakehouse`.gold.dim_estabelecimento
# MAGIC SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'uf,id_municipio');
# MAGIC ALTER TABLE `nordeste-health-lakehouse`.gold.fato_capacidade_hospitalar
# MAGIC SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'uf,id_municipio');
# MAGIC ALTER TABLE `nordeste-health-lakehouse`.gold.fato_alocacao_profissionais
# MAGIC SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'uf,id_municipio');
# MAGIC ANALYZE TABLE `nordeste-health-lakehouse`.gold.dim_estabelecimento COMPUTE DELTA STATISTICS;
# MAGIC ANALYZE TABLE `nordeste-health-lakehouse`.gold.fato_capacidade_hospitalar COMPUTE DELTA STATISTICS;
# MAGIC ANALYZE TABLE `nordeste-health-lakehouse`.gold.fato_alocacao_profissionais COMPUTE DELTA STATISTICS;
# MAGIC OPTIMIZE `nordeste-health-lakehouse`.gold.dim_estabelecimento ZORDER BY (uf, id_municipio);
# MAGIC OPTIMIZE `nordeste-health-lakehouse`.gold.fato_capacidade_hospitalar ZORDER BY (uf, id_municipio);
# MAGIC OPTIMIZE `nordeste-health-lakehouse`.gold.fato_alocacao_profissionais ZORDER BY (uf, id_municipio);

# COMMAND ----------

# MAGIC %md
# MAGIC ##Guardar história, plano e comparação de versões
# MAGIC A antiga célula com dois comandos SQL preservava apenas a última saída de exibição e não comprovava ganho de desempenho

# COMMAND ----------

historico = spark.sql(f"DESCRIBE HISTORY {TABELA_BENCHMARK}")
display(historico.select("version", "timestamp", "operation", "operationParameters", "operationMetrics"))
VERSAO_DEPOIS_OTIMIZACAO = int(historico.orderBy(F.desc("version")).first()["version"])
assert VERSAO_DEPOIS_OTIMIZACAO > VERSAO_ANTES_OTIMIZACAO
assert historico.filter((F.col("version") > VERSAO_ANTES_OTIMIZACAO) &
                        (F.col("operation") == "OPTIMIZE")).limit(1).count() > 0

def consulta_versao(versao):
    return f"""SELECT SUM(quantidade_leitos) AS quantidade
        FROM {TABELA_BENCHMARK} VERSION AS OF {int(versao)}
        WHERE uf = 'PE' AND id_municipio = '261160' AND tipo_recurso = 'LEITO'"""
# Alternar as versões reduz viés de executar todos os testes de uma delas primeiro.
amostras, valores = [], {}
for repeticao in range(4):
    fases = [("antes", VERSAO_ANTES_OTIMIZACAO), ("depois", VERSAO_DEPOIS_OTIMIZACAO)]
    if repeticao % 2: fases.reverse()
    for fase, versao in fases:
        inicio = perf_counter()
        resultado = spark.sql(consulta_versao(versao)).collect()[0]["quantidade"]
        segundos = perf_counter() - inicio
        valores[fase] = resultado
        if repeticao > 0:  # primeira rodada serve como aquecimento
            amostras.append((fase, int(versao), repeticao, float(segundos), str(resultado)))
assert valores["antes"] == valores["depois"], "O resultado da consulta mudou"
df_amostras = spark.createDataFrame(amostras,
    "fase string, versao long, repeticao long, duracao_segundos double, resultado_consulta string")
gravar(df_amostras.withColumn("executado_em", F.current_timestamp()), "auditoria_benchmark_zorder")
display(df_amostras)
for fase in ["antes", "depois"]:
    tempos = [r[3] for r in amostras if r[0] == fase]
    print(fase, "mediana em segundos:", round(median(tempos), 4))
plano = spark.sql("EXPLAIN FORMATTED " + consulta_versao(VERSAO_DEPOIS_OTIMIZACAO))
display(plano)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Operação e interpretação
# MAGIC
# MAGIC - **Ordem:** Bronze → Silver → Gold → Analytics. No Job, configurar quatro tarefas
# MAGIC   dependentes, uma por notebook, e uma única execução concorrente.
# MAGIC - **Bronze:** snapshot completo, origem preservada, arquivo e hora de carga.
# MAGIC - **Silver:** Nordeste, validade, chaves, rejeições e auditoria; HORAHOSP separado de
# MAGIC   HORA_AMB e HORAOUTR. Registrar a evidência de compatibilidade temporal da extração.
# MAGIC - **Gold:** quatro dimensões, duas fatos e ponte de habilitações; sem multiplicação
# MAGIC   de quantidades por joins; somas reconciliadas por CNES e competência.
# MAGIC - **KPI 1:** horas hospitalares cadastradas de intensivistas e enfermeiros por leito
# MAGIC   das modalidades UTI selecionadas. Sem meta, apresentar proporcionalidade e a
# MAGIC   ausência de referência quantitativa. O cadastro não comprova escala por turno.
# MAGIC - **KPI 2:** cenários explícitos por equipamento e modalidade. Déficit no cenário
# MAGIC   identifica uma diferença cadastral a investigar. O inventário não demonstra
# MAGIC   dedicação exclusiva do recurso nem disponibilidade física efetiva.
# MAGIC - **KPI 3:** ocupações médicas diferenciadas por município; o CBO informa ocupação,
# MAGIC   não comprova titulação. Os percentuais têm denominadores identificados e os
# MAGIC   médicos sem classificação permanecem visíveis.
# MAGIC - **Cobertura:** manter alerta_orfaos e competencia_estabelecimentos_atribuida
# MAGIC   nas saídas. Diferença zero na auditoria não comprova cobertura completa.
# MAGIC - **Evidências:** comparação de conteúdo Bronze, reprocessamento quando executado,
# MAGIC   reconciliação HORAHOSP, PK/FK, testes de negócio, PySpark × SQL e histórico Delta.
# MAGIC - **Desempenho:** guardar amostras e conferir arquivos/bytes no perfil da consulta.
# MAGIC   Caches e condições de execução influenciam os tempos.
# MAGIC - **Evolução mensal:** receber arquivos de outro período em diretório separado,
# MAGIC   registrar um manifesto de carga, validar o período e preservar o modelo histórico.
# MAGIC   Escolher CDC/MERGE somente se a fonte fornecer alterações ou versões por chave.
# MAGIC
# MAGIC Preencher as contagens e os resultados da apresentação com as novas saídas reais
# MAGIC do notebook. As contagens anteriores não representam respostas esperadas dos KPIs
# MAGIC depois da correção dos dicionários e das medidas.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Conceder acesso somente às views de consumo
# MAGIC A criação de catálogo e schemas organiza objetos; o acesso do consumidor precisa de concessões explícitas

# COMMAND ----------

GRUPO_CONSUMO = None  # substituir pelo nome de um grupo de conta existente no seu ambiente
if GRUPO_CONSUMO is None:
    print("Permissões de consumo não executadas: informe um grupo real existente.")
else:
    assert isinstance(GRUPO_CONSUMO, str) and GRUPO_CONSUMO.strip()
    principal = "`" + GRUPO_CONSUMO.replace("`", "``") + "`"
    spark.sql(f"GRANT USE CATALOG ON CATALOG `{CATALOGO}` TO {principal}")
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA `{CATALOGO}`.`gold` TO {principal}")
    for nome in ["vw_capacidade_hospitalar", "vw_suporte_intensivo",
                 "vw_densidade_tecnologica", "vw_concentracao_especialistas"]:
        spark.sql(f"GRANT SELECT ON VIEW {tb('gold', nome)} TO {principal}")
    print("Concessões de leitura nas quatro views executadas.")

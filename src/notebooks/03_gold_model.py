# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ##Conferir o contrato recebido da Silver
# MAGIC Uma Gold executada sobre a Silver antiga continuaria produzindo as horas incorretas.

# COMMAND ----------

from pyspark.sql import functions as F
CATALOGO = "nordeste-health-lakehouse"
def tb(camada, nome):
    return f"`{CATALOGO}`.`{camada}`.`{nome}`"
def ler(nome):
    return spark.table(tb("silver", nome))
def gravar(df, nome):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(tb("gold", nome)))
def sk(*campos):
    return F.sha2(F.to_json(F.struct(*[F.col(c).alias(c) for c in campos]),
                            options={"ignoreNullFields": "false"}), 256)
def chave_unica(df, campos, nome):
    nulos = F.lit(False)
    for campo in campos:
        nulos = nulos | F.col(campo).isNull()
    assert df.filter(nulos).limit(1).count() == 0, f"{nome}: chave nula"
    assert df.groupBy(*campos).count().filter("count > 1").limit(1).count() == 0, f"{nome}: chave duplicada"

est, hab, lei, pro, equ = [ler(n) for n in [
    "estabelecimentos_de_saude", "habilitacoes", "leitos", "profissionais", "equipamentos"]]
contrato = ler("contrato_medidas").first()
assert contrato is not None, "Contrato de medidas ausente"
for nome, df in [("est", est), ("hab", hab), ("lei", lei), ("pro", pro), ("equ", equ)]:
    assert df.filter(F.col("competencia") != contrato["competencia"]).limit(1).count() == 0, nome
    chave_unica(df, ["id_registro"], nome)

assert ler("contrato_medidas").count() == 1, "Contrato deve conter exatamente uma linha"
assert contrato["campo_horas_origem"] == "HORAHOSP", "Reexecutar a Silver corrigida"
assert "horas_hospitalares" in pro.columns, "Célula S05 ainda não foi aplicada"
assert pro.filter(~F.col("horas_semanais").eqNullSafe(F.col("horas_hospitalares"))).limit(1).count() == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criar referências imutáveis por versão
# MAGIC Os códigos precisam de significado documentado. A versão impede alterar silenciosamente uma classificação já usada.

# COMMAND ----------

VERSAO_REFERENCIAS = "2026-09-29_v1"
FONTE_CBO = "https://www.unasus.gov.br/programa_modular/4"
FONTE_RNDS = "https://rnds-fhir.saude.gov.br/CodeSystem-BRCBO.html"
FONTE_ANS = "https://fhir-hm.ans.gov.br/CodeSystem-tuss-24.html"
FONTE_LEITOS = "https://cnes2.datasus.gov.br/Mod_Ind_Tipo_Leito.asp?VEstado=29"
FONTE_EQUIPAMENTOS = "https://wiki.datasus.gov.br/cnes/index.php/Saiba_mais_sobre_o_CNES_Simplificado"
FONTE_HABILITACOES = "https://cnes2.datasus.gov.br/Mod_Ind_Habilitacoes.asp"

def salvar_referencia(df, nome, chave):
    df = df.withColumn("versao_referencia", F.lit(VERSAO_REFERENCIAS))
    chave_unica(df, ["versao_referencia", chave], nome)
    if not spark.catalog.tableExists(tb("gold", nome)):
        df.write.format("delta").mode("errorifexists").saveAsTable(tb("gold", nome))
    else:
        existentes = spark.table(tb("gold", nome))
        atual = existentes.filter(F.col("versao_referencia") == VERSAO_REFERENCIAS).select(*df.columns)
        if atual.limit(1).count():
            assert df.exceptAll(atual).limit(1).count() == 0, f"{nome}: mude VERSAO_REFERENCIAS para alterar regras"
            assert atual.exceptAll(df).limit(1).count() == 0, f"{nome}: versão existente tem outro conteúdo"
        else:
            df.write.format("delta").mode("append").saveAsTable(tb("gold", nome))
    return df

# Nomes e códigos ocupacionais. Os flags analíticos abaixo são critérios do projeto.
nomes_medicos = {
    "225103": "Medico infectologista", "225105": "Medico acupunturista",
    "225106": "Medico legista", "225109": "Medico nefrologista",
    "225110": "Medico alergista e imunologista", "225112": "Medico neurologista",
    "225115": "Medico angiologista", "225118": "Medico nutrologista",
    "225120": "Medico cardiologista", "225121": "Medico oncologista clinico",
    "225122": "Medico cancerologista pediatrico", "225124": "Medico pediatra",
    "225125": "Medico clinico", "225127": "Medico pneumologista",
    "225130": "Medico de familia e comunidade", "225133": "Medico psiquiatra",
    "225135": "Medico dermatologista", "225136": "Medico reumatologista",
    "225139": "Medico sanitarista", "225140": "Medico do trabalho",
    "225142": "Medico da estrategia de saude da familia", "225145": "Medico em medicina de trafego",
    "225148": "Medico anatomopatologista", "225150": "Medico em medicina intensiva",
    "225151": "Medico anestesiologista", "225154": "Medico antroposofico",
    "225155": "Medico endocrinologista e metabologista", "225160": "Medico fisiatra",
    "225165": "Medico gastroenterologista", "225170": "Medico generalista",
    "225175": "Medico geneticista", "225180": "Medico geriatra",
    "225185": "Medico hematologista", "225195": "Medico homeopata",
    "225203": "Medico em cirurgia vascular", "225210": "Medico cirurgiao cardiovascular",
    "225215": "Medico cirurgiao de cabeca e pescoco", "225220": "Medico cirurgiao do aparelho digestivo",
    "225225": "Medico cirurgiao geral", "225230": "Medico cirurgiao pediatrico",
    "225235": "Medico cirurgiao plastico", "225240": "Medico cirurgiao toracico",
    "225245": "Medico foniatra", "225250": "Medico ginecologista e obstetra",
    "225255": "Medico mastologista", "225260": "Medico neurocirurgiao",
    "225265": "Medico oftalmologista", "225270": "Medico ortopedista e traumatologista",
    "225275": "Medico otorrinolaringologista", "225280": "Medico coloproctologista",
    "225285": "Medico urologista", "225290": "Medico cancerologista cirurgico",
    "225295": "Medico cirurgiao da mao", "225305": "Medico citopatologista",
    "225310": "Medico em endoscopia", "225315": "Medico em medicina nuclear",
    "225320": "Medico em radiologia e diagnostico por imagem", "225325": "Medico patologista",
    "225330": "Medico radioterapeuta", "225335": "Medico patologista clinico medicina laboratorial",
    "225340": "Medico hemoterapeuta", "225345": "Medico hiperbarista",
    "225350": "Medico neurofisiologista clinico", "225355": "Medico radiologista intervencionista",
    "223119": "Medico em eletroencefalografia", "223150": "Medico perito",
    "2231A1": "Medico broncoesofalogista", "2231A2": "Medico hansenologista",
    "2231F8": "Medico em medicina preventiva e social", "2231F9": "Medico residente",
    "2231G1": "Medico cardiologista intervencionista",
}
nomes_enfermeiros = {
    "223505": "Enfermeiro", "223510": "Enfermeiro auditor",
    "223515": "Enfermeiro de bordo", "223520": "Enfermeiro de centro cirurgico",
    "223525": "Enfermeiro de terapia intensiva", "223530": "Enfermeiro do trabalho",
    "223535": "Enfermeiro nefrologista", "223540": "Enfermeiro neonatologista",
    "223545": "Enfermeiro obstetrico", "223550": "Enfermeiro psiquiatrico",
    "223555": "Enfermeiro puericultor e pediatrico", "223560": "Enfermeiro sanitarista",
    "223565": "Enfermeiro da estrategia de saude da familia",
    "223580": "Enfermeiro estomaterapeuta", "223585": "Enfermeiro forense",
    "2235C3": "Enfermeiro estomaterapeuta",
}
# Recorte: ocupações médicas diferenciadas; não é certificação de especialidade.
generalistas_no_criterio = {"225125", "225170", "225142"}
residentes_sem_especialidade = {"2231F9"}
cbo_linhas = []
for cod, descricao in nomes_medicos.items():
    especialista = None if cod in residentes_sem_especialidade else cod not in generalistas_no_criterio
    grupo = "medico_intensivista" if cod == "225150" else "outro"
    fonte = FONTE_RNDS if cod.startswith("2231") or cod in {"225245", "225154"} else FONTE_CBO
    if cod == "225355": fonte = FONTE_ANS
    if cod == "225122": fonte = FONTE_RNDS
    cbo_linhas.append((cod, descricao, grupo, True, especialista, True,
                       especialista is not None, fonte))
for cod, descricao in nomes_enfermeiros.items():
    fonte = FONTE_RNDS
    if cod == "223580": fonte = "https://cnes2.datasus.gov.br/Mod_Profissional.asp?VCo_Unidade=4219500727539"
    if cod == "223585": fonte = "https://cnes2.datasus.gov.br/Mod_Profissional.asp?VCo_Unidade=2800302816210"
    cbo_linhas.append((cod, descricao, "enfermeiro", False, False, True, True, fonte))
cbo_linhas += [
    ("223570", "Perfusionista", "outro", False, False, True, True, FONTE_ANS),
    ("223575", "Obstetriz", "outro", False, False, True, True,
     "https://cenits.saude.gov.br/wp-content/uploads/2024/10/Forca-de-Trabalho-em-Saude.pdf"),
]
ref_cbo = salvar_referencia(spark.createDataFrame(cbo_linhas, """
    cbo string, descricao_oficial string, grupo_kpi1 string, eh_medico boolean,
    eh_especialista boolean, classificacao_kpi1_validada boolean,
    classificacao_kpi3_validada boolean, fonte_referencia string
"""), "ref_classificacao_cbo", "cbo")

leitos_linhas = [
    ("10", "Obstetricia cirurgica", False, "FORA_ESCOPO"),
    ("43", "Obstetricia clinica", False, "FORA_ESCOPO"),
    ("74", "UTI adulto I", True, "ADULTO"), ("75", "UTI adulto II", True, "ADULTO"),
    ("76", "UTI adulto III", True, "ADULTO"),
    ("77", "UTI pediatrica I", True, "PEDIATRICA"), ("78", "UTI pediatrica II", True, "PEDIATRICA"),
    ("79", "UTI pediatrica III", True, "PEDIATRICA"),
    ("80", "UTI neonatal I", True, "NEONATAL"), ("81", "UTI neonatal II", True, "NEONATAL"),
    ("82", "UTI neonatal III", True, "NEONATAL"), ("83", "UTI queimados", True, "QUEIMADOS"),
    ("85", "UCO II", False, "CORONARIANA_FORA_ESCOPO"),
    ("86", "UCO III", False, "CORONARIANA_FORA_ESCOPO"),
]
ref_leito = salvar_referencia(spark.createDataFrame(leitos_linhas,
    "codigo_tipo_leito string, descricao_oficial string, eh_uti_escopo boolean, modalidade_uti string")
    .withColumn("fonte_referencia", F.when(F.col("codigo_tipo_leito").isin("74", "77"),
        F.lit("https://cnes2.datasus.gov.br/Mod_Ind_Tipo_Leito.asp?VComp=201306&VEstado=31&VMun="))
        .otherwise(F.lit(FONTE_LEITOS))), "ref_leitos_uti", "codigo_tipo_leito")

equipamentos_linhas = [
    ("11", "Tomografo computadorizado"), ("12", "Ressonancia magnetica"),
    ("56", "Desfibrilador"), ("58", "Incubadora"),
    ("60", "Monitor de ECG"), ("64", "Respirador ou ventilador"),
]
ref_equipamento = salvar_referencia(spark.createDataFrame(equipamentos_linhas,
    "codigo_equipamento string, descricao_oficial string")
    .withColumn("fonte_referencia", F.lit(FONTE_EQUIPAMENTOS)),
    "ref_equipamentos_analiticos", "codigo_equipamento")

habilitacoes_linhas = [
    ("2601", "UTI adulto II", "ADULTO"), ("2602", "UTI neonatal II", "NEONATAL"),
    ("2603", "UTI pediatrica II", "PEDIATRICA"), ("2604", "UTI adulto III", "ADULTO"),
    ("2605", "UTI neonatal III", "NEONATAL"), ("2606", "UTI pediatrica III", "PEDIATRICA"),
    ("2607", "UTI queimados", "QUEIMADOS"),
]
ref_habilitacao = salvar_referencia(spark.createDataFrame(habilitacoes_linhas,
    "codigo_habilitacao string, descricao_oficial string, modalidade_uti string")
    .withColumn("fonte_referencia", F.lit(FONTE_HABILITACOES)),
    "ref_habilitacoes_uti", "codigo_habilitacao")
print("Referências disponíveis na versão", VERSAO_REFERENCIAS)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Enriquecer a dimensão profissional sem expor CNS
# MAGIC A classificação e sua cobertura precisam acompanhar a dimensão e distinguir ausência de regra de ocupação fora do escopo médico.

# COMMAND ----------

dim_pro = (pro.groupBy("id_profissional", "cbo")
    .agg(F.min("descricao_cbo").alias("descricao_cbo_origem"))
    .join(ref_cbo, "cbo", "left")
    .withColumn("sk_profissional", sk("id_profissional", "cbo"))
    .withColumn("id_profissional_analitico", F.sha2(F.col("id_profissional"), 256)))
candidato_medico = F.col("cbo").rlike(r"^(225|2231)")
candidato_kpi1 = F.col("cbo").rlike(r"^(225|2231|2235)")
dim_pro = (dim_pro
    .withColumn("descricao_cbo", F.coalesce("descricao_oficial", "descricao_cbo_origem"))
    .withColumn("eh_medico", F.coalesce(F.col("eh_medico"), candidato_medico))
    .withColumn("grupo_kpi1", F.coalesce(F.col("grupo_kpi1"),
        F.when(candidato_kpi1, "nao_classificado").otherwise("outro")))
    .withColumn("classificacao_kpi1_validada", F.coalesce(F.col("classificacao_kpi1_validada"), ~candidato_kpi1))
    .withColumn("classificacao_kpi3_validada", F.coalesce(F.col("classificacao_kpi3_validada"), ~candidato_medico))
    .withColumn("eh_especialista", F.when(F.col("classificacao_kpi3_validada"),
        F.coalesce(F.col("eh_especialista"), F.lit(False))).otherwise(F.lit(None).cast("boolean")))
    .withColumn("versao_classificacao", F.lit(VERSAO_REFERENCIAS))
    .drop("id_profissional", "descricao_oficial", "descricao_cbo_origem", "versao_referencia"))
chave_unica(dim_pro, ["sk_profissional"], "dim_profissional")
assert dim_pro.filter("classificacao_kpi1_validada IS NULL OR classificacao_kpi3_validada IS NULL").limit(1).count() == 0
gravar(dim_pro, "dim_profissional")
display(dim_pro.select("cbo", "descricao_cbo", "grupo_kpi1", "eh_medico", "eh_especialista",
    "classificacao_kpi1_validada", "classificacao_kpi3_validada").distinct().orderBy("cbo"))

# COMMAND ----------

dim_est = est.select("cnes", "competencia", "nome_estabelecimento", "uf", "id_municipio", "nome_municipio")
dim_est = dim_est.withColumn("sk_estabelecimento", sk("cnes", "competencia"))
chave_unica(dim_est, ["sk_estabelecimento"], "dim_estabelecimento")
gravar(dim_est, "dim_estabelecimento")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Enriquecer as dimensões de recurso com códigos oficiais
# MAGIC As dimensões precisam descrever os recursos usados nos KPIs e manter somente os tipos realmente existentes no snapshot.

# COMMAND ----------

dim_lei = (lei.groupBy("codigo_tipo_leito")
    .agg(F.min("descricao_tipo_leito").alias("descricao_origem"))
    .join(ref_leito, "codigo_tipo_leito", "left")
    .withColumn("descricao_tipo_leito", F.coalesce("descricao_oficial", "descricao_origem"))
    .withColumn("eh_uti_escopo", F.coalesce(F.col("eh_uti_escopo"), F.lit(False)))
    .withColumn("modalidade_uti", F.coalesce(F.col("modalidade_uti"), F.lit("FORA_ESCOPO")))
    .withColumn("versao_classificacao", F.lit(VERSAO_REFERENCIAS))
    .withColumn("sk_tipo_leito", sk("codigo_tipo_leito"))
    .drop("descricao_oficial", "descricao_origem", "versao_referencia"))
na_leito = (spark.createDataFrame([("__NA__", "NAO SE APLICA", "NA_LEITO")],
    "codigo_tipo_leito string, descricao_tipo_leito string, sk_tipo_leito string")
    .withColumn("eh_uti_escopo", F.lit(False)).withColumn("modalidade_uti", F.lit("NAO_SE_APLICA"))
    .withColumn("versao_classificacao", F.lit(VERSAO_REFERENCIAS)))
assert dim_lei.filter("codigo_tipo_leito = '__NA__'").limit(1).count() == 0
dim_lei = dim_lei.unionByName(na_leito, allowMissingColumns=True)
chave_unica(dim_lei, ["sk_tipo_leito"], "dim_tipo_leito")
gravar(dim_lei, "dim_tipo_leito")

dim_equ = (equ.groupBy("codigo_equipamento")
    .agg(F.min("descricao_equipamento").alias("descricao_origem"))
    .join(ref_equipamento, "codigo_equipamento", "left")
    .withColumn("descricao_equipamento", F.coalesce("descricao_oficial", "descricao_origem"))
    .withColumn("recurso_kpi2_mapeado", F.col("descricao_oficial").isNotNull())
    .withColumn("versao_classificacao", F.lit(VERSAO_REFERENCIAS))
    .withColumn("sk_equipamento", sk("codigo_equipamento"))
    .drop("descricao_oficial", "descricao_origem", "versao_referencia"))
na_equipamento = (spark.createDataFrame([("__NA__", "NAO SE APLICA", "NA_EQUIPAMENTO")],
    "codigo_equipamento string, descricao_equipamento string, sk_equipamento string")
    .withColumn("recurso_kpi2_mapeado", F.lit(False))
    .withColumn("versao_classificacao", F.lit(VERSAO_REFERENCIAS)))
assert dim_equ.filter("codigo_equipamento = '__NA__'").limit(1).count() == 0
dim_equ = dim_equ.unionByName(na_equipamento, allowMissingColumns=True)
chave_unica(dim_equ, ["sk_equipamento"], "dim_equipamento")
gravar(dim_equ, "dim_equipamento")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Enriquecer a ponte de habilitações
# MAGIC A ponte precisa distinguir habilitações de UTI de outros serviços sem eliminar os demais registros

# COMMAND ----------

local = dim_est.select("cnes", "competencia", "sk_estabelecimento", "uf", "id_municipio")
ponte = (hab.groupBy("cnes", "competencia", "codigo_habilitacao")
    .agg(F.min("descricao_habilitacao").alias("descricao_origem"))
    .join(ref_habilitacao, "codigo_habilitacao", "left")
    .withColumn("descricao_habilitacao", F.coalesce("descricao_oficial", "descricao_origem"))
    .withColumn("habilitacao_uti_mapeada", F.col("descricao_oficial").isNotNull())
    .withColumn("versao_classificacao", F.lit(VERSAO_REFERENCIAS))
    .drop("descricao_oficial", "descricao_origem", "versao_referencia")
    .join(local, ["cnes", "competencia"], "inner")
    .withColumn("sk_habilitacao_estabelecimento", sk("cnes", "competencia", "codigo_habilitacao")))
chave_unica(ponte, ["sk_habilitacao_estabelecimento"], "ponte_estabelecimento_habilitacao")
gravar(ponte, "ponte_estabelecimento_habilitacao")

# COMMAND ----------

local = dim_est.select("cnes", "competencia", "sk_estabelecimento", "uf", "id_municipio")
leitos_agregados = lei.groupBy("cnes", "competencia", "codigo_tipo_leito").agg(
    F.sum("quantidade").cast("decimal(24,2)").alias("quantidade_leitos"))
equipamentos_agregados = equ.groupBy("cnes", "competencia", "codigo_equipamento").agg(
    F.sum("quantidade").cast("decimal(24,2)").alias("quantidade_equipamentos"))

fat_lei = (leitos_agregados
    .join(local, ["cnes", "competencia"], "inner")
    .join(dim_lei.select("codigo_tipo_leito", "sk_tipo_leito"), "codigo_tipo_leito", "inner")
    .withColumn("tipo_recurso", F.lit("LEITO"))
    .withColumn("codigo_recurso", F.col("codigo_tipo_leito"))
    .withColumn("sk_equipamento", F.lit("NA_EQUIPAMENTO"))
    .withColumn("quantidade_equipamentos", F.lit(0).cast("decimal(24,2)")))
fat_equ = (equipamentos_agregados
    .join(local, ["cnes", "competencia"], "inner")
    .join(dim_equ.select("codigo_equipamento", "sk_equipamento"), "codigo_equipamento", "inner")
    .withColumn("tipo_recurso", F.lit("EQUIPAMENTO"))
    .withColumn("codigo_recurso", F.col("codigo_equipamento"))
    .withColumn("sk_tipo_leito", F.lit("NA_LEITO"))
    .withColumn("quantidade_leitos", F.lit(0).cast("decimal(24,2)")))
colunas_cap = ["cnes", "competencia", "sk_estabelecimento", "uf", "id_municipio",
               "tipo_recurso", "codigo_recurso", "sk_tipo_leito", "sk_equipamento",
               "quantidade_leitos", "quantidade_equipamentos"]
fat_cap = fat_lei.select(*colunas_cap).unionByName(fat_equ.select(*colunas_cap))
fat_cap = fat_cap.withColumn("sk_capacidade", sk("cnes", "competencia", "tipo_recurso", "codigo_recurso"))
chave_unica(fat_cap, ["sk_capacidade"], "fato_capacidade_hospitalar")
gravar(fat_cap, "fato_capacidade_hospitalar")

# COMMAND ----------

pro_chave = pro.withColumn("sk_profissional", sk("id_profissional", "cbo"))
fat_pro = (pro_chave
    .join(local, ["cnes", "competencia"], "inner")
    .select(F.col("id_registro").alias("sk_alocacao"), "cnes", "competencia",
            "sk_estabelecimento", "sk_profissional", "uf", "id_municipio",
            "vinculo", "chave_extra", F.col("horas_semanais").cast("decimal(24,2)").alias("horas_semanais")))
chave_unica(fat_pro, ["sk_alocacao"], "fato_alocacao_profissionais")
gravar(fat_pro, "fato_alocacao_profissionais")

# COMMAND ----------

def validar_fk(fato, dimensao, chave, nome):
    assert fato.filter(F.col(chave).isNull()).limit(1).count() == 0, f"{nome}: FK nula"
    sem_dimensao = fato.select(chave).distinct().join(dimensao.select(chave).distinct(), chave, "left_anti")
    assert sem_dimensao.limit(1).count() == 0, f"{nome}: referência ausente"

validar_fk(fat_cap, dim_est, "sk_estabelecimento", "capacidade/estabelecimento")
validar_fk(fat_cap, dim_lei, "sk_tipo_leito", "capacidade/leito")
validar_fk(fat_cap, dim_equ, "sk_equipamento", "capacidade/equipamento")
validar_fk(fat_pro, dim_est, "sk_estabelecimento", "alocacao/estabelecimento")
validar_fk(fat_pro, dim_pro, "sk_profissional", "alocacao/profissional")
validar_fk(ponte, dim_est, "sk_estabelecimento", "habilitacao/estabelecimento")
assert fat_pro.count() == pro.count(), "O join perdeu ou multiplicou vínculos profissionais"
assert fat_cap.count() == leitos_agregados.count() + equipamentos_agregados.count()
print("PKs, FKs e cardinalidades conferidas.")

# COMMAND ----------

def reconciliar(origem, destino, medida_origem, medida_destino, nome):
    a = origem.groupBy("cnes", "competencia").agg(
        F.sum(medida_origem).cast("decimal(30,2)").alias("valor_silver"))
    b = destino.groupBy("cnes", "competencia").agg(
        F.sum(medida_destino).cast("decimal(30,2)").alias("valor_gold"))
    comparacao = a.join(b, ["cnes", "competencia"], "full")
    diferencas = comparacao.filter(~F.col("valor_silver").eqNullSafe(F.col("valor_gold")))
    assert diferencas.limit(1).count() == 0, f"Reconciliação divergente: {nome}"
    print(f"{nome}: soma preservada por CNES e competência.")

reconciliar(lei, fat_cap.filter("tipo_recurso = 'LEITO'"), "quantidade", "quantidade_leitos", "leitos")
reconciliar(equ, fat_cap.filter("tipo_recurso = 'EQUIPAMENTO'"), "quantidade", "quantidade_equipamentos", "equipamentos")
reconciliar(pro, fat_pro, "horas_semanais", "horas_semanais", "horas")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Atualizar a documentação da fato profissional
# MAGIC O comentário publicado precisa corresponder à medida hospitalar corrigida.

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE `nordeste-health-lakehouse`.gold.dim_estabelecimento
# MAGIC IS 'Estabelecimento CNES e localização no snapshot mensal do projeto.';
# MAGIC COMMENT ON TABLE `nordeste-health-lakehouse`.gold.dim_profissional
# MAGIC IS 'Profissional e CBO; identificadores pseudonimizados para análise.';
# MAGIC COMMENT ON TABLE `nordeste-health-lakehouse`.gold.fato_capacidade_hospitalar
# MAGIC IS 'Capacidade por unidade, competência, classe e tipo de recurso; leitos e equipamentos não são multiplicados por join.';
# MAGIC COMMENT ON TABLE `nordeste-health-lakehouse`.gold.fato_alocacao_profissionais
# MAGIC IS 'HORAHOSP: carga horária hospitalar semanal cadastrada por vínculo; não identifica dedicação exclusiva à UTI.';
# MAGIC CREATE OR REPLACE VIEW `nordeste-health-lakehouse`.gold.vw_capacidade_hospitalar AS
# MAGIC SELECT e.cnes, e.competencia, e.nome_estabelecimento, e.uf, e.id_municipio, e.nome_municipio,
# MAGIC        c.tipo_recurso, c.codigo_recurso,
# MAGIC        l.descricao_tipo_leito, q.descricao_equipamento,
# MAGIC        c.quantidade_leitos, c.quantidade_equipamentos
# MAGIC FROM `nordeste-health-lakehouse`.gold.fato_capacidade_hospitalar c
# MAGIC JOIN `nordeste-health-lakehouse`.gold.dim_estabelecimento e USING (sk_estabelecimento)
# MAGIC JOIN `nordeste-health-lakehouse`.gold.dim_tipo_leito l USING (sk_tipo_leito)
# MAGIC JOIN `nordeste-health-lakehouse`.gold.dim_equipamento q USING (sk_equipamento)

# COMMAND ----------

# Dados sintéticos: não gravar nas tabelas de negócio.
from decimal import Decimal as D
from pyspark.sql import functions as F

def consolidar_kpi2(df):
    estoque = (df.groupBy("cnes", "competencia", "codigo_equipamento")
        .agg(
            F.max("quantidade_cadastrada").alias("quantidade_cadastrada"),
            F.countDistinct("modalidade_uti").alias("_qtd_modalidades"))
        .withColumn("estoque_compartilhado_entre_modalidades",
            F.col("_qtd_modalidades") > 1)
        .drop("_qtd_modalidades"))

    resumo = (df.groupBy("cnes", "competencia", "modalidade_uti")
        .agg(
            F.count("codigo_habilitacao").alias("qtd_habilitacoes_modalidade"),
            F.max("quantidade_minima").alias("quantidade_minima"),
            F.max("deficit_cadastral").alias("deficit_cadastral"),
            F.countDistinct("coeficiente_por_leito").alias("_qtd_coeficientes"),
            F.max("status_analitico").alias("status_analitico"))
        .withColumn("status_analitico",
            F.when(F.col("_qtd_coeficientes") > 1,
                F.lit("REGRAS_DIVERGENTES_NAO_CONSOLIDAR"))
            .otherwise(F.col("status_analitico")))
        .withColumn("deficit_cadastral",
            F.when(F.col("_qtd_coeficientes") > 1,
                F.lit(None).cast("decimal(24,2)"))
            .otherwise(F.col("deficit_cadastral")))
        .withColumn("quantidade_minima",
            F.when(F.col("_qtd_coeficientes") > 1,
                F.lit(None).cast("decimal(24,2)"))
            .otherwise(F.col("quantidade_minima")))
        .drop("_qtd_coeficientes"))

    return estoque, resumo
schema_teste = """
cnes string, competencia string, codigo_equipamento string,
modalidade_uti string, codigo_habilitacao string,
quantidade_cadastrada decimal(24,2), leitos_base decimal(24,2),
base_calculo string, coeficiente_por_leito decimal(18,4),
quantidade_minima_fixa decimal(18,2), natureza_regra string,
fonte_regra string, versao_regra string,
quantidade_minima decimal(24,2), deficit_cadastral decimal(24,2),
razao_inventario_meta decimal(18,4), status_analitico string
"""
linhas = [
("0000001", "202607", "60", "ADULTO", "2601", D("25"), D("80"),
"por_leito", D("1"), None, "cenario_analitico", "teste", "v1",
D("80"), D("55"), D("0.3125"), "POTENCIAL_DEFICIT_NO_CENARIO"),
("0000001", "202607", "60", "ADULTO", "2604", D("25"), D("80"),
"por_leito", D("1"), None, "cenario_analitico", "teste", "v1",
D("80"), D("55"), D("0.3125"), "POTENCIAL_DEFICIT_NO_CENARIO"),
("0000001", "202607", "60", "PEDIATRICA", "2602", D("25"), D("10"),
"por_leito", D("1"), None, "cenario_analitico", "teste", "v1",
D("10"), D("0"), D("2.5"), "ATINGE_META_DO_CENARIO"),
]
teste = spark.createDataFrame(linhas, schema_teste)
estoque_teste, resumo_teste = consolidar_kpi2(teste)
assert estoque_teste.count() == 1
assert estoque_teste.first()["quantidade_cadastrada"] == D("25")
assert estoque_teste.first()["estoque_compartilhado_entre_modalidades"]
adulto = resumo_teste.filter("modalidade_uti = 'ADULTO'").first()
assert adulto["deficit_cadastral"] == D("55")
assert adulto["qtd_habilitacoes_modalidade"] == 2
assert resumo_teste.count() == 2
divergente = teste.withColumn("coeficiente_por_leito",
F.when(F.col("codigo_habilitacao") == "2604", F.lit(2))
.otherwise(F.col("coeficiente_por_leito")).cast("decimal(18,4)"))
_, resultado_divergente = consolidar_kpi2(divergente)
adulto_div = resultado_divergente.filter("modalidade_uti = 'ADULTO'").first()
assert adulto_div["deficit_cadastral"] is None
assert adulto_div["quantidade_minima"] is None
assert adulto_div["status_analitico"] == "REGRAS_DIVERGENTES_NAO_CONSOLIDAR"
print("PASSOU: estoque único, habilitações, compartilhamento e divergência.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Preservar snapshots Gold por mês e recorte
# MAGIC A Gold principal representa o snapshot atual; uma cópia histórica explícita permite demonstrar reprocessamento mensal sem apagar outros meses.

# COMMAND ----------

import re
competencia_snapshot = str(contrato["competencia"])
assert re.fullmatch(r"\d{4}(0[1-9]|1[0-2])", competencia_snapshot)
ufs = sorted(set(contrato["ufs_escopo"].split(",")))
assert ufs and set(ufs).issubset({"AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"})
escopo_snapshot = ",".join(ufs)
condicao = f"competencia_snapshot = '{competencia_snapshot}' AND escopo_ufs_snapshot = '{escopo_snapshot}'"
modelos = ["dim_estabelecimento", "dim_profissional", "dim_tipo_leito", "dim_equipamento",
           "fato_capacidade_hospitalar", "fato_alocacao_profissionais", "ponte_estabelecimento_habilitacao"]
for nome in modelos:
    snapshot = (spark.table(tb("gold", nome))
        .withColumn("competencia_snapshot", F.lit(competencia_snapshot))
        .withColumn("escopo_ufs_snapshot", F.lit(escopo_snapshot)))
    assert snapshot.limit(1).count() > 0, f"{nome}: investigar snapshot vazio antes de substituir histórico"
    destino = tb("gold", f"historico_{nome}")
    if not spark.catalog.tableExists(destino):
        snapshot.write.format("delta").mode("errorifexists").saveAsTable(destino)
    else:
        anterior = spark.table(destino)
        tipo = lambda df: [(c.name, c.dataType.simpleString()) for c in df.schema]
        assert tipo(snapshot) == tipo(anterior), f"{nome}: migrar schema histórico explicitamente"
        (snapshot.write.format("delta").mode("overwrite").option("replaceWhere", condicao)
         .saveAsTable(destino))
    gravado = spark.table(destino).filter(condicao).select(*snapshot.columns)
    assert snapshot.exceptAll(gravado).limit(1).count() == 0, nome
    assert gravado.exceptAll(snapshot).limit(1).count() == 0, nome
print("Histórico do modelo preservado por competência e recorte:", competencia_snapshot, escopo_snapshot)

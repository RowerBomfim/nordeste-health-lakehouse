# Databricks notebook source

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from functools import reduce
import re
import unicodedata

CATALOGO = "nordeste-health-lakehouse"
BASES = ["estabelecimentos_de_saude", "habilitacoes", "leitos", "profissionais", "equipamentos"]
UFS_ESCOPO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"]
def tb(camada, nome):
    return f"`{CATALOGO}`.`{camada}`.`{nome}`"

def nome_snake_case(nome):
    nome = unicodedata.normalize("NFKD", nome).encode("ascii", "ignore").decode("ascii")
    nome = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", nome)
    nome = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", nome)
    nome = re.sub(r"[^A-Za-z0-9]+", "_", nome).strip("_").lower()
    if not nome:
        raise ValueError("Nome vazio depois da normalização.")
    return f"col_{nome}" if nome[0].isdigit() else nome

def normalizar_colunas(df):
    novos = [nome_snake_case(c) for c in df.columns]
    if len(set(novos)) != len(novos):
        raise ValueError(f"Colunas colidem depois de normalizadas: {list(zip(df.columns, novos))}")
    return df.toDF(*novos)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definir a medida hospitalar e registrar o escopo
# MAGIC A medida atual soma três componentes enquanto o contrato a descreve como hospitalar.

# COMMAND ----------

# ---------------------------------------------------------------------------
# Configuração do tratamento Silver
# ---------------------------------------------------------------------------
# Competência dos dados (formato AAAAMM).  Confere-se que todas as bases que
# possuem o campo COMPETEN informam o mesmo valor; a base de estabelecimentos
# não traz competência na origem e herda o valor global abaixo.
COMPETENCIA = "202607"
# A validação temporal depende da evidência conferida na célula seguinte.
RECORTE_TEMPORAL_CONFERIDO = False

# Descrições das medidas quantitativas (metadados para a camada Silver/Gold)
DESCRICAO_MEDIDA_LEITOS = "leitos existentes cadastrados"          # campo QT_EXIST
DESCRICAO_MEDIDA_EQUIPAMENTOS = "equipamentos em uso cadastrados"     # campo QT_USO
DESCRICAO_MEDIDA_HORAS = "carga horária hospitalar semanal cadastrada"  # campo HORAHOSP

# ---------------------------------------------------------------------------
# Filtro geográfico: restringir ao recorte Nordeste
# ---------------------------------------------------------------------------
# O IBGE codifica cada UF com 2 dígitos.  As bases usam CO_UF (estabelecimentos)
# ou CODUFMUN (demais bases, cujos 2 primeiros dígitos = código da UF).
MAPA_UF_IBGE = {
    "MA": "21", "PI": "22", "CE": "23", "RN": "24",
    "PB": "25", "PE": "26", "AL": "27", "SE": "28",
    "BA": "29",
}

IBGE_UF_ESCOPO = [MAPA_UF_IBGE[uf] for uf in UFS_ESCOPO]

FILTRO_REGISTROS = {
    "estabelecimentos_de_saude": F.col("CO_UF").isin(IBGE_UF_ESCOPO),
    "habilitacoes":              F.substring("CODUFMUN", 1, 2).isin(IBGE_UF_ESCOPO),
    "leitos":                    F.substring("CODUFMUN", 1, 2).isin(IBGE_UF_ESCOPO),
    "profissionais":             F.substring("CODUFMUN", 1, 2).isin(IBGE_UF_ESCOPO),
    "equipamentos":              F.substring("CODUFMUN", 1, 2).isin(IBGE_UF_ESCOPO),
}

# ---------------------------------------------------------------------------
# Mapeamento de colunas da origem (Bronze) → rótulos semânticos (Silver)
# ---------------------------------------------------------------------------
# Cada entrada aponta o nome real da coluna na Bronze.  Campos sem
# correspondência direta ficam como None (serão derivados ou preenchidos
# posteriormente).  chave_extra lista colunas adicionais que compõem a
# chave única do registro.
MAPEAMENTO = {
    "estabelecimentos_de_saude": {
        "cnes": "CO_CNES",
        "uf": "CO_UF",
        "id_municipio": "CO_IBGE",
        "nome_municipio": None,          # não existe na origem — derivar via DIM municípios
        "nome_estabelecimento": "NO_FANTASIA",
        "competencia": None,             # estabelecimentos não trazem COMPETEN na origem
        "chave_extra": ["CO_UNIDADE"],
    },
    "habilitacoes": {
        "cnes": "CNES",
        "codigo_habilitacao": "SGRUPHAB",
        "descricao_habilitacao": None,   # não existe na origem — descrever por dicionário de habilitações na Gold
        "competencia": "COMPETEN",
        "chave_extra": [],
    },
    "leitos": {
        "cnes": "CNES",
        "codigo_tipo_leito": "CODLEITO",
        "descricao_tipo_leito": None,   # não existe na origem — derivar via tabela de leitos
        "quantidade": "QT_EXIST",
        "competencia": "COMPETEN",
        "chave_extra": ["TP_LEITO"],
    },
    "profissionais": {
        "cnes": "CNES",
        "id_profissional": "CNS_PROF",  # CPF está mascarado na origem; CNS é o identificador confiável
        "cbo": "CBO",
        "descricao_cbo": None,           # não existe na origem — derivar via tabela CBO
        "vinculo": "VINCULAC",
        "competencia": "COMPETEN",
        "colunas_horas": ["HORAHOSP"],
        "chave_extra": ["PROF_SUS"],  # distingue vínculo SUS vs não-SUS do mesmo profissional
    },
    "equipamentos": {
        "cnes": "CNES",
        "codigo_equipamento": "CODEQUIP",
        "descricao_equipamento": None,   # não existe na origem — derivar via tabela de equipamentos
        "quantidade": "QT_USO",
        "competencia": "COMPETEN",
        "chave_extra": ["TIPEQUIP"],
    },
}

# ---------------------------------------------------------------------------
# Colunas obrigatórias — se qualquer uma for nula o registro é rejeitado
# ---------------------------------------------------------------------------
OBRIGATORIOS = {
    "estabelecimentos_de_saude": ["cnes", "uf", "id_municipio"],
    "habilitacoes": ["cnes", "codigo_habilitacao"],
    "leitos": ["cnes", "codigo_tipo_leito", "quantidade"],
    "profissionais": ["cnes", "id_profissional", "cbo"],
    "equipamentos": ["cnes", "codigo_equipamento", "quantidade"],
}

# Metadados de interpretação; não são comprovação automática do período da fonte.
CAMPO_HORAS_ANALITICO = "HORAHOSP"
ESCOPO_REDE = "rede total representada nos cinco arquivos; sem filtro exclusivo SUS"
EVIDENCIA_COMPETENCIA_ESTABELECIMENTOS = (
    "Competência atribuída pelo parâmetro 202607; estabelecimento sem COMPETEN na origem. "
    "Registrar a evidência de compatibilidade temporal da extração."
)
LIMIAR_ALERTA_ORFAOS_PCT = 1.0  # limiar operacional de atenção, não regra assistencial

# COMMAND ----------

import json
# Preencha com a evidência real da extração de estabelecimentos.
# Ex.: documento de origem, registro de extração ou página da fonte.
EVIDENCIA_TEMPORAL = {
"competencia_snapshot": "202607", # AAAAMM comprovado pela fonte
"fonte": "DataSUS CNES – Cadastro Nacional de Estabelecimentos de Saúde",
"referencia_verificavel": "https://cnes2.datasus.gov.br/",
"conferida": False, # altere para True somente após conferir o snapshot real
}
campos_texto = ["fonte", "referencia_verificavel"]
RECORTE_TEMPORAL_CONFERIDO = (
EVIDENCIA_TEMPORAL["competencia_snapshot"] == COMPETENCIA
and EVIDENCIA_TEMPORAL["conferida"] is True
and all(isinstance(EVIDENCIA_TEMPORAL[c], str)
and EVIDENCIA_TEMPORAL[c].strip() for c in campos_texto)
)
EVIDENCIA_COMPETENCIA_ESTABELECIMENTOS = json.dumps(
EVIDENCIA_TEMPORAL, ensure_ascii=False, sort_keys=True
)
assert RECORTE_TEMPORAL_CONFERIDO, (
"Preencha e confira a evidência temporal antes de seguir."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validar o contrato antes de transformar
# MAGIC Um teste deve impedir que a soma de horas ambulatoriais e outras volte a alimentar a medida hospitalar.
# MAGIC
# MAGIC Conferir a saída e as validações desta etapa antes de seguir.

# COMMAND ----------

bronze = {
    base: spark.table(tb("bronze", base)).drop("_ARQUIVO_ORIGEM")
    for base in BASES
}
CONTAGENS_BRONZE = {base: df.count() for base, df in bronze.items()}
pendencias = []
for base, campos in OBRIGATORIOS.items():
    cfg = MAPEAMENTO[base]
    for campo in campos:
        if cfg[campo] is None:
            pendencias.append(f"{base}.{campo}: informe a coluna real")
    if base == "profissionais" and not cfg["colunas_horas"]:
        pendencias.append("profissionais.colunas_horas: informe pelo menos uma medida semanal")
    if base == "profissionais" and len(cfg["colunas_horas"]) != len(set(cfg["colunas_horas"])):
        pendencias.append("profissionais.colunas_horas: a mesma coluna foi incluída mais de uma vez")
    usadas = [valor for valor in cfg.values() if isinstance(valor, str)]
    usadas += cfg.get("chave_extra", []) + cfg.get("colunas_horas", [])
    for coluna in usadas:
        if coluna not in bronze[base].columns:
            pendencias.append(f"{base}: coluna inexistente '{coluna}'")
if not isinstance(COMPETENCIA, str) or not re.fullmatch(r"\d{4}(0[1-9]|1[0-2])", COMPETENCIA):
    pendencias.append("COMPETENCIA: informe o mês real no formato AAAAMM")
if not RECORTE_TEMPORAL_CONFERIDO:
    pendencias.append("RECORTE_TEMPORAL_CONFERIDO: confirme a compatibilidade temporal das fontes")
for nome, valor in [("DESCRICAO_MEDIDA_LEITOS", DESCRICAO_MEDIDA_LEITOS),
                    ("DESCRICAO_MEDIDA_EQUIPAMENTOS", DESCRICAO_MEDIDA_EQUIPAMENTOS),
                    ("DESCRICAO_MEDIDA_HORAS", DESCRICAO_MEDIDA_HORAS)]:
    if not valor:
        pendencias.append(f"{nome}: descreva o significado da medida escolhida")
if not set(UFS_ESCOPO).issubset({"AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"}):
    pendencias.append("UFS_ESCOPO contém UF fora do Nordeste")
if not UFS_ESCOPO:
    pendencias.append("UFS_ESCOPO está vazio")
if pendencias:
    raise ValueError("Configure a célula 2 antes de continuar:\n" + "\n".join(pendencias))
print("Mapeamento validado; competência declarada por configuração.")
assert MAPEAMENTO["profissionais"]["colunas_horas"] == ["HORAHOSP"]
assert CAMPO_HORAS_ANALITICO == "HORAHOSP"
EXCLUIDOS_FILTRO = {}
for base in BASES:
    condicao = FILTRO_REGISTROS.get(base)
    if condicao is not None:
        bronze[base] = bronze[base].filter(condicao)
    EXCLUIDOS_FILTRO[base] = CONTAGENS_BRONZE[base] - bronze[base].count()
bronze = {base: normalizar_colunas(df) for base, df in bronze.items()}

# COMMAND ----------

# MAGIC %md
# MAGIC ##Normalizar as referências de coluna e preservar os códigos
# MAGIC O mapeamento usa nomes em maiúsculas após renomear a origem para snake_case; a resolução atual depende de case-insensitive. Chaves com zeros à esquerda também precisam ser consistentes.

# COMMAND ----------

def coluna(nome):
    nome_normalizado = nome_snake_case(nome)
    return F.col(f"`{nome_normalizado.replace(chr(96), chr(96) * 2)}`")

def limpar_texto(expr):
    texto = F.trim(F.regexp_replace(expr.cast("string"), r"\s+", " "))
    return F.when(F.upper(texto).isin("", "NULL", "NONE", "NAN", "N/A"), F.lit(None)).otherwise(texto)

def texto_fonte(df, cfg, campo, padrao=None):
    nome = cfg.get(campo)
    expr = limpar_texto(coluna(nome)) if nome else F.lit(None).cast("string")
    return F.coalesce(expr, F.lit(padrao)) if padrao is not None else expr


def codigo_fonte(df, cfg, campo):
    valor = F.upper(F.regexp_replace(texto_fonte(df, cfg, campo), r"\.0+$", ""))
    tamanhos = {"cnes": 7, "id_municipio": 6, "codigo_habilitacao": 4,
                "codigo_tipo_leito": 2, "codigo_equipamento": 2}
    if campo in tamanhos:
        tamanho = tamanhos[campo]
        # Validar antes de completar: lpad truncaria um código maior que o tamanho.
        return F.when(valor.rlike(rf"^\d{{1,{tamanho}}}$"), F.lpad(valor, tamanho, "0"))
    return valor


def numero_fonte(nome):
    nome = nome_snake_case(nome)
    protegido = "`" + nome.replace("`", "``") + "`"
    return F.expr(f"try_cast(replace(trim(cast({protegido} as string)), ',', '.') as decimal(18,2))")

def competencia_fonte(df, cfg):
    nome = cfg.get("competencia")
    if not nome:
        return F.lit(COMPETENCIA)
    # Aceita AAAAMM, AAAA-MM ou AAAA-MM-DD. Outros formatos exigem adaptação explícita.
    texto = F.regexp_replace(limpar_texto(coluna(nome)), r"\.0+$", "")
    valido = texto.rlike(r"^(\d{6}|\d{4}-\d{2}(-\d{2})?)$")
    return F.when(valido, F.substring(F.regexp_replace(texto, "-", ""), 1, 6))

def hash_chave(colunas):
    return F.sha2(F.to_json(F.struct(*[F.col(c).alias(c) for c in colunas]),
                            options={"ignoreNullFields": "false"}), 256)

def campos_comuns(df, cfg):
    extras = cfg.get("chave_extra", [])
    extra = F.to_json(F.struct(*[limpar_texto(coluna(c)).alias(c) for c in extras]),
                      options={"ignoreNullFields": "false"}) if extras else F.lit("{}")
    conteudo = sorted(c for c in df.columns if c not in {"data_hora_carga", "arquivo_origem"})
    return [
        codigo_fonte(df, cfg, "cnes").alias("cnes"),
        competencia_fonte(df, cfg).alias("competencia"),
        extra.alias("chave_extra"),
        F.sha2(F.to_json(F.struct(*[coluna(c).alias(c) for c in conteudo]),
                         options={"ignoreNullFields": "false"}), 256).alias("hash_conteudo_origem"),
        F.col("data_hora_carga"), F.col("arquivo_origem"),
    ]

def gravar(df, nome):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(tb("silver", nome)))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Separar os três componentes de horas
# MAGIC A Gold precisa receber horas hospitalares sem perder a possibilidade de inspecionar os outros componentes.

# COMMAND ----------

uf_aliases = {
    "AL": "AL", "27": "AL", "ALAGOAS": "AL",
    "BA": "BA", "29": "BA", "BAHIA": "BA",
    "CE": "CE", "23": "CE", "CEARA": "CE", "CEARÁ": "CE",
    "MA": "MA", "21": "MA", "MARANHAO": "MA", "MARANHÃO": "MA",
    "PB": "PB", "25": "PB", "PARAIBA": "PB", "PARAÍBA": "PB",
    "PE": "PE", "26": "PE", "PERNAMBUCO": "PE",
    "PI": "PI", "22": "PI", "PIAUI": "PI", "PIAUÍ": "PI",
    "RN": "RN", "24": "RN", "RIO GRANDE DO NORTE": "RN",
    "SE": "SE", "28": "SE", "SERGIPE": "SE",
}
# Mapeie também os demais estados para diferenciar 'fora do Nordeste' de 'UF inválida'.
for sigla, codigo, nome in [
    ("AC", "12", "ACRE"), ("AP", "16", "AMAPA"), ("AM", "13", "AMAZONAS"),
    ("PA", "15", "PARA"), ("RO", "11", "RONDONIA"), ("RR", "14", "RORAIMA"),
    ("TO", "17", "TOCANTINS"), ("ES", "32", "ESPIRITO SANTO"), ("MG", "31", "MINAS GERAIS"),
    ("RJ", "33", "RIO DE JANEIRO"), ("SP", "35", "SAO PAULO"), ("PR", "41", "PARANA"),
    ("SC", "42", "SANTA CATARINA"), ("RS", "43", "RIO GRANDE DO SUL"),
    ("DF", "53", "DISTRITO FEDERAL"), ("GO", "52", "GOIAS"), ("MT", "51", "MATO GROSSO"),
    ("MS", "50", "MATO GROSSO DO SUL"),
]:
    for valor in (sigla, codigo, nome):
        uf_aliases[valor] = sigla
mapa_uf = F.create_map(*[F.lit(x) for par in uf_aliases.items() for x in par])
df = bronze["estabelecimentos_de_saude"]
cfg = MAPEAMENTO["estabelecimentos_de_saude"]
uf = F.upper(codigo_fonte(df, cfg, "uf"))
uf = F.translate(uf, "ÁÀÂÃÉÊÍÓÔÕÚÜÇ", "AAAAEEIOOOUUC")
tipados = {}
tipados["estabelecimentos_de_saude"] = df.select(
    *campos_comuns(df, cfg), mapa_uf[uf].alias("uf"),
    codigo_fonte(df, cfg, "id_municipio").alias("id_municipio"),
    texto_fonte(df, cfg, "nome_municipio", "NAO INFORMADO").alias("nome_municipio"),
    texto_fonte(df, cfg, "nome_estabelecimento", "NAO INFORMADO").alias("nome_estabelecimento"),
)
for base, campo_codigo, campo_descricao in [
    ("habilitacoes", "codigo_habilitacao", "descricao_habilitacao"),
    ("leitos", "codigo_tipo_leito", "descricao_tipo_leito"),
    ("equipamentos", "codigo_equipamento", "descricao_equipamento"),
]:
    df, cfg = bronze[base], MAPEAMENTO[base]
    campos = campos_comuns(df, cfg) + [
        codigo_fonte(df, cfg, campo_codigo).alias(campo_codigo),
        texto_fonte(df, cfg, campo_descricao, "NAO INFORMADO").alias(campo_descricao),
    ]
    if base != "habilitacoes":
        campos.append(numero_fonte(cfg["quantidade"]).alias("quantidade"))
    tipados[base] = df.select(*campos)


df, cfg = bronze["profissionais"], MAPEAMENTO["profissionais"]
tipados["profissionais"] = df.select(
    *campos_comuns(df, cfg),
    codigo_fonte(df, cfg, "id_profissional").alias("id_profissional"),
    codigo_fonte(df, cfg, "cbo").alias("cbo"),
    texto_fonte(df, cfg, "descricao_cbo", "NAO INFORMADO").alias("descricao_cbo"),
    texto_fonte(df, cfg, "vinculo", "NAO INFORMADO").alias("vinculo"),
    numero_fonte("HORAHOSP").alias("horas_hospitalares"),
    numero_fonte("HORA_AMB").alias("horas_ambulatoriais"),
    numero_fonte("HORAOUTR").alias("horas_outras"),
).withColumn("horas_semanais", F.col("horas_hospitalares").cast("decimal(18,2)"))

assert tipados["profissionais"].filter(
    ~F.col("horas_semanais").eqNullSafe(F.col("horas_hospitalares"))
).limit(1).count() == 0
print("DataFrames criados; horas_semanais deriva exclusivamente de HORAHOSP.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Acrescentar validade de formato às chaves
# MAGIC Uma chave preenchida pode ser inválida; as validações existentes verificam principalmente nulidade e unicidade.

# COMMAND ----------

CHAVES = {
    "estabelecimentos_de_saude": ["cnes", "competencia"],
    "habilitacoes": ["cnes", "competencia", "codigo_habilitacao", "chave_extra"],
    "leitos": ["cnes", "competencia", "codigo_tipo_leito", "chave_extra"],
    "profissionais": ["cnes", "competencia", "id_profissional", "cbo", "vinculo", "chave_extra"],
    "equipamentos": ["cnes", "competencia", "codigo_equipamento", "chave_extra"],
}
audit = []

def separar_qualidade(base):
    df = tipados[base]
    obrigatorias = [c for c in CHAVES[base] if c != "chave_extra"]
    if base == "estabelecimentos_de_saude":
        obrigatorias += ["uf", "id_municipio"]
    regras = [F.when(F.col(c).isNull(), F.lit(f"nulo_{c}")) for c in obrigatorias]
    regras += [F.when(~F.col("cnes").rlike(r"^\d{7}$"), F.lit("formato_cnes_invalido"))]
    regras += [F.when(~F.col("competencia").rlike(r"^\d{4}(0[1-9]|1[0-2])$"),
                      F.lit("formato_competencia_invalido"))]
    if base == "estabelecimentos_de_saude":
        regras += [F.when(~F.col("id_municipio").rlike(r"^\d{6}$"),
                          F.lit("formato_municipio_invalido"))]

    if base in {"leitos", "equipamentos"}:
        regras += [F.when(F.col("quantidade").isNull() | (F.col("quantidade") < 0)
                         | (F.col("quantidade") % 1 != 0), F.lit("quantidade_invalida"))]
    if base == "profissionais":
        regras += [F.when(F.col("horas_semanais").isNull() | (F.col("horas_semanais") < 0),
                         F.lit("horas_invalidas"))]
    df = df.withColumn("motivo_rejeicao", F.concat_ws(";", *regras))
    invalidos = df.filter(F.col("motivo_rejeicao") != "")
    validos = df.filter(F.col("motivo_rejeicao") == "").drop("motivo_rejeicao")
    outros_meses = validos.filter(F.col("competencia") != COMPETENCIA)
    selecionados = validos.filter(F.col("competencia") == COMPETENCIA)
    return selecionados, invalidos, outros_meses.count(), CONTAGENS_BRONZE[base]

def deduplicar_e_validar(base, df):
    # A impressão digital inclui toda a origem, inclusive colunas que não entram no modelo canônico.
    # Só registros de conteúdo idêntico são removidos automaticamente.
    antes = df.count()
    janela = Window.partitionBy("hash_conteudo_origem").orderBy(
        F.col("data_hora_carga").desc(), F.col("arquivo_origem").desc())
    df = df.withColumn("_rn", F.row_number().over(janela)).filter("_rn = 1").drop("_rn")
    chaves = CHAVES[base]
    conflitos = df.groupBy(*chaves).count().filter("count > 1")
    gravar(conflitos, f"{base}_conflitos_chave")
    if conflitos.limit(1).count():
        raise ValueError(
            f"{base}: chave de negócio não é única. Consulte silver.{base}_conflitos_chave. "
            "Revise chave_extra, vínculo, competência ou a versão do registro. "
            "Não elimine linhas legítimas e não use a quantidade/horas como parte da chave."
        )
    df = df.withColumn("id_registro", hash_chave(chaves))
    return df, antes - df.count()

# COMMAND ----------

base = "estabelecimentos_de_saude"
est_validos, rejeitados, outros_meses, entrada = separar_qualidade(base)
cnes_conhecidos = est_validos.select("cnes", "competencia").distinct()
fora_regiao = est_validos.filter(~F.col("uf").isin(UFS_ESCOPO))
est_ne = est_validos.filter(F.col("uf").isin(UFS_ESCOPO))
est_ne, duplicatas = deduplicar_e_validar(base, est_ne)
if est_ne.limit(1).count() == 0:
    raise ValueError("Nenhum estabelecimento no recorte: confira UF, competência e mapeamento.")
gravar(rejeitados, f"{base}_rejeitados")
gravar(est_ne, base)
cnes_ne = est_ne.select("cnes", "competencia").distinct()
audit.append((base, entrada, EXCLUIDOS_FILTRO[base], rejeitados.count(), outros_meses, fora_regiao.count(),
              0, duplicatas, est_ne.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Registrar o impacto dos órfãos por estabelecimento
# MAGIC Contar órfãos não mostra quanto de capacidade ou carga horária foi excluído.
# MAGIC
# MAGIC Conferir a saída e as validações desta etapa antes de seguir.

# COMMAND ----------

for base in ["habilitacoes", "leitos", "profissionais", "equipamentos"]:
    validos, invalidos, outros_meses, entrada = separar_qualidade(base)
    orfaos = validos.join(cnes_conhecidos, ["cnes", "competencia"], "left_anti")
    orfaos = orfaos.withColumn("motivo_rejeicao", F.lit("cnes_sem_estabelecimento_valido"))

    medida = "horas_semanais" if base == "profissionais" else (
        "quantidade" if base in {"leitos", "equipamentos"} else None)
    agregacoes = [F.count("*").alias("registros_orfaos")]
    if medida:
        agregacoes.append(F.sum(medida).alias("medida_orfa_excluida"))
    perfil = orfaos.groupBy("cnes", "competencia").agg(*agregacoes)
    gravar(perfil, f"{base}_perfil_orfaos")

    conhecidos = validos.join(cnes_conhecidos, ["cnes", "competencia"], "left_semi")
    fora_regiao = conhecidos.join(cnes_ne, ["cnes", "competencia"], "left_anti")
    recorte = conhecidos.join(cnes_ne, ["cnes", "competencia"], "left_semi")
    silver, duplicatas = deduplicar_e_validar(base, recorte)
    gravar(invalidos.unionByName(orfaos), f"{base}_rejeitados")
    gravar(silver, base)
    audit.append((base, entrada, EXCLUIDOS_FILTRO[base], invalidos.count(), outros_meses, fora_regiao.count(),
                  orfaos.count(), duplicatas, silver.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Persistir taxa de órfãos e significado das medidas
# MAGIC Diferença zero na auditoria contabiliza as exclusões, mas não comprova representatividade dos KPIs.

# COMMAND ----------

auditoria = spark.createDataFrame(audit,
    "base string, entrada long, excluidos_filtro long, invalidos long, outros_meses long, fora_regiao long, "
    "orfaos long, duplicatas_removidas long, aceitos long")
auditoria = auditoria.withColumn("diferenca", F.col("entrada") - (
    F.col("excluidos_filtro") + F.col("invalidos") + F.col("outros_meses") + F.col("fora_regiao") + F.col("orfaos")
    + F.col("duplicatas_removidas") + F.col("aceitos")))

assert auditoria.filter("diferenca <> 0").limit(1).count() == 0
auditoria = auditoria.withColumn("entrada_recorte", F.col("entrada") - F.col("excluidos_filtro"))
auditoria = auditoria.withColumn("taxa_orfaos_pct", F.when(F.col("entrada_recorte") > 0,
    F.round(100.0 * F.col("orfaos") / F.col("entrada_recorte"), 4)).otherwise(F.lit(0.0)))
auditoria = auditoria.withColumn("alerta_cobertura",
    F.col("taxa_orfaos_pct") > F.lit(LIMIAR_ALERTA_ORFAOS_PCT))
gravar(auditoria, "auditoria_qualidade")
display(auditoria)

max_taxa = float(auditoria.agg(F.max("taxa_orfaos_pct").alias("v")).first()["v"] or 0.0)
alerta_orfaos = max_taxa > LIMIAR_ALERTA_ORFAOS_PCT
contrato = spark.createDataFrame([(
    COMPETENCIA, ",".join(UFS_ESCOPO), DESCRICAO_MEDIDA_LEITOS,
    DESCRICAO_MEDIDA_EQUIPAMENTOS, DESCRICAO_MEDIDA_HORAS,
    CAMPO_HORAS_ANALITICO, ESCOPO_REDE, True,
    EVIDENCIA_COMPETENCIA_ESTABELECIMENTOS, alerta_orfaos, max_taxa,
)], """competencia string, ufs_escopo string, medida_leitos string,
    medida_equipamentos string, medida_horas string, campo_horas_origem string,
    escopo_rede string, competencia_estabelecimentos_atribuida boolean,
    evidencia_temporal string, alerta_orfaos boolean, maior_taxa_orfaos_pct double""")
gravar(contrato, "contrato_medidas")
if alerta_orfaos:
    print("ALERTA: investigar cobertura; há fontes com taxa de órfãos acima do limiar operacional.")
for base in BASES:
    df = spark.table(tb("silver", base))
    assert df.groupBy("id_registro").count().filter("count > 1").limit(1).count() == 0
    assert df.filter("id_registro IS NULL").limit(1).count() == 0
print("As cinco Silver têm chaves únicas. Consulte rejeitados e auditoria antes de seguir.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reconciliar a hora hospitalar com a fonte aceita
# MAGIC Além de verificar igualdade de aliases, conferir HORAHOSP da origem por registro efetivamente aceito.

# COMMAND ----------

cfg = MAPEAMENTO["profissionais"]
fonte = bronze["profissionais"]
referencia = (fonte.select(*campos_comuns(fonte, cfg),
    numero_fonte("HORAHOSP").alias("horas_hospitalares_fonte"))
    .select("hash_conteudo_origem", "horas_hospitalares_fonte")
    .dropDuplicates(["hash_conteudo_origem"]))
aceitos = spark.table(tb("silver", "profissionais"))
comparacao = aceitos.join(referencia, "hash_conteudo_origem", "left")
divergencias = comparacao.filter(
    ~F.col("horas_semanais").eqNullSafe(F.col("horas_hospitalares_fonte")))
assert divergencias.limit(1).count() == 0, "Silver não corresponde a HORAHOSP da fonte"
display(aceitos.agg(
    F.count("*").alias("vinculos_aceitos"),
    F.sum("horas_hospitalares").alias("horas_hospitalares"),
    F.sum("horas_ambulatoriais").alias("horas_ambulatoriais"),
    F.sum("horas_outras").alias("horas_outras")))
print("HORAHOSP reconciliado para todos os vínculos aceitos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Investigar órfãos sem recolocá-los artificialmente no modelo
# MAGIC A auditoria precisa separar ausência no cadastro bruto de registros presentes na Bronze que não chegaram ao cadastro válido da Silver.

# COMMAND ----------

cadastro_bruto = normalizar_colunas(
    spark.table(tb("bronze", "estabelecimentos_de_saude")).drop("_ARQUIVO_ORIGEM"))
cfg_est = MAPEAMENTO["estabelecimentos_de_saude"]
cadastro_bruto = (cadastro_bruto.select(
    codigo_fonte(cadastro_bruto, cfg_est, "cnes").alias("cnes"),
    codigo_fonte(cadastro_bruto, cfg_est, "uf").alias("uf_cadastro_bruto"))
    .filter(F.col("cnes").isNotNull())
    .groupBy("cnes").agg(F.count("*").alias("registros_cadastro_bruto"),
        F.collect_set("uf_cadastro_bruto").alias("ufs_cadastro_bruto")))
perfil = spark.table(tb("silver", "profissionais_perfil_orfaos"))
diagnostico = (perfil.join(cadastro_bruto, "cnes", "left")
    .withColumn("situacao_investigacao",
        F.when(F.col("registros_cadastro_bruto").isNull(), "CNES_AUSENTE_CADASTRO_BRONZE")
         .otherwise("CNES_PRESENTE_BRONZE_AUSENTE_CADASTRO_SILVER_VALIDO")))
gravar(diagnostico, "diagnostico_orfaos_profissionais")
display(diagnostico.groupBy("situacao_investigacao").agg(
    F.count("*").alias("unidades_competencias"),
    F.sum("registros_orfaos").alias("registros_profissionais_excluidos"),
    F.sum("medida_orfa_excluida").alias("horas_hospitalares_excluidas")))
display(diagnostico.orderBy(F.desc("registros_orfaos")).limit(30))
# Apenas motivos e contagens; não exibir CNS dos registros rejeitados.
display(spark.table(tb("silver", "estabelecimentos_de_saude_rejeitados"))
    .groupBy("motivo_rejeicao").count().orderBy(F.desc("count")))

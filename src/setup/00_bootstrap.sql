-- Databricks notebook source

CREATE CATALOG IF NOT EXISTS `nordeste-health-lakehouse`;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS `nordeste-health-lakehouse`.landing;
CREATE SCHEMA IF NOT EXISTS `nordeste-health-lakehouse`.bronze;
CREATE SCHEMA IF NOT EXISTS `nordeste-health-lakehouse`.silver;
CREATE SCHEMA IF NOT EXISTS `nordeste-health-lakehouse`.gold;

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS `nordeste-health-lakehouse`.landing.files
COMMENT 'Arquivos do dataset e checkpoints de ingestão do projeto';


import configparser
import boto3
import os
import time
import pyspark
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,IntegerType,TimestampType
from pyspark.sql.functions import *
from pyspark.sql import functions as F
import spark_sqls as spark_sqls
import write_s3 as write_s3

os.environ['AWS_ACCESS_KEY_ID']=<YOUR AWS_ACCESS_KEY_ID>
os.environ['AWS_SECRET_ACCESS_KEY']=<YOUR AWS_SECRET_ACCESS_KEY>

def create_spark_session():
    fluxo = 0
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    print('\n')
    print('                 ____|  \   __  /__  /_ _|                                                       ')
    print('                 |      _ \     /    /   |                                                       ')
    print('                 __|   ___ \   /    /    |                                                       ')
    print('                 _|  _/    _\____|____|___|                                                      ')
    print('                                                                                                ')
    print('                _|_|_|_|  _|_|_|_|_|  _|            _|_|_|_|_|    _|_|      _|_|    _|          ')
    print('                _|            _|      _|                _|      _|    _|  _|    _|  _|          ')
    print('                _|_|_|        _|      _|                _|      _|    _|  _|    _|  _|          ')
    print('                _|            _|      _|                _|      _|    _|  _|    _|  _|          ')
    print('                _|_|_|_|      _|      _|_|_|_|          _|        _|_|      _|_|    _|_|_|_|    ')

    value_input = raw_input('\nDigite o numero da categoria que ira ser realizado o processamento:\n\n[ 1 ] - LOCALIDADES \n[ 2 ] - ROTAS \n[ 3 ] - EQUIPAMENTOS \n[ 4 ] - RACK \n[ 5 ] - PORTA \n\n---> ')
    #value_input = param_entrada
    #SANITY_CHECK COM CONDIÇÕES E PLOT EM LOG
    if (value_input == '1'):
            fluxo = 'LOCALIDADES'
    if (value_input == '2'):
            fluxo = 'ROTAS'
    if (value_input == '3'):
            fluxo = 'EQUIPAMENTOS'
    if (value_input == '4'):
            fluxo = 'RACK'
    if (value_input == '5'):
            fluxo = 'PORTA'

    return fluxo, spark

def process_s3_data(spark, fluxo):
    if (fluxo == 'LOCALIDADES'):
        print('Iniciando o processamento de dados das LOCALIDADES...')
        retorno_sqls = spark_sqls.sqls_localidade(spark)
        fenix = retorno_sqls[0]
        smtx = retorno_sqls[1]
        #COLOCAR SEUS ENDERECOS DE BUCKETS
        bucket_path = "s3a://emr-job-spark/localidades"
        write_s3.write_csv_2_s3(fluxo,bucket_path, fenix, smtx)

    if (fluxo == 'ROTAS'):
        print('Iniciando o processamento de dados das ROTAS...')
        retorno_sqls = spark_sqls.sqls_rotas(spark)
        fenix = retorno_sqls[0]
        smtx = retorno_sqls[1]
        bucket_path = "s3a://emr-job-spark/rotas"
        write_s3.write_csv_2_s3(fluxo,bucket_path, fenix, smtx)

    if (fluxo == 'EQUIPAMENTOS'):
        print('Iniciando o processamento de dados dos: EQUIPAMENTOS...')
        retorno_sqls = spark_sqls.sqls_equipamentos(spark)
        fenix = retorno_sqls[0]
        smtx = retorno_sqls[1]
        bucket_path = "s3a://emr-job-spark/equipamentos"
        write_s3.write_csv_2_s3(fluxo,bucket_path, fenix, smtx)

def main(param_entrada):
    retorno = create_spark_session()
    spark = retorno[1]
    fluxo = retorno[0]
    #sanity_check()
    process_s3_data(spark, fluxo)

if __name__ == '__main__':
    main()
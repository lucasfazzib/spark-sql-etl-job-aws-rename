import pyspark
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,IntegerType,TimestampType
from pyspark.sql.functions import *
from pyspark.sql import functions as F
import cp_mv_equipamentos
import cp_mv_localidades
import cp_mv_rotas

def write_csv_2_s3(fluxo, bucket_path, dataframe1, dataframe2):
    print("[ - - - - - - - > Etapa 1: Processando os dados...")
    dfUnion = dataframe1.unionAll(dataframe2)
    print("[ - - - - - - - > Etapa 2: Escrevendo os dados...")
    dfUnion.repartition(1).write.format('com.databricks.spark.csv').option("header","true").mode("Overwrite").save(bucket_path)
    print("[ - - - - - - - > Etapa 3: Dados processados e inseridos no bucket...")
    print("[ - - - - - - - > Etapa 4: Renomenado dados inseridos no bucket...")
    # Renomenado arquivos de acordo com cada especificação do fluxo.
    if (fluxo == "LOCALIDADES"):
        cp_mv_localidades.cp_rename_mv_localidades()
    if (fluxo == "ROTAS"):
        cp_mv_rotas.cp_rename_mv_rotas()
    if (fluxo == "EQUIPAMENTOS"):
        cp_mv_equipamentos.cp_rename_mv_equipamentos()
    print("[ - - - - - - - > Etapa 5: Finalizando...")
    


    #INICIAR PROCESSO DE RENOMEAR ARQUIVOS part-000* no bucket
    # call  script.sh e executa
    #output = open('teste.txt', 'r').read()
    # output ->> nome_do_arquivo.txt -> variavel para usar no cp e no mv
    #os.system("aws s3 cp s3://emr-job-spark/rotas/" + output +  "s3://emr-job-spark/rotas/rotas.csv")
    #os.system("aws s3 mv s3://emr-job-spark/localidades/" + output + " s3://emr-job-spark/localidades/old/")

    


import pyspark
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,IntegerType,TimestampType
from pyspark.sql.functions import *
from pyspark.sql import functions as F

#COLOCAR SUAS RESPECTIVAS QUERYS E MUDAR DATAFRAMES.

def sqls_localidade(spark):
  df_smtx = spark.read.csv("s3a://osspocsmtx/raw_smtx/tabSites/tabSites_202001151036.csv", sep=";", inferSchema=True, header=True)
  df_fenix1 = spark.read.csv("s3a://osspocfenix/raw_fenix/TB_ESTACAO/TB_ESTACAO_202001161112.csv",sep=";", inferSchema=True, header=True)
  df_fenix2 = spark.read.csv("s3a://osspocfenix/raw_fenix/TB_AREA_TELEFONICA/TB_AREA_TELEFONICA_202001161119.csv",sep=";", inferSchema=True, header=True)
  df_fenix3 = spark.read.csv("s3a://osspocfenix/raw_fenix/TB_LOCALIDADE/TB_LOCALIDADE_202001161119.csv",sep=";", inferSchema=True, header=True)
  df_fenix4 = spark.read.csv("s3a://osspocfenix/raw_fenix/TB_TIPO_ESTACAO/TB_TIPO_ESTACAO_202001161119.csv",sep=";", inferSchema=True, header=True)

  df_smtx.createOrReplaceTempView('tabsites')
  df_fenix1.createOrReplaceTempView('tb_estacao')
  df_fenix2.createOrReplaceTempView('tb_area_telefonica')
  df_fenix3.createOrReplaceTempView('tb_localidade')
  df_fenix4.createOrReplaceTempView('tb_tipo_estacao')

  LocalidadeFENIXsql = spark.sql(""" SELECT
  concat('F', est_id) as loc_id
  ,"loc_sigla" as loc_sigla
  ,'NA' as loc_uf
  ,est_complemento as loc_regiao
  ,est_nome as loc_nome
  ,concat(est_ender, ' ', replace(est_ender_num, '"', '')) as loc_endereco
  ,replace(est_ender_num, '"', '') as loc_numero_endereco
  ,(CASE WHEN (art_descricao = '') THEN 'NAO INFORMADO' ELSE upper(art_descricao) END) as loc_area
  ,'00000-000' as loc_cep
  ,(CASE WHEN (loc_nome = '') THEN 'NAO INFORMADO' ELSE upper(loc_nome) END) as loc_cidade
  ,COALESCE(est_altitude, 0) as loc_altitude
  ,(CASE WHEN ((((((est_lat_graus IS NULL) OR (est_lat_minutos IS NULL)) OR (est_lat_segundos IS NULL)) OR (est_lon_graus IS NULL)) OR (est_lon_minutos IS NULL)) OR (est_lon_segundos IS NULL)) THEN -3.8667783 WHEN (est_lat_eixo = 'S') THEN ((est_lat_graus + ((est_lat_minutos + (est_lat_segundos * 0.016667)) * 0.016667)) * -1) ELSE (est_lat_graus + ((est_lat_minutos + (est_lat_segundos * 0.016667)) * 0.016667)) END) as loc_latitude
  ,(CASE WHEN ((((((est_lat_graus IS NULL) OR (est_lat_minutos IS NULL)) OR (est_lat_segundos IS NULL)) OR (est_lon_graus IS NULL)) OR (est_lon_minutos IS NULL)) OR (est_lon_segundos IS NULL)) THEN -33.8020459 WHEN (est_lon_eixo = 'W') THEN ((est_lon_graus + ((est_lon_minutos + (est_lon_segundos * 0.016667)) * 0.016667)) * -1) ELSE (est_lon_graus + ((est_lon_minutos + (est_lon_segundos * 0.016667)) * 0.016667)) END) as loc_longitude
  ,est_status as loc_status
  ,(CASE WHEN (est_ult_atualizacao = '') THEN '1900-01-01 12:00:00' ELSE est_ult_atualizacao END) as loc_data_ult_atualizacao
  ,'Fenix' as loc_origem
  FROM
  (((tb_estacao est
  LEFT JOIN tb_area_telefonica art ON (est.est_art_id = art.art_id))
  LEFT JOIN tb_localidade loc ON (art.art_loc_id = loc.loc_id))
  LEFT JOIN tb_tipo_estacao tes ON (tes.tes_id = est.est_tes_id)) """)

  LocalidadeSMTXsql = spark.sql(""" SELECT concat('S', idSite) as loc_id
  ,'NIS' as loc_sigla
  ,(CASE WHEN (UFDoSite = '') THEN 'NI' ELSE upper(UFDoSite) END) as loc_uf
  ,(CASE WHEN (replace(SiglaSite, '"', '') = '') THEN 'XXXXX' ELSE replace(SiglaSite, '"', '') END) as loc_regiao
  ,(CASE WHEN (NomeDoSite = '') THEN 'NAO INFORMADO' ELSE upper(NomeDoSite) END) as loc_nome
  ,(CASE WHEN (NomeDaVia = '') THEN 'NAO INFORMADO' ELSE upper(NomeDaVia) END) as loc_endereco
  ,(CASE WHEN (replace(NumeroDaVia, '"', '') = '') THEN '00' ELSE replace(replace(replace(NumeroDaVia, '"', ''), '/', '-'), '.', '-') END) as loc_numero_endereco
  ,(CASE WHEN (DescLocalidade = '') THEN 'NAO INFORMADO' ELSE upper(DescLocalidade) END) as loc_area
  ,(CASE WHEN (replace(CEP, '"', '') = '') THEN '00000-000' ELSE replace(CEP, '"', '') END) as loc_cep
  ,(CASE WHEN (DescMunicipioLocal = '') THEN 'NAO INFORMADO' ELSE upper(DescMunicipioLocal) END) as loc_cidade
  ,COALESCE(Altitude, 0) as loc_altitude
  ,(CASE WHEN ((LatitudeDecimal = 0) OR (LongitudeDecimal = 0)) THEN -3.8667783 ELSE LatitudeDecimal END) as loc_latitude
  ,(CASE WHEN ((LatitudeDecimal = 0) OR (LongitudeDecimal = 0)) THEN -33.8020459 ELSE LongitudeDecimal END) as loc_longitude
  ,(CASE WHEN (Status = 'Ativado') THEN 1 WHEN (Status = 'Desativado') THEN 0 ELSE 1 END) as loc_status
  ,DataUltAtual as loc_data_ult_atualizacao
  ,'SMTX' as loc_origem FROM tabsites """)

  return LocalidadeFENIXsql, LocalidadeSMTXsql

def sqls_rotas(spark):
  df_smtx = spark.read.csv("s3a://osspocsmtx/raw_smtx/tabSites/tabSites_202001151036.csv", sep=";", inferSchema=True, header=True)
  df_smtx2 = spark.read.csv("s3a://osspocsmtx/raw_smtx/tabRotas/tabRotas_202001151054.csv", sep=";", inferSchema=True, header=True)

  df_fenix1 = spark.read.csv("s3a://osspocfenix/raw_fenix/TB_ASSOC_EST_MEIOS_TRANS/TB_ASSOC_EST_MEIOS_TRANS_202001221828.csv",sep=";", inferSchema=True, header=True)
  df_fenix2 = spark.read.csv("s3a://osspocfenix/raw_fenix/TB_ELEMENTO_MEIOS_TRANS/TB_ELEMENTO_MEIOS_TRANS_202001161147.csv",sep=";", inferSchema=True, header=True)
  df_fenix3 = spark.read.csv("s3a://osspocfenix/raw_fenix/TB_FORMA_NOMEAR/TB_FORMA_NOMEAR_202001161138.csv",sep=";", inferSchema=True, header=True)

  df_smtx.createOrReplaceTempView('tabsites')
  df_smtx2.createOrReplaceTempView('tabrotas')

  df_fenix1.createOrReplaceTempView('tb_assoc_est_meios_trans')
  df_fenix2.createOrReplaceTempView('tb_elemento_meios_trans')
  df_fenix3.createOrReplaceTempView('tb_forma_nomear')

  sqlRotasSmtx = spark.sql(""" SELECT
  min(concat('S', rot.idrota)) as rot_id
  ,concat('S', rot.idsiteinic) as rot_localidade_ini
  ,concat('S', rot.idsitefim) as rot_localidade_fim
  ,'S0' as rot_equipamento_ini
  ,'S0' as rot_equipamento_fim
  ,count(*) as rot_numero
  ,replace(concat(sitA.siglasite, '-', sitB.siglasite), '"', '') as rot_nome
  ,0 as rot_distancia
  ,replace(rot.velocidaderota, '"', '') as rot_velocidade
  FROM
  ((tabrotas rot
  LEFT JOIN tabsites sitA ON (rot.idsiteinic = sitA.idsite))
  LEFT JOIN tabsites sitB ON (rot.idsitefim = sitB.idsite))
  WHERE (rot.status = 'Ativada')
  GROUP BY rot.idsiteinic, rot.idsitefim, sitA.siglasite, sitB.siglasite, replace(rot.velocidaderota, '"', '')
  ORDER BY rot.idsiteinic ASC, rot.idsitefim ASC """)



  sqlRotasFenix = spark.sql(""" SELECT
  min(concat('F', aem.aem_id)) as rot_id
  , concat('F', emt.emt_est_ori_id) as rot_localidade_ini
  , concat('F', emt.emt_est_des_id) as rot_localidade_fim
  , 'F0' as rot_equipamento_ini
  , 'F0' as rot_equipamento_fim
  , count(*) as rot_numero
  , fnm.fnm_descricao as rot_nome
  , emt.emt_comprimento as rot_distancia
  , ' ' as rot_velocidade
  FROM
  ((tb_assoc_est_meios_trans aem
  LEFT JOIN tb_elemento_meios_trans emt ON (aem.aem_emt_id = emt.emt_id))
  LEFT JOIN tb_forma_nomear fnm ON (fnm.fnm_id = emt.emt_fnm_id))
  GROUP BY emt.emt_est_ori_id, emt.emt_est_des_id, fnm.fnm_descricao, emt.emt_comprimento
  ORDER BY emt.emt_est_ori_id ASC, emt.emt_est_des_id ASC """)

  return sqlRotasFenix, sqlRotasSmtx

def sqls_equipamentos(spark):
  df_smtx = spark.read.csv("s3a://osspocsmtx/raw_smtx/tabEquipamentosTransmissao/tabEquipamentosTransmissao_202001151036.csv", sep=";", inferSchema=True, header=True)
  df_smtx2 = spark.read.csv("s3a://osspocsmtx/raw_smtx/tabMUX/tabMUX_202001151055.csv", sep=";", inferSchema=True, header=True)
  df_smtx3 = spark.read.csv("s3a://osspocsmtx/raw_smtx/tabModelosEquipTransmissao/tabModelosEquipTransmissao_202001210945.csv", sep=";", inferSchema=True, header=True)
  df_smtx4 = spark.read.csv("s3a://osspocsmtx/raw_smtx/tabFabricantesEquipTransmissao/tabFabricantesEquipTransmissao.csv", sep=";", inferSchema=True, header=True)
  df_smtx5 = spark.read.csv("s3a://osspocsmtx/raw_smtx/tabTipoEquipamento/tabTipoEquipamento_202001171022.csv", sep=";", inferSchema=True, header=True)

  df_fenix1 = spark.read.csv("s3a://osspocfenix/raw_fenix/TB_LIST_DOMINIO_ELEMENTO/TB_LIST_DOMINIO_ELEMENTO_202001161111.csv",sep=";", inferSchema=True, header=True)
  df_fenix2 = spark.read.csv("s3a://osspocfenix/raw_fenix/TB_FAMILIA_ELEMENTO/TB_FAMILIA_ELEMENTO_202001161111.csv",sep=";", inferSchema=True, header=True)
  df_fenix3 = spark.read.csv("s3a://osspocfenix/raw_fenix/TB_TIPO_ELEMENTO/TB_TIPO_ELEMENTO_202001161111.csv",sep=";", inferSchema=True, header=True)
  df_fenix4 = spark.read.csv("s3a://osspocfenix/raw_fenix/TB_ELEMENTO_EQUIPAMENTO/TB_ELEMENTO_EQUIPAMENTO_202001161111.csv",sep=";", inferSchema=True, header=True)
  df_fenix5 = spark.read.csv("s3a://osspocfenix/raw_fenix/TB_FABRICANTE/TB_FABRICANTE_202001161111.csv",sep=";", inferSchema=True, header=True)
  df_fenix6 = spark.read.csv("s3a://osspocfenix/raw_fenix/TB_MOD_ELEMENTO/TB_MOD_ELEMENTO_202001161139.csv",sep=";", inferSchema=True, header=True)

  df_smtx.createOrReplaceTempView('tabequipamentostransmissao')
  df_smtx2.createOrReplaceTempView('tabmux')
  df_smtx3.createOrReplaceTempView('tabmodelosequiptransmissao')
  df_smtx4.createOrReplaceTempView('tabfabricantesequiptransmissao')
  df_smtx5.createOrReplaceTempView('tabtipoequipamento')

  df_fenix1.createOrReplaceTempView('tb_list_dominio_elemento')
  df_fenix2.createOrReplaceTempView('tb_familia_elemento')
  df_fenix3.createOrReplaceTempView('tb_tipo_elemento')
  df_fenix4.createOrReplaceTempView('tb_elemento_equipamento')
  df_fenix5.createOrReplaceTempView('tb_fabricante')
  df_fenix6.createOrReplaceTempView('tb_mod_elemento')

  sqlEquipamentosSmtx = spark.sql(""" SELECT
  concat('S', et.idequiptrans) as eqp_id
  ,upper(et.siglaet) as eqp_sigla_equipamento
  ,replace(mux.nome, '"', '') as eqp_nome
  ,concat('S', et.idsite) as eqp_loc_id
  ,met.descricao as eqp_modelo_equipamento
  ,upper(replace(fet.descricao, '"', '')) as eqp_fabricante
  ,upper(replace(te.tipoequipamento, '"', '')) as eqp_tipo_equipamento
  FROM
  ((((tabequipamentostransmissao et
  INNER JOIN tabmux mux ON (mux.idequiptrans = et.idequiptrans))
  LEFT JOIN tabmodelosequiptransmissao met ON (replace(mux.modeloequip, '"', '') = met.codmodeloequip ))
  LEFT JOIN tabfabricantesequiptransmissao fet ON (mux.codfabricante = fet.codfabricante))
  LEFT JOIN tabtipoequipamento te ON (CAST(replace(et.codtipoequipamento, '"', '') AS integer) = te.codtipoequipamento)) """)

  sqlEquipamentosFenix = spark.sql(""" SELECT
  concat('F', eeq.eeq_id) as eqp_id
  ,upper(tel.tel_identificacao) as eqp_sigla_equipamento
  ,replace(tel.tel_descricao, '"', '') as eqp_nome
  ,concat('F', eeq.eeq_est_id) as eqp_loc_id
  ,replace(mel.mel_modelo, '"', '') as eqp_modelo_equipamento
  ,upper(replace(fab.fab_nome, '"', '')) as eqp_fabricante
  ,upper(replace(fel.fel_descricao, '"', '')) as eqp_tipo_equipamento
  FROM tb_list_dominio_elemento del
  ,tb_familia_elemento fel
  ,tb_tipo_elemento tel
  ,tb_elemento_equipamento eeq
  ,tb_fabricante fab
  ,tb_mod_elemento mel
  WHERE (((((((del.del_id = tel.tel_del_id) AND (fel.fel_id = tel.tel_fel_id)) AND (tel.tel_id = eeq.eeq_tel_id)) AND (fab.fab_id = eeq.eeq_fab_id)) AND (mel.mel_id = eeq.eeq_mel_id)) AND (del.del_codigo IN ('E', 'R'))) AND (fel.fel_id = 11)) """)

  return sqlEquipamentosFenix, sqlEquipamentosSmtx
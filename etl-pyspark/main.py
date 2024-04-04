import os
from google.cloud import storage
from google.cloud import bigquery
from pyspark.sql.functions import col, when, substring, input_file_name

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './credentials.json'
gcs_client = storage.Client()
bq_client = bigquery.Client()
bucket = 'dc-solution-bucket'
project = 'ac-data-challenge'


def start_or_create_spark():
    from pyspark.sql import SparkSession
    spark = (SparkSession
             .builder
             .appName("Processamento de Dados de Gasolina no Brasil")
             .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.0")
             .getOrCreate()
             )
    return spark


def rename_columns(df):
    renamed_columns = []
    columns = df.columns
    for column in columns:
        renamed_column = column.lower().replace(' - ', '_').replace(' ', '_').replace('-', '_').replace('.', '_').replace('#', 'num').replace('@', 'at').replace('$', 's').replace('%', 'percent').replace('/', '_').replace('[', '_').replace(']', '_').replace('(', '_').replace(')', '_').replace('{', '_').replace('}', '_')[:300]
        renamed_columns.append(renamed_column)
    
    renamed_dataframe = df
    for old_column, new_column in zip(columns, renamed_columns):
        renamed_dataframe = renamed_dataframe.withColumnRenamed(old_column, new_column)
    return renamed_dataframe


def add_semestre(dataframe, coluna: str):
    """
    Parametros: dataframe, coluna

    Escreva uma função que receba um Dataframe Spark e o nome da coluna que será baseada para criar 
    uma coluna `semestre` no Dataframe com os dados lidos do GCS. O resultado deverá ser 1 ou 2.
    E retorne o dataframe com os dados e a nova coluna criada.
    """
    dataframe_with_semestre = dataframe.withColumn("semestre", when(substring(col(coluna), 4, 2).cast("int") <= 6, 1).otherwise(2))
    return dataframe_with_semestre


def add_year(dataframe, coluna: str):
    """
    Parametros: dataframe, coluna

    Escreva uma função que receba um Dataframe Spark e o nome da coluna que será baseada para criar 
    uma coluna `year` no Dataframe com os dados lidos do GCS. 
    O resultado da coluna deverá ser o Ano.
    E retorne o dataframe com os dados e a nova coluna criada.
    """
    dataframe_with_year = dataframe.withColumn("year", substring(col(coluna), 7, 4).cast("int"))
    return dataframe_with_year


def add_filename_input(df):
    """
    Parametros: dataframe

    Escreva uma função que receba um Dataframe Spark que crie uma coluna `input_file_name` que será baseada no nome do arquivo lido.
    E retorne o dataframe com os dados e a nova coluna criada.
    """
    df_with_filename = df.withColumn('input_file_name', input_file_name())
    return df_with_filename


def put_file_gcs(path_output: str, df, formato: str):
    """
    :param path_output: path para save dos dados
    :param dataframe: conjunto de dados a serem salvos
    :param formato: tipo de arquivo a ser salvo
    :return: None

    Escreva uma função que salve os dados no GCS, utilizando o metodo write do Dataframe Spark.
    Tip: 
    """
    try:
        df.coalesce(1).write.format(formato).save(path_output, mode='append')
        return print("Data saved to GCS successfully!")
    except Exception as e:
        return print("Error:", e)

def write_bigquery(dataframe, tabela: str, temporaryGcsBucket):
    """
    Crie uma função que receba os parametros:
    :param dataframe: conjunto de dados a serem salvos
    :param tabela: Tabela do BigQuery que será salvo os dados. Ex: dataset.tabela_exemplo
    :param temporaryGcsBucket: Bucket temporário para salvar area de staging do BigQuery.
    
    E escreva dentro do BigQuery.
    Utilize o material de referencia:
    https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example#running_the_code
    """
    try:       
        dataframe.write.format("bigquery").option("temporaryGcsBucket",temporaryGcsBucket).mode("overwrite").save(tabela)
        return print("Data writed to BigQuery successfully!")
    except Exception as e:
        return print("Error:", e)


def main(path_input, path_output, formato, table_bq):
    try:
        """
        Crie uma função main que receba como parametro:
        path_input: Caminho dos dados no GCS gerados pela API coletora. Ex: gs://bucket_name/file_name
        path_output: Caminho de onde será salvo os dados processados. Ex: gs://bucket_name_2/file_name
        formato_file_save: Formato de arquivo a ser salvo no path_output. Ex: PARQUET
        tabela_bq: Tabela do BigQuery que será salvo os dados. Ex: dataset.tabela_exemplo
        """
        spark = start_or_create_spark()
        # 1 - Faça a leitura dos dados de acordo com o path_input informado      
        df = spark.read.csv(path_input, sep=';',inferSchema=True, header=True)

        # 2 - Realize o rename de colunas do arquivo, respeitando os padroes do BigQuer
        df = rename_columns(df)

        # 3 - Adicione uma coluna de Ano, baseado na coluna `Data da Coleta`
        df = add_year(df, 'data_da_coleta')
        
        # 4 - Adicione uma coluna de Semestre, baseado na coluna de `Data da Coleta`
        df = add_semestre(df, 'data_da_coleta')

        # 5 - Adicione uma coluna Filename. Tip: pyspark.sql.functions.input_file_name
        df = add_filename_input(df)

        # 6 - Faça o parse dos dados lidos de acordo com a tabela no BigQuery
        df_bq = spark.read.format("bigquery").option("table", f"{project}.{table_bq}").load()
        df_bq.printSchema()

        df = df \
                .withColumn("regiao_sigla",df["regiao_sigla"].cast('string')) \
                .withColumn("estado_sigla",df["estado_sigla"].cast('string')) \
                .withColumn("municipio",df["municipio"].cast('string')) \
                .withColumn("revenda",df["revenda"].cast('string')) \
                .withColumn("cnpj_da_revenda",df["cnpj_da_revenda"].cast('string')) \
                .withColumn("nome_da_rua",df["nome_da_rua"].cast('string')) \
                .withColumn("numero_rua",df["numero_rua"].cast('long')) \
                .withColumn("complemento",df["complemento"].cast('string')) \
                .withColumn("bairro",df["bairro"].cast('string'))  \
                .withColumn("cep",df["cep"].cast('string')) \
                .withColumn("produto",df["produto"].cast('string')) \
                .withColumn("data_da_coleta",df["data_da_coleta"].cast('date')) \
                .withColumn("valor_de_venda",df["valor_de_venda"].cast('double')) \
                .withColumn("valor_de_compra",df["valor_de_compra"].cast('double')) \
                .withColumn("unidade_de_medida",df["unidade_de_medida"].cast('string')) \
                .withColumn("bandeira:",df["bandeira"].cast('string')) \
                .withColumn("year",df["year"].cast('long')) \
                .withColumn("semestre",df["semestre"].cast('long')) \
                .withColumn("input_file_name",df["input_file_name"].cast('string'))
        df.show()
        df.printSchema()

        # 7 - Escreva os dados no Bucket GCS, no caminho informado `path_output` 
        #     no formato especificado no atributo `formato_file_save`.
        put_file_gcs(path_output, df, formato)

        # 8 - Escreva os dados no BigQuery de acordo com a tabela especificada no atributo `tabela_bq`
        write_bigquery(df, table_bq, 'dc-bq-staging')
    
    except Exception as ex:
        print(ex)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--path_input',
        type=str,
        dest='path_input',
        required=True,
        help='URI of the GCS bucket, for example, gs://bucket_name/file_name')

    parser.add_argument(
        '--path_output',
        type=str,
        dest='path_output',
        required=True,
        help='URI of the GCS bucket, for example, gs://bucket_name/file_name')

    parser.add_argument(
        '--formato',
        type=str,
        dest='formato',
        required=True,
        help='Type format save file')

    parser.add_argument(
        '--table_bq',
        type=str,
        dest='table_bq',
        required=True,
        help='Tabela do BigQuery Destino')
    known_args, pipeline_args = parser.parse_known_args()

    main(path_input=known_args.path_input,
         path_output=known_args.path_output,
         formato=known_args.formato,
         table_bq=known_args.table_bq
         )
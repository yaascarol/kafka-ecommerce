import json
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, sum as spark_sum, count

spark = SparkSession.builder \
    .appName("KafkaEcommerceConsumer") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

consumer = KafkaConsumer(
    'vendas',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='grupo-ecommerce',
    value_deserializer=lambda msg: json.loads(msg.decode('utf-8'))
)

def processar_batch(mensagens):
    linhas = []
    for venda in mensagens:
        for produto in venda['produtos_comprados']:
            linhas.append(Row(
                id_ordem          = venda['id_ordem'],
                documento_cliente = venda['documento_cliente'],
                data_hora_venda   = venda['data_hora_venda'],
                produto           = produto['nome'],
                preco_unitario    = float(produto['preco_unitario']),
                quantidade        = int(produto['quantidade']),
                subtotal          = float(produto['subtotal'])
            ))

    if not linhas:
        return

    df = spark.createDataFrame(linhas)

    df_agrupado = df.groupBy("produto") \
        .agg(
            spark_sum("subtotal").alias("total_vendido"),
            spark_sum("quantidade").alias("total_unidades"),
            count("id_ordem").alias("num_pedidos")
        ) \
        .orderBy(col("total_vendido").desc())

    print("\n" + "═" * 60)
    print(" RELATÓRIO DE VENDAS POR PRODUTO (batch atual)")
    print("═" * 60)
    df_agrupado.show(truncate=False)
    print("═" * 60)

print(" Consumer iniciado. Aguardando mensagens do Kafka...\n")

batch = []
TAMANHO_BATCH = 5

try:
    for mensagem in consumer:
        venda = mensagem.value
        batch.append(venda)
        print(f"Recebida: {venda['id_ordem']} | "
              f"R$ {venda['valor_total']:.2f} | "
              f"{venda['data_hora_venda']}")

        if len(batch) >= TAMANHO_BATCH:
            processar_batch(batch)
            batch = []

except KeyboardInterrupt:
    if batch:
        print(f"\n Processando {len(batch)} restante(s)...")
        processar_batch(batch)
    print("\n Consumer encerrado.")
    consumer.close()
    spark.stop()

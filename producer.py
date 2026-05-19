import json
import time
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker('pt_BR')

PRODUTOS_DISPONIVEIS = [
    {"nome": "Notebook Dell",        "preco": 3500.00},
    {"nome": "Mouse Sem Fio",        "preco": 89.90},
    {"nome": "Teclado Mecânico",     "preco": 250.00},
    {"nome": "Monitor 24 polegadas", "preco": 1200.00},
    {"nome": "Headset Gamer",        "preco": 350.00},
    {"nome": "Webcam Full HD",       "preco": 180.00},
    {"nome": "SSD 1TB",              "preco": 420.00},
    {"nome": "Memória RAM 16GB",     "preco": 310.00},
    {"nome": "Cadeira Gamer",        "preco": 1800.00},
    {"nome": "Mousepad XL",          "preco": 75.00},
]

def gerar_venda():
    quantidade_de_itens = random.randint(1, 4)
    produtos_sorteados = random.sample(PRODUTOS_DISPONIVEIS, quantidade_de_itens)
    produtos_comprados = []
    valor_total = 0.00

    for produto in produtos_sorteados:
        qtd = random.randint(1, 5)
        subtotal = produto["preco"] * qtd
        valor_total += subtotal
        produtos_comprados.append({
            "nome": produto["nome"],
            "preco_unitario": produto["preco"],
            "quantidade": qtd,
            "subtotal":  round(subtotal, 2)
        })

    return{
        "id_ordem": f"ORD-{fake.unique.random_int(min=1, max=999999):06d}",
        "documento_cliente": fake.cpf(),
        "produtos_comprados": produtos_comprados,
        "valor_total": round(valor_total, 2),
        "data_hora_venda": datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    }

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda dado: json.dumps(dado, ensure_ascii=False).encode('utf-8')
)

print("Producer iniciado. Enviando vendas para o Kafka.\n")

try:
   while True:
        venda = gerar_venda()
        producer.send('vendas', value=venda)
        producer.flush()

        print(f"Venda enviada!")
        print(f"ID da Ordem:       {venda['id_ordem']}")
        print(f"CPF do Cliente:    {venda['documento_cliente']}")
        print(f"Produtos:")
        for p in venda['produtos_comprados']:
            print(f"- {p['nome']} | Qtd: {p['quantidade']} | Subtotal: R$ {p['subtotal']:.2f}")
        print(f"Valor Total: R$ {venda['valor_total']:.2f}")
        print(f"Data/Hora: {venda['data_hora_venda']}")
        print("-" * 60)

        time.sleep(2)

except KeyboardInterrupt:
    print("\n Producer encerrado pelo usuário.")
    producer.close()


# Simulação de Vendas E-commerce com Apache Kafka + PySpark


## Sobre o projeto
Projeto desenvolvido como parte do Programa de Talentos: Estágio em Engenharia de Dados, da Bulk Consulting.
O projeto  demonstra o uso do Apache Kafka no gerenciamento de dados em tempo real com Producer (Faker) e Consumer (PySpark).


---

## 🧩 Tecnologias Utilizadas

![Python](https://img.shields.io/badge/Python-3.8+-blue?logo=python)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.x-black?logo=apachekafka&logoColor=white)
![Apache ZooKeeper](https://img.shields.io/badge/Apache%20ZooKeeper-3.x-orange?logo=apachezooper&logoColor=white)
![Kafka--python](https://img.shields.io/badge/Kafka--python-2.0+-blue?logo=python&logoColor=white)
![Faker](https://img.shields.io/badge/Faker-v33+-darkgreen?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.x-red?logo=apachepyspark&logoColor=white)

---

## Pré-requisitos

- Sistema Linux ou WSL
- Java 8 ou superior instalado
- Python 3.10 ou superior
- Apache Kafka baixado e configurado localmente

---

## Como Executar

### 1. Clone o repositório
```bash
git clone https://github.com/seu-usuario/kafka-ecommerce.git
cd kafka-ecommerce
```

### 2. Instale as dependências
```bash
pip install -r requirements.txt
```

### 3. Inicie o Zookeeper (Terminal 1)
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

### 4. Inicie o Kafka (Terminal 2)
```bash
bin/kafka-server-start.sh config/server.properties
```

### 5. Crie o tópico (Terminal 3)
```bash
bin/kafka-topics.sh --create \
  --topic vendas \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### 6. Rode o Producer (Terminal 3)
```bash
python producer.py
```

### 7. Rode o Consumer (Terminal 4)
```bash
python consumer.py
```

---

## Exemplo de Saída

```Consumer iniciado! Aguardando mensagens do Kafka...
Recebida: ORD-004521 | R$ 1190.00 | 11/05/2026 14:32:07
RELATÓRIO DE VENDAS POR PRODUTO
+--------------------+-------------+---------------+
|produto             |total_vendido|total_unidades |
+--------------------+-------------+---------------+
|Notebook Dell       |10500.00     |3              |
|Cadeira Gamer       |3600.00      |2              |
+--------------------+-------------+---------------+
```

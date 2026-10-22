import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

TOPIC_NAME = "transacoes-banco"
BOOTSTRAP_SERVERS = ["localhost:9092"]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

clientes = ["cliente_01", "cliente_02", "cliente_03", "cliente_04", "cliente_05"]
cidades = [
    "Juazeiro do Norte",
    "Crato",
    "Barbalha",
    "Fortaleza",
    "São Paulo",
    "Rio de Janeiro",
]

print(
    "🚀 Produtor iniciado. Enviando transações a cada 3 segundos para o"
    " Kafka..."
)

try:
  while True:
    transaction = {
        "transaction_id": f"tx-{random.randint(100000, 999999)}",
        "client_id": random.choice(clientes),
        "amount": round(random.uniform(20.0, 15000.0), 2),
        "city": random.choice(cidades),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    producer.send(TOPIC_NAME, value=transaction)
    print(
        f"📤 Enviado -> Cliente: {transaction['client_id']} | R$"
        f" {transaction['amount']} | Cidade: {transaction['city']}"
    )

    time.sleep(3)
except KeyboardInterrupt:
  print("\n🛑 Produtor encerrado.")
  producer.close()
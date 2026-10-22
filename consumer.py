from datetime import datetime, timezone
import json
from database import init_db, salvar_transacao
from kafka import KafkaConsumer

TOPIC_NAME = "transacoes-banco"
BOOTSTRAP_SERVERS = ["localhost:9092"]

# Inicializa o banco de dados local
init_db()

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="fraud-detection-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print(
    "👀 Consumidor iniciado e aguardando transações do Kafka com regras de"
    " fraude..."
)

# Dicionário para armazenar o histórico recente de cada cliente em memória
# Estrutura: { client_id: [ {amount, city, timestamp}, ... ] }
historico_clientes = {}

for message in consumer:
    tx = message.value
    client_id = tx["client_id"]
    amount = tx["amount"]
    city = tx["city"]
    tx_time_str = tx["timestamp"]

    # Converte o timestamp string para objeto datetime com timezone UTC
    tx_time = datetime.fromisoformat(tx_time_str)

    # Inicializa histórico do cliente se não existir
    if client_id not in historico_clientes:
        historico_clientes[client_id] = []

    alertas = []

    # ==========================================
    # REGRA 1: Case Alto Valor (amount >= 10000)
    # ==========================================
    if amount >= 10000.0:
        alertas.append("ALTO_VALOR")

    # Adiciona a transação atual ao histórico do cliente
    historico_clientes[client_id].append(
        {"amount": amount, "city": city, "timestamp": tx_time}
    )

    # Limpa transações muito antigas do histórico em memória (mantém apenas as últimas para análise)
    # Isso evita consumo excessivo de memória
    historico_clientes[client_id] = [
        t
        for t in historico_clientes[client_id]
        if (tx_time - t["timestamp"]).total_seconds() <= 600
    ]  # Mantém até 10 minutos

    transicoes_cliente = historico_clientes[client_id]

    # ==========================================
    # REGRA 2: Case Tempo (4 transações em < 60s)
    # ==========================================
    transicoes_ultimos_60s = [
        t
        for t in transicoes_cliente
        if (tx_time - t["timestamp"]).total_seconds() < 60
    ]
    if len(transicoes_ultimos_60s) >= 4:
        if "TEMPO_60s" not in alertas:
            alertas.append("TEMPO_60s")

    # ==========================================
    # REGRA 3: Case GEO (2 cidades diferentes em < 10 minutos)
    # ==========================================
    transicoes_ultimos_10m = [
        t
        for t in transicoes_cliente
        if (tx_time - t["timestamp"]).total_seconds() <= 600
    ]
    cidades_distintas = set(t["city"] for t in transicoes_ultimos_10m)
    if len(cidades_distintas) >= 2:
        if "GEO_10m" not in alertas:
            alertas.append("GEO_10m")

    # Salva no banco de dados SQLite
    salvar_transacao(tx, alertas)

    # Exibe alertas ou detalhes no console
    print("-" * 50)
    print(f"📥 Transação Recebida ID: {tx['transaction_id']}")
    print(f"   Cliente: {client_id} | Valor: R$ {amount:.2f} | Local: {city}")

    if alertas:
        print(f"🚨 [ALERTA DE FRAUDE DETECTADA!] -> Motivo(s): {', '.join(alertas)}")
    else:
        print("✅ Status: Transação Normal (Aprovada)")
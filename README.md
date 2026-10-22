# Sistema de Detecção de Fraudes com Apache Kafka (TDE 03)

Projeto desenvolvido para a disciplina, simulando um pipeline de mensageria em tempo real para detecção de fraudes em transações financeiras utilizando **Apache Kafka** e **Python**.

## Regras de Fraude Implementadas
1. **ALTO_VALOR**: Transações com valor maior ou igual a R$ 10.000,00.
2. **TEMPO_60s**: 4 ou mais transações realizadas pelo mesmo cliente em um intervalo inferior a 60 segundos.
3. **GEO_10m**: Transações do mesmo cliente em cidades diferentes em um intervalo de até 10 minutos.

## Como Executar

### 1. Subir o Kafka via Docker
Certifique-se de ter o Docker instalado e execute na raiz:
```bash
docker-compose up -d
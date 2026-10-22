import sqlite3

DB_NAME = "transacoes.db"


def init_db():
  conn = sqlite3.connect(DB_NAME)
  cursor = conn.cursor()
  cursor.execute("""
        CREATE TABLE IF NOT EXISTS transacoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id TEXT,
            client_id TEXT,
            amount REAL,
            city TEXT,
            timestamp TEXT,
            fraudes TEXT
        )
    """)
  conn.commit()
  conn.close()


def salvar_transacao(tx, fraudes_detectadas):
  conn = sqlite3.connect(DB_NAME)
  cursor = conn.cursor()
  fraudes_str = (
      ",".join(fraudes_detectadas) if fraudes_detectadas else "NENHUMA"
  )

  cursor.execute(
      """
        INSERT INTO transacoes (transaction_id, client_id, amount, city, timestamp, fraudes)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
      (
          tx["transaction_id"],
          tx["client_id"],
          tx["amount"],
          tx["city"],
          tx["timestamp"],
          fraudes_str,
      ),
  )
  conn.commit()
  conn.close()
import sqlite3
import hashlib
import os

DB_PATH = 'users.db'

def create_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    username TEXT UNIQUE,
                    password_hash TEXT
                )''')
    conn.commit()
    conn.close()

def add_user(username, password) -> bool:
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)", (username, password_hash))
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        conn.close()
        return False

def set_user_password(username, password):
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id FROM users WHERE username = ?", (username,))
    if c.fetchone():
        c.execute("UPDATE users SET password_hash = ? WHERE username = ?", (password_hash, username))
        print(f"Senha do usuário {username} atualizada.")
    else:
        c.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)", (username, password_hash))
        print(f"Usuário {username} adicionado com sucesso.")
    conn.commit()
    conn.close()

def verify_user(username, password):
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT password_hash FROM users WHERE username = ?", (username,))
    result = c.fetchone()
    conn.close()
    if result and result[0] == password_hash:
        return True
    return False

# Criar DB e garantir usuários de exemplo
if __name__ == "__main__":
    create_db()
    set_user_password("joaoheinke", "123456")
    set_user_password("joaorocha", "123456")
    set_user_password("albertocarrera", "123456")
    set_user_password("vitoriafarias", "123456") 
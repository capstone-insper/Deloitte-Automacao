import sqlite3
import hashlib
import os
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "users.db")


def create_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    username TEXT UNIQUE,
                    password_hash TEXT,
                    is_master INTEGER DEFAULT 0
                )''')
    
    # Migração: adicionar coluna is_master se não existir
    c.execute("PRAGMA table_info(users)")
    columns = [col[1] for col in c.fetchall()]
    if "is_master" not in columns:
        c.execute("ALTER TABLE users ADD COLUMN is_master INTEGER DEFAULT 0")
    
    conn.commit()
    conn.close()


def add_user(username, password) -> bool:
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO users (username, password_hash, is_master) VALUES (?, ?, 0)", (username, password_hash))
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
        c.execute("INSERT INTO users (username, password_hash, is_master) VALUES (?, ?, 0)", (username, password_hash))
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


def is_user_master(username: str) -> bool:
    """Verifica se o usuário é um master."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT is_master FROM users WHERE username = ?", (username,))
    result = c.fetchone()
    conn.close()
    return result and result[0] == 1


def set_user_as_master(username: str, is_master: bool = True) -> bool:
    """Define um usuário como master ou remove o status."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id FROM users WHERE username = ?", (username,))
    if c.fetchone():
        c.execute("UPDATE users SET is_master = ? WHERE username = ?", (1 if is_master else 0, username))
        conn.commit()
        conn.close()
        return True
    conn.close()
    return False


def list_all_users() -> list[dict]:
    """Retorna lista de todos os usuários com seus status de master."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT username, is_master FROM users ORDER BY username")
    users = [{"username": row[0], "is_master": bool(row[1])} for row in c.fetchall()]
    conn.close()
    return users


def delete_user(username: str) -> bool:
    """Deleta um usuário do banco de dados."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM users WHERE username = ?", (username,))
    conn.commit()
    deleted = c.rowcount > 0
    conn.close()
    return deleted


def user_exists(username: str) -> bool:
    """Verifica se um usuário existe."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id FROM users WHERE username = ?", (username,))
    exists = c.fetchone() is not None
    conn.close()
    return exists


if __name__ == "__main__":
    create_db()
    users = {
        "joaoheinke":    os.getenv("INIT_PASSWORD_JOAOHEINKE", ""),
        "joaorocha":     os.getenv("INIT_PASSWORD_JOAOROCHA", ""),
        "albertocarrera":os.getenv("INIT_PASSWORD_ALBERTOCARRERA", ""),
        "vitoriafarias": os.getenv("INIT_PASSWORD_VITORIAFARIAS", ""),
    }
    for username, password in users.items():
        if password:
            set_user_password(username, password)
            if username == "vitoriafarias":
                set_user_as_master(username, True)
                print(f"✓ '{username}' definido como MASTER.")
        else:
            print(f"⚠️  Senha de '{username}' não definida no .env — usuário ignorado.")

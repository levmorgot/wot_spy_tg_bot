from datetime import datetime

from mysql.connector import connect

import config

SUB = "sub"
UNSUB = "unsub"


class DBBot:

    def __init__(self):
        self.db = connect(
            host="localhost",
            user=config.DB_USERNAME,
            password=config.DB_PASS,
            auth_plugin='mysql_native_password',
            database=config.DB_NAME,
        )
        self.sql = self.db.cursor()
        self._create_db()

    def _create_db(self):
        self.sql.execute("""CREATE TABLE IF NOT EXISTS chats (
    chat_id INT PRIMARY KEY,
    chat_name VARCHAR(100),
    status VARCHAR(5)
)""")

        self.sql.execute("""CREATE TABLE IF NOT EXISTS subscriptions ( 
    id INT AUTO_INCREMENT PRIMARY KEY,
    chat_id INT,
    player_id INT,   
    FOREIGN KEY(chat_id) REFERENCES chats(chat_id));
""")
        self.db.commit()

    def new_user(self, chat_id, chat_name):
        self.sql.execute(f"SELECT chat_id FROM chats WHERE chat_id = '{chat_id}'")
        if self.sql.fetchone() is None:
            self.sql.execute(f"""INSERT INTO chats (chat_id, chat_name, status) VALUES ({chat_id}, '{chat_name}', '{SUB}')""")
            print("Зарегистрирован новый пользователь {id}/{name} {time}".format(
                id=chat_id,
                name=chat_name,
                time=datetime.now()))
        else:
            self.sql.execute(f"UPDATE chats SET status = '{SUB}' WHERE chat_id = '{chat_id}'")
            print("Вернулся пользователь {id}/{name} {time}".format(
                id=chat_id,
                name=chat_name,
                time=datetime.now()))
        self.db.commit()

    def subscribe_to_player(self, chat_id, player_id):
        self.sql.execute(
            f"SELECT player_id FROM subscriptions WHERE chat_id = '{chat_id}' AND player_id = '{player_id}'")
        if self.sql.fetchone() is None:
            self.sql.execute(f"INSERT INTO subscriptions (chat_id, player_id) VALUES ({chat_id}, {player_id})")
            self.db.commit()
            print("Пользователь {id} подписался на врача {player_id} {time}".format(
                id=chat_id,
                player_id=player_id,
                time=datetime.now()))

    def get_all_users(self):
        self.sql.execute(f"SELECT chat_id FROM chats WHERE status = '{SUB}'")
        result = self.sql.fetchall()
        return [chat for chat in result]

    def is_subscribe(self, chat_id, player_id):
        self.sql.execute(
            f"SELECT player_id FROM subscriptions WHERE chat_id = '{chat_id}' AND player_id = '{player_id}'")
        return False if self.sql.fetchone() is None else True

    def get_subscribes(self, chat_id):
        self.sql.execute(f"SELECT player_id FROM subscriptions WHERE chat_id = {chat_id}")
        result = self.sql.fetchall()
        return [player for player in result]

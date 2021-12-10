import logging
import asyncio
from datetime import datetime

from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import json

import config
from common.db import DBBot
from common.kafka import connect_kafka_producer, publish_message, connect_kafka_consumer, connect_async_kafka_consumer

logging.basicConfig(level=logging.INFO)

bot = Bot(token=config.API_TOKEN)
dp = Dispatcher(bot)

db = DBBot()


@dp.message_handler(commands=["start", "help"])
async def send_welcome(message: types.Message):
    """
    This handler will be called when user sends `/start` or `/help` command
    """
    db.new_user(message.chat.id, message.chat.username or message.chat.title)
    await message.reply("Привет!\nЯ Бот для слежки за игроками WOT!\nРад знакомству!")


@dp.message_handler(commands=["stop"])
async def unsubscrip_user(message: types.Message):
    await message.reply("Пока!\nЖаль тебя терять!\nШутка! Команда бесполезна)")


@dp.message_handler()
async def echo_message(msg: types.Message):
    with connect_kafka_producer() as producer:
        publish_message(producer, "search_player", f"{msg.chat.id}", f"{msg.text}")

    players = []
    with connect_kafka_consumer("fond_players") as consumer:
        for message in consumer:
            chat_id = int(message.key)
            info = json.loads(message.value.decode("utf8").replace("'", '"'))
            players.append({"key": chat_id, "info": info})

    if len(players):
        players_inline_kb = InlineKeyboardMarkup()
        for player in players:
            players_inline_kb.add(InlineKeyboardButton(
                player["info"]["nickname"],
                callback_data=",".join(
                    [
                        "chosen_player",
                        str(player["info"]["account_id"]),
                    ]
                )))
        await bot.send_message(msg.chat.id, "Вот каких игроков я нашел:", reply_markup=players_inline_kb)
    else:
        await bot.send_message(msg.chat.id, "Я не нашел ирока с таким именем :(")


async def _fetch_player_info(chat_id, player_id):
    with connect_kafka_producer() as producer:
        publish_message(producer, "chosen_player", f"{chat_id}", f"{player_id}")

    players = []
    with connect_kafka_consumer("player_info") as consumer:
        for message in consumer:
            chat_id = int(message.key)
            info = json.loads(message.value.decode("utf8").replace("None", "null").replace("'", '"'))
            players.append({"key": chat_id, "info": info})

    if len(players):
        return players[0]["info"]


def _get_message(info):
    message = f"nickname : {info['nickname']}\n" \
              f"Дата последнего боя : {datetime.fromtimestamp(info['last_battle_time'])}\n" \
              f"Рейтинг : {info['global_rating']}\n"
    return message


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("chosen_player"))
async def chosen_player(callback_query: types.CallbackQuery):
    code = callback_query.data.split(",")
    if len(code) > 1:
        await bot.answer_callback_query(callback_query.id, text=f"{code[1]}")
        player_info = await _fetch_player_info(callback_query.from_user.id, code[1])
        if player_info:
            message = _get_message(player_info)
            add_player_inline_kb = InlineKeyboardMarkup()
            add_player_inline_kb.add(InlineKeyboardButton(
                "Подписаться на обновления",
                callback_data=",".join(
                    [
                        "add_player",
                        str(player_info["account_id"]),
                    ]
                )))
            await bot.send_message(callback_query.from_user.id, message, reply_markup=add_player_inline_kb)
    else:
        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(callback_query.from_user.id, f"Ошибка при получении данных игрока")


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("add_player"))
async def add_player(callback_query: types.CallbackQuery):
    code = callback_query.data.split(",")
    if len(code) > 1:
        with connect_kafka_producer() as producer:
            publish_message(producer, "add_player", f"{callback_query.from_user.id}", f"{code[1]}")
        await bot.answer_callback_query(callback_query.id, text=f"{code[1]}")

        db.subscribe_to_player(callback_query.from_user.id, code[1])
        await bot.send_message(callback_query.from_user.id, f"Вы подписаны на игрока")
    else:
        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(callback_query.from_user.id, "Не удалось подписаться на игрока")


async def send_schedule(wait_for: int):
    while True:
        await asyncio.sleep(wait_for)
        await request_player_info()


async def request_player_info():
    with connect_kafka_producer() as producer:
        publish_message(producer, "get_players", "tg_bot", "tg_bot")


async def send_player_info():
    async with connect_async_kafka_consumer("tg_bot") as consumer:
        async for msg in consumer:
            chats = db.get_all_users()
            message = json.loads(msg.value.decode("utf8").replace("None", "null").replace("'", '"'))
            players_info = []
            player_ids = []
            for info in message:
                if info["account_id"] not in player_ids:
                    players_info.append(info)
                    player_ids.append(info["account_id"])

            for chat in chats:
                chat_id, = chat
                players_for_chat = "\n".join([_get_message(info) for info in players_info if
                                              db.is_subscribe(chat_id, info["account_id"])])
                if players_for_chat != "":
                    await bot.send_message(chat_id, players_for_chat)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(send_schedule(60*5))
    loop.create_task(send_player_info())
    executor.start_polling(dp, skip_updates=True)

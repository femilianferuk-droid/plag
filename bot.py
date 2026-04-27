# bot.py
import asyncio
import logging
import re
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import asyncpg
import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup,
    InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton,
    ReplyKeyboardRemove
)
from aiogram.enums import ParseMode
from telethon import TelegramClient
from telethon.errors import (
    SessionPasswordNeededError, PhoneCodeInvalidError,
    PhoneCodeExpiredError, PhoneNumberInvalidError
)
from telethon.sessions import StringSession

# --- Конфигурация ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
API_ID = 32480523
API_HASH = "147839735c9fa4e83451209e9b55cfc5"
CRYPTO_BOT_API = "https://pay.crypt.bot/api"
USDT_RATE = 90

# --- Логирование ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Инициализация ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
db_pool: Optional[asyncpg.Pool] = None

# --- Хранилища ---
pending_logins: Dict[int, Dict[str, Any]] = {}
active_telethon_clients: Dict[str, TelegramClient] = {}
active_orders: Dict[int, Dict[str, Any]] = {}


# --- Состояния ---
class WorkAccountStates(StatesGroup):
    waiting_phone = State()
    waiting_code = State()
    waiting_2fa = State()


class SaleAccountStates(StatesGroup):
    waiting_phone = State()
    waiting_code = State()
    waiting_2fa = State()
    waiting_country = State()


class BroadcastStates(StatesGroup):
    waiting_message = State()


class CryptoTokensStates(StatesGroup):
    waiting_token = State()


class GreetingStates(StatesGroup):
    waiting_greeting = State()


class CommandStates(StatesGroup):
    waiting_command_name = State()
    waiting_command_response = State()
    waiting_edit_choice = State()
    waiting_edit_value = State()


class OrderStates(StatesGroup):
    waiting_country = State()
    waiting_payment_confirmation = State()
    waiting_code_number = State()


# --- База данных ---
async def init_db():
    global db_pool
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL не найден в переменных окружения")
    
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                crypto_token TEXT,
                greeting_message TEXT DEFAULT '<b>Привет, {FULLNAME}!</b>',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS work_accounts (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                phone TEXT NOT NULL,
                session_string TEXT NOT NULL,
                is_2fa BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(user_id, phone)
            )
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS sale_accounts (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                phone TEXT NOT NULL,
                session_string TEXT NOT NULL,
                country TEXT NOT NULL,
                is_2fa BOOLEAN DEFAULT FALSE,
                is_sold BOOLEAN DEFAULT FALSE,
                sold_to BIGINT,
                sold_at TIMESTAMP,
                price_rub DECIMAL DEFAULT 90,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(user_id, phone)
            )
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS sales_history (
                id SERIAL PRIMARY KEY,
                seller_id BIGINT REFERENCES users(user_id),
                buyer_id BIGINT,
                phone TEXT,
                country TEXT,
                price_rub DECIMAL,
                invoice_id TEXT,
                sold_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS custom_commands (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                command TEXT NOT NULL,
                response TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(user_id, command)
            )
        """)


async def ensure_user(user_id: int, username: str = "", full_name: str = ""):
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        if not user:
            await conn.execute(
                "INSERT INTO users (user_id, username, full_name) VALUES ($1, $2, $3)",
                user_id, username, full_name
            )
        return user


async def get_user(user_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)


# --- Telethon ---
async def create_telethon_client(session_string: str = None):
    if session_string:
        client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    else:
        client = TelegramClient(StringSession(), API_ID, API_HASH)
    return client


async def get_code_from_telegram(phone: str, session_string: str, is_2fa: bool = False, password_2fa: str = ""):
    try:
        client = await create_telethon_client(session_string)
        await client.connect()
        
        if not await client.is_user_authorized():
            if is_2fa and password_2fa:
                try:
                    await client.sign_in(password=password_2fa)
                except:
                    pass
            await client.disconnect()
            return None, None
        
        dialogs = await client.get_dialogs(limit=5)
        
        for dialog in dialogs:
            if dialog.is_user or dialog.is_group or dialog.is_channel:
                messages = await client.get_messages(dialog.id, limit=20)
                for message in messages:
                    if message.message:
                        codes = re.findall(r'\b\d{5}\b', message.message)
                        if codes:
                            await client.disconnect()
                            return codes[0], dialog.name
                
                await client.disconnect()
                return None, None
        
        await client.disconnect()
        return None, None
    except Exception as e:
        logger.error(f"Ошибка получения кода: {e}")
        return None, None


# --- Crypto Bot ---
async def create_invoice(amount_rub: float, crypto_token: str, description: str = "Покупка аккаунта") -> Optional[Dict]:
    try:
        amount_usdt = round(amount_rub / USDT_RATE, 2)
        
        async with aiohttp.ClientSession() as session:
            headers = {
                "Crypto-Pay-API-Token": crypto_token
            }
            
            data = {
                "asset": "USDT",
                "amount": str(amount_usdt),
                "description": description,
                "allow_comments": False,
                "allow_anonymous": False,
                "expires_in": 1800
            }
            
            async with session.post(
                f"{CRYPTO_BOT_API}/createInvoice",
                headers=headers,
                json=data
            ) as response:
                result = await response.json()
                
                if result.get("ok"):
                    return result["result"]
                else:
                    logger.error(f"Ошибка создания счета: {result}")
                    return None
    except Exception as e:
        logger.error(f"Ошибка API Crypto Bot: {e}")
        return None


async def check_invoice(invoice_id: int, crypto_token: str) -> Optional[Dict]:
    try:
        async with aiohttp.ClientSession() as session:
            headers = {
                "Crypto-Pay-API-Token": crypto_token
            }
            
            async with session.get(
                f"{CRYPTO_BOT_API}/getInvoices",
                headers=headers,
                params={"invoice_ids": str(invoice_id)}
            ) as response:
                result = await response.json()
                
                if result.get("ok") and result["result"]["items"]:
                    return result["result"]["items"][0]
                return None
    except Exception as e:
        logger.error(f"Ошибка проверки счета: {e}")
        return None


# --- Клавиатуры ---
def get_main_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(
                    text="Менеджер аккаунтов",
                    icon_custom_emoji_id="6030400221232501136"
                )
            ],
            [
                KeyboardButton(
                    text="Для продажи",
                    icon_custom_emoji_id="5884479287171485878"
                )
            ],
            [
                KeyboardButton(
                    text="Редактирование",
                    icon_custom_emoji_id="5870676941614354370"
                )
            ],
            [
                KeyboardButton(
                    text="Профиль",
                    icon_custom_emoji_id="5870994129244131212"
                )
            ]
        ],
        resize_keyboard=True
    )


def get_profile_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Обновить",
                callback_data="refresh_profile",
                icon_custom_emoji_id="5345906554510012647"
            )
        ]
    ])


def get_manager_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Добавить аккаунт",
                callback_data="add_work_account",
                icon_custom_emoji_id="5870633910337015697"
            ),
            InlineKeyboardButton(
                text="Мои аккаунты",
                callback_data="list_work_accounts",
                icon_custom_emoji_id="6030400221232501136"
            )
        ],
        [
            InlineKeyboardButton(
                text="Назад",
                callback_data="back_to_main",
                icon_custom_emoji_id="5774022692642492953"
            )
        ]
    ])


def get_sale_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Добавить для продажи",
                callback_data="add_sale_account",
                icon_custom_emoji_id="5890848474563352982"
            ),
            InlineKeyboardButton(
                text="Список на продажу",
                callback_data="list_sale_accounts",
                icon_custom_emoji_id="5884479287171485878"
            )
        ],
        [
            InlineKeyboardButton(
                text="Назад",
                callback_data="back_to_main",
                icon_custom_emoji_id="5774022692642492953"
            )
        ]
    ])


def get_edit_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Приветствие",
                callback_data="edit_greeting",
                icon_custom_emoji_id="5769289093221454192"
            )
        ],
        [
            InlineKeyboardButton(
                text="Crypto Bot Token",
                callback_data="edit_crypto_token",
                icon_custom_emoji_id="5260752406890711732"
            )
        ],
        [
            InlineKeyboardButton(
                text="Управление командами",
                callback_data="manage_commands",
                icon_custom_emoji_id="5870801517140775623"
            )
        ],
        [
            InlineKeyboardButton(
                text="Назад",
                callback_data="back_to_main",
                icon_custom_emoji_id="5774022692642492953"
            )
        ]
    ])


def get_commands_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Добавить команду",
                callback_data="add_command",
                icon_custom_emoji_id="5870633910337015697"
            ),
            InlineKeyboardButton(
                text="Мои команды",
                callback_data="list_commands",
                icon_custom_emoji_id="5870930636742595124"
            )
        ],
        [
            InlineKeyboardButton(
                text="Назад",
                callback_data="back_to_edit",
                icon_custom_emoji_id="5774022692642492953"
            )
        ]
    ])


def get_back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Назад",
                callback_data="back_to_main",
                icon_custom_emoji_id="5774022692642492953"
            )
        ]
    ])


async def get_work_accounts_list(user_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM work_accounts WHERE user_id = $1 ORDER BY created_at DESC",
            user_id
        )


async def get_sale_accounts_list(user_id: int, only_available: bool = True):
    async with db_pool.acquire() as conn:
        if only_available:
            return await conn.fetch(
                "SELECT * FROM sale_accounts WHERE user_id = $1 AND is_sold = FALSE ORDER BY created_at DESC",
                user_id
            )
        else:
            return await conn.fetch(
                "SELECT * FROM sale_accounts WHERE user_id = $1 ORDER BY created_at DESC",
                user_id
            )


# --- Обработчики команд ---
@dp.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username or ""
    full_name = message.from_user.full_name or ""
    
    await ensure_user(user_id, username, full_name)
    
    await message.answer(
        '<b><tg-emoji emoji-id="6030400221232501136">🤖</tg-emoji> Добро пожаловать в VEST PANEL!</b>\n'
        '<i>Выберите действие в меню:</i>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )


@dp.message(F.text == "Менеджер аккаунтов")
async def manager_menu(message: Message):
    user_id = message.from_user.id
    await ensure_user(user_id)
    
    await message.answer(
        '<b><tg-emoji emoji-id="6030400221232501136">🤖</tg-emoji> Менеджер аккаунтов</b>\n'
        '<i>Здесь вы можете добавить аккаунты для работы панели (до 150 шт.)</i>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_manager_keyboard()
    )


@dp.message(F.text == "Для продажи")
async def sale_menu(message: Message):
    user_id = message.from_user.id
    await ensure_user(user_id)
    
    await message.answer(
        '<b><tg-emoji emoji-id="5884479287171485878">📦</tg-emoji> Аккаунты для продажи</b>\n'
        '<i>Добавьте аккаунты для продажи с указанием страны</i>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_sale_keyboard()
    )


@dp.message(F.text == "Редактирование")
async def edit_menu(message: Message):
    user_id = message.from_user.id
    await ensure_user(user_id)
    
    await message.answer(
        '<b><tg-emoji emoji-id="5870676941614354370">🖋</tg-emoji> Редактирование</b>\n'
        '<i>Настройте параметры вашей панели</i>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_edit_keyboard()
    )


@dp.message(F.text == "Профиль")
async def profile_menu(message: Message):
    user_id = message.from_user.id
    await ensure_user(user_id)
    
    user = await get_user(user_id)
    
    async with db_pool.acquire() as conn:
        work_count = await conn.fetchval(
            "SELECT COUNT(*) FROM work_accounts WHERE user_id = $1", user_id
        )
        sale_count = await conn.fetchval(
            "SELECT COUNT(*) FROM sale_accounts WHERE user_id = $1 AND is_sold = FALSE", user_id
        )
        sold_count = await conn.fetchval(
            "SELECT COUNT(*) FROM sales_history WHERE seller_id = $1", user_id
        )
        revenue = await conn.fetchval(
            "SELECT COALESCE(SUM(price_rub), 0) FROM sales_history WHERE seller_id = $1", user_id
        )
    
    await message.answer(
        f'<b><tg-emoji emoji-id="5870994129244131212">👤</tg-emoji> Профиль</b>\n\n'
        f'<tg-emoji emoji-id="6030400221232501136">🤖</tg-emoji> Аккаунтов в работе: <b>{work_count}/150</b>\n'
        f'<tg-emoji emoji-id="5884479287171485878">📦</tg-emoji> Аккаунтов для продажи: <b>{sale_count}</b>\n'
        f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Всего продано: <b>{sold_count}</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Выручка: <b>{revenue}₽</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_profile_keyboard()
    )


# --- Callback обработчики ---
@dp.callback_query(F.data == "back_to_main")
async def back_to_main(callback: CallbackQuery):
    await callback.message.delete()
    await callback.message.answer(
        '<b><tg-emoji emoji-id="5870982283724328568">⚙</tg-emoji> Главное меню</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )
    await callback.answer()


@dp.callback_query(F.data == "back_to_edit")
async def back_to_edit(callback: CallbackQuery):
    await callback.message.delete()
    await callback.message.answer(
        '<b><tg-emoji emoji-id="5870676941614354370">🖋</tg-emoji> Редактирование</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_edit_keyboard()
    )
    await callback.answer()


@dp.callback_query(F.data == "refresh_profile")
async def refresh_profile(callback: CallbackQuery):
    await profile_menu(callback.message)
    await callback.answer("Профиль обновлен")


# --- Добавление рабочих аккаунтов ---
@dp.callback_query(F.data == "add_work_account")
async def add_work_account(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM work_accounts WHERE user_id = $1", user_id
        )
    
    if count >= 150:
        await callback.answer("Достигнут лимит в 150 аккаунтов", show_alert=True)
        return
    
    await callback.message.answer(
        '<b><tg-emoji emoji-id="5870753782874246579">✍</tg-emoji> Введите номер телефона в формате +7XXXXXXXXXX:</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(WorkAccountStates.waiting_phone)
    await callback.answer()


@dp.message(WorkAccountStates.waiting_phone)
async def process_work_phone(message: Message, state: FSMContext):
    phone = message.text.strip()
    
    if not re.match(r'^\+\d{10,15}$', phone):
        await message.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Неверный формат. Введите номер в формате +7XXXXXXXXXX',
            parse_mode=ParseMode.HTML
        )
        return
    
    try:
        client = await create_telethon_client()
        await client.connect()
        
        pending_logins[message.from_user.id] = {
            "client": client,
            "phone": phone,
            "type": "work"
        }
        
        sent = await client.send_code_request(phone)
        
        await message.answer(
            f'<b><tg-emoji emoji-id="6039486778597970865">🔔</tg-emoji> Код отправлен на номер {phone}</b>\n'
            '<i>Введите код из SMS/Telegram:</i>',
            parse_mode=ParseMode.HTML
        )
        await state.set_state(WorkAccountStates.waiting_code)
    except Exception as e:
        await message.answer(
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Ошибка: {str(e)}',
            parse_mode=ParseMode.HTML
        )
        await state.clear()


@dp.message(WorkAccountStates.waiting_code)
async def process_work_code(message: Message, state: FSMContext):
    code = message.text.strip()
    user_id = message.from_user.id
    
    if user_id not in pending_logins:
        await message.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Сессия не найдена',
            parse_mode=ParseMode.HTML
        )
        await state.clear()
        return
    
    login_data = pending_logins[user_id]
    client = login_data["client"]
    phone = login_data["phone"]
    
    try:
        await client.sign_in(phone=phone, code=code)
        
        session_string = client.session.save()
        
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO work_accounts (user_id, phone, session_string, is_2fa) VALUES ($1, $2, $3, $4)",
                user_id, phone, session_string, False
            )
        
        await client.disconnect()
        del pending_logins[user_id]
        
        await message.answer(
            '<b><tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Аккаунт успешно добавлен!</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        await state.clear()
    except SessionPasswordNeededError:
        await message.answer(
            '<b><tg-emoji emoji-id="6037249452824072506">🔒</tg-emoji> Введите пароль 2FA:</b>',
            parse_mode=ParseMode.HTML
        )
        await state.set_state(WorkAccountStates.waiting_2fa)
    except Exception as e:
        await message.answer(
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Ошибка: {str(e)}',
            parse_mode=ParseMode.HTML
        )
        await state.clear()
        if user_id in pending_logins:
            del pending_logins[user_id]


@dp.message(WorkAccountStates.waiting_2fa)
async def process_work_2fa(message: Message, state: FSMContext):
    password = message.text.strip()
    user_id = message.from_user.id
    
    if user_id not in pending_logins:
        await message.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Сессия не найдена',
            parse_mode=ParseMode.HTML
        )
        await state.clear()
        return
    
    login_data = pending_logins[user_id]
    client = login_data["client"]
    phone = login_data["phone"]
    
    try:
        await client.sign_in(password=password)
        
        session_string = client.session.save()
        
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO work_accounts (user_id, phone, session_string, is_2fa) VALUES ($1, $2, $3, $4)",
                user_id, phone, session_string, True
            )
        
        await client.disconnect()
        del pending_logins[user_id]
        
        await message.answer(
            '<b><tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Аккаунт с 2FA успешно добавлен!</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        await state.clear()
    except Exception as e:
        await message.answer(
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Ошибка: {str(e)}',
            parse_mode=ParseMode.HTML
        )
        await state.clear()
        if user_id in pending_logins:
            del pending_logins[user_id]


@dp.callback_query(F.data == "list_work_accounts")
async def list_work_accounts(callback: CallbackQuery):
    user_id = callback.from_user.id
    accounts = await get_work_accounts_list(user_id)
    
    if not accounts:
        await callback.message.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Нет добавленных аккаунтов',
            parse_mode=ParseMode.HTML,
            reply_markup=get_manager_keyboard()
        )
        await callback.answer()
        return
    
    text = '<b><tg-emoji emoji-id="6030400221232501136">🤖</tg-emoji> Ваши рабочие аккаунты:</b>\n\n'
    for i, acc in enumerate(accounts[:10], 1):
        fa_status = "<tg-emoji emoji-id='6037249452824072506'>🔒</tg-emoji>" if acc['is_2fa'] else "<tg-emoji emoji-id='6037496202990194718'>🔓</tg-emoji>"
        text += f"{i}. {acc['phone']} {fa_status}\n"
    
    if len(accounts) > 10:
        text += f"\n<i>... и еще {len(accounts) - 10} аккаунтов</i>"
    
    await callback.message.answer(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=get_manager_keyboard()
    )
    await callback.answer()


# --- Добавление аккаунтов для продажи ---
@dp.callback_query(F.data == "add_sale_account")
async def add_sale_account(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer(
        '<b><tg-emoji emoji-id="5870753782874246579">✍</tg-emoji> Введите номер телефона для продажи (+7XXXXXXXXXX):</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(SaleAccountStates.waiting_phone)
    await callback.answer()


@dp.message(SaleAccountStates.waiting_phone)
async def process_sale_phone(message: Message, state: FSMContext):
    phone = message.text.strip()
    
    if not re.match(r'^\+\d{10,15}$', phone):
        await message.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Неверный формат',
            parse_mode=ParseMode.HTML
        )
        return
    
    try:
        client = await create_telethon_client()
        await client.connect()
        
        pending_logins[message.from_user.id] = {
            "client": client,
            "phone": phone,
            "type": "sale"
        }
        
        await client.send_code_request(phone)
        
        await message.answer(
            f'<b><tg-emoji emoji-id="6039486778597970865">🔔</tg-emoji> Код отправлен на {phone}</b>\n<i>Введите код:</i>',
            parse_mode=ParseMode.HTML
        )
        await state.set_state(SaleAccountStates.waiting_code)
    except Exception as e:
        await message.answer(
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Ошибка: {str(e)}',
            parse_mode=ParseMode.HTML
        )
        await state.clear()


@dp.message(SaleAccountStates.waiting_code)
async def process_sale_code(message: Message, state: FSMContext):
    code = message.text.strip()
    user_id = message.from_user.id
    
    if user_id not in pending_logins:
        await message.answer('<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Сессия не найдена', parse_mode=ParseMode.HTML)
        await state.clear()
        return
    
    login_data = pending_logins[user_id]
    client = login_data["client"]
    phone = login_data["phone"]
    
    try:
        await client.sign_in(phone=phone, code=code)
        
        login_data["session_string"] = client.session.save()
        
        await message.answer(
            '<b><tg-emoji emoji-id="5870753782874246579">✍</tg-emoji> Введите страну аккаунта (например: RU, KZ, UA):</b>',
            parse_mode=ParseMode.HTML
        )
        await state.set_state(SaleAccountStates.waiting_country)
    except SessionPasswordNeededError:
        await message.answer(
            '<b><tg-emoji emoji-id="6037249452824072506">🔒</tg-emoji> Введите пароль 2FA:</b>',
            parse_mode=ParseMode.HTML
        )
        await state.set_state(SaleAccountStates.waiting_2fa)
    except Exception as e:
        await message.answer(f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Ошибка: {str(e)}', parse_mode=ParseMode.HTML)
        await state.clear()
        if user_id in pending_logins:
            del pending_logins[user_id]


@dp.message(SaleAccountStates.waiting_2fa)
async def process_sale_2fa(message: Message, state: FSMContext):
    password = message.text.strip()
    user_id = message.from_user.id
    
    if user_id not in pending_logins:
        await message.answer('<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Сессия не найдена', parse_mode=ParseMode.HTML)
        await state.clear()
        return
    
    login_data = pending_logins[user_id]
    client = login_data["client"]
    
    try:
        await client.sign_in(password=password)
        
        login_data["session_string"] = client.session.save()
        login_data["is_2fa"] = True
        
        await message.answer(
            '<b><tg-emoji emoji-id="5870753782874246579">✍</tg-emoji> Введите страну аккаунта (например: RU, KZ, UA):</b>',
            parse_mode=ParseMode.HTML
        )
        await state.set_state(SaleAccountStates.waiting_country)
    except Exception as e:
        await message.answer(f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Ошибка: {str(e)}', parse_mode=ParseMode.HTML)
        await state.clear()
        if user_id in pending_logins:
            del pending_logins[user_id]


@dp.message(SaleAccountStates.waiting_country)
async def process_sale_country(message: Message, state: FSMContext):
    country = message.text.strip().upper()
    user_id = message.from_user.id
    
    if user_id not in pending_logins:
        await message.answer('<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Сессия не найдена', parse_mode=ParseMode.HTML)
        await state.clear()
        return
    
    login_data = pending_logins[user_id]
    session_string = login_data.get("session_string")
    phone = login_data["phone"]
    is_2fa = login_data.get("is_2fa", False)
    
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO sale_accounts (user_id, phone, session_string, country, is_2fa) VALUES ($1, $2, $3, $4, $5)",
                user_id, phone, session_string, country, is_2fa
            )
        
        await login_data["client"].disconnect()
        del pending_logins[user_id]
        
        await message.answer(
            f'<b><tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Аккаунт {phone} ({country}) добавлен для продажи!</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        await state.clear()
    except Exception as e:
        await message.answer(f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Ошибка: {str(e)}', parse_mode=ParseMode.HTML)
        await state.clear()


@dp.callback_query(F.data == "list_sale_accounts")
async def list_sale_accounts(callback: CallbackQuery):
    user_id = callback.from_user.id
    accounts = await get_sale_accounts_list(user_id)
    
    if not accounts:
        await callback.message.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Нет аккаунтов для продажи',
            parse_mode=ParseMode.HTML,
            reply_markup=get_sale_keyboard()
        )
        await callback.answer()
        return
    
    text = '<b><tg-emoji emoji-id="5884479287171485878">📦</tg-emoji> Аккаунты для продажи:</b>\n\n'
    for acc in accounts:
        text += f"• {acc['phone']} | {acc['country']} | {acc['price_rub']}₽\n"
    
    await callback.message.answer(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=get_sale_keyboard()
    )
    await callback.answer()


# --- Редактирование ---
@dp.callback_query(F.data == "edit_greeting")
async def edit_greeting(callback: CallbackQuery, state: FSMContext):
    user = await get_user(callback.from_user.id)
    current = user['greeting_message'] if user else '<b>Привет, {FULLNAME}!</b>'
    
    await callback.message.answer(
        f'<b><tg-emoji emoji-id="5769289093221454192">🔗</tg-emoji> Текущее приветствие:</b>\n'
        f'<code>{current}</code>\n\n'
        '<i>Доступные переменные: {FULLNAME}, {USERNAME}</i>\n'
        '<i>Поддерживается HTML формат</i>\n\n'
        '<tg-emoji emoji-id="5870753782874246579">✍</tg-emoji> Введите новое приветствие:',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(GreetingStates.waiting_greeting)
    await callback.answer()


@dp.message(GreetingStates.waiting_greeting)
async def save_greeting(message: Message, state: FSMContext):
    greeting = message.text.strip()
    user_id = message.from_user.id
    
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET greeting_message = $1 WHERE user_id = $2",
            greeting, user_id
        )
    
    await message.answer(
        '<b><tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Приветствие сохранено!</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )
    await state.clear()


@dp.callback_query(F.data == "edit_crypto_token")
async def edit_crypto_token(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer(
        '<b><tg-emoji emoji-id="5260752406890711732">👾</tg-emoji> Введите ваш Crypto Bot API Token:</b>\n'
        '<i>Получить можно в @CryptoBot</i>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(CryptoTokensStates.waiting_token)
    await callback.answer()


@dp.message(CryptoTokensStates.waiting_token)
async def save_crypto_token(message: Message, state: FSMContext):
    token = message.text.strip()
    user_id = message.from_user.id
    
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET crypto_token = $1 WHERE user_id = $2",
            token, user_id
        )
    
    await message.answer(
        '<b><tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Crypto Bot Token сохранен!</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )
    await state.clear()


# --- Управление командами ---
@dp.callback_query(F.data == "manage_commands")
async def manage_commands(callback: CallbackQuery):
    await callback.message.answer(
        '<b><tg-emoji emoji-id="5870801517140775623">🔗</tg-emoji> Управление командами</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_commands_keyboard()
    )
    await callback.answer()


@dp.callback_query(F.data == "add_command")
async def add_command(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer(
        '<b><tg-emoji emoji-id="5870753782874246579">✍</tg-emoji> Введите команду (начиная с точки):</b>\n'
        '<i>Пример: .прайс</i>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(CommandStates.waiting_command_name)
    await callback.answer()


@dp.message(CommandStates.waiting_command_name)
async def process_command_name(message: Message, state: FSMContext):
    command = message.text.strip()
    
    if not command.startswith('.'):
        await message.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Команда должна начинаться с точки',
            parse_mode=ParseMode.HTML
        )
        return
    
    await state.update_data(command=command)
    await message.answer(
        '<b><tg-emoji emoji-id="5870753782874246579">✍</tg-emoji> Введите текст ответа (поддерживается HTML):</b>',
        parse_mode=ParseMode.HTML
    )
    await state.set_state(CommandStates.waiting_command_response)


@dp.message(CommandStates.waiting_command_response)
async def save_command(message: Message, state: FSMContext):
    response = message.text.strip()
    user_id = message.from_user.id
    data = await state.get_data()
    command = data['command']
    
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO custom_commands (user_id, command, response) VALUES ($1, $2, $3) ON CONFLICT (user_id, command) DO UPDATE SET response = $3",
            user_id, command, response
        )
    
    await message.answer(
        f'<b><tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Команда {command} сохранена!</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )
    await state.clear()


@dp.callback_query(F.data == "list_commands")
async def list_commands(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        commands = await conn.fetch(
            "SELECT * FROM custom_commands WHERE user_id = $1 ORDER BY created_at DESC",
            user_id
        )
    
    if not commands:
        await callback.message.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Нет созданных команд',
            parse_mode=ParseMode.HTML,
            reply_markup=get_commands_keyboard()
        )
        await callback.answer()
        return
    
    text = '<b><tg-emoji emoji-id="5870930636742595124">📊</tg-emoji> Ваши команды:</b>\n\n'
    for cmd in commands:
        text += f"<code>{cmd['command']}</code> - {cmd['response'][:50]}...\n"
    
    await callback.message.answer(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=get_commands_keyboard()
    )
    await callback.answer()


# --- Обработка команд от пользователей (через рабочие аккаунты) ---
@dp.message(F.text.startswith('.'))
async def handle_custom_command(message: Message):
    command = message.text.split()[0].lower()
    user_id = message.from_user.id
    
    # Проверяем, является ли отправитель владельцем фермы
    seller = await get_user(user_id)
    
    if command == '.наличие':
        async with db_pool.acquire() as conn:
            # Ищем аккаунты для продажи у создателя фермы
            # Находим всех пользователей с sale аккаунтами
            availability = await conn.fetch("""
                SELECT s.country, COUNT(*) as count, s.user_id 
                FROM sale_accounts s 
                WHERE s.is_sold = FALSE 
                GROUP BY s.country, s.user_id
            """)
        
        if not availability:
            await message.answer(
                '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Нет доступных аккаунтов',
                parse_mode=ParseMode.HTML
            )
            return
        
        text = '<b><tg-emoji emoji-id="5884479287171485878">📦</tg-emoji> Наличие аккаунтов:</b>\n\n'
        for item in availability:
            text += f"<b>{item['country']}</b>: {item['count']} шт.\n"
        
        await message.answer(text, parse_mode=ParseMode.HTML)
    elif command == '.купить':
        async with db_pool.acquire() as conn:
            availability = await conn.fetch("""
                SELECT country, COUNT(*) as count 
                FROM sale_accounts 
                WHERE is_sold = FALSE 
                GROUP BY country
            """)
        
        if not availability:
            await message.answer(
                '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Нет доступных аккаунтов',
                parse_mode=ParseMode.HTML
            )
            return
        
        text = '<b><tg-emoji emoji-id="5884479287171485878">📦</tg-emoji> Доступные аккаунты:</b>\n\n'
        for item in availability:
            text += f"<b>{item['country']}</b>: {item['count']} шт. - 90₽\n"
        
        text += '\n<i>Для покупки введите:</i> <code>.покупка СТРАНА</code>'
        
        await message.answer(text, parse_mode=ParseMode.HTML)
    elif command == '.оплатил':
        user_id = message.from_user.id
        
        async with db_pool.acquire() as conn:
            order = await conn.fetchrow(
                "SELECT * FROM active_orders WHERE buyer_id = $1 AND status = 'pending' ORDER BY created_at DESC LIMIT 1",
                user_id
            )
        
        if not order:
            await message.answer(
                '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Нет активных заказов',
                parse_mode=ParseMode.HTML
            )
            return
        
        seller = await get_user(order['user_id'])
        
        if not seller or not seller['crypto_token']:
            await message.answer(
                '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Ошибка конфигурации продавца',
                parse_mode=ParseMode.HTML
            )
            return
        
        invoice = await check_invoice(int(order['invoice_id']), seller['crypto_token'])
        
        if invoice and invoice['status'] == 'paid':
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE active_orders SET status = 'paid' WHERE id = $1",
                    order['id']
                )
                
                # Получаем аккаунт для продажи
                sale_account = await conn.fetchrow(
                    "SELECT * FROM sale_accounts WHERE user_id = $1 AND country = $2 AND is_sold = FALSE LIMIT 1",
                    order['user_id'], order['country']
                )
                
                if not sale_account:
                    await message.answer(
                        '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Аккаунты закончились',
                        parse_mode=ParseMode.HTML
                    )
                    return
                
                await conn.execute(
                    "UPDATE sale_accounts SET is_sold = TRUE, sold_to = $1, sold_at = NOW() WHERE id = $2",
                    user_id, sale_account['id']
                )
                
                await conn.execute(
                    "INSERT INTO sales_history (seller_id, buyer_id, phone, country, price_rub, invoice_id) VALUES ($1, $2, $3, $4, $5, $6)",
                    order['user_id'], user_id, sale_account['phone'], sale_account['country'], order['price_rub'], order['invoice_id']
                )
            
            await message.answer(
                f'<b><tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Оплата подтверждена!</b>\n'
                f'<i>Номер аккаунта: {sale_account["phone"]}</i>\n\n'
                '<i>Для получения кода введите:</i> <code>.код НОМЕР</code>\n'
                '<i>Где НОМЕР - номер телефона аккаунта</i>',
                parse_mode=ParseMode.HTML
            )
        else:
            await message.answer(
                '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Оплата не найдена. Проверьте оплату',
                parse_mode=ParseMode.HTML
            )
    elif command == '.код':
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer(
                '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Укажите номер: <code>.код +7XXXXXXXXXX</code>',
                parse_mode=ParseMode.HTML
            )
            return
        
        phone = parts[1]
        user_id = message.from_user.id
        
        async with db_pool.acquire() as conn:
            sale_account = await conn.fetchrow(
                "SELECT * FROM sale_accounts WHERE phone = $1 AND sold_to = $2 AND is_sold = TRUE",
                phone, user_id
            )
        
        if not sale_account:
            await message.answer(
                '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Аккаунт не найден или не принадлежит вам',
                parse_mode=ParseMode.HTML
            )
            return
        
        await message.answer(
            '<tg-emoji emoji-id="5345906554510012647">🔄</tg-emoji> Получаю код...',
            parse_mode=ParseMode.HTML
        )
        
        code, chat_name = await get_code_from_telegram(
            sale_account['phone'],
            sale_account['session_string'],
            sale_account['is_2fa']
        )
        
        if code:
            response = f'<b><tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Код для {phone}:</b>\n\n'
            response += f'<code>{code}</code>\n'
            if sale_account['is_2fa']:
                response += f'\n<tg-emoji emoji-id="6037249452824072506">🔒</tg-emoji> 2FA активен'
            
            await message.answer(response, parse_mode=ParseMode.HTML)
        else:
            await message.answer(
                '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Не удалось получить код. Попробуйте позже',
                parse_mode=ParseMode.HTML
            )
    elif command == '.помощь':
        await message.answer(
            '<b><tg-emoji emoji-id="6028435952299413210">ℹ</tg-emoji> Доступные команды:</b>\n\n'
            '<code>.наличие</code> - проверить наличие аккаунтов\n'
            '<code>.купить</code> - посмотреть доступные для покупки\n'
            '<code>.покупка СТРАНА</code> - купить аккаунт\n'
            '<code>.оплатил</code> - подтвердить оплату\n'
            '<code>.код НОМЕР</code> - получить код\n'
            '<code>.прайс</code> - цены\n'
            '<code>.помощь</code> - это сообщение',
            parse_mode=ParseMode.HTML
        )
    elif command == '.покупка':
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer(
                '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Укажите страну: <code>.покупка RU</code>',
                parse_mode=ParseMode.HTML
            )
            return
        
        country = parts[1].upper()
        buyer_id = message.from_user.id
        
        async with db_pool.acquire() as conn:
            # Находим продавца с аккаунтами в этой стране
            available = await conn.fetchrow("""
                SELECT s.user_id, COUNT(*) as count 
                FROM sale_accounts s 
                JOIN users u ON s.user_id = u.user_id
                WHERE s.country = $1 AND s.is_sold = FALSE AND u.crypto_token IS NOT NULL
                GROUP BY s.user_id 
                LIMIT 1
            """, country)
            
            if not available:
                await message.answer(
                    f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Нет доступных аккаунтов для страны {country}',
                    parse_mode=ParseMode.HTML
                )
                return
            
            seller_id = available['user_id']
            seller = await get_user(seller_id)
            
            if not seller or not seller['crypto_token']:
                await message.answer(
                    '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Продавец не настроил прием платежей',
                    parse_mode=ParseMode.HTML
                )
                return
            
            invoice = await create_invoice(90, seller['crypto_token'], f"Покупка аккаунта {country}")
            
            if not invoice:
                await message.answer(
                    '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Ошибка создания счета',
                    parse_mode=ParseMode.HTML
                )
                return
            
            await conn.execute(
                "INSERT INTO active_orders (user_id, buyer_id, country, price_rub, invoice_id) VALUES ($1, $2, $3, $4, $5)",
                seller_id, buyer_id, country, 90, str(invoice['invoice_id'])
            )
            
            await message.answer(
                f'<b><tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Счет на оплату создан!</b>\n\n'
                f'<i>Страна:</i> <b>{country}</b>\n'
                f'<i>Сумма:</i> <b>90₽</b>\n\n'
                f'<i>Ссылка на оплату:</i> {invoice["pay_url"]}\n\n'
                '<i>После оплаты введите:</i> <code>.оплатил</code>',
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
    elif command == '.прайс':
        await message.answer(
            '<b><tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Прайс-лист:</b>\n\n'
            '<b>Аккаунт Telegram</b> - 90₽\n'
            '<i>Все страны</i>',
            parse_mode=ParseMode.HTML
        )
    else:
        # Проверяем пользовательские команды
        async with db_pool.acquire() as conn:
            custom_cmd = await conn.fetchrow(
                "SELECT * FROM custom_commands WHERE user_id = $1 AND command = $2",
                user_id, command
            )
        
        if custom_cmd:
            await message.answer(
                custom_cmd['response'],
                parse_mode=ParseMode.HTML
            )
        else:
            await message.answer(
                '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Неизвестная команда. Введите .помощь для списка команд',
                parse_mode=ParseMode.HTML
            )


# --- Обработка приветственного сообщения ---
@dp.message(F.text & ~F.text.startswith('.') & ~F.text.startswith('/'))
async def handle_message_to_work_account(message: Message):
    # Проверяем, подключен ли этот аккаунт как рабочий
    user_id = message.from_user.id
    
    async with db_pool.acquire() as conn:
        work_account = await conn.fetchrow(
            "SELECT * FROM work_accounts WHERE user_id = $1",
            user_id
        )
    
    if not work_account:
        return
    
    # Получаем приветствие владельца
    owner = await get_user(work_account['user_id'])
    
    if owner and owner['greeting_message']:
        greeting = owner['greeting_message']
        greeting = greeting.replace('{FULLNAME}', message.from_user.full_name or 'Пользователь')
        greeting = greeting.replace('{USERNAME}', f"@{message.from_user.username}" if message.from_user.username else 'Нет')
        
        await message.answer(greeting, parse_mode=ParseMode.HTML)


# --- Запуск ---
async def main():
    await init_db()
    logger.info("Бот запущен")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

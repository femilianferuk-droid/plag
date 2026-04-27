import asyncio
import logging
import os
import re
import sys
import traceback
import importlib.util
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any, Callable

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from telethon import TelegramClient, errors, events
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import SendReactionRequest
from telethon.tl.types import Channel, Chat, User, ReactionEmoji
from telethon.errors import FloodWaitError

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Конфигурация из переменных окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден в переменных окружения!")

# Константы
API_ID = 32480523
API_HASH = "147839735c9fa4e83451209e9b55cfc5"
SUPPORT_USERNAME = "@VestSupport"
ADMIN_ID = 7973988177
BUY_ACCOUNTS = "@v3estnikov"
DONATION_CHANNEL = "@VestSoftTG"

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ============ ХРАНИЛИЩА ДАННЫХ ============

# Аккаунты пользователей: user_id -> {phone: {"client": TelegramClient, "phone": str}}
user_sessions: Dict[int, Dict[str, dict]] = {}

# Плагины пользователей: user_id -> {plugin_name: module}
user_plugins: Dict[int, Dict[str, Any]] = {}

# Настройки плагинов: user_id -> {plugin_name: {key: value}}
plugin_settings: Dict[int, Dict[str, Dict]] = {}

# Ожидающие подтверждения логины: user_id -> {client, phone, phone_code_hash}
pending_logins: Dict[int, Dict] = {}

# Выбранный аккаунт пользователя: user_id -> phone
user_selected_account: Dict[int, str] = {}

# ============ СОСТОЯНИЯ FSM ============

class AccountStates(StatesGroup):
    """Состояния для добавления аккаунта"""
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_2fa = State()

class PluginStates(StatesGroup):
    """Состояния для загрузки плагина"""
    waiting_for_plugin = State()
    waiting_for_setting_value = State()

# ============ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ============

def get_active_account(user_id: int) -> Optional[Tuple[str, TelegramClient]]:
    """Возвращает выбранный аккаунт или первый доступный"""
    accounts = user_sessions.get(user_id, {})
    if not accounts:
        return None
    
    selected_phone = user_selected_account.get(user_id)
    if selected_phone and selected_phone in accounts:
        return selected_phone, accounts[selected_phone]["client"]
    
    # Если нет выбранного - берем первый
    phone = list(accounts.keys())[0]
    return phone, accounts[phone]["client"]

async def safe_send(chat_id: int, text: str, reply_markup=None, **kwargs):
    """Безопасная отправка сообщения с fallback при ошибке парсинга HTML"""
    try:
        return await bot.send_message(
            chat_id,
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
            **kwargs
        )
    except TelegramBadRequest as e:
        err_msg = str(e)
        if "can't parse" in err_msg:
            clean = re.sub(r'<[^>]+>', '', text)
            return await bot.send_message(
                chat_id,
                clean,
                reply_markup=reply_markup,
                **kwargs
            )
        raise

async def safe_edit(message: types.Message, text: str, reply_markup=None):
    """Безопасное редактирование сообщения с fallback на новое сообщение"""
    try:
        return await message.edit_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup
        )
    except TelegramBadRequest as e:
        err = str(e)
        if "message can't be edited" in err or "message is not modified" in err:
            try:
                await message.delete()
            except:
                pass
            return await safe_send(message.chat.id, text, reply_markup=reply_markup)
        if "can't parse" in err:
            try:
                await message.delete()
            except:
                pass
            clean = re.sub(r'<[^>]+>', '', text)
            return await safe_send(message.chat.id, clean, reply_markup=reply_markup)
        raise
    except Exception:
        try:
            await message.delete()
        except:
            pass
        return await safe_send(message.chat.id, text, reply_markup=reply_markup)

# ============ КЛАВИАТУРЫ ============

def get_main_keyboard():
    """Главное меню с ReplyKeyboard"""
    builder = ReplyKeyboardBuilder()
    builder.button(text="⚙️ Менеджер аккаунтов")
    builder.button(text="🧩 Плагины")
    builder.button(text="📣 Поддержка")
    builder.adjust(2, 1)
    return builder.as_markup(resize_keyboard=True)

def get_accounts_menu_keyboard():
    """Меню управления аккаунтами"""
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить аккаунт", callback_data="add_account")
    builder.button(text="👤 Мои аккаунты", callback_data="my_accounts")
    builder.button(text="✅ Выбрать аккаунт", callback_data="select_account")
    builder.button(text="◁ Назад", callback_data="main_menu")
    builder.adjust(2, 1)
    return builder.as_markup()

def get_plugins_menu_keyboard():
    """Меню управления плагинами"""
    builder = InlineKeyboardBuilder()
    builder.button(text="📦 Загрузить плагин", callback_data="upload_plugin")
    builder.button(text="📁 Мои плагины", callback_data="my_plugins")
    builder.button(text="◁ Назад", callback_data="main_menu")
    builder.adjust(1)
    return builder.as_markup()

def get_back_keyboard(callback_data: str = "main_menu"):
    """Клавиатура с кнопкой Назад"""
    builder = InlineKeyboardBuilder()
    builder.button(text="◁ Назад", callback_data=callback_data)
    return builder.as_markup()

# ============ КОМАНДА START ============

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Обработчик команды /start"""
    text = (
        "🤖 <b>Добро пожаловать!</b>\n\n"
        "<b>Главное меню:</b>\n"
        "⚙️ <b>Менеджер аккаунтов</b> — управление аккаунтами\n"
        "🧩 <b>Плагины</b> — загрузка и управление плагинами\n"
        "📣 <b>Поддержка</b> — связь с поддержкой\n\n"
        "👛 <b>Купить аккаунт для рассылки:</b> " + BUY_ACCOUNTS + "\n"
        "🔗 <b>Новости и обновления:</b> " + DONATION_CHANNEL
    )
    await safe_send(message.chat.id, text, reply_markup=get_main_keyboard())

# ============ ОБРАБОТЧИКИ ТЕКСТОВЫХ КНОПОК ============

@dp.message(F.text == "⚙️ Менеджер аккаунтов")
async def accounts_manager(message: types.Message):
    """Обработчик кнопки Менеджер аккаунтов"""
    user_id = message.from_user.id
    count = len(user_sessions.get(user_id, {}))
    selected = user_selected_account.get(user_id, "Не выбран")
    
    text = (
        "⚙️ <b>Менеджер аккаунтов</b>\n"
        "👤 Активных аккаунтов: " + str(count) + " (безлимит)\n"
        "✅ Выбран: <code>" + selected + "</code>"
    )
    await safe_send(message.chat.id, text, reply_markup=get_accounts_menu_keyboard())

@dp.message(F.text == "🧩 Плагины")
async def plugins_menu(message: types.Message):
    """Обработчик кнопки Плагины"""
    user_id = message.from_user.id
    plugins_count = len(user_plugins.get(user_id, {}))
    
    text = (
        "🧩 <b>Плагины</b>\n"
        "📦 Загружено плагинов: " + str(plugins_count) + " (безлимит)\n\n"
        "ℹ️ Плагины поддерживают Python и TXT формат"
    )
    await safe_send(message.chat.id, text, reply_markup=get_plugins_menu_keyboard())

@dp.message(F.text == "📣 Поддержка")
async def support(message: types.Message):
    """Обработчик кнопки Поддержка"""
    text = (
        "📣 <b>Поддержка</b>\n"
        "🔗 Свяжитесь с нами: " + SUPPORT_USERNAME
    )
    await safe_send(message.chat.id, text, reply_markup=get_main_keyboard())

@dp.callback_query(F.data == "main_menu")
async def back_to_main(callback: types.CallbackQuery):
    """Возврат в главное меню"""
    try:
        await callback.message.delete()
    except:
        pass
    
    text = "🤖 <b>Главное меню</b>"
    await safe_send(callback.message.chat.id, text, reply_markup=get_main_keyboard())
    await callback.answer()

# ============ ДОБАВЛЕНИЕ АККАУНТА ============

@dp.callback_query(F.data == "add_account")
async def add_account(callback: types.CallbackQuery, state: FSMContext):
    """Начало процесса добавления аккаунта"""
    text = (
        "📦 <b>Добавление аккаунта</b>\n"
        "✍️ Введите номер телефона: <code>+79123456789</code>"
    )
    await safe_edit(callback.message, text, reply_markup=get_back_keyboard("accounts_manager"))
    await state.set_state(AccountStates.waiting_for_phone)
    await callback.answer()

@dp.message(AccountStates.waiting_for_phone)
async def process_phone(message: types.Message, state: FSMContext):
    """Обработка введенного номера телефона"""
    phone = message.text.strip()
    
    # Валидация номера
    if not re.match(r'^\+\d{10,15}$', phone):
        await safe_send(
            message.chat.id,
            "❌ Неверный формат. Пример: <code>+79123456789</code>",
            reply_markup=get_back_keyboard("accounts_manager")
        )
        return
    
    user_id = message.from_user.id
    
    # Проверка на дубликат
    if user_id in user_sessions and phone in user_sessions[user_id]:
        await safe_send(
            message.chat.id,
            "❌ Этот аккаунт уже добавлен!",
            reply_markup=get_accounts_menu_keyboard()
        )
        await state.clear()
        return
    
    # Создание клиента Telethon
    client = TelegramClient(
        'sessions/' + str(user_id) + '_' + phone.replace("+", ""),
        API_ID,
        API_HASH
    )
    
    try:
        await client.connect()
        sent_code = await client.send_code_request(phone)
        
        # Сохраняем данные для подтверждения
        pending_logins[user_id] = {
            "client": client,
            "phone": phone,
            "phone_code_hash": sent_code.phone_code_hash
        }
        
        text = (
            "🎁 Код отправлен на <code>" + phone + "</code>\n"
            "✍️ Введите код из SMS:"
        )
        await safe_send(
            message.chat.id,
            text,
            reply_markup=get_back_keyboard("accounts_manager")
        )
        await state.set_state(AccountStates.waiting_for_code)
        
    except Exception as ex:
        await client.disconnect()
        await safe_send(
            message.chat.id,
            "❌ Ошибка: " + str(ex),
            reply_markup=get_accounts_menu_keyboard()
        )
        await state.clear()

@dp.message(AccountStates.waiting_for_code)
async def process_code(message: types.Message, state: FSMContext):
    """Обработка кода подтверждения"""
    code = message.text.strip()
    user_id = message.from_user.id
    
    if user_id not in pending_logins:
        await safe_send(
            message.chat.id,
            "❌ Сессия истекла",
            reply_markup=get_accounts_menu_keyboard()
        )
        await state.clear()
        return
    
    data = pending_logins[user_id]
    client = data["client"]
    phone = data["phone"]
    phone_code_hash = data["phone_code_hash"]
    
    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
        await on_successful_login(user_id, phone, client, message, state)
    except errors.SessionPasswordNeededError:
        # Требуется 2FA
        text = (
            "🔒 Требуется двухфакторная аутентификация\n"
            "✍️ Введите пароль 2FA:"
        )
        await safe_send(
            message.chat.id,
            text,
            reply_markup=get_back_keyboard("accounts_manager")
        )
        await state.set_state(AccountStates.waiting_for_2fa)
    except Exception as ex:
        await client.disconnect()
        pending_logins.pop(user_id, None)
        await safe_send(
            message.chat.id,
            "❌ Ошибка: " + str(ex),
            reply_markup=get_accounts_menu_keyboard()
        )
        await state.clear()

@dp.message(AccountStates.waiting_for_2fa)
async def process_2fa(message: types.Message, state: FSMContext):
    """Обработка пароля 2FA"""
    password = message.text.strip()
    user_id = message.from_user.id
    
    if user_id not in pending_logins:
        await safe_send(
            message.chat.id,
            "❌ Сессия истекла",
            reply_markup=get_accounts_menu_keyboard()
        )
        await state.clear()
        return
    
    data = pending_logins[user_id]
    client = data["client"]
    phone = data["phone"]
    
    try:
        await client.sign_in(password=password)
        await on_successful_login(user_id, phone, client, message, state)
    except Exception as ex:
        await client.disconnect()
        pending_logins.pop(user_id, None)
        await safe_send(
            message.chat.id,
            "❌ Ошибка 2FA: " + str(ex),
            reply_markup=get_accounts_menu_keyboard()
        )
        await state.clear()

async def on_successful_login(user_id: int, phone: str, client: TelegramClient, 
                              message: types.Message, state: FSMContext):
    """Действия при успешном входе в аккаунт"""
    if user_id not in user_sessions:
        user_sessions[user_id] = {}
    
    user_sessions[user_id][phone] = {"client": client, "phone": phone}
    pending_logins.pop(user_id, None)
    
    # Автоматически выбираем первый аккаунт
    if user_id not in user_selected_account:
        user_selected_account[user_id] = phone
    
    await safe_send(
        message.chat.id,
        "✅ Аккаунт <code>" + phone + "</code> успешно добавлен!",
        reply_markup=get_accounts_menu_keyboard()
    )
    await state.clear()

# ============ УПРАВЛЕНИЕ АККАУНТАМИ ============

@dp.callback_query(F.data == "my_accounts")
async def my_accounts(callback: types.CallbackQuery):
    """Просмотр списка аккаунтов"""
    user_id = callback.from_user.id
    accounts = user_sessions.get(user_id, {})
    
    if not accounts:
        text = (
            "🔓 <b>Мои аккаунты</b>\n\n"
            "❌ Нет добавленных аккаунтов"
        )
        await safe_edit(callback.message, text, reply_markup=get_accounts_menu_keyboard())
        await callback.answer()
        return
    
    builder = InlineKeyboardBuilder()
    for phone in accounts:
        builder.button(text=phone, callback_data="acc_" + phone)
    builder.button(text="◁ Назад", callback_data="accounts_manager")
    builder.adjust(1)
    
    text = (
        "👤 <b>Мои аккаунты</b>\n"
        "🔓 Всего: " + str(len(accounts)) + "\n"
        "Выберите аккаунт для управления:"
    )
    await safe_edit(callback.message, text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "select_account")
async def select_account_menu(callback: types.CallbackQuery):
    """Меню выбора активного аккаунта"""
    user_id = callback.from_user.id
    accounts = user_sessions.get(user_id, {})
    
    if not accounts:
        await callback.answer("Нет добавленных аккаунтов!", show_alert=True)
        return
    
    builder = InlineKeyboardBuilder()
    for phone in accounts:
        is_selected = user_selected_account.get(user_id) == phone
        prefix = "✅ " if is_selected else ""
        builder.button(text=prefix + phone, callback_data="sel_" + phone)
    builder.button(text="◁ Назад", callback_data="accounts_manager")
    builder.adjust(1)
    
    selected = user_selected_account.get(user_id, "Не выбран")
    text = (
        "✅ <b>Выбор аккаунта</b>\n"
        "Текущий: <code>" + selected + "</code>\n\n"
        "Выберите аккаунт для работы:"
    )
    await safe_edit(callback.message, text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("sel_"))
async def select_account(callback: types.CallbackQuery):
    """Выбор аккаунта"""
    phone = callback.data.replace("sel_", "")
    user_id = callback.from_user.id
    
    if user_id in user_sessions and phone in user_sessions[user_id]:
        user_selected_account[user_id] = phone
        await callback.answer("Выбран аккаунт " + phone, show_alert=True)
        await select_account_menu(callback)
    else:
        await callback.answer("Аккаунт не найден", show_alert=True)

@dp.callback_query(F.data.startswith("acc_"))
async def account_info(callback: types.CallbackQuery):
    """Информация об аккаунте"""
    phone = callback.data.replace("acc_", "")
    user_id = callback.from_user.id
    is_selected = user_selected_account.get(user_id) == phone
    
    builder = InlineKeyboardBuilder()
    if not is_selected:
        builder.button(text="✅ Выбрать", callback_data="sel_" + phone)
    builder.button(text="🗑 Удалить", callback_data="del_" + phone)
    builder.button(text="◁ Назад", callback_data="my_accounts")
    builder.adjust(1)
    
    status = "✅ Выбран" if is_selected else "⚪ Доступен"
    text = (
        "👤 <b>Аккаунт:</b> <code>" + phone + "</code>\n"
        "✅ Статус: " + status
    )
    await safe_edit(callback.message, text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("del_"))
async def delete_account(callback: types.CallbackQuery):
    """Удаление аккаунта"""
    phone = callback.data.replace("del_", "")
    user_id = callback.from_user.id
    
    if user_id in user_sessions and phone in user_sessions[user_id]:
        try:
            await user_sessions[user_id][phone]["client"].disconnect()
        except:
            pass
        
        del user_sessions[user_id][phone]
        
        # Если удалили выбранный аккаунт - выбираем следующий
        if user_selected_account.get(user_id) == phone:
            user_selected_account.pop(user_id, None)
            if user_sessions.get(user_id):
                user_selected_account[user_id] = list(user_sessions[user_id].keys())[0]
    
    await safe_edit(
        callback.message,
        "✅ Аккаунт <code>" + phone + "</code> удален",
        reply_markup=get_accounts_menu_keyboard()
    )
    await callback.answer("Аккаунт удален!", show_alert=True)

@dp.callback_query(F.data == "accounts_manager")
async def back_to_accounts(callback: types.CallbackQuery):
    """Возврат в меню аккаунтов"""
    user_id = callback.from_user.id
    count = len(user_sessions.get(user_id, {}))
    selected = user_selected_account.get(user_id, "Не выбран")
    
    text = (
        "⚙️ <b>Менеджер аккаунтов</b>\n"
        "👤 Активных аккаунтов: " + str(count) + " (безлимит)\n"
        "✅ Выбран: <code>" + selected + "</code>"
    )
    await safe_edit(callback.message, text, reply_markup=get_accounts_menu_keyboard())
    await callback.answer()

# ============ СИСТЕМА ПЛАГИНОВ ============

class PluginManager:
    """Менеджер для загрузки и управления плагинами"""
    
    PLUGINS_DIR = Path("plugins")
    
    @classmethod
    def init(cls):
        """Инициализация директории плагинов"""
        cls.PLUGINS_DIR.mkdir(exist_ok=True)
    
    @classmethod
    def load_from_code(cls, code: str, user_id: int, plugin_name: str) -> Any:
        """Загружает плагин из строки кода"""
        # Сохраняем файл плагина
        plugin_file = cls.PLUGINS_DIR / f"{user_id}_{plugin_name}.py"
        plugin_file.write_text(code, encoding='utf-8')
        
        # Загружаем модуль
        spec = importlib.util.spec_from_file_location(
            f"plugin_{user_id}_{plugin_name}", 
            plugin_file
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Проверяем наличие класса Plugin
        if not hasattr(module, 'Plugin'):
            raise ValueError("Плагин должен содержать класс 'Plugin'")
        
        return module
    
    @classmethod
    def load_from_file(cls, file_path: Path, user_id: int) -> Any:
        """Загружает плагин из файла"""
        # Если это txt файл, читаем и конвертируем в py
        if file_path.suffix == '.txt':
            code = file_path.read_text(encoding='utf-8')
            plugin_name = file_path.stem
            return cls.load_from_code(code, user_id, plugin_name)
        
        # Если это py файл
        spec = importlib.util.spec_from_file_location(
            f"plugin_{user_id}_{file_path.stem}", 
            file_path
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        if not hasattr(module, 'Plugin'):
            raise ValueError("Плагин должен содержать класс 'Plugin'")
        
        return module
    
    @classmethod
    def get_plugin_settings_schema(cls, plugin_module: Any) -> List[Dict]:
        """Получает схему настроек из плагина"""
        if hasattr(plugin_module, 'get_settings_schema'):
            return plugin_module.get_settings_schema()
        return []

@dp.callback_query(F.data == "upload_plugin")
async def upload_plugin(callback: types.CallbackQuery, state: FSMContext):
    """Начало загрузки плагина"""
    text = (
        "📦 <b>Загрузка плагина</b>\n\n"
        "✍️ Отправьте файл плагина (.py или .txt) или код:\n\n"
        "ℹ️ <b>Формат плагина:</b>\n"
        "<code>class Plugin:\n"
        "    name = \"Мой плагин\"\n"
        "    description = \"Описание\"\n"
        "    \n"
        "    async def setup(self, client, bot, user_id):\n"
        "        # Код при активации\n"
        "        pass\n"
        "    \n"
        "    async def run(self, **kwargs):\n"
        "        # Основная логика\n"
        "        pass</code>\n\n"
        "ℹ️ <b>Функция настроек:</b>\n"
        "<code>def get_settings_schema():\n"
        "    return [\n"
        "        {\n"
        "            'name': 'delay',\n"
        "            'type': 'float',\n"
        "            'default': 5.0,\n"
        "            'description': 'Задержка'\n"
        "        },\n"
        "        {\n"
        "            'name': 'text',\n"
        "            'type': 'str',\n"
        "            'default': 'Привет',\n"
        "            'description': 'Текст'\n"
        "        }\n"
        "    ]</code>\n\n"
        "ℹ️ <b>Доступные типы:</b> str, int, float, bool, list, dict"
    )
    await safe_edit(callback.message, text, reply_markup=get_back_keyboard("plugins"))
    await state.set_state(PluginStates.waiting_for_plugin)
    await callback.answer()

@dp.message(PluginStates.waiting_for_plugin)
async def process_plugin_upload(message: types.Message, state: FSMContext):
    """Обработка загруженного файла или кода плагина"""
    user_id = message.from_user.id
    
    # Обработка файла
    if message.document:
        document = message.document
        file_name = document.file_name or "plugin"
        
        # Проверяем расширение
        if not (file_name.endswith('.py') or file_name.endswith('.txt')):
            await safe_send(
                message.chat.id,
                "❌ Поддерживаются только .py и .txt файлы",
                reply_markup=get_plugins_menu_keyboard()
            )
            await state.clear()
            return
        
        # Скачиваем файл
        file_path = PluginManager.PLUGINS_DIR / f"{user_id}_{file_name}"
        await bot.download(document, destination=file_path)
        
        try:
            # Загружаем плагин
            plugin_module = PluginManager.load_from_file(file_path, user_id)
            plugin_class = plugin_module.Plugin
            
            plugin_name = getattr(plugin_class, 'name', file_name.replace('.py', '').replace('.txt', ''))
            
            # Сохраняем плагин
            if user_id not in user_plugins:
                user_plugins[user_id] = {}
            user_plugins[user_id][plugin_name] = plugin_module
            
            # Проверяем наличие настроек
            settings_schema = PluginManager.get_plugin_settings_schema(plugin_module)
            
            if settings_schema:
                # Создаем настройки по умолчанию
                if user_id not in plugin_settings:
                    plugin_settings[user_id] = {}
                plugin_settings[user_id][plugin_name] = {
                    s['name']: s['default'] for s in settings_schema
                }
                
                settings_text = "\n".join(
                    f"• <b>{s['name']}</b> = <code>{s['default']}</code> - {s.get('description', '')}"
                    for s in settings_schema
                )
                
                text = (
                    "✅ <b>Плагин '" + plugin_name + "' загружен!</b>\n\n"
                    "⚙️ <b>Настройки (по умолчанию):</b>\n"
                    + settings_text + "\n\n"
                    "ℹ️ Для изменения используйте кнопку настроек"
                )
            else:
                text = (
                    "✅ <b>Плагин '" + plugin_name + "' загружен!</b>\n\n"
                    "ℹ️ Плагин не имеет настраиваемых параметров"
                )
            
        except Exception as ex:
            text = (
                "❌ <b>Ошибка загрузки плагина:</b>\n"
                "<code>" + str(ex) + "</code>"
            )
        
        await safe_send(message.chat.id, text, reply_markup=get_plugins_menu_keyboard())
        await state.clear()
        return
    
    # Обработка текста (кода)
    if message.text or message.html_text:
        code = message.html_text or message.text
        
        # Проверяем что код содержит класс Plugin
        if "class Plugin" not in code:
            await safe_send(
                message.chat.id,
                "❌ Код должен содержать класс <code>Plugin</code>",
                reply_markup=get_back_keyboard("plugins")
            )
            return
        
        # Генерируем имя плагина
        plugin_name = "plugin_" + datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            plugin_module = PluginManager.load_from_code(code, user_id, plugin_name)
            plugin_class = plugin_module.Plugin
            
            # Используем имя из плагина если есть
            if hasattr(plugin_class, 'name'):
                custom_name = plugin_class.name
                old_file = PluginManager.PLUGINS_DIR / f"{user_id}_{plugin_name}.py"
                new_file = PluginManager.PLUGINS_DIR / f"{user_id}_{custom_name}.py"
                old_file.rename(new_file)
                plugin_name = custom_name
            
            if user_id not in user_plugins:
                user_plugins[user_id] = {}
            user_plugins[user_id][plugin_name] = plugin_module
            
            # Проверяем настройки
            settings_schema = PluginManager.get_plugin_settings_schema(plugin_module)
            
            if settings_schema:
                if user_id not in plugin_settings:
                    plugin_settings[user_id] = {}
                plugin_settings[user_id][plugin_name] = {
                    s['name']: s['default'] for s in settings_schema
                }
                
                settings_text = "\n".join(
                    f"• <b>{s['name']}</b> = <code>{s['default']}</code> - {s.get('description', '')}"
                    for s in settings_schema
                )
                
                text = (
                    "✅ <b>Плагин '" + plugin_name + "' загружен!</b>\n\n"
                    "⚙️ <b>Настройки (по умолчанию):</b>\n"
                    + settings_text + "\n\n"
                    "ℹ️ Для изменения используйте кнопку настроек"
                )
            else:
                text = (
                    "✅ <b>Плагин '" + plugin_name + "' загружен!</b>\n\n"
                    "ℹ️ Плагин не имеет настраиваемых параметров"
                )
            
        except Exception as ex:
            text = (
                "❌ <b>Ошибка загрузки плагина:</b>\n"
                "<code>" + str(ex) + "</code>"
            )
        
        await safe_send(message.chat.id, text, reply_markup=get_plugins_menu_keyboard())
        await state.clear()
        return
    
    # Если пришло что-то другое
    await safe_send(
        message.chat.id,
        "❌ Отправьте файл .py/.txt или код плагина",
        reply_markup=get_back_keyboard("plugins")
    )

@dp.callback_query(F.data == "my_plugins")
async def my_plugins(callback: types.CallbackQuery):
    """Список плагинов пользователя"""
    user_id = callback.from_user.id
    plugins = user_plugins.get(user_id, {})
    
    if not plugins:
        text = (
            "🔓 <b>Мои плагины</b>\n\n"
            "❌ Нет загруженных плагинов"
        )
        builder = InlineKeyboardBuilder()
        builder.button(text="◁ Назад", callback_data="plugins")
        await safe_edit(callback.message, text, reply_markup=builder.as_markup())
        await callback.answer()
        return
    
    builder = InlineKeyboardBuilder()
    for plugin_name in plugins:
        builder.button(text=plugin_name, callback_data="plugin_" + plugin_name)
    builder.button(text="◁ Назад", callback_data="plugins")
    builder.adjust(1)
    
    text = (
        "📦 <b>Мои плагины (" + str(len(plugins)) + "):</b>\n"
        "Выберите плагин для управления:"
    )
    await safe_edit(callback.message, text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("plugin_"))
async def plugin_info(callback: types.CallbackQuery):
    """Информация о плагине и управление"""
    plugin_name = callback.data.replace("plugin_", "")
    user_id = callback.from_user.id
    
    if user_id not in user_plugins or plugin_name not in user_plugins[user_id]:
        await callback.answer("Плагин не найден", show_alert=True)
        return
    
    plugin_module = user_plugins[user_id][plugin_name]
    plugin_class = plugin_module.Plugin
    settings_schema = PluginManager.get_plugin_settings_schema(plugin_module)
    current_settings = plugin_settings.get(user_id, {}).get(plugin_name, {})
    
    builder = InlineKeyboardBuilder()
    
    # Кнопки настроек для каждого параметра
    for setting in settings_schema:
        current_value = current_settings.get(setting['name'], setting['default'])
        builder.button(
            text=f"⚙ {setting['name']} = {current_value}",
            callback_data=f"pset_{plugin_name}_{setting['name']}"
        )
    
    builder.button(text="🚀 Запустить", callback_data="prun_" + plugin_name)
    builder.button(text="🗑 Удалить", callback_data="pdel_" + plugin_name)
    builder.button(text="◁ Назад", callback_data="my_plugins")
    builder.adjust(1)
    
    desc = getattr(plugin_class, 'description', 'Нет описания')
    settings_info = ""
    
    if settings_schema:
        settings_info = "\n\n⚙️ <b>Текущие настройки:</b>\n"
        for s in settings_schema:
            val = current_settings.get(s['name'], s['default'])
            settings_info += f"• <b>{s['name']}</b> = <code>{val}</code>\n"
    
    text = (
        "🧩 <b>Плагин:</b> " + plugin_name + "\n\n"
        "ℹ️ <b>Описание:</b>\n"
        "<blockquote>" + desc + "</blockquote>"
        + settings_info
    )
    
    await safe_edit(callback.message, text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("pset_"))
async def plugin_setting_prompt(callback: types.CallbackQuery, state: FSMContext):
    """Запрос на изменение настройки плагина"""
    parts = callback.data.replace("pset_", "").split("_", 1)
    plugin_name = parts[0]
    setting_name = parts[1]
    
    user_id = callback.from_user.id
    settings_schema = PluginManager.get_plugin_settings_schema(
        user_plugins[user_id][plugin_name]
    )
    
    # Находим описание настройки
    setting_info = next((s for s in settings_schema if s['name'] == setting_name), None)
    if not setting_info:
        await callback.answer("Настройка не найдена", show_alert=True)
        return
    
    current_value = plugin_settings.get(user_id, {}).get(plugin_name, {}).get(
        setting_name, 
        setting_info['default']
    )
    
    text = (
        "⚙️ <b>Настройка: " + setting_name + "</b>\n\n"
        "ℹ️ Текущее значение: <code>" + str(current_value) + "</code>\n"
        "ℹ️ Тип: <code>" + setting_info['type'] + "</code>\n"
        "ℹ️ Описание: " + setting_info.get('description', '') + "\n\n"
        "✍️ Введите новое значение:"
    )
    
    await safe_edit(callback.message, text, reply_markup=get_back_keyboard("plugin_" + plugin_name))
    await state.set_state(PluginStates.waiting_for_setting_value)
    await state.update_data(
        plugin_name=plugin_name,
        setting_name=setting_name,
        setting_type=setting_info['type']
    )
    await callback.answer()

@dp.message(PluginStates.waiting_for_setting_value)
async def plugin_setting_value(message: types.Message, state: FSMContext):
    """Обработка нового значения настройки"""
    data = await state.get_data()
    plugin_name = data.get("plugin_name")
    setting_name = data.get("setting_name")
    setting_type = data.get("setting_type", "str")
    
    user_id = message.from_user.id
    value = message.text.strip()
    
    # Конвертируем значение в нужный тип
    try:
        if setting_type == "int":
            value = int(value)
        elif setting_type == "float":
            value = float(value)
        elif setting_type == "bool":
            value = value.lower() in ("true", "1", "yes", "да")
        elif setting_type == "list":
            value = [x.strip() for x in value.split(",") if x.strip()]
        elif setting_type == "dict":
            value = json.loads(value)
        # str оставляем как есть
    except Exception as ex:
        await safe_send(
            message.chat.id,
            "❌ Ошибка преобразования: " + str(ex) + "\n"
            "Ожидаемый тип: <code>" + setting_type + "</code>",
            reply_markup=get_back_keyboard("plugin_" + plugin_name)
        )
        return
    
    # Сохраняем настройку
    if user_id not in plugin_settings:
        plugin_settings[user_id] = {}
    if plugin_name not in plugin_settings[user_id]:
        plugin_settings[user_id][plugin_name] = {}
    
    plugin_settings[user_id][plugin_name][setting_name] = value
    
    await state.clear()
    
    text = (
        "✅ <b>Настройка обновлена!</b>\n"
        "<code>" + setting_name + "</code> = <code>" + str(value) + "</code>"
    )
    await safe_send(message.chat.id, text, reply_markup=get_plugins_menu_keyboard())

@dp.callback_query(F.data.startswith("prun_"))
async def plugin_run(callback: types.CallbackQuery):
    """Запуск плагина"""
    plugin_name = callback.data.replace("prun_", "")
    user_id = callback.from_user.id
    
    if user_id not in user_plugins or plugin_name not in user_plugins[user_id]:
        await callback.answer("Плагин не найден", show_alert=True)
        return
    
    account = get_active_account(user_id)
    if not account:
        await callback.answer("Сначала выберите аккаунт!", show_alert=True)
        return
    
    phone, client = account
    plugin_module = user_plugins[user_id][plugin_name]
    plugin_class = plugin_module.Plugin
    current_settings = plugin_settings.get(user_id, {}).get(plugin_name, {})
    
    try:
        # Создаем экземпляр плагина и запускаем
        plugin_instance = plugin_class()
        
        status_msg = await safe_send(
            callback.message.chat.id,
            "🔄 <b>Запуск плагина '" + plugin_name + "'...</b>"
        )
        
        # Запускаем плагин с настройками
        if hasattr(plugin_instance, 'setup'):
            await plugin_instance.setup(client, bot, user_id)
        
        result = await plugin_instance.run(
            client=client,
            bot=bot,
            user_id=user_id,
            settings=current_settings,
            **current_settings
        )
        
        result_text = result if isinstance(result, str) else "Плагин выполнен успешно"
        
        await safe_edit(
            status_msg,
            "✅ <b>Плагин '" + plugin_name + "' выполнен!</b>\n\n"
            "<blockquote>" + str(result_text)[:500] + "</blockquote>",
            reply_markup=get_plugins_menu_keyboard()
        )
        
    except Exception as ex:
        await safe_send(
            callback.message.chat.id,
            "❌ <b>Ошибка выполнения плагина:</b>\n"
            "<code>" + str(ex) + "</code>",
            reply_markup=get_plugins_menu_keyboard()
        )
    
    await callback.answer("Плагин запущен!", show_alert=True)

@dp.callback_query(F.data.startswith("pdel_"))
async def plugin_delete(callback: types.CallbackQuery):
    """Удаление плагина"""
    plugin_name = callback.data.replace("pdel_", "")
    user_id = callback.from_user.id
    
    if user_id in user_plugins and plugin_name in user_plugins[user_id]:
        del user_plugins[user_id][plugin_name]
        
        # Удаляем настройки
        if user_id in plugin_settings and plugin_name in plugin_settings[user_id]:
            del plugin_settings[user_id][plugin_name]
        
        # Пытаемся удалить файл плагина
        plugin_file = PluginManager.PLUGINS_DIR / f"{user_id}_{plugin_name}.py"
        if plugin_file.exists():
            plugin_file.unlink()
        
        await callback.answer("Плагин удален!", show_alert=True)
    else:
        await callback.answer("Плагин не найден", show_alert=True)
    
    await my_plugins(callback)

# ============ MAIN ============

async def main():
    """Главная функция запуска бота"""
    # Создаем папки
    os.makedirs("sessions", exist_ok=True)
    PluginManager.init()
    
    # Удаляем вебхук и запускаем поллинг
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except:
        pass
    
    logger.info("Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен")
    except Exception as ex:
        logger.critical(f"Critical error: {ex}", exc_info=True)

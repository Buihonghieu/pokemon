import os
import random
import sqlite3
import asyncio
from datetime import datetime, timedelta
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

# =========================
# CAU HINH
# =========================
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    raise RuntimeError("Thieu DISCORD_TOKEN trong file .env")

DB_BASE = os.getenv("RAILWAY_VOLUME_MOUNT_PATH") or os.getenv("DB_DIR") or "/data"
DB_PATH = os.path.join(DB_BASE, "pet_game.db")
os.makedirs(DB_BASE, exist_ok=True)

print("DB_BASE =", DB_BASE)
print("DB_PATH =", DB_PATH)

conn = sqlite3.connect(DB_PATH, timeout=30)
conn.row_factory = sqlite3.Row
cur = conn.cursor()
cur.execute("PRAGMA foreign_keys = ON")
GUILD_ID = os.getenv("GUILD_ID")

ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = {int(x.strip()) for x in ADMIN_IDS_RAW.split(",") if x.strip().isdigit()}


# =========================
# DATABASE
# =========================
def column_exists(table_name: str, column_name: str) -> bool:
    cur.execute(f"PRAGMA table_info({table_name})")
    cols = cur.fetchall()
    return any(col["name"] == column_name for col in cols)


def init_db():
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            coins INTEGER NOT NULL DEFAULT 1000,
            gems INTEGER NOT NULL DEFAULT 0,
            level INTEGER NOT NULL DEFAULT 1,
            exp INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            last_daily TEXT,
            last_coinflip TEXT,
            blackjack_wins INTEGER NOT NULL DEFAULT 0,
            blackjack_losses INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pets (
            pet_id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            species TEXT NOT NULL,
            gender TEXT NOT NULL,
            rarity TEXT NOT NULL DEFAULT 'Thuong',
            element TEXT NOT NULL DEFAULT 'Thuong',
            level INTEGER NOT NULL DEFAULT 1,
            exp INTEGER NOT NULL DEFAULT 0,
            hp INTEGER NOT NULL DEFAULT 100,
            atk INTEGER NOT NULL DEFAULT 12,
            defense INTEGER NOT NULL DEFAULT 8,
            speed INTEGER NOT NULL DEFAULT 10,
            hunger INTEGER NOT NULL DEFAULT 100,
            mood INTEGER NOT NULL DEFAULT 100,
            health INTEGER NOT NULL DEFAULT 100,
            bond INTEGER NOT NULL DEFAULT 0,
            last_feed TEXT,
            last_hunt TEXT,
            last_decay TEXT,
            feed_streak INTEGER NOT NULL DEFAULT 0,
            breed_cd_until TEXT,
            mutated INTEGER NOT NULL DEFAULT 0,
            generation INTEGER NOT NULL DEFAULT 1,
            parent_a INTEGER,
            parent_b INTEGER,
            is_active INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )
        """
    )

    if not column_exists("pets", "last_decay"):
        cur.execute("ALTER TABLE pets ADD COLUMN last_decay TEXT")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS farm_plots (
            plot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id INTEGER NOT NULL,
            crop_name TEXT NOT NULL,
            planted_at TEXT NOT NULL,
            ready_at TEXT NOT NULL,
            harvested INTEGER NOT NULL DEFAULT 0,
            yield_amount INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS inventory (
            owner_id INTEGER NOT NULL,
            item_name TEXT NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (owner_id, item_name)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS battles (
            battle_id INTEGER PRIMARY KEY AUTOINCREMENT,
            attacker_id INTEGER NOT NULL,
            defender_id INTEGER NOT NULL,
            winner_id INTEGER NOT NULL,
            coins_delta INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_logs (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            target_user_id INTEGER,
            target_pet_id INTEGER,
            details TEXT,
            created_at TEXT NOT NULL
        )
        """
    )

    conn.commit()


init_db()


# =========================
# HAM HO TRO QUYEN HAN / GIAO DIEN
# =========================
def is_thien_dao(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def require_thien_dao(interaction: discord.Interaction) -> bool:
    return is_thien_dao(interaction.user.id) or interaction.user.guild_permissions.administrator


def make_embed(title: str, description: str, color: int) -> discord.Embed:
    return discord.Embed(title=title, description=description, color=color)


def send_error_embed(message: str) -> discord.Embed:
    return make_embed("❌ Thong bao", message, 0xE74C3C)


def send_success_embed(message: str) -> discord.Embed:
    return make_embed("✅ Thanh cong", message, 0x2ECC71)


def send_info_embed(message: str) -> discord.Embed:
    return make_embed("📢 Thong tin", message, 0x3498DB)


def now_str() -> str:
    return datetime.utcnow().isoformat()


def dt_from_str(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    return datetime.fromisoformat(s)


def clamp(value: int, min_value: int, max_value: int) -> int:
    return max(min_value, min(value, max_value))


def log_admin_action(
    admin_id: int,
    action: str,
    target_user_id: Optional[int] = None,
    target_pet_id: Optional[int] = None,
    details: str = "",
):
    cur.execute(
        """
        INSERT INTO admin_logs(admin_id, action, target_user_id, target_pet_id, details, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (admin_id, action, target_user_id, target_pet_id, details, now_str()),
    )
    conn.commit()


# =========================
# DU LIEU MAU / HANG SO
# =========================
SPECIES_DATA = {
    "cho": {"hp": 110, "atk": 14, "defense": 9, "speed": 9},
    "meo": {"hp": 95, "atk": 13, "defense": 8, "speed": 13},
    "tho": {"hp": 90, "atk": 10, "defense": 8, "speed": 15},
    "rong": {"hp": 120, "atk": 16, "defense": 10, "speed": 8},
    "cao": {"hp": 100, "atk": 15, "defense": 7, "speed": 12},
}

ELEMENTS = ["Lua", "Nuoc", "Cay", "Dien", "Anh Sang", "Bong Toi"]
CROPS = {
    "co": {"minutes": 5, "yield": (2, 4), "food_value": 10},
    "ngo": {"minutes": 10, "yield": (3, 6), "food_value": 18},
    "carot": {"minutes": 15, "yield": (2, 5), "food_value": 25},
}

COIN_FRAMES = ["🪙", "✨🪙", "💫🪙", "🪙💫", "✨🪙✨"]
CARD_FRAMES = ["🂠", "🂠 🂠", "🂠 🂠 🂠"]


# =========================
# PHIEN XI DACH TAM TRONG RAM
# =========================
class BlackjackSession:
    def __init__(self, user_id: int, bet: int):
        self.user_id = user_id
        self.bet = bet
        self.player = [self.draw(), self.draw()]
        self.dealer = [self.draw(), self.draw()]
        self.finished = False

    def draw(self):
        cards = [2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 10, 10, 11]
        return random.choice(cards)

    def score(self, hand):
        total = sum(hand)
        aces = hand.count(11)
        while total > 21 and aces > 0:
            total -= 10
            aces -= 1
        return total


blackjack_sessions: dict[int, BlackjackSession] = {}


# =========================
# HAM HO TRO DATABASE / THOI GIAN
# =========================
def get_user(user_id: int):
    cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    if row:
        return row
    cur.execute(
        "INSERT INTO users(user_id, created_at) VALUES(?, ?)",
        (user_id, now_str()),
    )
    conn.commit()
    cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    return cur.fetchone()


def add_item(owner_id: int, item_name: str, amount: int):
    cur.execute(
        """
        INSERT INTO inventory(owner_id, item_name, quantity)
        VALUES(?, ?, ?)
        ON CONFLICT(owner_id, item_name)
        DO UPDATE SET quantity = quantity + excluded.quantity
        """,
        (owner_id, item_name, amount),
    )
    conn.commit()


def remove_item(owner_id: int, item_name: str, amount: int) -> bool:
    cur.execute(
        "SELECT quantity FROM inventory WHERE owner_id = ? AND item_name = ?",
        (owner_id, item_name),
    )
    row = cur.fetchone()
    if not row or row["quantity"] < amount:
        return False
    cur.execute(
        "UPDATE inventory SET quantity = quantity - ? WHERE owner_id = ? AND item_name = ?",
        (amount, owner_id, item_name),
    )
    conn.commit()
    return True


def get_inventory_text(owner_id: int) -> str:
    cur.execute(
        "SELECT item_name, quantity FROM inventory WHERE owner_id = ? AND quantity > 0 ORDER BY item_name",
        (owner_id,),
    )
    rows = cur.fetchall()
    if not rows:
        return "Kho do trong."
    return "\n".join([f"- {r['item_name']}: {r['quantity']}" for r in rows])


def get_active_pet(owner_id: int):
    cur.execute(
        "SELECT * FROM pets WHERE owner_id = ? AND is_active = 1 ORDER BY pet_id LIMIT 1",
        (owner_id,),
    )
    return cur.fetchone()


def calc_pet_power(pet) -> int:
    if not pet:
        return 0
    hunger_factor = max(0.5, pet["hunger"] / 100)
    mood_factor = max(0.5, pet["mood"] / 100)
    health_factor = max(0.5, pet["health"] / 100)
    base = pet["hp"] // 5 + pet["atk"] * 3 + pet["defense"] * 2 + pet["speed"] * 2 + pet["level"] * 5
    return int(base * hunger_factor * mood_factor * health_factor)


def pet_gain_exp(pet_id: int, amount: int):
    cur.execute("SELECT level, exp, hp, atk, defense, speed FROM pets WHERE pet_id = ?", (pet_id,))
    pet = cur.fetchone()
    if not pet:
        return None

    exp = pet["exp"] + amount
    level = pet["level"]
    hp = pet["hp"]
    atk = pet["atk"]
    defense = pet["defense"]
    speed = pet["speed"]
    leveled = 0

    while exp >= level * 25:
        exp -= level * 25
        level += 1
        hp += random.randint(6, 10)
        atk += random.randint(2, 4)
        defense += random.randint(1, 3)
        speed += random.randint(1, 3)
        leveled += 1

    cur.execute(
        "UPDATE pets SET exp = ?, level = ?, hp = ?, atk = ?, defense = ?, speed = ? WHERE pet_id = ?",
        (exp, level, hp, atk, defense, speed, pet_id),
    )
    conn.commit()
    return leveled, level


def user_gain_exp(user_id: int, amount: int):
    user = get_user(user_id)
    exp = user["exp"] + amount
    level = user["level"]
    while exp >= level * 50:
        exp -= level * 50
        level += 1
    cur.execute("UPDATE users SET exp = ?, level = ? WHERE user_id = ?", (exp, level, user_id))
    conn.commit()


def decay_pet_stats(owner_id: int):
    active = get_active_pet(owner_id)
    if not active:
        return

    now = datetime.utcnow()
    last_decay = dt_from_str(active["last_decay"])
    fallback_time = dt_from_str(active["last_feed"]) or dt_from_str(active["created_at"]) or now
    base_time = last_decay or fallback_time

    hours_passed = int((now - base_time).total_seconds() // 3600)
    if hours_passed <= 0:
        return

    hunger = clamp(active["hunger"] - hours_passed * 4, 0, 100)
    mood = clamp(active["mood"] - hours_passed * 2, 0, 100)

    cur.execute(
        "UPDATE pets SET hunger = ?, mood = ?, last_decay = ? WHERE pet_id = ?",
        (hunger, mood, now_str(), active["pet_id"]),
    )
    conn.commit()


def bar(value: int, max_value: int = 100, length: int = 10, filled: str = "🟩", empty: str = "⬛") -> str:
    value = max(0, min(value, max_value))
    filled_count = round((value / max_value) * length)
    return filled * filled_count + empty * (length - filled_count)


def make_box(title: str, lines: list[str]) -> str:
    return f"╔════ {title} ════\n" + "\n".join(lines) + "\n╚═══════════════"


def rarity_icon(rarity: str) -> str:
    return {
        "Thuong": "⚪",
        "Hiem": "🔵",
        "Quy": "🟣",
        "Su Thi": "🟠",
        "Huyen Thoai": "🟡",
        "Di Bien": "✨",
    }.get(rarity, "⚪")


def species_name_vi(species: str) -> str:
    return {
        "cho": "Cho",
        "meo": "Meo",
        "tho": "Tho",
        "rong": "Rong",
        "cao": "Cao",
    }.get(species, species)


def make_pet_embed(pet) -> discord.Embed:
    power = calc_pet_power(pet)
    title_icon = "✨" if pet["mutated"] else "🐾"
    em = discord.Embed(
        title=f"{title_icon} {pet['name']} • {species_name_vi(pet['species'])}",
        description=(
            f"{rarity_icon(pet['rarity'])} **{pet['rarity']}** • "
            f"🌌 **{pet['element']}** • "
            f"🚻 **{pet['gender']}**"
        ),
        color=0x6A5ACD,
    )
    em.add_field(
        name="📊 Chi so chien dau",
        value=(
            f"❤️ HP: **{pet['hp']}**\n"
            f"⚔️ ATK: **{pet['atk']}**\n"
            f"🛡️ DEF: **{pet['defense']}**\n"
            f"💨 SPD: **{pet['speed']}**\n"
            f"🔥 Power: **{power}**"
        ),
        inline=True,
    )
    em.add_field(
        name="📈 Trang thai",
        value=(
            f"🍖 Doi: `{bar(pet['hunger'])}` **{pet['hunger']}/100**\n"
            f"😺 Tam trang: `{bar(pet['mood'])}` **{pet['mood']}/100**\n"
            f"💚 Suc khoe: `{bar(pet['health'])}` **{pet['health']}/100**\n"
            f"🤝 Than thiet: **{pet['bond']}**"
        ),
        inline=True,
    )
    em.add_field(
        name="🧬 Thong tin khac",
        value=(
            f"⭐ Cap: **{pet['level']}**\n"
            f"🍽️ Streak an: **{pet['feed_streak']}**\n"
            f"🌀 Dot bien: **{'Co' if pet['mutated'] else 'Chua'}**\n"
            f"🌱 The he: **F{pet['generation']}**"
        ),
        inline=False,
    )
    em.set_footer(text=f"Pet ID: {pet['pet_id']} • Cham dung gio se manh hon")
    return em


def make_coinflip_embed(title: str, description: str, color: int) -> discord.Embed:
    em = discord.Embed(title=title, description=description, color=color)
    em.set_footer(text="Van may la mot phan, quan ly xu moi la phan con lai")
    return em


# =========================
# DISCORD SETUP
# =========================
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)


@bot.event
async def on_ready():
    try:
        if GUILD_ID:
            guild = discord.Object(id=int(GUILD_ID))
            bot.tree.copy_global_to(guild=guild)
            await bot.tree.sync(guild=guild)
            print(f"Da sync slash commands cho guild test {GUILD_ID}")
        else:
            await bot.tree.sync()
            print("Da sync slash commands global")
    except Exception as e:
        print("Loi sync:", e)

    print(f"Bot da online: {bot.user}")


# =========================
# COMMANDS - ACCOUNT
# =========================
@bot.tree.command(name="batdau", description="Bat dau choi game pet")
async def batdau(interaction: discord.Interaction):
    user = get_user(interaction.user.id)
    em = make_embed(
        "🎮 Bat dau hanh trinh",
        f"{interaction.user.mention} da vao game.\n🪙 Xu hien tai: **{user['coins']}**\n🐾 Dung `/taopet` de tao pet dau tien.",
        0x5DADE2,
    )
    await interaction.response.send_message(embed=em, ephemeral=True)


@bot.tree.command(name="diemdanh", description="Nhan qua moi ngay")
async def diemdanh(interaction: discord.Interaction):
    user = get_user(interaction.user.id)
    now = datetime.utcnow()
    last_daily = dt_from_str(user["last_daily"])

    if last_daily and (now - last_daily) < timedelta(hours=24):
        remain = timedelta(hours=24) - (now - last_daily)
        hours, rem = divmod(int(remain.total_seconds()), 3600)
        minutes = rem // 60
        await interaction.response.send_message(
            embed=send_info_embed(f"Ban da diem danh roi. Con {hours}h {minutes}m nua."),
            ephemeral=True,
        )
        return

    reward = random.randint(800, 1600)
    cur.execute(
        "UPDATE users SET coins = coins + ?, last_daily = ? WHERE user_id = ?",
        (reward, now_str(), interaction.user.id),
    )
    conn.commit()
    user_gain_exp(interaction.user.id, 15)
    await interaction.response.send_message(
        embed=send_success_embed(f"Ban nhan duoc **{reward} xu** tu diem danh hom nay."),
        ephemeral=True,
    )


@bot.tree.command(name="hoso", description="Xem ho so cua ban")
async def hoso(interaction: discord.Interaction):
    user = get_user(interaction.user.id)
    decay_pet_stats(interaction.user.id)
    pet = get_active_pet(interaction.user.id)

    em = discord.Embed(
        title=f"👤 Ho so nguoi choi • {interaction.user.display_name}",
        description="🌟 Quan ly tai san, pet va kho do cua ban",
        color=0x2ECC71,
    )
    em.add_field(
        name="💰 Tai san",
        value=(
            f"🪙 Xu: **{user['coins']}**\n"
            f"💎 Ngoc: **{user['gems']}**\n"
            f"🎖️ Cap: **{user['level']}**\n"
            f"📚 EXP: **{user['exp']}**"
        ),
        inline=True,
    )
    em.add_field(name="🎒 Kho do", value=get_inventory_text(interaction.user.id), inline=True)
    if pet:
        em.add_field(
            name="🐾 Pet dang dung",
            value=(
                f"**{pet['name']}** ({species_name_vi(pet['species'])})\n"
                f"⭐ Lv {pet['level']} • 🔥 Power {calc_pet_power(pet)}\n"
                f"🍖 {bar(pet['hunger'])} {pet['hunger']}/100"
            ),
            inline=False,
        )
    else:
        em.add_field(name="🐾 Pet dang dung", value="Chua co pet hoat dong", inline=False)

    em.set_footer(text="Dung /danhsachpet de xem toan bo thu cung")
    await interaction.response.send_message(embed=em)


# =========================
# COMMANDS - PET
# =========================
@app_commands.choices(
    species=[
        app_commands.Choice(name="Cho", value="cho"),
        app_commands.Choice(name="Meo", value="meo"),
        app_commands.Choice(name="Tho", value="tho"),
        app_commands.Choice(name="Rong", value="rong"),
        app_commands.Choice(name="Cao", value="cao"),
    ]
)
@bot.tree.command(name="taopet", description="Tao pet dau tien")
async def taopet(interaction: discord.Interaction, name: str, species: app_commands.Choice[str]):
    get_user(interaction.user.id)
    cur.execute("SELECT COUNT(*) AS total FROM pets WHERE owner_id = ?", (interaction.user.id,))
    total = cur.fetchone()["total"]
    if total >= 9:
        await interaction.response.send_message(
            embed=send_error_embed("Ban hien chi cho tao toi da 9 pet moi nguoi."),
            ephemeral=True,
        )
        return

    sp = SPECIES_DATA[species.value]
    gender = random.choice(["Duc", "Cai"])
    element = random.choice(ELEMENTS)
    now_text = now_str()

    cur.execute(
        """
        INSERT INTO pets(
            owner_id, name, species, gender, element,
            hp, atk, defense, speed,
            hunger, mood, health, bond,
            is_active, created_at, last_feed, last_decay
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 100, 100, 100, 0, ?, ?, ?, ?)
        """,
        (
            interaction.user.id,
            name,
            species.value,
            gender,
            element,
            sp["hp"],
            sp["atk"],
            sp["defense"],
            sp["speed"],
            1 if total == 0 else 0,
            now_text,
            now_text,
            now_text,
        ),
    )
    conn.commit()

    em = discord.Embed(
        title="🎉 Tao pet thanh cong",
        description=(
            f"╔════ Thu cung moi ════\n"
            f"🐾 Ten: **{name}**\n"
            f"🧩 Loai: **{species.name}**\n"
            f"🚻 Gioi tinh: **{gender}**\n"
            f"🌌 He: **{element}**\n"
            f"╚═══════════════"
        ),
        color=0xF1C40F,
    )
    em.add_field(
        name="📌 Ghi chu",
        value="🌟 Day la pet dang hoat dong cua ban." if total == 0 else "Dung `/chonpet` de chon pet nay.",
        inline=False,
    )
    await interaction.response.send_message(embed=em)


@bot.tree.command(name="danhsachpet", description="Xem danh sach pet")
async def danhsachpet(interaction: discord.Interaction):
    decay_pet_stats(interaction.user.id)
    cur.execute("SELECT * FROM pets WHERE owner_id = ? ORDER BY pet_id", (interaction.user.id,))
    pets = cur.fetchall()
    if not pets:
        await interaction.response.send_message(
            embed=send_info_embed("Ban chua co pet nao. Dung `/taopet` de tao."),
            ephemeral=True,
        )
        return

    em = discord.Embed(title=f"🐾 Pet cua {interaction.user.display_name}", color=0x3498DB)
    for p in pets:
        active_mark = "🌟" if p["is_active"] else ""
        em.add_field(
            name=f"#{p['pet_id']} {active_mark} {p['name']}",
            value=(
                f"Loai: {species_name_vi(p['species'])} | Lv {p['level']}\n"
                f"Power: {calc_pet_power(p)}\n"
                f"Doi: {p['hunger']}/100 | Mood: {p['mood']}/100"
            ),
            inline=False,
        )
    await interaction.response.send_message(embed=em)


@bot.tree.command(name="xempet", description="Xem chi tiet pet")
async def xempet(interaction: discord.Interaction, pet_id: int):
    decay_pet_stats(interaction.user.id)
    cur.execute("SELECT * FROM pets WHERE owner_id = ? AND pet_id = ?", (interaction.user.id, pet_id))
    pet = cur.fetchone()
    if not pet:
        await interaction.response.send_message(embed=send_error_embed("Khong tim thay pet nay."), ephemeral=True)
        return
    await interaction.response.send_message(embed=make_pet_embed(pet))


@bot.tree.command(name="chonpet", description="Dat pet dang su dung")
async def chonpet(interaction: discord.Interaction, pet_id: int):
    cur.execute("SELECT * FROM pets WHERE owner_id = ? AND pet_id = ?", (interaction.user.id, pet_id))
    pet = cur.fetchone()
    if not pet:
        await interaction.response.send_message(embed=send_error_embed("Khong tim thay pet do."), ephemeral=True)
        return

    cur.execute("UPDATE pets SET is_active = 0 WHERE owner_id = ?", (interaction.user.id,))
    cur.execute("UPDATE pets SET is_active = 1 WHERE pet_id = ?", (pet_id,))
    conn.commit()
    await interaction.response.send_message(embed=send_success_embed(f"Da chon **{pet['name']}** lam pet hoat dong."))


@app_commands.choices(
    food=[
        app_commands.Choice(name="Co", value="co"),
        app_commands.Choice(name="Ngo", value="ngo"),
        app_commands.Choice(name="Ca rot", value="carot"),
    ]
)
@bot.tree.command(name="chopetan", description="Cho pet dang hoat dong an")
async def chopetan(interaction: discord.Interaction, food: app_commands.Choice[str]):
    decay_pet_stats(interaction.user.id)
    pet = get_active_pet(interaction.user.id)
    if not pet:
        await interaction.response.send_message(embed=send_error_embed("Ban chua co pet dang hoat dong."), ephemeral=True)
        return

    crop = CROPS[food.value]
    if not remove_item(interaction.user.id, food.value, 1):
        await interaction.response.send_message(embed=send_error_embed(f"Ban khong co **{food.name}** trong kho."), ephemeral=True)
        return

    now = datetime.utcnow()
    last_feed = dt_from_str(pet["last_feed"])
    good_window = False
    if last_feed:
        hours = (now - last_feed).total_seconds() / 3600
        if 6 <= hours <= 10:
            good_window = True

    hunger = min(100, pet["hunger"] + crop["food_value"])
    mood = min(100, pet["mood"] + random.randint(4, 8))
    bond = pet["bond"] + (3 if good_window else 1)
    streak = pet["feed_streak"] + 1 if good_window else max(0, pet["feed_streak"] - 1)

    cur.execute(
        "UPDATE pets SET hunger = ?, mood = ?, bond = ?, feed_streak = ?, last_feed = ?, last_decay = ? WHERE pet_id = ?",
        (hunger, mood, bond, streak, now_str(), now_str(), pet["pet_id"]),
    )
    conn.commit()
    leveled = pet_gain_exp(pet["pet_id"], 8 if good_window else 4)

    bonus = "⏰ Cho an dung nhip, pet rat vui!" if good_window else "🍽️ Pet da duoc an no hon."
    em = discord.Embed(
        title="🍽️ Cho an thanh cong",
        description=(
            f"╔════ Bua an pet ════\n"
            f"🐾 Pet: **{pet['name']}**\n"
            f"🥕 Thuc an: **{food.name}**\n"
            f"🔥 Streak: **{streak}**\n"
            f"╚═══════════════"
        ),
        color=0x58D68D,
    )
    em.add_field(name="📢 Thong bao", value=bonus, inline=False)
    if leveled and leveled[0] > 0:
        em.add_field(name="🎉 Thang cap", value=f"Pet len **{leveled[0]}** cap • Hien tai: **Lv {leveled[1]}**", inline=False)
    await interaction.response.send_message(embed=em)


# =========================
# COMMANDS - FARM
# =========================
@app_commands.choices(
    crop=[
        app_commands.Choice(name="Co", value="co"),
        app_commands.Choice(name="Ngo", value="ngo"),
        app_commands.Choice(name="Ca rot", value="carot"),
    ]
)
@bot.tree.command(name="trongcay", description="Trong luong thuc")
async def trongcay(interaction: discord.Interaction, crop: app_commands.Choice[str]):
    get_user(interaction.user.id)
    cfg = CROPS[crop.value]
    now = datetime.utcnow()
    ready = now + timedelta(minutes=cfg["minutes"])
    yield_amount = random.randint(*cfg["yield"])

    cur.execute(
        "SELECT COUNT(*) AS total FROM farm_plots WHERE owner_id = ? AND harvested = 0",
        (interaction.user.id,),
    )
    total = cur.fetchone()["total"]
    if total >= 5:
        await interaction.response.send_message(embed=send_error_embed("Toi da 5 o dang trong cung luc trong ban MVP."), ephemeral=True)
        return

    cur.execute(
        "INSERT INTO farm_plots(owner_id, crop_name, planted_at, ready_at, yield_amount) VALUES (?, ?, ?, ?, ?)",
        (interaction.user.id, crop.value, now_str(), ready.isoformat(), yield_amount),
    )
    conn.commit()
    await interaction.response.send_message(embed=send_success_embed(f"Da trong **{crop.name}**. Se thu hoach sau **{cfg['minutes']} phut**."))


@bot.tree.command(name="nongtrai", description="Xem cac o trong")
async def nongtrai(interaction: discord.Interaction):
    cur.execute(
        "SELECT * FROM farm_plots WHERE owner_id = ? AND harvested = 0 ORDER BY plot_id",
        (interaction.user.id,),
    )
    rows = cur.fetchall()
    if not rows:
        await interaction.response.send_message(embed=send_info_embed("Ban chua trong gi ca."), ephemeral=True)
        return

    now = datetime.utcnow()
    lines = []
    for r in rows:
        ready_at = dt_from_str(r["ready_at"])
        if ready_at <= now:
            status = "✅ San sang"
        else:
            remain = ready_at - now
            mins = int(remain.total_seconds() // 60) + 1
            status = f"⏳ Con khoang {mins} phut"
        lines.append(f"O #{r['plot_id']} - {r['crop_name']} - {status} - san luong {r['yield_amount']}")
    await interaction.response.send_message(embed=send_info_embed("\n".join(lines)))


@bot.tree.command(name="thunong", description="Thu hoach 1 o hoac tat ca")
async def thunong(interaction: discord.Interaction, plot_id: Optional[int] = None):
    now = datetime.utcnow()
    if plot_id is None:
        cur.execute("SELECT * FROM farm_plots WHERE owner_id = ? AND harvested = 0", (interaction.user.id,))
        rows = cur.fetchall()
    else:
        cur.execute(
            "SELECT * FROM farm_plots WHERE owner_id = ? AND harvested = 0 AND plot_id = ?",
            (interaction.user.id, plot_id),
        )
        rows = cur.fetchall()

    if not rows:
        await interaction.response.send_message(embed=send_error_embed("Khong co o nao hop le de thu hoach."), ephemeral=True)
        return

    harvested_lines = []
    total_items = 0
    for r in rows:
        ready_at = dt_from_str(r["ready_at"])
        if ready_at and ready_at <= now:
            add_item(interaction.user.id, r["crop_name"], r["yield_amount"])
            cur.execute("UPDATE farm_plots SET harvested = 1 WHERE plot_id = ?", (r["plot_id"],))
            harvested_lines.append(f"- O #{r['plot_id']}: +{r['yield_amount']} {r['crop_name']}")
            total_items += r["yield_amount"]

    conn.commit()
    if not harvested_lines:
        await interaction.response.send_message(embed=send_info_embed("Chua co o nao chin de thu hoach."), ephemeral=True)
        return

    user_gain_exp(interaction.user.id, 5)
    em = discord.Embed(
        title="🌾 Thu hoach thanh cong",
        description=make_box("Nong trai", harvested_lines),
        color=0x27AE60,
    )
    em.add_field(name="📦 Tong vat pham", value=f"**{total_items}**", inline=False)
    await interaction.response.send_message(embed=em)


# =========================
# COMMANDS - HUNT / PK
# =========================
@bot.tree.command(name="disan", description="Dung pet dang hoat dong di san tien")
async def disan(interaction: discord.Interaction):
    decay_pet_stats(interaction.user.id)
    pet = get_active_pet(interaction.user.id)
    if not pet:
        await interaction.response.send_message(embed=send_error_embed("Ban chua co pet dang hoat dong."), ephemeral=True)
        return

    last_hunt = dt_from_str(pet["last_hunt"])
    now = datetime.utcnow()
    if last_hunt and (now - last_hunt) < timedelta(minutes=15):
        remain = timedelta(minutes=15) - (now - last_hunt)
        mins = int(remain.total_seconds() // 60) + 1
        await interaction.response.send_message(embed=send_info_embed(f"Pet dang met, doi khoang {mins} phut nua."), ephemeral=True)
        return

    power = calc_pet_power(pet)
    success_rate = min(90, 45 + power // 25)
    roll = random.randint(1, 100)

    hunger = max(0, pet["hunger"] - random.randint(6, 12))
    mood = max(0, pet["mood"] - random.randint(3, 8))
    cur.execute(
        "UPDATE pets SET hunger = ?, mood = ?, last_hunt = ?, last_decay = ? WHERE pet_id = ?",
        (hunger, mood, now_str(), now_str(), pet["pet_id"]),
    )

    if roll <= success_rate:
        coins = random.randint(80, 180) + power // 8
        extra = ""
        if random.randint(1, 100) <= 20:
            item = random.choice(["co", "ngo", "carot"])
            add_item(interaction.user.id, item, 1)
            extra = f"1 {item}"
        cur.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (coins, interaction.user.id))
        pet_gain_exp(pet["pet_id"], 10)
        user_gain_exp(interaction.user.id, 8)
        conn.commit()
        em = discord.Embed(
            title="🏹 Chuyen san thanh cong",
            description=(
                f"╔════ Ket qua san ════\n"
                f"🐾 Pet: **{pet['name']}**\n"
                f"💰 Xu kiem duoc: **{coins}**\n"
                f"🔥 Power hien tai: **{power}**\n"
                f"╚═══════════════"
            ),
            color=0x3498DB,
        )
        if extra:
            em.add_field(name="🎁 Nhat them", value=extra, inline=False)
        await interaction.response.send_message(embed=em)
    else:
        loss = random.randint(20, 60)
        cur.execute("UPDATE users SET coins = MAX(0, coins - ?) WHERE user_id = ?", (loss, interaction.user.id))
        conn.commit()
        em = discord.Embed(
            title="💥 Chuyen san that bai",
            description=(
                f"╔════ Ket qua san ════\n"
                f"🐾 Pet: **{pet['name']}**\n"
                f"💸 Lam roi: **{loss} xu**\n"
                f"😿 Hay cho pet nghi ngoi roi thu lai\n"
                f"╚═══════════════"
            ),
            color=0xE74C3C,
        )
        await interaction.response.send_message(embed=em)


@bot.tree.command(name="thachdau", description="Thach dau nguoi khac bang pet dang hoat dong")
async def thachdau(interaction: discord.Interaction, user: discord.Member):
    if user.bot:
        await interaction.response.send_message(embed=send_error_embed("Khong the PK voi bot."), ephemeral=True)
        return
    if user.id == interaction.user.id:
        await interaction.response.send_message(embed=send_error_embed("Khong the PK voi chinh minh."), ephemeral=True)
        return

    decay_pet_stats(interaction.user.id)
    decay_pet_stats(user.id)
    my_pet = get_active_pet(interaction.user.id)
    enemy_pet = get_active_pet(user.id)
    if not my_pet:
        await interaction.response.send_message(embed=send_error_embed("Ban chua co pet dang hoat dong."), ephemeral=True)
        return
    if not enemy_pet:
        await interaction.response.send_message(embed=send_error_embed("Doi thu chua co pet dang hoat dong."), ephemeral=True)
        return

    my_power = calc_pet_power(my_pet) + random.randint(-15, 20)
    enemy_power = calc_pet_power(enemy_pet) + random.randint(-15, 20)
    my_user = get_user(interaction.user.id)
    enemy_user = get_user(user.id)

    stake = min(200, my_user["coins"], enemy_user["coins"])
    if stake <= 0:
        await interaction.response.send_message(embed=send_error_embed("Mot trong hai nguoi khong du xu de PK."), ephemeral=True)
        return

    if my_power >= enemy_power:
        winner_id = interaction.user.id
        winner_name = interaction.user.display_name
        winner_pet = my_pet["name"]
        cur.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (stake, interaction.user.id))
        cur.execute("UPDATE users SET coins = MAX(0, coins - ?) WHERE user_id = ?", (stake, user.id))
        pet_gain_exp(my_pet["pet_id"], 15)
    else:
        winner_id = user.id
        winner_name = user.display_name
        winner_pet = enemy_pet["name"]
        cur.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (stake, user.id))
        cur.execute("UPDATE users SET coins = MAX(0, coins - ?) WHERE user_id = ?", (stake, interaction.user.id))
        pet_gain_exp(enemy_pet["pet_id"], 15)

    cur.execute(
        "INSERT INTO battles(attacker_id, defender_id, winner_id, coins_delta, created_at) VALUES (?, ?, ?, ?, ?)",
        (interaction.user.id, user.id, winner_id, stake, now_str()),
    )
    conn.commit()

    em = discord.Embed(
        title="⚔️ Ket qua PK",
        description=(
            f"╔════ Dau truong ════\n"
            f"🟥 {interaction.user.display_name} • {my_pet['name']} • **{my_power}** power\n"
            f"🟦 {user.display_name} • {enemy_pet['name']} • **{enemy_power}** power\n"
            f"╚═══════════════"
        ),
        color=0x9B59B6,
    )
    em.add_field(name="🏆 Nguoi thang", value=f"**{winner_name}** cung pet **{winner_pet}**", inline=False)
    em.add_field(name="💰 Phan thuong", value=f"**{stake} xu**", inline=False)
    await interaction.response.send_message(embed=em)


# =========================
# COMMANDS - BREED (MVP BASIC)
# =========================
@bot.tree.command(name="phoigiong", description="Phoi giong 2 pet cua ban")
async def phoigiong(interaction: discord.Interaction, pet_a_id: int, pet_b_id: int, child_name: str):
    cur.execute("SELECT * FROM pets WHERE owner_id = ? AND pet_id = ?", (interaction.user.id, pet_a_id))
    pet_a = cur.fetchone()
    cur.execute("SELECT * FROM pets WHERE owner_id = ? AND pet_id = ?", (interaction.user.id, pet_b_id))
    pet_b = cur.fetchone()

    if not pet_a or not pet_b:
        await interaction.response.send_message(embed=send_error_embed("Khong tim thay mot trong hai pet."), ephemeral=True)
        return
    if pet_a_id == pet_b_id:
        await interaction.response.send_message(embed=send_error_embed("Phai chon 2 pet khac nhau."), ephemeral=True)
        return
    if pet_a["gender"] == pet_b["gender"]:
        await interaction.response.send_message(embed=send_error_embed("Hai pet phai khac gioi tinh."), ephemeral=True)
        return
    if pet_a["level"] < 5 or pet_b["level"] < 5:
        await interaction.response.send_message(embed=send_error_embed("Ca hai pet phai dat cap 5 tro len moi phoi giong."), ephemeral=True)
        return

    cd_a = dt_from_str(pet_a["breed_cd_until"])
    cd_b = dt_from_str(pet_b["breed_cd_until"])
    now = datetime.utcnow()
    if (cd_a and cd_a > now) or (cd_b and cd_b > now):
        await interaction.response.send_message(embed=send_info_embed("Mot trong hai pet van dang cooldown phoi giong."), ephemeral=True)
        return

    user = get_user(interaction.user.id)
    cost = 500
    if user["coins"] < cost:
        await interaction.response.send_message(embed=send_error_embed("Ban khong du 500 xu de phoi giong."), ephemeral=True)
        return

    mutation_rate = 5
    if pet_a["feed_streak"] >= 3:
        mutation_rate += 2
    if pet_b["feed_streak"] >= 3:
        mutation_rate += 2

    mutated = 1 if random.randint(1, 100) <= mutation_rate else 0
    rarity = "Di Bien" if mutated else random.choice(["Thuong", "Hiem", "Quy"])
    species = random.choice([pet_a["species"], pet_b["species"]])
    element = random.choice([pet_a["element"], pet_b["element"]])
    gender = random.choice(["Duc", "Cai"])

    base = SPECIES_DATA.get(species, SPECIES_DATA["cho"])
    hp = int((pet_a["hp"] + pet_b["hp"] + base["hp"]) / 3) + random.randint(-5, 8)
    atk = int((pet_a["atk"] + pet_b["atk"] + base["atk"]) / 3) + random.randint(-2, 3)
    defense = int((pet_a["defense"] + pet_b["defense"] + base["defense"]) / 3) + random.randint(-2, 3)
    speed = int((pet_a["speed"] + pet_b["speed"] + base["speed"]) / 3) + random.randint(-2, 3)

    hp = max(50, hp)
    atk = max(5, atk)
    defense = max(5, defense)
    speed = max(5, speed)

    cur.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (cost, interaction.user.id))
    cd_until = (now + timedelta(hours=12)).isoformat()
    cur.execute("UPDATE pets SET breed_cd_until = ? WHERE pet_id IN (?, ?)", (cd_until, pet_a_id, pet_b_id))

    child_now = now_str()
    cur.execute(
        """
        INSERT INTO pets(
            owner_id, name, species, gender, rarity, element,
            level, exp, hp, atk, defense, speed,
            hunger, mood, health, bond,
            last_feed, last_decay,
            feed_streak, breed_cd_until, mutated, generation,
            parent_a, parent_b, is_active, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            interaction.user.id,
            child_name,
            species,
            gender,
            rarity,
            element,
            1,
            0,
            hp,
            atk,
            defense,
            speed,
            100,
            100,
            100,
            0,
            child_now,
            child_now,
            0,
            None,
            mutated,
            max(pet_a["generation"], pet_b["generation"]) + 1,
            pet_a_id,
            pet_b_id,
            0,
            child_now,
        ),
    )
    conn.commit()

    em = discord.Embed(
        title="🧬 Phoi giong thanh cong",
        description=(
            f"╔════ Pet con moi ════\n"
            f"🐣 Ten: **{child_name}**\n"
            f"🧩 Loai: **{species_name_vi(species)}**\n"
            f"🚻 Gioi tinh: **{gender}**\n"
            f"🌌 He: **{element}**\n"
            f"{rarity_icon(rarity)} Pham chat: **{rarity}**\n"
            f"💸 Phi phoi: **{cost} xu**\n"
            f"╚═══════════════"
        ),
        color=0xFF66CC if mutated else 0xAF7AC5,
    )
    em.add_field(name="✨ Ket qua di bien", value="Pet con da dot bien!" if mutated else "Pet con sinh ra binh thuong.", inline=False)
    await interaction.response.send_message(embed=em)


# =========================
# COMMANDS - CASINO
# =========================
@app_commands.choices(
    side=[
        app_commands.Choice(name="Ngua", value="ngua"),
        app_commands.Choice(name="Sap", value="sap"),
    ]
)
@bot.tree.command(name="upngua", description="Tung dong xu an xu")
async def upngua(interaction: discord.Interaction, bet: int, side: app_commands.Choice[str]):
    user = get_user(interaction.user.id)
    if bet <= 0:
        await interaction.response.send_message(embed=send_error_embed("Tien cuoc phai > 0."), ephemeral=True)
        return
    if user["coins"] < bet:
        await interaction.response.send_message(embed=send_error_embed("Ban khong du xu."), ephemeral=True)
        return

    cur.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (bet, interaction.user.id))
    conn.commit()

    await interaction.response.defer()
    for i, frame in enumerate(COIN_FRAMES[:4], start=1):
        em = make_coinflip_embed(
            "🪙 Dang tung dong xu",
            f"{frame}\n\nBan chon: **{side.name}**\nTien cuoc: **{bet} xu**\n\nLan xoay thu **{i}**...",
            0xF1C40F,
        )
        await interaction.edit_original_response(embed=em)
        await asyncio.sleep(0.45)

    result = random.choice(["ngua", "sap"])
    result_name = "Ngua" if result == "ngua" else "Sap"

    if result == side.value:
        profit = int(bet * 0.95)
        total_return = bet + profit
        cur.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (total_return, interaction.user.id))
        conn.commit()
        em = make_coinflip_embed(
            "🎉 Up ngua chien thang",
            f"{COIN_FRAMES[-1]}\n\nRa mat: **{result_name}**\nBan thang rong: **{profit} xu**\nTien hoan ca cuoc: **{total_return} xu**",
            0x2ECC71,
        )
        await interaction.edit_original_response(embed=em)
    else:
        em = make_coinflip_embed(
            "💸 Up ngua that bai",
            f"{COIN_FRAMES[-1]}\n\nRa mat: **{result_name}**\nBan mat: **{bet} xu**",
            0xE74C3C,
        )
        await interaction.edit_original_response(embed=em)


class BlackjackView(discord.ui.View):
    def __init__(self, owner_id: int):
        super().__init__(timeout=120)
        self.owner_id = owner_id

    def make_blackjack_embed(self, session: BlackjackSession, reveal_dealer: bool = False, result_text: Optional[str] = None, animation_text: Optional[str] = None) -> discord.Embed:
        player_score = session.score(session.player)
        dealer_score = session.score(session.dealer)
        dealer_cards = str(session.dealer) if reveal_dealer else f"[{session.dealer[0]}, ?]"
        dealer_value = f"**{dealer_score}**" if reveal_dealer else "?"

        em = discord.Embed(
            title="🃏 Ban Xi Dach",
            description="Khung giai tri do den dung xu trong game.",
            color=0x1ABC9C,
        )
        if animation_text:
            em.add_field(name="🎬 Hoat canh", value=animation_text, inline=False)
        em.add_field(name="🎴 Bai cua ban", value=f"{session.player}\nTong diem: **{player_score}**", inline=True)
        em.add_field(name="🏦 Bai nha cai", value=f"{dealer_cards}\nTong diem: {dealer_value}", inline=True)
        em.add_field(name="💰 Tien cuoc", value=f"**{session.bet} xu**", inline=False)
        if result_text:
            em.add_field(name="📢 Ket qua", value=result_text, inline=False)
        em.set_footer(text="Rut de lay them bai • Dan de so diem voi nha cai")
        return em

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Day khong phai ban xi dach cua ban.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Rut", style=discord.ButtonStyle.primary)
    async def hit(self, interaction: discord.Interaction, button: discord.ui.Button):
        session = blackjack_sessions.get(self.owner_id)
        if not session or session.finished:
            await interaction.response.send_message("Van nay da ket thuc.", ephemeral=True)
            return

        await interaction.response.defer()
        await interaction.message.edit(
            embed=self.make_blackjack_embed(session, reveal_dealer=False, result_text="Ban dang rut them 1 la bai...", animation_text="🂠 ➜ ✨"),
            view=self,
        )
        await asyncio.sleep(0.55)

        session.player.append(session.draw())
        player_score = session.score(session.player)

        if player_score > 21:
            session.finished = True
            cur.execute(
                "UPDATE users SET blackjack_losses = blackjack_losses + 1 WHERE user_id = ?",
                (self.owner_id,),
            )
            conn.commit()
            self.disable_all_items()
            await interaction.message.edit(
                embed=self.make_blackjack_embed(session, reveal_dealer=True, result_text=f"💥 Quac! Ban thua **{session.bet} xu**.", animation_text="🂠 ... Boom!"),
                view=self,
            )
            blackjack_sessions.pop(self.owner_id, None)
            return

        await interaction.message.edit(
            embed=self.make_blackjack_embed(session, reveal_dealer=False, result_text="Ban van co the rut hoac dan.", animation_text="✨ Them 1 la bai moi vua duoc chia"),
            view=self,
        )

    @discord.ui.button(label="Dan", style=discord.ButtonStyle.success)
    async def stand(self, interaction: discord.Interaction, button: discord.ui.Button):
        session = blackjack_sessions.get(self.owner_id)
        if not session or session.finished:
            await interaction.response.send_message("Van nay da ket thuc.", ephemeral=True)
            return

        await interaction.response.defer()
        session.finished = True

        await interaction.message.edit(
            embed=self.make_blackjack_embed(session, reveal_dealer=True, result_text="Nha cai dang mo bai...", animation_text="🂠🂠 ➜ 🃏"),
            view=self,
        )
        await asyncio.sleep(0.7)

        while session.score(session.dealer) < 17:
            session.dealer.append(session.draw())
            await interaction.message.edit(
                embed=self.make_blackjack_embed(session, reveal_dealer=True, result_text="Nha cai dang rut them bai...", animation_text="🏦 Rut them 1 la bai"),
                view=self,
            )
            await asyncio.sleep(0.55)

        player_score = session.score(session.player)
        dealer_score = session.score(session.dealer)

        if dealer_score > 21 or player_score > dealer_score:
            total_return = session.bet * 2
            cur.execute(
                "UPDATE users SET coins = coins + ?, blackjack_wins = blackjack_wins + 1 WHERE user_id = ?",
                (total_return, self.owner_id),
            )
            result_text = f"🎉 Ban thang! Hoan cuoc va tien thuong: **{total_return} xu**."
        elif player_score == dealer_score:
            cur.execute(
                "UPDATE users SET coins = coins + ? WHERE user_id = ?",
                (session.bet, self.owner_id),
            )
            result_text = f"🤝 Hoa, nha cai hoan lai **{session.bet} xu** cho ban."
        else:
            cur.execute(
                "UPDATE users SET blackjack_losses = blackjack_losses + 1 WHERE user_id = ?",
                (self.owner_id,),
            )
            result_text = f"💸 Ban thua **{session.bet} xu**."

        conn.commit()
        self.disable_all_items()
        await interaction.message.edit(
            embed=self.make_blackjack_embed(session, reveal_dealer=True, result_text=result_text, animation_text="🎬 Van bai da ha man"),
            view=self,
        )
        blackjack_sessions.pop(self.owner_id, None)

    async def on_timeout(self):
        session = blackjack_sessions.get(self.owner_id)
        if session and not session.finished:
            blackjack_sessions.pop(self.owner_id, None)
        self.disable_all_items()


@bot.tree.command(name="xidach", description="Choi xi dach")
async def xidach(interaction: discord.Interaction, bet: int):
    user = get_user(interaction.user.id)
    if bet <= 0:
        await interaction.response.send_message(embed=send_error_embed("Tien cuoc phai > 0."), ephemeral=True)
        return
    if user["coins"] < bet:
        await interaction.response.send_message(embed=send_error_embed("Ban khong du xu."), ephemeral=True)
        return
    if interaction.user.id in blackjack_sessions:
        await interaction.response.send_message(embed=send_error_embed("Ban dang co 1 van xi dach chua ket thuc."), ephemeral=True)
        return

    cur.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (bet, interaction.user.id))
    conn.commit()

    session = BlackjackSession(interaction.user.id, bet)
    blackjack_sessions[interaction.user.id] = session

    await interaction.response.defer(ephemeral=True)
    for frame in CARD_FRAMES:
        em = discord.Embed(title="🃏 Dang chia bai", description="Ban bai dang duoc sap xep...", color=0x16A085)
        em.add_field(name="🎬 Hoat canh", value=f"{frame}\nNha cai dang chia bai...", inline=False)
        em.add_field(name="💰 Tien cuoc", value=f"**{bet} xu**", inline=False)
        await interaction.edit_original_response(embed=em, view=None)
        await asyncio.sleep(0.5)

    player_score = session.score(session.player)

    if player_score == 21:
        total_return = int(bet * 2.5)
        cur.execute(
            "UPDATE users SET coins = coins + ?, blackjack_wins = blackjack_wins + 1 WHERE user_id = ?",
            (total_return, interaction.user.id),
        )
        conn.commit()
        em = discord.Embed(title="🃏 Blackjack!", description="Ban rut duoc 21 ngay tu dau.", color=0x16A085)
        em.add_field(name="🎴 Bai cua ban", value=str(session.player), inline=False)
        em.add_field(name="💰 Tien nhan", value=f"**{total_return} xu**", inline=False)
        await interaction.edit_original_response(embed=em, view=None)
        blackjack_sessions.pop(interaction.user.id, None)
        return

    view = BlackjackView(interaction.user.id)
    await interaction.edit_original_response(
        embed=view.make_blackjack_embed(session, reveal_dealer=False, result_text="Chon rut hoac dan.", animation_text="✨ Bai da chia xong"),
        view=view,
    )


# =========================
# COMMANDS - LEADERBOARD
# =========================
@bot.tree.command(name="topxu", description="Top nguoi choi giau nhat")
async def topxu(interaction: discord.Interaction):
    cur.execute("SELECT user_id, coins, level FROM users ORDER BY coins DESC LIMIT 10")
    rows = cur.fetchall()
    if not rows:
        await interaction.response.send_message(embed=send_info_embed("Chua co du lieu."), ephemeral=True)
        return

    medals = ["🥇", "🥈", "🥉"]
    em = discord.Embed(title="🏆 Bang Xep Hang Xu", description="Top 10 nguoi choi giau nhat hien tai", color=0xF1C40F)
    text_lines = []
    for i, row in enumerate(rows, start=1):
        user = interaction.guild.get_member(row["user_id"]) if interaction.guild else None
        name = user.display_name if user else f"User {row['user_id']}"
        medal = medals[i - 1] if i <= 3 else f"`#{i}`"
        text_lines.append(f"{medal} **{name}** • 🪙 {row['coins']} • 🎖️ Lv {row['level']}")
    em.add_field(name="📜 BXH", value="\n".join(text_lines), inline=False)
    em.set_footer(text="Choi cham de leo top va nhan thuong")
    await interaction.response.send_message(embed=em)


@bot.tree.command(name="toppet", description="Top pet manh nhat")
async def toppet(interaction: discord.Interaction):
    cur.execute("SELECT * FROM pets ORDER BY level DESC, atk DESC, hp DESC LIMIT 10")
    rows = cur.fetchall()
    if not rows:
        await interaction.response.send_message(embed=send_info_embed("Chua co pet nao."), ephemeral=True)
        return

    medals = ["🥇", "🥈", "🥉"]
    em = discord.Embed(title="🐾 Bang Xep Hang Pet", description="Top 10 pet manh nhat may chu", color=0x9B59B6)
    text_lines = []
    for i, pet in enumerate(rows, start=1):
        user = interaction.guild.get_member(pet["owner_id"]) if interaction.guild else None
        owner_name = user.display_name if user else f"User {pet['owner_id']}"
        medal = medals[i - 1] if i <= 3 else f"`#{i}`"
        text_lines.append(
            f"{medal} **{pet['name']}** ({species_name_vi(pet['species'])}) • ⭐ Lv {pet['level']} • 🔥 {calc_pet_power(pet)} • Chu: **{owner_name}**"
        )
    em.add_field(name="📜 BXH", value="\n".join(text_lines), inline=False)
    em.set_footer(text="Pet khoe, cham dung gio va lai tao tot se leo top nhanh")
    await interaction.response.send_message(embed=em)


# =========================
# THIEN DAO / ADMIN
# =========================
@bot.tree.command(name="thiendaohelp", description="Xem cac lenh Thien Dao")
async def thiendaohelp(interaction: discord.Interaction):
    if not require_thien_dao(interaction):
        await interaction.response.send_message(embed=send_error_embed("Ban khong co quyen Thien Dao."), ephemeral=True)
        return

    em = make_embed(
        "👑 Bang lenh Thien Dao",
        "Cac lenh quan tri va ban phat quyen nang trong game.",
        0xF4D03F,
    )
    em.add_field(name="🪙 Kinh te", value="`/choxu`\n`/setxu`", inline=True)
    em.add_field(name="🐾 Pet", value="`/suapet`\n`/hoipet`", inline=True)
    em.add_field(name="🎁 Vat pham", value="`/chovatpham`", inline=True)
    em.add_field(name="📚 Nhat ky", value="`/nhatkythiendao`", inline=True)
    em.add_field(name="📢 Khac", value="`/thiendaohelp`", inline=False)
    await interaction.response.send_message(embed=em, ephemeral=True)


@bot.tree.command(name="choxu", description="[Thien Dao] Ban phat xu cho nguoi choi")
async def choxu(interaction: discord.Interaction, user: discord.Member, coins: int):
    if not require_thien_dao(interaction):
        await interaction.response.send_message(embed=send_error_embed("Ban khong co quyen Thien Dao."), ephemeral=True)
        return
    if coins <= 0:
        await interaction.response.send_message(embed=send_error_embed("So xu phai lon hon 0."), ephemeral=True)
        return
    get_user(user.id)
    cur.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (coins, user.id))
    conn.commit()
    log_admin_action(interaction.user.id, "give_coin", target_user_id=user.id, details=f"+{coins} coins")
    em = make_embed("👑 Thien Dao ban phat", f"Da cong **{coins} xu** cho {user.mention}.", 0xF1C40F)
    await interaction.response.send_message(embed=em)


@bot.tree.command(name="setxu", description="[Thien Dao] Dat so xu cho nguoi choi")
async def setxu(interaction: discord.Interaction, user: discord.Member, coins: int):
    if not require_thien_dao(interaction):
        await interaction.response.send_message(embed=send_error_embed("Ban khong co quyen Thien Dao."), ephemeral=True)
        return
    if coins < 0:
        await interaction.response.send_message(embed=send_error_embed("So xu khong duoc am."), ephemeral=True)
        return
    get_user(user.id)
    cur.execute("UPDATE users SET coins = ? WHERE user_id = ?", (coins, user.id))
    conn.commit()
    log_admin_action(interaction.user.id, "set_coin", target_user_id=user.id, details=f"set to {coins}")
    em = make_embed("⚖️ Thien Dao dinh menh", f"Da dat xu cua {user.mention} thanh **{coins}**.", 0xF39C12)
    await interaction.response.send_message(embed=em)


@bot.tree.command(name="chovatpham", description="[Thien Dao] Ban phat vat pham")
async def chovatpham(interaction: discord.Interaction, user: discord.Member, item_name: str, amount: int):
    if not require_thien_dao(interaction):
        await interaction.response.send_message(embed=send_error_embed("Ban khong co quyen Thien Dao."), ephemeral=True)
        return
    if amount <= 0:
        await interaction.response.send_message(embed=send_error_embed("So luong phai lon hon 0."), ephemeral=True)
        return
    get_user(user.id)
    add_item(user.id, item_name.lower(), amount)
    log_admin_action(interaction.user.id, "give_item", target_user_id=user.id, details=f"{item_name} x{amount}")
    em = make_embed("🎁 Ban phat vat pham", f"Da trao **{amount} {item_name}** cho {user.mention}.", 0x8E44AD)
    await interaction.response.send_message(embed=em)


@bot.tree.command(name="hoipet", description="[Thien Dao] Hoi phuc pet")
async def hoipet(interaction: discord.Interaction, owner: discord.Member, pet_id: int):
    if not require_thien_dao(interaction):
        await interaction.response.send_message(embed=send_error_embed("Ban khong co quyen Thien Dao."), ephemeral=True)
        return
    cur.execute("SELECT * FROM pets WHERE owner_id = ? AND pet_id = ?", (owner.id, pet_id))
    pet = cur.fetchone()
    if not pet:
        await interaction.response.send_message(embed=send_error_embed("Khong tim thay pet can hoi phuc."), ephemeral=True)
        return
    cur.execute("UPDATE pets SET hunger = 100, mood = 100, health = 100, last_decay = ? WHERE pet_id = ?", (now_str(), pet_id))
    conn.commit()
    log_admin_action(interaction.user.id, "heal_pet", target_user_id=owner.id, target_pet_id=pet_id, details="full restore")
    em = make_embed("💚 Thien Dao hoi phuc", f"Da hoi day trang thai cho pet **{pet['name']}** cua {owner.mention}.", 0x58D68D)
    await interaction.response.send_message(embed=em)


@app_commands.choices(
    stat=[
        app_commands.Choice(name="HP", value="hp"),
        app_commands.Choice(name="ATK", value="atk"),
        app_commands.Choice(name="DEF", value="defense"),
        app_commands.Choice(name="SPD", value="speed"),
        app_commands.Choice(name="Hunger", value="hunger"),
        app_commands.Choice(name="Mood", value="mood"),
        app_commands.Choice(name="Health", value="health"),
        app_commands.Choice(name="Bond", value="bond"),
        app_commands.Choice(name="Level", value="level"),
    ]
)
@bot.tree.command(name="suapet", description="[Thien Dao] Chinh chi so pet")
async def suapet(
    interaction: discord.Interaction,
    owner: discord.Member,
    pet_id: int,
    stat: app_commands.Choice[str],
    value: int,
):
    if not require_thien_dao(interaction):
        await interaction.response.send_message(embed=send_error_embed("Ban khong co quyen Thien Dao."), ephemeral=True)
        return

    allowed_stats = {"hp", "atk", "defense", "speed", "hunger", "mood", "health", "bond", "level"}
    if stat.value not in allowed_stats:
        await interaction.response.send_message(embed=send_error_embed("Chi so khong hop le."), ephemeral=True)
        return

    cur.execute("SELECT * FROM pets WHERE owner_id = ? AND pet_id = ?", (owner.id, pet_id))
    pet = cur.fetchone()
    if not pet:
        await interaction.response.send_message(embed=send_error_embed("Khong tim thay pet can chinh."), ephemeral=True)
        return

    limits = {
        "hp": (1, 9999),
        "atk": (1, 999),
        "defense": (1, 999),
        "speed": (1, 999),
        "hunger": (0, 100),
        "mood": (0, 100),
        "health": (0, 100),
        "bond": (0, 9999),
        "level": (1, 999),
    }
    min_v, max_v = limits[stat.value]
    safe_value = clamp(value, min_v, max_v)

    cur.execute(f"UPDATE pets SET {stat.value} = ? WHERE pet_id = ?", (safe_value, pet_id))
    conn.commit()
    log_admin_action(interaction.user.id, "set_pet_stat", target_user_id=owner.id, target_pet_id=pet_id, details=f"{stat.value}={safe_value}")
    em = make_embed(
        "🛠️ Thien Dao chinh sua",
        f"Da chinh **{stat.name}** cua pet **{pet['name']}** thanh **{safe_value}**.",
        0xEB984E,
    )
    await interaction.response.send_message(embed=em)


@bot.tree.command(name="nhatkythiendao", description="[Thien Dao] Xem nhat ky admin gan nhat")
async def nhatkythiendao(interaction: discord.Interaction, limit: Optional[int] = 10):
    if not require_thien_dao(interaction):
        await interaction.response.send_message(embed=send_error_embed("Ban khong co quyen Thien Dao."), ephemeral=True)
        return

    limit = max(1, min(limit or 10, 20))
    cur.execute("SELECT * FROM admin_logs ORDER BY log_id DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    if not rows:
        await interaction.response.send_message(embed=send_info_embed("Chua co nhat ky Thien Dao nao."), ephemeral=True)
        return

    lines = []
    for row in rows:
        admin_member = interaction.guild.get_member(row["admin_id"]) if interaction.guild else None
        admin_name = admin_member.display_name if admin_member else f"Admin {row['admin_id']}"
        lines.append(
            f"• **{admin_name}** → `{row['action']}` | user: `{row['target_user_id']}` | pet: `{row['target_pet_id']}` | {row['details']}"
        )

    em = discord.Embed(title="📚 Nhat ky Thien Dao", description="\n".join(lines), color=0x5D6D7E)
    em.set_footer(text="Chi hien thi cac thao tac gan nhat")
    await interaction.response.send_message(embed=em, ephemeral=True)


# =========================
# MO RONG HE THONG DOI HINH / GACHA / THA PET
# =========================
# Goi y nang cap tiep theo cho ban code hien tai:
# 1) owner toi da 9 pet
# 2) doi hinh toi da 3 pet: dame / tank / buff
# 3) them lenh tha pet
# 4) gacha doc tu bang pet_pool, co the cap nhat bang lenh Thien Dao
# 5) co the gan avatar_url / image_url cho tung mau pet de render embed dep hon

# Database mo rong de dung cho he thong moi.
def init_extended_pet_system():
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pet_pool (
            template_id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            display_name TEXT NOT NULL,
            species TEXT NOT NULL,
            role TEXT NOT NULL,
            rarity TEXT NOT NULL DEFAULT 'Thuong',
            element TEXT NOT NULL DEFAULT 'Thuong',
            base_hp INTEGER NOT NULL DEFAULT 100,
            base_atk INTEGER NOT NULL DEFAULT 10,
            base_defense INTEGER NOT NULL DEFAULT 10,
            base_speed INTEGER NOT NULL DEFAULT 10,
            skill_name TEXT,
            skill_desc TEXT,
            image_url TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_team (
            owner_id INTEGER NOT NULL,
            slot_no INTEGER NOT NULL,
            pet_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            PRIMARY KEY(owner_id, slot_no)
        )
        """
    )
    conn.commit()


init_extended_pet_system()


def count_user_pets(owner_id: int) -> int:
    cur.execute("SELECT COUNT(*) AS total FROM pets WHERE owner_id = ?", (owner_id,))
    return cur.fetchone()["total"]


def get_team_pets(owner_id: int):
    cur.execute(
        """
        SELECT ut.slot_no, ut.role, p.*
        FROM user_team ut
        JOIN pets p ON p.pet_id = ut.pet_id
        WHERE ut.owner_id = ?
        ORDER BY ut.slot_no
        """,
        (owner_id,),
    )
    return cur.fetchall()


def seed_100_pet_templates_if_empty():
    cur.execute("SELECT COUNT(*) AS total FROM pet_pool")
    total = cur.fetchone()["total"]
    if total > 0:
        return

    roles = ["dame", "tank", "buff"]
    species_list = ["cho", "meo", "tho", "rong", "cao"]
    rarities = ["Thuong", "Hiem", "Quy", "Su Thi", "Huyen Thoai"]
    elements = ["Lua", "Nuoc", "Cay", "Dien", "Anh Sang", "Bong Toi"]

    for i in range(1, 101):
        role = random.choice(roles)
        species = random.choice(species_list)
        rarity = random.choices(
            rarities,
            weights=[50, 25, 15, 8, 2],
            k=1,
        )[0]
        element = random.choice(elements)

        if role == "dame":
            hp = random.randint(85, 120)
            atk = random.randint(18, 30)
            defense = random.randint(6, 14)
            speed = random.randint(12, 24)
        elif role == "tank":
            hp = random.randint(130, 190)
            atk = random.randint(8, 18)
            defense = random.randint(18, 30)
            speed = random.randint(5, 14)
        else:
            hp = random.randint(90, 130)
            atk = random.randint(10, 18)
            defense = random.randint(10, 18)
            speed = random.randint(10, 20)

        name = f"Pet Mau {i:03d}"
        code = f"PET_{i:03d}"
        skill_name = {
            "dame": "Don Sat Thuong",
            "tank": "Khien Chong Do",
            "buff": "Loi Chuc Tien Ho Tro",
        }[role]
        skill_desc = {
            "dame": "+sat thuong cho doi hinh",
            "tank": "+giap va hut sat thuong",
            "buff": "+toc danh / hoi mau / tang noi luc",
        }[role]

        now = now_str()
        cur.execute(
            """
            INSERT INTO pet_pool(
                code, display_name, species, role, rarity, element,
                base_hp, base_atk, base_defense, base_speed,
                skill_name, skill_desc, image_url, is_active, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            """,
            (
                code, name, species, role, rarity, element,
                hp, atk, defense, speed,
                skill_name, skill_desc, None, now, now,
            ),
        )
    conn.commit()


seed_100_pet_templates_if_empty()


def summon_from_pool(owner_id: int, pet_name: Optional[str] = None):
    if count_user_pets(owner_id) >= 9:
        return None, "Ban da dat toi da 9 pet. Hay tha bot pet truoc khi quay them."

    cur.execute("SELECT * FROM pet_pool WHERE is_active = 1 ORDER BY RANDOM() LIMIT 1")
    template = cur.fetchone()
    if not template:
        return None, "Chua co pet nao trong pet_pool."

    final_name = pet_name or template["display_name"]
    gender = random.choice(["Duc", "Cai"])
    now = now_str()

    cur.execute(
        """
        INSERT INTO pets(
            owner_id, name, species, gender, rarity, element,
            level, exp, hp, atk, defense, speed,
            hunger, mood, health, bond,
            last_feed, last_decay, feed_streak,
            breed_cd_until, mutated, generation,
            parent_a, parent_b, is_active, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            owner_id,
            final_name,
            template["species"],
            gender,
            template["rarity"],
            template["element"],
            1,
            0,
            template["base_hp"],
            template["base_atk"],
            template["base_defense"],
            template["base_speed"],
            100,
            100,
            100,
            0,
            now,
            now,
            0,
            None,
            0,
            1,
            None,
            None,
            0,
            now,
        ),
    )
    pet_id = cur.lastrowid
    conn.commit()
    return {"pet_id": pet_id, "template": template}, None


ROLE_LABELS = {
    "dame": "⚔️ Dame",
    "tank": "🛡️ Tank",
    "buff": "✨ Buff",
}

ROLE_ORDER = {"dame": 1, "tank": 2, "buff": 3}


def get_pet_role_from_template_like(pet) -> str:
    hp = pet["hp"]
    atk = pet["atk"]
    defense = pet["defense"]
    speed = pet["speed"]
    if defense >= atk and hp >= 130:
        return "tank"
    if atk >= defense + 5:
        return "dame"
    return "buff"


def render_stat_bar(current: int, maximum: int, length: int = 14, filled: str = "🟩", empty: str = "⬛") -> str:
    maximum = max(1, maximum)
    current = max(0, min(current, maximum))
    filled_count = round((current / maximum) * length)
    return filled * filled_count + empty * (length - filled_count)


def team_pet_summary_line(pet, role: str) -> str:
    return (
        f"{ROLE_LABELS.get(role, role)} • **{pet['name']}**\n"
        f"HP `{pet['health']}/100` {render_stat_bar(pet['health'], 100, 8)}\n"
        f"ATK **{pet['atk']}** | DEF **{pet['defense']}** | SPD **{pet['speed']}**"
    )


def render_battle_board_embed(attacker_name: str, defender_name: str, left_team: list, right_team: list, title: str, note: str):
    em = discord.Embed(title=title, description=note, color=0x2C3E50)

    left_header = []
    for p in left_team:
        left_header.append(f"Lv.{p['level']} {p['name']} {ROLE_LABELS.get(p['role'], p['role'])}")
    right_header = []
    for p in right_team:
        right_header.append(f"Lv.{p['level']} {p['name']} {ROLE_LABELS.get(p['role'], p['role'])}")

    em.add_field(
        name=f"🟦 Team {attacker_name}",
        value="\n".join(left_header) if left_header else "Chua xep doi hinh",
        inline=True,
    )
    em.add_field(
        name=f"🟥 Team {defender_name}",
        value="\n".join(right_header) if right_header else "Chua xep doi hinh",
        inline=True,
    )

    max_rows = max(len(left_team), len(right_team), 1)
    for i in range(max_rows):
        lp = left_team[i] if i < len(left_team) else None
        rp = right_team[i] if i < len(right_team) else None
        left_value = team_pet_summary_line(lp, lp['role']) if lp else "-"
        right_value = team_pet_summary_line(rp, rp['role']) if rp else "-"
        em.add_field(name=f"Pet hang {i + 1} ben trai", value=left_value, inline=True)
        em.add_field(name=f"Pet hang {i + 1} ben phai", value=right_value, inline=True)
    return em


def get_team_with_roles(owner_id: int):
    cur.execute(
        """
        SELECT ut.slot_no, ut.role, p.*
        FROM user_team ut
        JOIN pets p ON p.pet_id = ut.pet_id
        WHERE ut.owner_id = ?
        ORDER BY ut.slot_no
        """,
        (owner_id,),
    )
    return cur.fetchall()


def calc_team_power(team_rows) -> tuple[int, list[str]]:
    total = 0
    notes = []
    has_dame = False
    has_tank = False
    has_buff = False

    for row in team_rows:
        base = calc_pet_power(row)
        role = row["role"]
        if role == "dame":
            base = int(base * 1.15)
            has_dame = True
        elif role == "tank":
            base = int(base * 1.12)
            has_tank = True
        elif role == "buff":
            base = int(base * 1.08)
            has_buff = True
        total += base

    if has_dame and has_tank and has_buff:
        total = int(total * 1.18)
        notes.append("Bo 3 Dame - Tank - Buff kich hoat cong huong +18%")
    elif has_tank and has_buff:
        total = int(total * 1.10)
        notes.append("Tank + Buff kich hoat ho tro +10%")
    elif has_dame and has_buff:
        total = int(total * 1.10)
        notes.append("Dame + Buff kich hoat sat thuong +10%")
    elif has_dame and has_tank:
        total = int(total * 1.08)
        notes.append("Dame + Tank kich hoat tan cong - phong thu +8%")

    return total, notes


def build_battle_unit(row):
    role = row["role"]
    max_hp = max(1, row["hp"] * 4)
    atk = row["atk"]
    defense = row["defense"]
    speed = row["speed"]

    if role == "tank":
        max_hp = int(max_hp * 1.35)
        defense = int(defense * 1.2)
    elif role == "dame":
        atk = int(atk * 1.2)
        speed = int(speed * 1.1)
    elif role == "buff":
        max_hp = int(max_hp * 1.1)
        speed = int(speed * 1.05)

    return {
        "pet_id": row["pet_id"],
        "name": row["name"],
        "role": role,
        "level": row["level"],
        "max_hp": max_hp,
        "hp": max_hp,
        "atk": max(1, atk),
        "defense": max(1, defense),
        "speed": max(1, speed),
        "alive": True,
    }


def choose_target(team_units):
    living = [u for u in team_units if u["alive"]]
    if not living:
        return None
    tanks = [u for u in living if u["role"] == "tank"]
    if tanks:
        return tanks[0]
    return sorted(living, key=lambda x: (x["hp"], x["defense"]))[0]


def render_unit_card(unit):
    if not unit:
        return "-"
    state = "Song" if unit["alive"] else "Ha guc"
    return (
        f"{ROLE_LABELS.get(unit['role'], unit['role'])} • **{unit['name']}**\n"
        f"HP: **{unit['hp']} / {unit['max_hp']}**\n"
        f"{render_stat_bar(unit['hp'], unit['max_hp'], 10)}\n"
        f"ATK **{unit['atk']}** | DEF **{unit['defense']}** | SPD **{unit['speed']}**\n"
        f"Trang thai: **{state}**"
    )


def render_live_battle_embed(attacker_name: str, defender_name: str, left_units: list, right_units: list, title: str, logs: list[str]):
    em = discord.Embed(title=title, description="Mo phong tran dau doi hinh 3v3", color=0x34495E)
    em.add_field(name=f"🟦 {attacker_name}", value="Con song: **{}**".format(sum(1 for u in left_units if u['alive'])), inline=True)
    em.add_field(name=f"🟥 {defender_name}", value="Con song: **{}**".format(sum(1 for u in right_units if u['alive'])), inline=True)

    max_rows = max(len(left_units), len(right_units), 1)
    for i in range(max_rows):
        lu = left_units[i] if i < len(left_units) else None
        ru = right_units[i] if i < len(right_units) else None
        em.add_field(name=f"Ben trai #{i + 1}", value=render_unit_card(lu), inline=True)
        em.add_field(name=f"Ben phai #{i + 1}", value=render_unit_card(ru), inline=True)

    log_text = "\n".join(logs[-8:]) if logs else "Tran dau sap bat dau..."
    em.add_field(name="📜 Nhat ky giao tranh", value=log_text, inline=False)
    return em


def simulate_team_battle(left_team_rows, right_team_rows):
    left_units = [build_battle_unit(r) for r in left_team_rows]
    right_units = [build_battle_unit(r) for r in right_team_rows]
    logs = []
    turn = 1

    def apply_team_buffs(team_units, side_name):
        living = [u for u in team_units if u["alive"]]
        for u in living:
            if u["role"] == "buff":
                for mate in living:
                    if mate["pet_id"] == u["pet_id"]:
                        continue
                    mate["atk"] = int(mate["atk"] * 1.08)
                    mate["defense"] = int(mate["defense"] * 1.05)
                logs.append(f"✨ {side_name}: {u['name']} kich hoat buff cho dong doi")

    apply_team_buffs(left_units, "Ben trai")
    apply_team_buffs(right_units, "Ben phai")

    snapshots = []
    snapshots.append((turn, [dict(u) for u in left_units], [dict(u) for u in right_units], list(logs)))

    while any(u["alive"] for u in left_units) and any(u["alive"] for u in right_units) and turn <= 12:
        action_order = sorted(
            [u for u in left_units + right_units if u["alive"]],
            key=lambda x: (x["speed"], x["level"], random.randint(0, 9)),
            reverse=True,
        )

        logs.append(f"— Turn {turn} —")
        for actor in action_order:
            if not actor["alive"]:
                continue

            allies = left_units if actor in left_units else right_units
            enemies = right_units if actor in left_units else left_units
            if not any(e["alive"] for e in enemies):
                break

            if actor["role"] == "buff":
                ally_targets = [a for a in allies if a["alive"] and a["pet_id"] != actor["pet_id"]]
                low_hp = sorted(ally_targets, key=lambda x: x["hp"])[:1]
                if low_hp:
                    target = low_hp[0]
                    heal = int(actor["atk"] * 1.2) + random.randint(8, 18)
                    old_hp = target["hp"]
                    target["hp"] = min(target["max_hp"], target["hp"] + heal)
                    logs.append(f"✨ {actor['name']} hoi cho {target['name']} {target['hp'] - old_hp} HP")
                    continue

            target = choose_target(enemies)
            if not target:
                break

            raw = actor["atk"] * random.uniform(0.9, 1.15)
            if actor["role"] == "dame":
                raw *= 1.15
            if target["role"] == "tank":
                raw *= 0.9
            damage = max(1, int(raw - (target["defense"] * random.uniform(0.45, 0.8))))

            if actor["role"] == "tank":
                damage = int(damage * 0.9)

            target["hp"] = max(0, target["hp"] - damage)
            logs.append(f"⚔️ {actor['name']} tan cong {target['name']} gay **{damage}** sat thuong")
            if target["hp"] <= 0:
                target["alive"] = False
                logs.append(f"💥 {target['name']} da bi ha guc")

        snapshots.append((turn, [dict(u) for u in left_units], [dict(u) for u in right_units], list(logs)))
        turn += 1

    left_alive = sum(1 for u in left_units if u["alive"])
    right_alive = sum(1 for u in right_units if u["alive"])

    if left_alive > right_alive:
        winner_side = "left"
    elif right_alive > left_alive:
        winner_side = "right"
    else:
        left_hp_total = sum(u["hp"] for u in left_units)
        right_hp_total = sum(u["hp"] for u in right_units)
        winner_side = "left" if left_hp_total >= right_hp_total else "right"

    return {
        "winner_side": winner_side,
        "left_units": left_units,
        "right_units": right_units,
        "logs": logs,
        "snapshots": snapshots,
    }


class PkConfirmView(discord.ui.View):
    def __init__(self, challenger_id: int, target_id: int):
        super().__init__(timeout=60)
        self.challenger_id = challenger_id
        self.target_id = target_id
        self.accepted = False

    @discord.ui.button(label="Chap nhan PK", style=discord.ButtonStyle.success)
    async def accept_pk(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.target_id:
            await interaction.response.send_message("Ban khong phai nguoi duoc moi PK.", ephemeral=True)
            return
        self.accepted = True
        self.disable_all_items()
        await interaction.response.edit_message(view=self)
        self.stop()

    @discord.ui.button(label="Tu choi", style=discord.ButtonStyle.danger)
    async def reject_pk(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.target_id:
            await interaction.response.send_message("Ban khong phai nguoi duoc moi PK.", ephemeral=True)
            return
        self.accepted = False
        self.disable_all_items()
        await interaction.response.edit_message(view=self)
        self.stop()

    async def on_timeout(self):
        self.disable_all_items()


@bot.tree.command(name="thapet", description="Tha bot 1 pet de giai phong o trong")
async def thapet(interaction: discord.Interaction, pet_id: int):
    cur.execute("SELECT * FROM pets WHERE owner_id = ? AND pet_id = ?", (interaction.user.id, pet_id))
    pet = cur.fetchone()
    if not pet:
        await interaction.response.send_message(embed=send_error_embed("Khong tim thay pet de tha."), ephemeral=True)
        return

    cur.execute("DELETE FROM user_team WHERE owner_id = ? AND pet_id = ?", (interaction.user.id, pet_id))
    cur.execute("DELETE FROM pets WHERE owner_id = ? AND pet_id = ?", (interaction.user.id, pet_id))
    conn.commit()
    await interaction.response.send_message(embed=send_success_embed(f"Da tha pet **{pet['name']}** ve tu nhien."))


@bot.tree.command(name="quaypet", description="Quay ngau nhien 1 pet tu kho 100 pet")
async def quaypet(interaction: discord.Interaction):
    result, err = summon_from_pool(interaction.user.id)
    if err:
        await interaction.response.send_message(embed=send_error_embed(err), ephemeral=True)
        return

    template = result["template"]
    pet_id = result["pet_id"]
    em = discord.Embed(
        title="🎰 Quay pet thanh cong",
        description=(
            f"🐾 Ban vua nhan duoc **{template['display_name']}**\n"
            f"ID pet: **{pet_id}**\n"
            f"Vai tro: **{ROLE_LABELS.get(template['role'], template['role'])}**\n"
            f"Pham chat: **{template['rarity']}**\n"
            f"He: **{template['element']}**"
        ),
        color=0x8E44AD,
    )
    em.add_field(
        name="📊 Chi so goc",
        value=(
            f"HP: **{template['base_hp']}**\n"
            f"ATK: **{template['base_atk']}**\n"
            f"DEF: **{template['base_defense']}**\n"
            f"SPD: **{template['base_speed']}**"
        ),
        inline=True,
    )
    em.add_field(
        name="✨ Ky nang",
        value=f"**{template['skill_name'] or 'Chua dat ten'}**\n{template['skill_desc'] or 'Chua co mo ta'}",
        inline=True,
    )
    if template["image_url"]:
        em.set_thumbnail(url=template["image_url"])
    await interaction.response.send_message(embed=em)


@app_commands.choices(
    role=[
        app_commands.Choice(name="Dame", value="dame"),
        app_commands.Choice(name="Tank", value="tank"),
        app_commands.Choice(name="Buff", value="buff"),
    ]
)
@bot.tree.command(name="xepdoi", description="Xep 1 pet vao doi hinh dame tank buff")
async def xepdoi(interaction: discord.Interaction, role: app_commands.Choice[str], pet_id: int):
    cur.execute("SELECT * FROM pets WHERE owner_id = ? AND pet_id = ?", (interaction.user.id, pet_id))
    pet = cur.fetchone()
    if not pet:
        await interaction.response.send_message(embed=send_error_embed("Khong tim thay pet nay."), ephemeral=True)
        return

    slot_map = {"dame": 1, "tank": 2, "buff": 3}
    slot_no = slot_map[role.value]

    cur.execute("SELECT slot_no, role FROM user_team WHERE owner_id = ? AND pet_id = ?", (interaction.user.id, pet_id))
    existing = cur.fetchone()
    if existing and existing["slot_no"] != slot_no:
        await interaction.response.send_message(
            embed=send_error_embed(f"Pet nay da duoc xep o vai tro **{ROLE_LABELS.get(existing['role'], existing['role'])}**. Hay doi pet khac hoac thay the truc tiep o o do."),
            ephemeral=True,
        )
        return

    cur.execute(
        """
        INSERT INTO user_team(owner_id, slot_no, pet_id, role)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(owner_id, slot_no)
        DO UPDATE SET pet_id = excluded.pet_id, role = excluded.role
        """,
        (interaction.user.id, slot_no, pet_id, role.value),
    )
    conn.commit()
    await interaction.response.send_message(embed=send_success_embed(f"Da xep **{pet['name']}** vao o **{ROLE_LABELS[role.value]}**."))


@bot.tree.command(name="doihinh", description="Xem doi hinh 3 pet hien tai")
async def doihinh(interaction: discord.Interaction):
    rows = get_team_pets(interaction.user.id)
    if not rows:
        await interaction.response.send_message(embed=send_info_embed("Ban chua xep doi hinh nao. Dung `/xepdoi` de xep Dame / Tank / Buff."), ephemeral=True)
        return

    em = discord.Embed(
        title=f"🧩 Doi hinh cua {interaction.user.display_name}",
        description="Toi da 3 pet chien dau: 1 Dame • 1 Tank • 1 Buff",
        color=0x3498DB,
    )
    for row in rows:
        em.add_field(
            name=f"{ROLE_LABELS.get(row['role'], row['role'])} • {row['name']}",
            value=(
                f"Pet ID: **{row['pet_id']}**\n"
                f"Loai: **{species_name_vi(row['species'])}**\n"
                f"Lv: **{row['level']}** • Power: **{calc_pet_power(row)}**\n"
                f"HP view: `{row['health']}/100` {render_stat_bar(row['health'], 100, 8)}"
            ),
            inline=False,
        )
    await interaction.response.send_message(embed=em)


@bot.tree.command(name="pkdoihinh", description="Moi 1 nguoi choi PK va doi xac nhan")
async def pkdoihinh(interaction: discord.Interaction, user: discord.Member):
    if user.bot:
        await interaction.response.send_message(embed=send_error_embed("Khong the PK voi bot."), ephemeral=True)
        return
    if user.id == interaction.user.id:
        await interaction.response.send_message(embed=send_error_embed("Khong the PK voi chinh minh."), ephemeral=True)
        return

    my_team = get_team_with_roles(interaction.user.id)
    enemy_team = get_team_with_roles(user.id)

    if not my_team:
        await interaction.response.send_message(embed=send_error_embed("Ban chua co doi hinh PK. Dung `/xepdoi` truoc."), ephemeral=True)
        return
    if not enemy_team:
        await interaction.response.send_message(embed=send_error_embed("Doi thu chua co doi hinh PK."), ephemeral=True)
        return

    invite_view = PkConfirmView(interaction.user.id, user.id)
    preview = render_battle_board_embed(
        interaction.user.display_name,
        user.display_name,
        my_team,
        enemy_team,
        "⚔️ Loi moi PK doi hinh",
        f"{user.mention}, ban co muon chap nhan tran dau voi {interaction.user.mention} khong?",
    )
    preview.set_footer(text="Nhan Chap nhan PK de bat dau tran dau")
    await interaction.response.send_message(content=user.mention, embed=preview, view=invite_view)

    await invite_view.wait()
    if not invite_view.accepted:
        try:
            reject_embed = discord.Embed(
                title="🚫 Loi moi PK ket thuc",
                description=f"{user.mention} da tu choi hoac khong phan hoi loi moi PK.",
                color=0xE74C3C,
            )
            await interaction.edit_original_response(embed=reject_embed, view=invite_view)
        except Exception:
            pass
        return

    my_team = get_team_with_roles(interaction.user.id)
    enemy_team = get_team_with_roles(user.id)
    if not my_team or not enemy_team:
        await interaction.edit_original_response(
            embed=send_error_embed("Mot trong hai ben vua thay doi doi hinh, vui long PK lai."),
            view=None,
        )
        return

    battle = simulate_team_battle(my_team, enemy_team)
    for turn_no, left_state, right_state, log_state in battle["snapshots"]:
        live_embed = render_live_battle_embed(
            interaction.user.display_name,
            user.display_name,
            left_state,
            right_state,
            f"🎬 Tran dau dang dien ra • Turn {turn_no}",
            log_state,
        )
        await interaction.edit_original_response(embed=live_embed, view=None)
        await asyncio.sleep(1.0)

    winner = interaction.user if battle["winner_side"] == "left" else user
    loser = user if winner.id == interaction.user.id else interaction.user
    winner_team_rows = my_team if winner.id == interaction.user.id else enemy_team

    get_user(interaction.user.id)
    get_user(user.id)
    cur.execute("SELECT coins FROM users WHERE user_id = ?", (interaction.user.id,))
    my_coins = cur.fetchone()["coins"]
    cur.execute("SELECT coins FROM users WHERE user_id = ?", (user.id,))
    enemy_coins = cur.fetchone()["coins"]
    stake = min(300, my_coins, enemy_coins)

    if stake > 0:
        cur.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (stake, winner.id))
        cur.execute("UPDATE users SET coins = MAX(0, coins - ?) WHERE user_id = ?", (stake, loser.id))

    for row in winner_team_rows:
        pet_gain_exp(row["pet_id"], 12)
    user_gain_exp(winner.id, 10)
    cur.execute(
        "INSERT INTO battles(attacker_id, defender_id, winner_id, coins_delta, created_at) VALUES (?, ?, ?, ?, ?)",
        (interaction.user.id, user.id, winner.id, stake, now_str()),
    )
    conn.commit()

    result_embed = render_live_battle_embed(
        interaction.user.display_name,
        user.display_name,
        battle["left_units"],
        battle["right_units"],
        "🏆 Ket qua PK doi hinh",
        battle["logs"],
    )
    result_embed.add_field(name="👑 Nguoi thang", value=winner.mention, inline=False)
    result_embed.add_field(name="🎁 Thuong xu", value=f"**{stake} xu**" if stake > 0 else "Hai ben khong du xu de dat cuoc", inline=False)
    result_embed.set_footer(text="Tran dau nay can xac nhan truoc moi bat dau")
    await interaction.edit_original_response(embed=result_embed, view=None)


@app_commands.choices(
    role=[
        app_commands.Choice(name="Dame", value="dame"),
        app_commands.Choice(name="Tank", value="tank"),
        app_commands.Choice(name="Buff", value="buff"),
    ]
)
@bot.tree.command(name="themtemplatepet", description="[Thien Dao] Them pet moi vao kho gacha")
async def themtemplatepet(
    interaction: discord.Interaction,
    code: str,
    display_name: str,
    species: str,
    role: app_commands.Choice[str],
    rarity: str,
    element: str,
    base_hp: int,
    base_atk: int,
    base_defense: int,
    base_speed: int,
    skill_name: Optional[str] = None,
    skill_desc: Optional[str] = None,
    image_url: Optional[str] = None,
):
    if not require_thien_dao(interaction):
        await interaction.response.send_message(embed=send_error_embed("Ban khong co quyen Thien Dao."), ephemeral=True)
        return

    now = now_str()
    cur.execute(
        """
        INSERT INTO pet_pool(
            code, display_name, species, role, rarity, element,
            base_hp, base_atk, base_defense, base_speed,
            skill_name, skill_desc, image_url, is_active, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        """,
        (
            code.upper(), display_name, species, role.value, rarity, element,
            base_hp, base_atk, base_defense, base_speed,
            skill_name, skill_desc, image_url, now, now,
        ),
    )
    conn.commit()
    await interaction.response.send_message(embed=send_success_embed(f"Da them template pet **{display_name}** vao kho gacha."))


# =========================
# RUN
# =========================
bot.run(TOKEN)

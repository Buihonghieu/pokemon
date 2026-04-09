import os
import random
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

# =========================
# CẤU HÌNH
# =========================
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    raise RuntimeError("Thiếu DISCORD_TOKEN trong file .env")

DB_PATH = os.getenv("DB_PATH", "/data/pet_game.db")
GUILD_ID = os.getenv("GUILD_ID")

# ID Thiên Đạo: có thể khai báo nhiều người, ngăn cách bằng dấu phẩy trong file .env
# Ví dụ: ADMIN_IDS=123456789,987654321
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = {int(x.strip()) for x in ADMIN_IDS_RAW.split(",") if x.strip().isdigit()}

# =========================
# DATABASE
# =========================
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()


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
            rarity TEXT NOT NULL DEFAULT 'Thường',
            element TEXT NOT NULL DEFAULT 'Thường',
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
# HÀM HỖ TRỢ QUYỀN HẠN / GIAO DIỆN
# =========================
def is_thien_dao(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def require_thien_dao(interaction: discord.Interaction) -> bool:
    return is_thien_dao(interaction.user.id) or interaction.user.guild_permissions.administrator


def make_embed(title: str, description: str, color: int) -> discord.Embed:
    return discord.Embed(title=title, description=description, color=color)


def send_error_embed(message: str) -> discord.Embed:
    return make_embed("❌ Thông báo", message, 0xE74C3C)


def send_success_embed(message: str) -> discord.Embed:
    return make_embed("✅ Thành công", message, 0x2ECC71)


def send_info_embed(message: str) -> discord.Embed:
    return make_embed("📢 Thông tin", message, 0x3498DB)


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
# DỮ LIỆU MẪU / HẰNG SỐ
# =========================
SPECIES_DATA = {
    "cho": {"hp": 110, "atk": 14, "defense": 9, "speed": 9},
    "meo": {"hp": 95, "atk": 13, "defense": 8, "speed": 13},
    "tho": {"hp": 90, "atk": 10, "defense": 8, "speed": 15},
    "rong": {"hp": 120, "atk": 16, "defense": 10, "speed": 8},
    "cao": {"hp": 100, "atk": 15, "defense": 7, "speed": 12},
}

ELEMENTS = ["Lửa", "Nước", "Cây", "Điện", "Ánh Sáng", "Bóng Tối"]
CROPS = {
    "co": {"minutes": 5, "yield": (2, 4), "food_value": 10},
    "ngo": {"minutes": 10, "yield": (3, 6), "food_value": 18},
    "carot": {"minutes": 15, "yield": (2, 5), "food_value": 25},
}


# =========================
# PHIÊN XÌ DÁCH TẠM TRONG RAM
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
# HÀM HỖ TRỢ DATABASE / THỜI GIAN
# =========================
def now_str() -> str:
    return datetime.utcnow().isoformat()


def dt_from_str(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    return datetime.fromisoformat(s)


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
        return "Kho đồ trống."
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
    last_feed = dt_from_str(active["last_feed"])
    hunger = active["hunger"]
    mood = active["mood"]

    if last_feed:
        hours_passed = int((now - last_feed).total_seconds() // 3600)
        if hours_passed > 0:
            hunger = max(0, hunger - hours_passed * 4)
            mood = max(0, mood - hours_passed * 2)
            cur.execute(
                "UPDATE pets SET hunger = ?, mood = ? WHERE pet_id = ?",
                (hunger, mood, active["pet_id"]),
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
        "Thường": "⚪",
        "Hiếm": "🔵",
        "Quý": "🟣",
        "Sử Thi": "🟠",
        "Huyền Thoại": "🟡",
        "Dị Biến": "✨",
    }.get(rarity, "⚪")


def species_name_vi(species: str) -> str:
    return {
        "cho": "Chó",
        "meo": "Mèo",
        "tho": "Thỏ",
        "rong": "Rồng",
        "cao": "Cáo",
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
        name="📊 Chỉ số chiến đấu",
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
        name="📈 Trạng thái",
        value=(
            f"🍖 Đói: `{bar(pet['hunger'])}` **{pet['hunger']}/100**\n"
            f"😺 Tâm trạng: `{bar(pet['mood'])}` **{pet['mood']}/100**\n"
            f"💚 Sức khỏe: `{bar(pet['health'])}` **{pet['health']}/100**\n"
            f"🤝 Thân thiết: **{pet['bond']}**"
        ),
        inline=True,
    )
    em.add_field(
        name="🧬 Thông tin khác",
        value=(
            f"⭐ Cấp: **{pet['level']}**\n"
            f"🍽️ Streak ăn: **{pet['feed_streak']}**\n"
            f"🌀 Đột biến: **{'Có' if pet['mutated'] else 'Chưa'}**\n"
            f"🌱 Thế hệ: **F{pet['generation']}**"
        ),
        inline=False,
    )
    em.set_footer(text=f"Pet ID: {pet['pet_id']} • Chủ nuôi chăm đúng giờ sẽ mạnh hơn")
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
            print(f"Đã sync slash commands cho guild test {GUILD_ID}")
        else:
            await bot.tree.sync()
            print("Đã sync slash commands global")
    except Exception as e:
        print("Lỗi sync:", e)

    print(f"Bot đã online: {bot.user}")


# =========================
# COMMANDS - ACCOUNT
# =========================
@bot.tree.command(name="startgame", description="Bắt đầu chơi game pet")
async def startgame(interaction: discord.Interaction):
    user = get_user(interaction.user.id)
    em = make_embed(
        "🎮 Bắt đầu hành trình",
        f"{interaction.user.mention} đã vào game.\n🪙 Xu hiện tại: **{user['coins']}**\n🐾 Dùng `/pet_create` để tạo pet đầu tiên.",
        0x5DADE2,
    )
    await interaction.response.send_message(embed=em, ephemeral=True)


@bot.tree.command(name="daily", description="Nhận quà mỗi ngày")
async def daily(interaction: discord.Interaction):
    user = get_user(interaction.user.id)
    now = datetime.utcnow()
    last_daily = dt_from_str(user["last_daily"])

    if last_daily and (now - last_daily) < timedelta(hours=24):
        remain = timedelta(hours=24) - (now - last_daily)
        hours, rem = divmod(int(remain.total_seconds()), 3600)
        minutes = rem // 60
        await interaction.response.send_message(
            embed=send_info_embed(f"Bạn đã nhận daily rồi. Còn {hours}h {minutes}m nữa."),
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
        embed=send_success_embed(f"Bạn nhận được **{reward} xu** từ daily."),
        ephemeral=True,
    )


@bot.tree.command(name="profile", description="Xem hồ sơ của bạn")
async def profile(interaction: discord.Interaction):
    user = get_user(interaction.user.id)
    decay_pet_stats(interaction.user.id)
    pet = get_active_pet(interaction.user.id)

    em = discord.Embed(
        title=f"👤 Hồ sơ người chơi • {interaction.user.display_name}",
        description="🌟 Quản lý tài sản, pet và kho đồ của bạn",
        color=0x2ECC71,
    )
    em.add_field(
        name="💰 Tài sản",
        value=(
            f"🪙 Xu: **{user['coins']}**\n"
            f"💎 Ngọc: **{user['gems']}**\n"
            f"🎖️ Cấp: **{user['level']}**\n"
            f"📚 EXP: **{user['exp']}**"
        ),
        inline=True,
    )
    em.add_field(name="🎒 Kho đồ", value=get_inventory_text(interaction.user.id), inline=True)
    if pet:
        em.add_field(
            name="🐾 Pet đang dùng",
            value=(
                f"**{pet['name']}** ({species_name_vi(pet['species'])})\n"
                f"⭐ Lv {pet['level']} • 🔥 Power {calc_pet_power(pet)}\n"
                f"🍖 {bar(pet['hunger'])} {pet['hunger']}/100"
            ),
            inline=False,
        )
    else:
        em.add_field(name="🐾 Pet đang dùng", value="Chưa có pet hoạt động", inline=False)

    em.set_footer(text="Dùng /pet_list để xem toàn bộ thú cưng")
    await interaction.response.send_message(embed=em)


# =========================
# COMMANDS - PET
# =========================
@app_commands.choices(
    species=[
        app_commands.Choice(name="Chó", value="cho"),
        app_commands.Choice(name="Mèo", value="meo"),
        app_commands.Choice(name="Thỏ", value="tho"),
        app_commands.Choice(name="Rồng", value="rong"),
        app_commands.Choice(name="Cáo", value="cao"),
    ]
)
@bot.tree.command(name="pet_create", description="Tạo pet đầu tiên")
async def pet_create(interaction: discord.Interaction, name: str, species: app_commands.Choice[str]):
    get_user(interaction.user.id)
    cur.execute("SELECT COUNT(*) AS total FROM pets WHERE owner_id = ?", (interaction.user.id,))
    total = cur.fetchone()["total"]
    if total >= 3:
        await interaction.response.send_message(
            embed=send_error_embed("Bản MVP hiện chỉ cho tạo tối đa 3 pet mỗi người."),
            ephemeral=True,
        )
        return

    sp = SPECIES_DATA[species.value]
    gender = random.choice(["Đực", "Cái"])
    element = random.choice(ELEMENTS)

    cur.execute(
        """
        INSERT INTO pets(
            owner_id, name, species, gender, element,
            hp, atk, defense, speed,
            hunger, mood, health, bond,
            is_active, created_at, last_feed
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 100, 100, 100, 0, ?, ?, ?)
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
            now_str(),
            now_str(),
        ),
    )
    conn.commit()

    em = discord.Embed(
        title="🎉 Tạo pet thành công",
        description=(
            f"╔════ Thú cưng mới ════\n"
            f"🐾 Tên: **{name}**\n"
            f"🧩 Loài: **{species.name}**\n"
            f"🚻 Giới tính: **{gender}**\n"
            f"🌌 Hệ: **{element}**\n"
            f"╚═══════════════"
        ),
        color=0xF1C40F,
    )
    em.add_field(
        name="📌 Ghi chú",
        value="🌟 Đây là pet đang hoạt động của bạn." if total == 0 else "Dùng `/pet_setactive` để chọn pet này.",
        inline=False,
    )
    await interaction.response.send_message(embed=em)


@bot.tree.command(name="pet_list", description="Xem danh sách pet")
async def pet_list(interaction: discord.Interaction):
    decay_pet_stats(interaction.user.id)
    cur.execute("SELECT * FROM pets WHERE owner_id = ? ORDER BY pet_id", (interaction.user.id,))
    pets = cur.fetchall()
    if not pets:
        await interaction.response.send_message(
            embed=send_info_embed("Bạn chưa có pet nào. Dùng `/pet_create` để tạo."),
            ephemeral=True,
        )
        return

    em = discord.Embed(title=f"🐾 Pet của {interaction.user.display_name}", color=0x3498DB)
    for p in pets:
        active_mark = "🌟" if p["is_active"] else ""
        em.add_field(
            name=f"#{p['pet_id']} {active_mark} {p['name']}",
            value=(
                f"Loài: {species_name_vi(p['species'])} | Lv {p['level']}\n"
                f"Power: {calc_pet_power(p)}\n"
                f"Đói: {p['hunger']}/100 | Mood: {p['mood']}/100"
            ),
            inline=False,
        )
    await interaction.response.send_message(embed=em)


@bot.tree.command(name="pet_info", description="Xem chi tiết pet")
async def pet_info(interaction: discord.Interaction, pet_id: int):
    decay_pet_stats(interaction.user.id)
    cur.execute("SELECT * FROM pets WHERE owner_id = ? AND pet_id = ?", (interaction.user.id, pet_id))
    pet = cur.fetchone()
    if not pet:
        await interaction.response.send_message(embed=send_error_embed("Không tìm thấy pet này."), ephemeral=True)
        return
    await interaction.response.send_message(embed=make_pet_embed(pet))


@bot.tree.command(name="pet_setactive", description="Đặt pet đang sử dụng")
async def pet_setactive(interaction: discord.Interaction, pet_id: int):
    cur.execute("SELECT * FROM pets WHERE owner_id = ? AND pet_id = ?", (interaction.user.id, pet_id))
    pet = cur.fetchone()
    if not pet:
        await interaction.response.send_message(embed=send_error_embed("Không tìm thấy pet đó."), ephemeral=True)
        return

    cur.execute("UPDATE pets SET is_active = 0 WHERE owner_id = ?", (interaction.user.id,))
    cur.execute("UPDATE pets SET is_active = 1 WHERE pet_id = ?", (pet_id,))
    conn.commit()
    await interaction.response.send_message(embed=send_success_embed(f"Đã chọn **{pet['name']}** làm pet hoạt động."))


@app_commands.choices(
    food=[
        app_commands.Choice(name="Cỏ", value="co"),
        app_commands.Choice(name="Ngô", value="ngo"),
        app_commands.Choice(name="Cà rốt", value="carot"),
    ]
)
@bot.tree.command(name="pet_feed", description="Cho pet đang hoạt động ăn")
async def pet_feed(interaction: discord.Interaction, food: app_commands.Choice[str]):
    decay_pet_stats(interaction.user.id)
    pet = get_active_pet(interaction.user.id)
    if not pet:
        await interaction.response.send_message(embed=send_error_embed("Bạn chưa có pet đang hoạt động."), ephemeral=True)
        return

    crop = CROPS[food.value]
    if not remove_item(interaction.user.id, food.value, 1):
        await interaction.response.send_message(embed=send_error_embed(f"Bạn không có **{food.name}** trong kho."), ephemeral=True)
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
        "UPDATE pets SET hunger = ?, mood = ?, bond = ?, feed_streak = ?, last_feed = ? WHERE pet_id = ?",
        (hunger, mood, bond, streak, now_str(), pet["pet_id"]),
    )
    conn.commit()
    leveled = pet_gain_exp(pet["pet_id"], 8 if good_window else 4)

    bonus = "⏰ Cho ăn đúng nhịp, pet rất vui!" if good_window else "🍽️ Pet đã được ăn no hơn."
    em = discord.Embed(
        title="🍽️ Cho ăn thành công",
        description=(
            f"╔════ Bữa ăn pet ════\n"
            f"🐾 Pet: **{pet['name']}**\n"
            f"🥕 Thức ăn: **{food.name}**\n"
            f"🔥 Streak: **{streak}**\n"
            f"╚═══════════════"
        ),
        color=0x58D68D,
    )
    em.add_field(name="📢 Thông báo", value=bonus, inline=False)
    if leveled and leveled[0] > 0:
        em.add_field(name="🎉 Thăng cấp", value=f"Pet lên **{leveled[0]}** cấp • Hiện tại: **Lv {leveled[1]}**", inline=False)
    await interaction.response.send_message(embed=em)


# =========================
# COMMANDS - FARM
# =========================
@app_commands.choices(
    crop=[
        app_commands.Choice(name="Cỏ", value="co"),
        app_commands.Choice(name="Ngô", value="ngo"),
        app_commands.Choice(name="Cà rốt", value="carot"),
    ]
)
@bot.tree.command(name="farm_plant", description="Trồng lương thực")
async def farm_plant(interaction: discord.Interaction, crop: app_commands.Choice[str]):
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
        await interaction.response.send_message(embed=send_error_embed("Tối đa 5 ô đang trồng cùng lúc trong bản MVP."), ephemeral=True)
        return

    cur.execute(
        "INSERT INTO farm_plots(owner_id, crop_name, planted_at, ready_at, yield_amount) VALUES (?, ?, ?, ?, ?)",
        (interaction.user.id, crop.value, now_str(), ready.isoformat(), yield_amount),
    )
    conn.commit()
    await interaction.response.send_message(embed=send_success_embed(f"Đã trồng **{crop.name}**. Sẽ thu hoạch sau **{cfg['minutes']} phút**."))


@bot.tree.command(name="farm_list", description="Xem các ô trồng")
async def farm_list(interaction: discord.Interaction):
    cur.execute(
        "SELECT * FROM farm_plots WHERE owner_id = ? AND harvested = 0 ORDER BY plot_id",
        (interaction.user.id,),
    )
    rows = cur.fetchall()
    if not rows:
        await interaction.response.send_message(embed=send_info_embed("Bạn chưa trồng gì cả."), ephemeral=True)
        return

    now = datetime.utcnow()
    lines = []
    for r in rows:
        ready_at = dt_from_str(r["ready_at"])
        if ready_at <= now:
            status = "✅ Sẵn sàng"
        else:
            remain = ready_at - now
            mins = int(remain.total_seconds() // 60) + 1
            status = f"⏳ Còn khoảng {mins} phút"
        lines.append(f"Ô #{r['plot_id']} - {r['crop_name']} - {status} - sản lượng {r['yield_amount']}")
    await interaction.response.send_message(embed=send_info_embed("\n".join(lines)))


@bot.tree.command(name="farm_harvest", description="Thu hoạch 1 ô hoặc tất cả")
async def farm_harvest(interaction: discord.Interaction, plot_id: Optional[int] = None):
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
        await interaction.response.send_message(embed=send_error_embed("Không có ô nào hợp lệ để thu hoạch."), ephemeral=True)
        return

    harvested_lines = []
    total_items = 0
    for r in rows:
        ready_at = dt_from_str(r["ready_at"])
        if ready_at and ready_at <= now:
            add_item(interaction.user.id, r["crop_name"], r["yield_amount"])
            cur.execute("UPDATE farm_plots SET harvested = 1 WHERE plot_id = ?", (r["plot_id"],))
            harvested_lines.append(f"- Ô #{r['plot_id']}: +{r['yield_amount']} {r['crop_name']}")
            total_items += r["yield_amount"]

    conn.commit()
    if not harvested_lines:
        await interaction.response.send_message(embed=send_info_embed("Chưa có ô nào chín để thu hoạch."), ephemeral=True)
        return

    user_gain_exp(interaction.user.id, 5)
    em = discord.Embed(
        title="🌾 Thu hoạch thành công",
        description=make_box("Nông trại", harvested_lines),
        color=0x27AE60,
    )
    em.add_field(name="📦 Tổng vật phẩm", value=f"**{total_items}**", inline=False)
    await interaction.response.send_message(embed=em)


# =========================
# COMMANDS - HUNT / PK
# =========================
@bot.tree.command(name="hunt", description="Dùng pet đang hoạt động đi săn tiền")
async def hunt(interaction: discord.Interaction):
    decay_pet_stats(interaction.user.id)
    pet = get_active_pet(interaction.user.id)
    if not pet:
        await interaction.response.send_message(embed=send_error_embed("Bạn chưa có pet đang hoạt động."), ephemeral=True)
        return

    last_hunt = dt_from_str(pet["last_hunt"])
    now = datetime.utcnow()
    if last_hunt and (now - last_hunt) < timedelta(minutes=15):
        remain = timedelta(minutes=15) - (now - last_hunt)
        mins = int(remain.total_seconds() // 60) + 1
        await interaction.response.send_message(embed=send_info_embed(f"Pet đang mệt, đợi khoảng {mins} phút nữa."), ephemeral=True)
        return

    power = calc_pet_power(pet)
    success_rate = min(90, 45 + power // 25)
    roll = random.randint(1, 100)

    hunger = max(0, pet["hunger"] - random.randint(6, 12))
    mood = max(0, pet["mood"] - random.randint(3, 8))
    cur.execute(
        "UPDATE pets SET hunger = ?, mood = ?, last_hunt = ? WHERE pet_id = ?",
        (hunger, mood, now_str(), pet["pet_id"]),
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
            title="🏹 Chuyến săn thành công",
            description=(
                f"╔════ Kết quả săn ════\n"
                f"🐾 Pet: **{pet['name']}**\n"
                f"💰 Xu kiếm được: **{coins}**\n"
                f"🔥 Power hiện tại: **{power}**\n"
                f"╚═══════════════"
            ),
            color=0x3498DB,
        )
        if extra:
            em.add_field(name="🎁 Nhặt thêm", value=extra, inline=False)
        await interaction.response.send_message(embed=em)
    else:
        loss = random.randint(20, 60)
        cur.execute("UPDATE users SET coins = MAX(0, coins - ?) WHERE user_id = ?", (loss, interaction.user.id))
        conn.commit()
        em = discord.Embed(
            title="💥 Chuyến săn thất bại",
            description=(
                f"╔════ Kết quả săn ════\n"
                f"🐾 Pet: **{pet['name']}**\n"
                f"💸 Làm rơi: **{loss} xu**\n"
                f"😿 Hãy cho pet nghỉ ngơi rồi thử lại\n"
                f"╚═══════════════"
            ),
            color=0xE74C3C,
        )
        await interaction.response.send_message(embed=em)


@bot.tree.command(name="pk", description="Thách đấu người khác bằng pet đang hoạt động")
async def pk(interaction: discord.Interaction, user: discord.Member):
    if user.bot:
        await interaction.response.send_message(embed=send_error_embed("Không thể PK với bot."), ephemeral=True)
        return
    if user.id == interaction.user.id:
        await interaction.response.send_message(embed=send_error_embed("Không thể PK với chính mình."), ephemeral=True)
        return

    decay_pet_stats(interaction.user.id)
    decay_pet_stats(user.id)
    my_pet = get_active_pet(interaction.user.id)
    enemy_pet = get_active_pet(user.id)
    if not my_pet:
        await interaction.response.send_message(embed=send_error_embed("Bạn chưa có pet đang hoạt động."), ephemeral=True)
        return
    if not enemy_pet:
        await interaction.response.send_message(embed=send_error_embed("Đối thủ chưa có pet đang hoạt động."), ephemeral=True)
        return

    my_power = calc_pet_power(my_pet) + random.randint(-15, 20)
    enemy_power = calc_pet_power(enemy_pet) + random.randint(-15, 20)
    my_user = get_user(interaction.user.id)
    enemy_user = get_user(user.id)

    stake = min(200, my_user["coins"], enemy_user["coins"])
    if stake <= 0:
        await interaction.response.send_message(embed=send_error_embed("Một trong hai người không đủ xu để PK."), ephemeral=True)
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
        title="⚔️ Kết quả PK",
        description=(
            f"╔════ Đấu trường ════\n"
            f"🟥 {interaction.user.display_name} • {my_pet['name']} • **{my_power}** power\n"
            f"🟦 {user.display_name} • {enemy_pet['name']} • **{enemy_power}** power\n"
            f"╚═══════════════"
        ),
        color=0x9B59B6,
    )
    em.add_field(name="🏆 Người thắng", value=f"**{winner_name}** cùng pet **{winner_pet}**", inline=False)
    em.add_field(name="💰 Phần thưởng", value=f"**{stake} xu**", inline=False)
    await interaction.response.send_message(embed=em)


# =========================
# COMMANDS - BREED (MVP BASIC)
# =========================
@bot.tree.command(name="breed", description="Phối giống 2 pet của bạn")
async def breed(interaction: discord.Interaction, pet_a_id: int, pet_b_id: int, child_name: str):
    cur.execute("SELECT * FROM pets WHERE owner_id = ? AND pet_id = ?", (interaction.user.id, pet_a_id))
    pet_a = cur.fetchone()
    cur.execute("SELECT * FROM pets WHERE owner_id = ? AND pet_id = ?", (interaction.user.id, pet_b_id))
    pet_b = cur.fetchone()

    if not pet_a or not pet_b:
        await interaction.response.send_message(embed=send_error_embed("Không tìm thấy một trong hai pet."), ephemeral=True)
        return
    if pet_a_id == pet_b_id:
        await interaction.response.send_message(embed=send_error_embed("Phải chọn 2 pet khác nhau."), ephemeral=True)
        return
    if pet_a["gender"] == pet_b["gender"]:
        await interaction.response.send_message(embed=send_error_embed("Hai pet phải khác giới tính."), ephemeral=True)
        return
    if pet_a["level"] < 5 or pet_b["level"] < 5:
        await interaction.response.send_message(embed=send_error_embed("Cả hai pet phải đạt cấp 5 trở lên mới phối giống."), ephemeral=True)
        return

    cd_a = dt_from_str(pet_a["breed_cd_until"])
    cd_b = dt_from_str(pet_b["breed_cd_until"])
    now = datetime.utcnow()
    if (cd_a and cd_a > now) or (cd_b and cd_b > now):
        await interaction.response.send_message(embed=send_info_embed("Một trong hai pet vẫn đang cooldown phối giống."), ephemeral=True)
        return

    user = get_user(interaction.user.id)
    cost = 500
    if user["coins"] < cost:
        await interaction.response.send_message(embed=send_error_embed("Bạn không đủ 500 xu để phối giống."), ephemeral=True)
        return

    mutation_rate = 5
    if pet_a["feed_streak"] >= 3:
        mutation_rate += 2
    if pet_b["feed_streak"] >= 3:
        mutation_rate += 2

    mutated = 1 if random.randint(1, 100) <= mutation_rate else 0
    rarity = "Dị Biến" if mutated else random.choice(["Thường", "Hiếm", "Quý"])
    species = random.choice([pet_a["species"], pet_b["species"]])
    element = random.choice([pet_a["element"], pet_b["element"]])
    gender = random.choice(["Đực", "Cái"])

    base = SPECIES_DATA.get(species, SPECIES_DATA["cho"])
    hp = int((pet_a["hp"] + pet_b["hp"] + base["hp"]) / 3) + random.randint(-5, 8)
    atk = int((pet_a["atk"] + pet_b["atk"] + base["atk"]) / 3) + random.randint(-2, 3)
    defense = int((pet_a["defense"] + pet_b["defense"] + base["defense"]) / 3) + random.randint(-2, 3)
    speed = int((pet_a["speed"] + pet_b["speed"] + base["speed"]) / 3) + random.randint(-2, 3)

    cur.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (cost, interaction.user.id))
    cd_until = (now + timedelta(hours=12)).isoformat()
    cur.execute("UPDATE pets SET breed_cd_until = ? WHERE pet_id IN (?, ?)", (cd_until, pet_a_id, pet_b_id))
    cur.execute(
        """
        INSERT INTO pets(
            owner_id, name, species, gender, rarity, element,
            hp, atk, defense, speed, hunger, mood, health,
            generation, parent_a, parent_b, mutated, is_active, created_at, last_feed
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 100, 100, 100, ?, ?, ?, ?, 0, ?, ?)
        """,
        (
            interaction.user.id,
            child_name,
            species,
            gender,
            rarity,
            element,
            hp,
            atk,
            defense,
            speed,
            max(pet_a["generation"], pet_b["generation"]) + 1,
            pet_a_id,
            pet_b_id,
            mutated,
            now_str(),
            now_str(),
        ),
    )
    conn.commit()

    em = discord.Embed(
        title="🧬 Phối giống thành công",
        description=(
            f"╔════ Pet con mới ════\n"
            f"🐣 Tên: **{child_name}**\n"
            f"🧩 Loài: **{species_name_vi(species)}**\n"
            f"🚻 Giới tính: **{gender}**\n"
            f"🌌 Hệ: **{element}**\n"
            f"{rarity_icon(rarity)} Phẩm chất: **{rarity}**\n"
            f"💸 Phí phối: **{cost} xu**\n"
            f"╚═══════════════"
        ),
        color=0xFF66CC if mutated else 0xAF7AC5,
    )
    em.add_field(name="✨ Kết quả dị biến", value="Pet con đã đột biến!" if mutated else "Pet con sinh ra bình thường.", inline=False)
    await interaction.response.send_message(embed=em)


# =========================
# COMMANDS - CASINO
# =========================
@app_commands.choices(
    side=[
        app_commands.Choice(name="Ngửa", value="ngua"),
        app_commands.Choice(name="Sấp", value="sap"),
    ]
)
@bot.tree.command(name="coinflip", description="Úp ngửa ăn xu")
async def coinflip(interaction: discord.Interaction, bet: int, side: app_commands.Choice[str]):
    user = get_user(interaction.user.id)
    if bet <= 0:
        await interaction.response.send_message(embed=send_error_embed("Tiền cược phải > 0."), ephemeral=True)
        return
    if user["coins"] < bet:
        await interaction.response.send_message(embed=send_error_embed("Bạn không đủ xu."), ephemeral=True)
        return

    result = random.choice(["ngua", "sap"])
    if result == side.value:
        win = int(bet * 0.95)
        cur.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (win, interaction.user.id))
        conn.commit()
        em = discord.Embed(
            title="🪙 Coinflip chiến thắng",
            description=make_box("Kết quả", [f"Lựa chọn: **{side.name}**", f"Tiền thắng: **{win} xu**"]),
            color=0x2ECC71,
        )
        await interaction.response.send_message(embed=em)
    else:
        cur.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (bet, interaction.user.id))
        conn.commit()
        em = discord.Embed(
            title="🪙 Coinflip thất bại",
            description=make_box("Kết quả", [f"Ra mặt: **{'Ngửa' if result == 'ngua' else 'Sấp'}**", f"Tiền thua: **{bet} xu**"]),
            color=0xE74C3C,
        )
        await interaction.response.send_message(embed=em)


class BlackjackView(discord.ui.View):
    def __init__(self, owner_id: int):
        super().__init__(timeout=120)
        self.owner_id = owner_id

    def make_blackjack_embed(self, session: BlackjackSession, reveal_dealer: bool = False, result_text: Optional[str] = None) -> discord.Embed:
        player_score = session.score(session.player)
        dealer_score = session.score(session.dealer)
        dealer_cards = str(session.dealer) if reveal_dealer else f"[{session.dealer[0]}, ?]"
        dealer_value = f"**{dealer_score}**" if reveal_dealer else "?"

        em = discord.Embed(
            title="🃏 Bàn Xì Dách",
            description="Khung giải trí đỏ đen dùng xu trong game.",
            color=0x1ABC9C,
        )
        em.add_field(name="🎴 Bài của bạn", value=f"{session.player}\nTổng điểm: **{player_score}**", inline=True)
        em.add_field(name="🏦 Bài nhà cái", value=f"{dealer_cards}\nTổng điểm: {dealer_value}", inline=True)
        em.add_field(name="💰 Tiền cược", value=f"**{session.bet} xu**", inline=False)
        if result_text:
            em.add_field(name="📢 Kết quả", value=result_text, inline=False)
        em.set_footer(text="Rút để lấy thêm bài • Dằn để so điểm với nhà cái")
        return em

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Đây không phải bàn xì dách của bạn.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Rút", style=discord.ButtonStyle.primary)
    async def hit(self, interaction: discord.Interaction, button: discord.ui.Button):
        session = blackjack_sessions.get(self.owner_id)
        if not session or session.finished:
            await interaction.response.send_message("Ván này đã kết thúc.", ephemeral=True)
            return

        session.player.append(session.draw())
        player_score = session.score(session.player)

        if player_score > 21:
            session.finished = True
            cur.execute(
                "UPDATE users SET coins = coins - ?, blackjack_losses = blackjack_losses + 1 WHERE user_id = ?",
                (session.bet, self.owner_id),
            )
            conn.commit()
            self.disable_all_items()
            await interaction.response.edit_message(
                embed=self.make_blackjack_embed(session, reveal_dealer=True, result_text=f"💥 Quắc! Bạn thua **{session.bet} xu**."),
                view=self,
            )
            return

        await interaction.response.edit_message(
            embed=self.make_blackjack_embed(session, reveal_dealer=False, result_text="Bạn vẫn có thể rút hoặc dằn."),
            view=self,
        )

    @discord.ui.button(label="Dằn", style=discord.ButtonStyle.success)
    async def stand(self, interaction: discord.Interaction, button: discord.ui.Button):
        session = blackjack_sessions.get(self.owner_id)
        if not session or session.finished:
            await interaction.response.send_message("Ván này đã kết thúc.", ephemeral=True)
            return

        session.finished = True
        while session.score(session.dealer) < 17:
            session.dealer.append(session.draw())

        player_score = session.score(session.player)
        dealer_score = session.score(session.dealer)

        if dealer_score > 21 or player_score > dealer_score:
            win = int(session.bet * 1.5)
            cur.execute(
                "UPDATE users SET coins = coins + ?, blackjack_wins = blackjack_wins + 1 WHERE user_id = ?",
                (win, self.owner_id),
            )
            result_text = f"🎉 Bạn thắng **{win} xu**."
        elif player_score == dealer_score:
            result_text = "🤝 Hòa, không mất xu."
        else:
            cur.execute(
                "UPDATE users SET coins = coins - ?, blackjack_losses = blackjack_losses + 1 WHERE user_id = ?",
                (session.bet, self.owner_id),
            )
            result_text = f"💸 Bạn thua **{session.bet} xu**."

        conn.commit()
        self.disable_all_items()
        await interaction.response.edit_message(
            embed=self.make_blackjack_embed(session, reveal_dealer=True, result_text=result_text),
            view=self,
        )


@bot.tree.command(name="blackjack", description="Chơi xì dách")
async def blackjack(interaction: discord.Interaction, bet: int):
    user = get_user(interaction.user.id)
    if bet <= 0:
        await interaction.response.send_message(embed=send_error_embed("Tiền cược phải > 0."), ephemeral=True)
        return
    if user["coins"] < bet:
        await interaction.response.send_message(embed=send_error_embed("Bạn không đủ xu."), ephemeral=True)
        return

    session = BlackjackSession(interaction.user.id, bet)
    blackjack_sessions[interaction.user.id] = session
    player_score = session.score(session.player)

    if player_score == 21:
        win = int(bet * 2)
        cur.execute(
            "UPDATE users SET coins = coins + ?, blackjack_wins = blackjack_wins + 1 WHERE user_id = ?",
            (win, interaction.user.id),
        )
        conn.commit()
        em = discord.Embed(title="🃏 Blackjack!", description="Bạn rút được 21 ngay từ đầu.", color=0x16A085)
        em.add_field(name="🎴 Bài của bạn", value=str(session.player), inline=False)
        em.add_field(name="💰 Tiền thắng", value=f"**{win} xu**", inline=False)
        await interaction.response.send_message(embed=em)
        return

    view = BlackjackView(interaction.user.id)
    await interaction.response.send_message(
        embed=view.make_blackjack_embed(session, reveal_dealer=False, result_text="Chọn rút hoặc dằn."),
        view=view,
        ephemeral=True,
    )


# =========================
# COMMANDS - LEADERBOARD
# =========================
@bot.tree.command(name="top_coins", description="Top người chơi giàu nhất")
async def top_coins(interaction: discord.Interaction):
    cur.execute("SELECT user_id, coins, level FROM users ORDER BY coins DESC LIMIT 10")
    rows = cur.fetchall()
    if not rows:
        await interaction.response.send_message(embed=send_info_embed("Chưa có dữ liệu."), ephemeral=True)
        return

    medals = ["🥇", "🥈", "🥉"]
    em = discord.Embed(title="🏆 Bảng Xếp Hạng Xu", description="Top 10 người chơi giàu nhất hiện tại", color=0xF1C40F)
    text_lines = []
    for i, row in enumerate(rows, start=1):
        user = interaction.guild.get_member(row["user_id"]) if interaction.guild else None
        name = user.display_name if user else f"User {row['user_id']}"
        medal = medals[i - 1] if i <= 3 else f"`#{i}`"
        text_lines.append(f"{medal} **{name}** • 🪙 {row['coins']} • 🎖️ Lv {row['level']}")
    em.add_field(name="📜 BXH", value="\n".join(text_lines), inline=False)
    em.set_footer(text="Chơi chăm để leo top và nhận thưởng")
    await interaction.response.send_message(embed=em)


@bot.tree.command(name="top_pets", description="Top pet mạnh nhất")
async def top_pets(interaction: discord.Interaction):
    cur.execute("SELECT * FROM pets ORDER BY level DESC, atk DESC, hp DESC LIMIT 10")
    rows = cur.fetchall()
    if not rows:
        await interaction.response.send_message(embed=send_info_embed("Chưa có pet nào."), ephemeral=True)
        return

    medals = ["🥇", "🥈", "🥉"]
    em = discord.Embed(title="🐾 Bảng Xếp Hạng Pet", description="Top 10 pet mạnh nhất máy chủ", color=0x9B59B6)
    text_lines = []
    for i, pet in enumerate(rows, start=1):
        user = interaction.guild.get_member(pet["owner_id"]) if interaction.guild else None
        owner_name = user.display_name if user else f"User {pet['owner_id']}"
        medal = medals[i - 1] if i <= 3 else f"`#{i}`"
        text_lines.append(
            f"{medal} **{pet['name']}** ({species_name_vi(pet['species'])}) • ⭐ Lv {pet['level']} • 🔥 {calc_pet_power(pet)} • Chủ: **{owner_name}**"
        )
    em.add_field(name="📜 BXH", value="\n".join(text_lines), inline=False)
    em.set_footer(text="Pet khỏe, chăm đúng giờ và lai tạo tốt sẽ leo top nhanh")
    await interaction.response.send_message(embed=em)


# =========================
# THIÊN ĐẠO / ADMIN
# =========================
@bot.tree.command(name="thien_dao_help", description="Xem các lệnh Thiên Đạo")
async def thien_dao_help(interaction: discord.Interaction):
    if not require_thien_dao(interaction):
        await interaction.response.send_message(embed=send_error_embed("Bạn không có quyền Thiên Đạo."), ephemeral=True)
        return

    em = make_embed(
        "👑 Bảng lệnh Thiên Đạo",
        "Các lệnh quản trị và ban phát quyền năng trong game.",
        0xF4D03F,
    )
    em.add_field(name="🪙 Kinh tế", value="`/admin_give_coin`\n`/admin_set_coin`", inline=True)
    em.add_field(name="🐾 Pet", value="`/admin_set_pet_stat`\n`/admin_heal_pet`", inline=True)
    em.add_field(name="🎁 Vật phẩm", value="`/admin_give_item`", inline=True)
    em.add_field(name="📚 Nhật ký", value="`/admin_logs_view`", inline=True)
    em.add_field(name="📢 Khác", value="`/thien_dao_help`", inline=False)
    await interaction.response.send_message(embed=em, ephemeral=True)


@bot.tree.command(name="admin_give_coin", description="[Thiên Đạo] Ban phát xu cho người chơi")
async def admin_give_coin(interaction: discord.Interaction, user: discord.Member, coins: int):
    if not require_thien_dao(interaction):
        await interaction.response.send_message(embed=send_error_embed("Bạn không có quyền Thiên Đạo."), ephemeral=True)
        return
    if coins <= 0:
        await interaction.response.send_message(embed=send_error_embed("Số xu phải lớn hơn 0."), ephemeral=True)
        return
    get_user(user.id)
    cur.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (coins, user.id))
    conn.commit()
    log_admin_action(interaction.user.id, "give_coin", target_user_id=user.id, details=f"+{coins} coins")
    em = make_embed("👑 Thiên Đạo ban phát", f"Đã cộng **{coins} xu** cho {user.mention}.", 0xF1C40F)
    await interaction.response.send_message(embed=em)


@bot.tree.command(name="admin_set_coin", description="[Thiên Đạo] Đặt số xu cho người chơi")
async def admin_set_coin(interaction: discord.Interaction, user: discord.Member, coins: int):
    if not require_thien_dao(interaction):
        await interaction.response.send_message(embed=send_error_embed("Bạn không có quyền Thiên Đạo."), ephemeral=True)
        return
    if coins < 0:
        await interaction.response.send_message(embed=send_error_embed("Số xu không được âm."), ephemeral=True)
        return
    get_user(user.id)
    cur.execute("UPDATE users SET coins = ? WHERE user_id = ?", (coins, user.id))
    conn.commit()
    log_admin_action(interaction.user.id, "set_coin", target_user_id=user.id, details=f"set to {coins}")
    em = make_embed("⚖️ Thiên Đạo định mệnh", f"Đã đặt xu của {user.mention} thành **{coins}**.", 0xF39C12)
    await interaction.response.send_message(embed=em)


@bot.tree.command(name="admin_give_item", description="[Thiên Đạo] Ban phát vật phẩm")
async def admin_give_item(interaction: discord.Interaction, user: discord.Member, item_name: str, amount: int):
    if not require_thien_dao(interaction):
        await interaction.response.send_message(embed=send_error_embed("Bạn không có quyền Thiên Đạo."), ephemeral=True)
        return
    if amount <= 0:
        await interaction.response.send_message(embed=send_error_embed("Số lượng phải lớn hơn 0."), ephemeral=True)
        return
    get_user(user.id)
    add_item(user.id, item_name.lower(), amount)
    log_admin_action(interaction.user.id, "give_item", target_user_id=user.id, details=f"{item_name} x{amount}")
    em = make_embed("🎁 Ban phát vật phẩm", f"Đã trao **{amount} {item_name}** cho {user.mention}.", 0x8E44AD)
    await interaction.response.send_message(embed=em)


@bot.tree.command(name="admin_heal_pet", description="[Thiên Đạo] Hồi phục pet")
async def admin_heal_pet(interaction: discord.Interaction, owner: discord.Member, pet_id: int):
    if not require_thien_dao(interaction):
        await interaction.response.send_message(embed=send_error_embed("Bạn không có quyền Thiên Đạo."), ephemeral=True)
        return
    cur.execute("SELECT * FROM pets WHERE owner_id = ? AND pet_id = ?", (owner.id, pet_id))
    pet = cur.fetchone()
    if not pet:
        await interaction.response.send_message(embed=send_error_embed("Không tìm thấy pet cần hồi phục."), ephemeral=True)
        return
    cur.execute("UPDATE pets SET hunger = 100, mood = 100, health = 100 WHERE pet_id = ?", (pet_id,))
    conn.commit()
    log_admin_action(interaction.user.id, "heal_pet", target_user_id=owner.id, target_pet_id=pet_id, details="full restore")
    em = make_embed("💚 Thiên Đạo hồi phục", f"Đã hồi đầy trạng thái cho pet **{pet['name']}** của {owner.mention}.", 0x58D68D)
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
@bot.tree.command(name="admin_set_pet_stat", description="[Thiên Đạo] Chỉnh chỉ số pet")
async def admin_set_pet_stat(
    interaction: discord.Interaction,
    owner: discord.Member,
    pet_id: int,
    stat: app_commands.Choice[str],
    value: int,
):
    if not require_thien_dao(interaction):
        await interaction.response.send_message(embed=send_error_embed("Bạn không có quyền Thiên Đạo."), ephemeral=True)
        return

    allowed_stats = {"hp", "atk", "defense", "speed", "hunger", "mood", "health", "bond", "level"}
    if stat.value not in allowed_stats:
        await interaction.response.send_message(embed=send_error_embed("Chỉ số không hợp lệ."), ephemeral=True)
        return

    cur.execute("SELECT * FROM pets WHERE owner_id = ? AND pet_id = ?", (owner.id, pet_id))
    pet = cur.fetchone()
    if not pet:
        await interaction.response.send_message(embed=send_error_embed("Không tìm thấy pet cần chỉnh."), ephemeral=True)
        return

    cur.execute(f"UPDATE pets SET {stat.value} = ? WHERE pet_id = ?", (value, pet_id))
    conn.commit()
    log_admin_action(interaction.user.id, "set_pet_stat", target_user_id=owner.id, target_pet_id=pet_id, details=f"{stat.value}={value}")
    em = make_embed(
        "🛠️ Thiên Đạo chỉnh sửa",
        f"Đã chỉnh **{stat.name}** của pet **{pet['name']}** thành **{value}**.",
        0xEB984E,
    )
    await interaction.response.send_message(embed=em)


@bot.tree.command(name="admin_logs_view", description="[Thiên Đạo] Xem nhật ký admin gần nhất")
async def admin_logs_view(interaction: discord.Interaction, limit: Optional[int] = 10):
    if not require_thien_dao(interaction):
        await interaction.response.send_message(embed=send_error_embed("Bạn không có quyền Thiên Đạo."), ephemeral=True)
        return

    limit = max(1, min(limit or 10, 20))
    cur.execute("SELECT * FROM admin_logs ORDER BY log_id DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    if not rows:
        await interaction.response.send_message(embed=send_info_embed("Chưa có nhật ký Thiên Đạo nào."), ephemeral=True)
        return

    lines = []
    for row in rows:
        admin_member = interaction.guild.get_member(row["admin_id"]) if interaction.guild else None
        admin_name = admin_member.display_name if admin_member else f"Admin {row['admin_id']}"
        lines.append(
            f"• **{admin_name}** → `{row['action']}` | user: `{row['target_user_id']}` | pet: `{row['target_pet_id']}` | {row['details']}"
        )

    em = discord.Embed(title="📚 Nhật ký Thiên Đạo", description="\n".join(lines), color=0x5D6D7E)
    em.set_footer(text="Chỉ hiển thị các thao tác gần nhất")
    await interaction.response.send_message(embed=em, ephemeral=True)


# =========================
# RUN
# =========================
bot.run(TOKEN)

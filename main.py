"""
Weather Telegram Bot - Production Ready with Debug
"""

import os
import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import aiohttp
from pyrogram import Client, filters
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ============ CONFIG ============
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
CHANNEL_ID = os.environ.get("CHANNEL_ID", "")
WEATHER_API_KEY = os.environ.get("WEATHER_API_KEY", "")

TEHRAN_LAT = 35.6892
TEHRAN_LON = 51.3890
TEHRAN_TZ = ZoneInfo("Asia/Tehran")

SCHEDULE_HOUR = int(os.environ.get("SCHEDULE_HOUR", "8"))
SCHEDULE_MINUTE = int(os.environ.get("SCHEDULE_MINUTE", "0"))

# Allowed users for bot commands (your Telegram user ID)
ALLOWED_USERS = [int(x) for x in os.environ.get("ALLOWED_USERS", "").split(",") if x.strip()]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("weather-bot")


app = Client(
    name="weather-bot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
)


# ============ BOT COMMANDS (for debugging) ============
@app.on_message(filters.command("start") & filters.user(ALLOWED_USERS))
async def cmd_start(client, message):
    await message.reply_text(
        f"✅ ربات فعال است.\n"
        f"🕒 ساعت فعلی سرور: {datetime.now().isoformat()}\n"
        f"🕒 ساعت تهران: {datetime.now(TEHRAN_TZ).strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"⏰ زمان ارسال گزارش: {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d}\n"
        f"📢 کانال مقصد: `{CHANNEL_ID}`"
    )


@app.on_message(filters.command("weather") & filters.user(ALLOWED_USERS))
async def cmd_weather(client, message):
    await message.reply_text("⏳ در حال دریافت اطلاعات آب‌وهوا...")
    try:
        report = await build_weather_report()
        await message.reply_text(report)
    except Exception as e:
        log.exception("Error in /weather command")
        await message.reply_text(f"❌ خطا: {e}")


@app.on_message(filters.command("test") & filters.user(ALLOWED_USERS))
async def cmd_test(client, message):
    """Test sending to channel."""
    try:
        me = await client.get_me()
        await message.reply_text(f"✅ ربات فعال است. اکانت: {me.first_name}")
        await client.send_message(CHANNEL_ID, "🧪 <b>تست ارسال به کانال موفقیت‌آمیز بود.</b>")
        await message.reply_text(f"✅ پیام تست به کانال {CHANNEL_ID} ارسال شد.")
    except Exception as e:
        log.exception("Test send failed")
        await message.reply_text(f"❌ خطا در ارسال به کانال: {e}")


# ============ WEATHER FETCHERS ============
async def fetch_open_meteo(session: aiohttp.ClientSession) -> dict:
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={TEHRAN_LAT}&longitude={TEHRAN_LON}"
        "&current=temperature_2m,relative_humidity_2m,weather_code,wind_speed_10m"
        "&daily=temperature_2m_max,temperature_2m_min,sunrise,sunset,uv_index_max"
        "&timezone=Asia%2FTehran&forecast_days=1"
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                return {"source": "Open-Meteo", "data": await resp.json(), "ok": True}
            return {"source": "Open-Meteo", "ok": False, "error": f"HTTP {resp.status}"}
    except Exception as e:
        return {"source": "Open-Meteo", "ok": False, "error": str(e)}


async def fetch_open_meteo_air_quality(session: aiohttp.ClientSession) -> dict:
    url = (
        "https://air-quality-api.open-meteo.com/v1/air-quality"
        f"?latitude={TEHRAN_LAT}&longitude={TEHRAN_LON}"
        "&current=us_aqi,pm2_5,pm10&timezone=Asia%2FTehran"
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                return {"source": "Open-Meteo AQI", "data": await resp.json(), "ok": True}
            return {"source": "Open-Meteo AQI", "ok": False, "error": f"HTTP {resp.status}"}
    except Exception as e:
        return {"source": "Open-Meteo AQI", "ok": False, "error": str(e)}


async def fetch_wttr(session: aiohttp.ClientSession) -> dict:
    url = "https://wttr.in/Tehran?format=j1"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                return {"source": "wttr.in", "data": await resp.json(), "ok": True}
            return {"source": "wttr.in", "ok": False, "error": f"HTTP {resp.status}"}
    except Exception as e:
        return {"source": "wttr.in", "ok": False, "error": str(e)}


# ============ WEATHER CODES ============
WMO_CODES = {
    0: ("☀️ آفتابی صاف", "☀️"), 1: ("🌤️ عمدتاً آفتابی", "🌤️"),
    2: ("⛅ نیمه‌ابری", "⛅"), 3: ("☁️ ابری", "☁️"),
    45: ("🌫️ مه‌آلود", "🌫️"), 48: ("🌫️ مه یخ‌زده", "🌫️"),
    51: ("🌦️ نم‌نم باران خفیف", "🌦️"), 53: ("🌦️ نم‌نم باران", "🌦️"),
    55: ("🌧️ نم‌نم باران شدید", "🌧️"), 61: ("🌧️ باران خفیف", "🌧️"),
    63: ("🌧️ باران متوسط", "🌧️"), 65: ("🌧️ باران شدید", "🌧️"),
    71: ("🌨️ برف خفیف", "🌨️"), 73: ("🌨️ برف متوسط", "🌨️"),
    75: ("❄️ برف شدید", "❄️"), 80: ("🌦️ رگبار خفیف", "🌦️"),
    81: ("🌧️ رگبار متوسط", "🌧️"), 82: ("⛈️ رگبار شدید", "⛈️"),
    95: ("⛈️ رعدوبرق", "⛈️"), 96: ("⛈️ رعدوبرق با تگرگ", "⛈️"),
    99: ("⛈️ رعدوبرق شدید", "⛈️"),
}


def aqi_level(v):
    if v <= 50: return ("پاک", "🟢")
    if v <= 100: return ("متوسط", "🟡")
    if v <= 150: return ("ناسالم برای گروه‌های حساس", "🟠")
    if v <= 200: return ("ناسالم", "🔴")
    if v <= 300: return ("بسیار ناسالم", "🟣")
    return ("خطرناک", "🟤")


# ============ REPORT BUILDER ============
async def build_weather_report() -> str:
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            fetch_open_meteo(session),
            fetch_open_meteo_air_quality(session),
            fetch_wttr(session),
            return_exceptions=True,
        )

    ok = [r for r in results if isinstance(r, dict) and r.get("ok")]
    for f in results:
        if isinstance(f, dict) and not f.get("ok"):
            log.warning(f"Source failed: {f.get('source')} -> {f.get('error')}")

    if not ok:
        return "❌ <b>خطا در دریافت اطلاعات آب‌وهوا</b>\nهیچ منبعی در دسترس نبود."

    open_meteo_data = aqi_data = wttr_data = None
    for r in ok:
        if r["source"] == "Open-Meteo": open_meteo_data = r["data"]
        elif r["source"] == "Open-Meteo AQI": aqi_data = r["data"]
        elif r["source"] == "wttr.in": wttr_data = r["data"]

    # Temps
    temps = []
    if open_meteo_data:
        try:
            temps.append({
                "cur": open_meteo_data["current"]["temperature_2m"],
                "min": open_meteo_data["daily"]["temperature_2m_min"][0],
                "max": open_meteo_data["daily"]["temperature_2m_max"][0],
            })
        except Exception: pass
    if wttr_data:
        try:
            c = wttr_data["current_condition"][0]
            t = wttr_data["weather"][0]
            temps.append({"cur": float(c["temp_C"]), "min": float(t["mintempC"]), "max": float(t["maxtempC"])})
        except Exception: pass

    if temps:
        avg_cur = sum(t["cur"] for t in temps) / len(temps)
        avg_min = sum(t["min"] for t in temps) / len(temps)
        avg_max = sum(t["max"] for t in temps) / len(temps)
    else:
        avg_cur = avg_min = avg_max = None

    # Humidity & Wind
    hum, wind = [], []
    if open_meteo_data:
        try:
            hum.append(open_meteo_data["current"]["relative_humidity_2m"])
            wind.append(open_meteo_data["current"]["wind_speed_10m"])
        except Exception: pass
    if wttr_data:
        try:
            c = wttr_data["current_condition"][0]
            hum.append(int(c["humidity"]))
            wind.append(float(c["windspeedKmph"]))
        except Exception: pass

    avg_hum = sum(hum) / len(hum) if hum else None
    avg_wind = sum(wind) / len(wind) if wind else None

    # Condition
    condition, emoji = "🌡️ نامشخص", "🌡️"
    if open_meteo_data:
        try:
            code = open_meteo_data["current"]["weather_code"]
            condition, emoji = WMO_CODES.get(code, ("🌡️ نامشخص", "🌡️"))
        except Exception: pass

    # AQI
    aqi_value = pm25 = pm10 = None
    aqi_label = aqi_emoji = None
    if aqi_data:
        try:
            c = aqi_data["current"]
            aqi_value = c.get("us_aqi")
            if aqi_value is not None: aqi_label, aqi_emoji = aqi_level(aqi_value)
            pm25 = c.get("pm2_5"); pm10 = c.get("pm10")
        except Exception: pass

    # Sunrise/Sunset/UV
    sunrise = sunset = uv = None
    if open_meteo_data:
        try:
            sunrise = open_meteo_data["daily"]["sunrise"][0].split("T")[1]
            sunset = open_meteo_data["daily"]["sunset"][0].split("T")[1]
            uv = open_meteo_data["daily"]["uv_index_max"][0]
        except Exception: pass

    # Analysis
    if avg_cur is not None:
        if avg_cur >= 35: analysis = "🔥 هوای گرم؛ مصرف آب را فراموش نکنید."
        elif avg_cur >= 25: analysis = "🌡️ هوای معتدل و دلپذیر."
        elif avg_cur >= 15: analysis = "🍃 هوای خنک؛ لباس گرم همراه داشته باشید."
        else: analysis = "❄️ هوای سرد؛ از پوشش مناسب استفاده کنید."
    else:
        analysis = "📊 اطلاعات کافی نبود."

    now = datetime.now(TEHRAN_TZ)
    lines = [
        f"🌆 <b>گزارش آب‌وهوای تهران</b>",
        f"📅 <i>{now.strftime('%Y/%m/%d')}</i>  ⏰ <i>{now.strftime('%H:%M')}</i>",
        "━━━━━━━━━━━━━━━━━━━━━━", "",
        f"{emoji} <b>وضعیت کلی:</b> {condition}", "",
        "🌡️ <b>دما:</b>",
    ]
    if avg_cur is not None:
        lines.append(f"   • لحظه‌ای: <b>{avg_cur:.1f}°C</b>")
        lines.append(f"   • حداقل: <b>{avg_min:.1f}°C</b>  |  حداکثر: <b>{avg_max:.1f}°C</b>")
    lines.append("")
    if avg_hum is not None: lines.append(f"💧 <b>رطوبت:</b> {avg_hum:.0f}%")
    if avg_wind is not None: lines.append(f"💨 <b>سرعت باد:</b> {avg_wind:.1f} km/h")
    lines.append("")
    if aqi_value is not None:
        lines.append(f"🫁 <b>کیفیت هوا:</b> {aqi_emoji} <b>{int(aqi_value)}</b> — {aqi_label}")
        if pm25 is not None: lines.append(f"   • PM2.5: <code>{pm25:.1f}</code> µg/m³")
        if pm10 is not None: lines.append(f"   • PM10: <code>{pm10:.1f}</code> µg/m³")
        lines.append("")
    if sunrise: lines.append(f"🌅 <b>طلوع:</b> {sunrise}   |   🌇 <b>غروب:</b> {sunset}")
    if uv is not None: lines.append(f"🔆 <b>شاخص UV:</b> {uv:.1f}")
    lines += ["", "━━━━━━━━━━━━━━━━━━━━━━",
              f"📝 <b>تحلیل:</b> {analysis}", "",
              f"🔗 <i>منابع: {len(temps)} سرویس</i>",
              "🤖 <i>ارسال خودکار</i>"]
    return "\n".join(lines)


# ============ SCHEDULED JOB ============
async def send_daily_report():
    log.info("🔔 Running daily weather report job...")
    try:
        if not app.is_connected:
            log.info("Reconnecting client...")
            await app.start()
        report = await build_weather_report()
        log.info(f"Sending report to {CHANNEL_ID} ({len(report)} chars)")
        await app.send_message(CHANNEL_ID, report, disable_web_page_preview=True)
        log.info("✅ Weather report sent successfully.")
    except Exception as e:
        log.exception(f"❌ Failed to send weather report: {e}")


async def heartbeat():
    """Keep Railway worker alive and log status."""
    log.info(f"💓 Heartbeat - {datetime.now(TEHRAN_TZ).strftime('%Y-%m-%d %H:%M:%S')} Tehran")


# ============ MAIN ============
async def main():
    log.info("=" * 50)
    log.info("🚀 Starting Weather Bot...")
    log.info(f"API_ID: {API_ID}")
    log.info(f"CHANNEL_ID: {CHANNEL_ID}")
    log.info(f"Schedule: {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} Tehran")
    log.info("=" * 50)

    try:
        await app.start()
        me = await app.get_me()
        log.info(f"✅ Logged in as: {me.first_name} (ID: {me.id})")
    except Exception as e:
        log.exception(f"❌ Login failed: {e}")
        return

    # Scheduler
    scheduler = AsyncIOScheduler(timezone=TEHRAN_TZ)
    scheduler.add_job(
        send_daily_report,
        trigger=CronTrigger(hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE),
        id="daily_weather",
        replace_existing=True,
    )
    # Heartbeat every 5 minutes
    scheduler.add_job(heartbeat, "interval", minutes=5, id="heartbeat")
    scheduler.start()

    jobs = scheduler.get_jobs()
    for j in jobs:
        log.info(f"📅 Job scheduled: {j.id} -> next run: {j.next_run_time}")

    # Optional: send test on start
    if os.environ.get("SEND_ON_START", "0") == "1":
        log.info("SEND_ON_START=1 -> sending test report now")
        await send_daily_report()

    # Idle
    log.info("🟢 Bot is running. Press Ctrl+C to stop.")
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        log.info("Shutting down...")
    finally:
        scheduler.shutdown(wait=False)
        await app.stop()


if __name__ == "__main__":
    asyncio.run(main())

"""
Tehran Weather Reporter — Telegram Userbot (Pyrogram)
=============================================================
به‌صورت پیش‌فرض هر ۱ ساعت یک‌بار (قابل تنظیم با REPORT_INTERVAL_HOURS)،
وضعیت آب‌وهوای تهران را از چند منبع مستقل دریافت می‌کند، آن‌ها را با هم
مقایسه/ترکیب کرده و یک گزارش زیبا (HTML) به کانال تلگرامی مشخص‌شده ارسال
می‌کند. همچنین می‌توان با REPORT_MODE=daily آن را به حالت «یک‌بار در روز
در ساعت مشخص» تغییر داد.

منابع داده:
  1) Open-Meteo (Forecast API)      -> دمای حداقل/حداکثر، دمای لحظه‌ای، وضعیت هوا، رطوبت، باد [بدون کلید]
  2) Open-Meteo (Air Quality API)   -> شاخص کیفیت هوا AQI                                      [بدون کلید]
  3) wttr.in                        -> منبع دوم برای صحت‌سنجی دما/وضعیت (شامل دمای لحظه‌ای)      [بدون کلید]
  4) MET Norway / Yr (Locationforecast) -> منبع سوم مستقل برای صحت‌سنجی دما/رطوبت/باد            [بدون کلید]
  5) WeatherAPI.com (اختیاری)       -> منبع چهارم، فقط اگر WEATHER_API_KEY تنظیم شده باشد

قابلیت‌های اضافی (هرکدام با env var قابل خاموش/روشن کردن):
  - 📊 نمودار روند دمای امروز (تصویر PNG ارسالی به کانال قبل از متن گزارش)
  - 🚨 هشدار فوری و مستقل برای گرمای شدید / سرمای شدید / آلودگی هوای خطرناک
  - 🔮 پیش‌بینی خلاصهٔ ۲ روز آینده در انتهای گزارش
  - 🔁 مقایسهٔ دمای لحظه‌ای با همین ساعت دیروز (بر اساس تاریخچهٔ محلی SQLite)

اجرا روی Railway به عنوان Worker (نه Web Service) — رجوع کنید به Procfile.
"""

import os
import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta
from statistics import mean

import pytz
import aiohttp
import matplotlib
matplotlib.use("Agg")  # بدون نیاز به نمایشگر، مناسب سرور
import matplotlib.pyplot as plt
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from pyrogram import Client
from pyrogram.enums import ParseMode

# ---------------------------------------------------------------------------
# پیکربندی از طریق Environment Variables
# ---------------------------------------------------------------------------
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_STRING = os.environ["SESSION_STRING"]
CHANNEL_ID = os.environ["CHANNEL_ID"]  # مثلاً "-1001234567890" یا "@my_channel"
WEATHER_API_KEY = os.environ.get("WEATHER_API_KEY")  # اختیاری

# حالت زمان‌بندی گزارش:
#   "interval" -> هر N ساعت یک‌بار ارسال می‌شود (پیش‌فرض، هر ۱ ساعت)
#   "daily"    -> فقط یک‌بار در روز، در ساعت مشخص، ارسال می‌شود
REPORT_MODE = os.environ.get("REPORT_MODE", "interval").lower()

# برای حالت interval: هر چند ساعت یک‌بار گزارش ارسال شود (پیش‌فرض: هر ۱ ساعت)
REPORT_INTERVAL_HOURS = int(os.environ.get("REPORT_INTERVAL_HOURS", 1))

# برای حالت daily: ساعت و دقیقه ارسال گزارش (به وقت تهران)
REPORT_HOUR = int(os.environ.get("REPORT_HOUR", 8))
REPORT_MINUTE = int(os.environ.get("REPORT_MINUTE", 0))

# اگر True باشد، بلافاصله پس از بالا آمدن ربات یک گزارش تست ارسال می‌شود
RUN_ON_START = os.environ.get("RUN_ON_START", "false").lower() == "true"

# --- ویژگی‌های اضافی (هرکدام قابل خاموش/روشن‌کردن) ---
ENABLE_CHART = os.environ.get("ENABLE_CHART", "true").lower() == "true"
ENABLE_ALERTS = os.environ.get("ENABLE_ALERTS", "true").lower() == "true"
ENABLE_HISTORY = os.environ.get("ENABLE_HISTORY", "true").lower() == "true"

# آستانه‌های هشدار فوری (مستقل از زمان‌بندی معمول گزارش روزانه)
AQI_ALERT_THRESHOLD = int(os.environ.get("AQI_ALERT_THRESHOLD", 100))
HEAT_ALERT_C = float(os.environ.get("HEAT_ALERT_C", 38))
COLD_ALERT_C = float(os.environ.get("COLD_ALERT_C", -5))

# مسیر فایل دیتابیس تاریخچهٔ ساده (برای مقایسه با دیروز)
HISTORY_DB_PATH = os.environ.get("HISTORY_DB_PATH", "weather_history.db")

TEHRAN_LAT, TEHRAN_LON = 35.6892, 51.3890
TIMEZONE = pytz.timezone("Asia/Tehran")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("weather-bot")

# ---------------------------------------------------------------------------
# جدول تبدیل کد وضعیت هوا (WMO) به متن و ایموجی
# ---------------------------------------------------------------------------
WMO_CODES = {
    0: ("آسمان صاف", "☀️"),
    1: ("عمدتاً صاف", "🌤️"),
    2: ("نیمه‌ابری", "⛅"),
    3: ("ابری", "☁️"),
    45: ("مه‌آلود", "🌫️"),
    48: ("مه یخ‌زده", "🌫️"),
    51: ("نم‌نم باران سبک", "🌦️"),
    53: ("نم‌نم باران", "🌦️"),
    55: ("نم‌نم باران شدید", "🌧️"),
    56: ("باران یخ‌زدهٔ سبک", "🌧️"),
    57: ("باران یخ‌زدهٔ شدید", "🌧️"),
    61: ("باران سبک", "🌧️"),
    63: ("باران متوسط", "🌧️"),
    65: ("باران شدید", "🌧️"),
    66: ("باران یخ‌زدهٔ سبک", "🌧️"),
    67: ("باران یخ‌زدهٔ شدید", "🌧️"),
    71: ("برف سبک", "❄️"),
    73: ("برف متوسط", "❄️"),
    75: ("برف شدید", "❄️"),
    77: ("دانه‌های برف", "❄️"),
    80: ("رگبار سبک", "🌦️"),
    81: ("رگبار متوسط", "🌧️"),
    82: ("رگبار شدید", "⛈️"),
    85: ("رگبار برف سبک", "🌨️"),
    86: ("رگبار برف شدید", "🌨️"),
    95: ("رعدوبرق", "⛈️"),
    96: ("رعدوبرق همراه با تگرگ سبک", "⛈️"),
    99: ("رعدوبرق همراه با تگرگ شدید", "⛈️"),
}


def describe_weather_code(code):
    return WMO_CODES.get(code, ("نامشخص", "❔"))


def describe_aqi(aqi):
    """European AQI scale used by Open-Meteo."""
    if aqi is None:
        return "نامشخص", "❔"
    if aqi <= 20:
        return "خوب", "🟢"
    if aqi <= 40:
        return "قابل قبول", "🟡"
    if aqi <= 60:
        return "متوسط", "🟠"
    if aqi <= 80:
        return "ناسالم برای گروه‌های حساس", "🔴"
    if aqi <= 100:
        return "ناسالم", "🟣"
    return "بسیار ناسالم/خطرناک", "🟤"


# ---------------------------------------------------------------------------
# دریافت داده از منابع مختلف
# ---------------------------------------------------------------------------
async def fetch_open_meteo(session: aiohttp.ClientSession):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": TEHRAN_LAT,
        "longitude": TEHRAN_LON,
        "daily": "temperature_2m_max,temperature_2m_min,weathercode,"
        "relative_humidity_2m_mean,wind_speed_10m_max",
        "current": "temperature_2m,apparent_temperature,relative_humidity_2m,"
        "wind_speed_10m,weather_code",
        "timezone": "Asia/Tehran",
        "forecast_days": 1,
    }
    try:
        async with session.get(url, params=params, timeout=15) as resp:
            resp.raise_for_status()
            data = await resp.json()
            daily = data["daily"]
            current = data.get("current", {})
            return {
                "source": "Open-Meteo",
                "temp_max": daily["temperature_2m_max"][0],
                "temp_min": daily["temperature_2m_min"][0],
                "weather_code": daily["weathercode"][0],
                "humidity": daily.get("relative_humidity_2m_mean", [None])[0],
                "wind": daily.get("wind_speed_10m_max", [None])[0],
                "current_temp": current.get("temperature_2m"),
                "current_feels": current.get("apparent_temperature"),
                "current_humidity": current.get("relative_humidity_2m"),
                "current_wind": current.get("wind_speed_10m"),
            }
    except Exception as e:
        logger.warning(f"Open-Meteo forecast fetch failed: {e}")
        return None


async def fetch_air_quality(session: aiohttp.ClientSession):
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    params = {
        "latitude": TEHRAN_LAT,
        "longitude": TEHRAN_LON,
        "current": "european_aqi",
        "timezone": "Asia/Tehran",
    }
    try:
        async with session.get(url, params=params, timeout=15) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data.get("current", {}).get("european_aqi")
    except Exception as e:
        logger.warning(f"Air quality fetch failed: {e}")
        return None


async def fetch_wttr(session: aiohttp.ClientSession):
    """منبع دوم و مستقل برای صحت‌سنجی."""
    url = "https://wttr.in/Tehran"
    params = {"format": "j1"}
    try:
        async with session.get(url, params=params, timeout=15) as resp:
            resp.raise_for_status()
            data = await resp.json()
            today = data["weather"][0]
            current = data["current_condition"][0]
            return {
                "source": "wttr.in",
                "temp_max": float(today["maxtempC"]),
                "temp_min": float(today["mintempC"]),
                "condition": current["weatherDesc"][0]["value"],
                "humidity": float(current["humidity"]),
                "wind_kmph": float(current["windspeedKmph"]),
                "current_temp": float(current["temp_C"]),
                "current_feels": float(current.get("FeelsLikeC", current["temp_C"])),
            }
    except Exception as e:
        logger.warning(f"wttr.in fetch failed: {e}")
        return None


async def fetch_weatherapi(session: aiohttp.ClientSession):
    """منبع اختیاری — فقط اگر WEATHER_API_KEY تنظیم شده باشد."""
    if not WEATHER_API_KEY:
        return None
    url = "https://api.weatherapi.com/v1/forecast.json"
    params = {"key": WEATHER_API_KEY, "q": "Tehran", "days": 1, "aqi": "no"}
    try:
        async with session.get(url, params=params, timeout=15) as resp:
            resp.raise_for_status()
            data = await resp.json()
            day = data["forecast"]["forecastday"][0]["day"]
            current = data.get("current", {})
            return {
                "source": "WeatherAPI",
                "temp_max": day["maxtemp_c"],
                "temp_min": day["mintemp_c"],
                "condition": day["condition"]["text"],
                "humidity": day.get("avghumidity"),
                "wind_kmph": day.get("maxwind_kph"),
                "current_temp": current.get("temp_c"),
                "current_feels": current.get("feelslike_c"),
            }
    except Exception as e:
        logger.warning(f"WeatherAPI fetch failed: {e}")
        return None


async def fetch_met_norway(session: aiohttp.ClientSession):
    """
    منبع مستقل چهارم: MET Norway / Yr.no (Locationforecast API).
    رایگان و بدون نیاز به کلید، اما طبق قوانین سرویس باید یک هدر
    User-Agent معتبر و شناسا ارسال شود.
    """
    url = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
    params = {"lat": TEHRAN_LAT, "lon": TEHRAN_LON}
    headers = {"User-Agent": "TehranWeatherReporterBot/1.0 (contact: set-your-email-here)"}
    try:
        async with session.get(url, params=params, headers=headers, timeout=15) as resp:
            resp.raise_for_status()
            data = await resp.json()
            details = data["properties"]["timeseries"][0]["data"]["instant"]["details"]
            wind_ms = details.get("wind_speed")
            return {
                "source": "MET Norway (Yr)",
                "current_temp": details.get("air_temperature"),
                "current_humidity": details.get("relative_humidity"),
                "current_wind": round(wind_ms * 3.6, 1) if wind_ms is not None else None,  # m/s -> km/h
            }
    except Exception as e:
        logger.warning(f"MET Norway fetch failed: {e}")
        return None


async def fetch_hourly_forecast(session: aiohttp.ClientSession):
    """دمای ساعتی امروز — برای رسم نمودار روند دما."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": TEHRAN_LAT,
        "longitude": TEHRAN_LON,
        "hourly": "temperature_2m",
        "timezone": "Asia/Tehran",
        "forecast_days": 1,
    }
    try:
        async with session.get(url, params=params, timeout=15) as resp:
            resp.raise_for_status()
            data = await resp.json()
            hourly = data["hourly"]
            return {"times": hourly["time"], "temps": hourly["temperature_2m"]}
    except Exception as e:
        logger.warning(f"Hourly forecast fetch failed: {e}")
        return None


async def fetch_3day_forecast(session: aiohttp.ClientSession):
    """پیش‌بینی خلاصهٔ ۳ روز آینده."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": TEHRAN_LAT,
        "longitude": TEHRAN_LON,
        "daily": "temperature_2m_max,temperature_2m_min,weathercode",
        "timezone": "Asia/Tehran",
        "forecast_days": 3,
    }
    try:
        async with session.get(url, params=params, timeout=15) as resp:
            resp.raise_for_status()
            data = await resp.json()
            daily = data["daily"]
            days = []
            for i in range(len(daily["time"])):
                days.append({
                    "date": daily["time"][i],
                    "temp_max": daily["temperature_2m_max"][i],
                    "temp_min": daily["temperature_2m_min"][i],
                    "weather_code": daily["weathercode"][i],
                })
            return days
    except Exception as e:
        logger.warning(f"3-day forecast fetch failed: {e}")
        return None


# ---------------------------------------------------------------------------
# نمودار روند دما (تصویر)
# ---------------------------------------------------------------------------
def build_temperature_chart(hourly_data, path="chart_temp.png"):
    """یک نمودار PNG ساده از روند دمای امروز می‌سازد و مسیر فایل را برمی‌گرداند."""
    if not hourly_data or not hourly_data.get("times"):
        return None
    try:
        times = [t.split("T")[1] for t in hourly_data["times"]]  # فقط ساعت
        temps = hourly_data["temps"]

        plt.figure(figsize=(9, 4))
        plt.plot(times, temps, color="#ff7f0e", linewidth=2.5, marker="o", markersize=3)
        plt.fill_between(range(len(times)), temps, color="#ff7f0e", alpha=0.12)
        plt.title("روند دمای امروز - تهران (°C)", fontsize=13)
        plt.xticks(range(0, len(times), 2), [times[i] for i in range(0, len(times), 2)], rotation=45)
        plt.grid(alpha=0.25)
        plt.tight_layout()
        plt.savefig(path, dpi=130)
        plt.close()
        return path
    except Exception as e:
        logger.warning(f"Chart generation failed: {e}")
        return None


# ---------------------------------------------------------------------------
# تاریخچهٔ ساده (برای مقایسه با دیروز)
# ---------------------------------------------------------------------------
def init_history_db():
    if not ENABLE_HISTORY:
        return
    conn = sqlite3.connect(HISTORY_DB_PATH)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS history (
            ts TEXT PRIMARY KEY,
            current_temp REAL,
            temp_max REAL,
            temp_min REAL,
            aqi REAL
        )"""
    )
    conn.commit()
    conn.close()


def save_history(current_temp, temp_max, temp_min, aqi):
    if not ENABLE_HISTORY:
        return
    try:
        conn = sqlite3.connect(HISTORY_DB_PATH)
        conn.execute(
            "INSERT OR REPLACE INTO history (ts, current_temp, temp_max, temp_min, aqi) VALUES (?, ?, ?, ?, ?)",
            (datetime.now(TIMEZONE).isoformat(), current_temp, temp_max, temp_min, aqi),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"Saving history failed: {e}")


def get_yesterday_comparison(current_temp):
    """میانگین دمای لحظه‌ای حدوداً ۲۴ ساعت قبل را برمی‌گرداند تا با امروز مقایسه شود."""
    if not ENABLE_HISTORY or current_temp is None:
        return None
    try:
        conn = sqlite3.connect(HISTORY_DB_PATH)
        now = datetime.now(TIMEZONE)
        window_start = (now - timedelta(hours=25)).isoformat()
        window_end = (now - timedelta(hours=23)).isoformat()
        rows = conn.execute(
            "SELECT current_temp FROM history WHERE ts BETWEEN ? AND ? AND current_temp IS NOT NULL",
            (window_start, window_end),
        ).fetchall()
        conn.close()
        if not rows:
            return None
        yesterday_avg = mean([r[0] for r in rows])
        diff = round(current_temp - yesterday_avg, 1)
        return diff
    except Exception as e:
        logger.warning(f"Yesterday comparison failed: {e}")
        return None


# ---------------------------------------------------------------------------
# ترکیب و ساخت گزارش نهایی
# ---------------------------------------------------------------------------
def build_report(open_meteo, aqi, wttr, weatherapi, met_norway, forecast_3day=None, yesterday_diff=None) -> str:
    now = datetime.now(TIMEZONE)
    weekday_fa = {
        0: "دوشنبه", 1: "سه‌شنبه", 2: "چهارشنبه", 3: "پنجشنبه",
        4: "جمعه", 5: "شنبه", 6: "یکشنبه",
    }[now.weekday()]
    date_str = f"{weekday_fa} {now.strftime('%Y/%m/%d')} — ساعت {now.strftime('%H:%M')}"

    # --- جمع‌آوری دماهای معتبر از همهٔ منابع برای میانگین‌گیری (حداقل/حداکثر روز) ---
    max_temps, min_temps, humidities = [], [], []
    # --- جمع‌آوری دمای لحظه‌ای از همهٔ منابعی که آن را ارائه می‌دهند ---
    current_temps, current_feels, current_humidities, current_winds = [], [], [], []
    sources_used = []  # فقط برای لاگ داخلی؛ در متن پیام نمایش داده نمی‌شود

    if open_meteo:
        max_temps.append(open_meteo["temp_max"])
        min_temps.append(open_meteo["temp_min"])
        if open_meteo["humidity"] is not None:
            humidities.append(open_meteo["humidity"])
        if open_meteo.get("current_temp") is not None:
            current_temps.append(open_meteo["current_temp"])
        if open_meteo.get("current_feels") is not None:
            current_feels.append(open_meteo["current_feels"])
        if open_meteo.get("current_humidity") is not None:
            current_humidities.append(open_meteo["current_humidity"])
        if open_meteo.get("current_wind") is not None:
            current_winds.append(open_meteo["current_wind"])
        sources_used.append("Open-Meteo")

    if wttr:
        max_temps.append(wttr["temp_max"])
        min_temps.append(wttr["temp_min"])
        humidities.append(wttr["humidity"])
        current_temps.append(wttr["current_temp"])
        current_feels.append(wttr["current_feels"])
        current_humidities.append(wttr["humidity"])
        current_winds.append(wttr["wind_kmph"])
        sources_used.append("wttr.in")

    if met_norway:
        if met_norway.get("current_temp") is not None:
            current_temps.append(met_norway["current_temp"])
        if met_norway.get("current_humidity") is not None:
            current_humidities.append(met_norway["current_humidity"])
        if met_norway.get("current_wind") is not None:
            current_winds.append(met_norway["current_wind"])
        sources_used.append("MET Norway")

    if weatherapi:
        max_temps.append(weatherapi["temp_max"])
        min_temps.append(weatherapi["temp_min"])
        if weatherapi["humidity"]:
            humidities.append(weatherapi["humidity"])
        if weatherapi.get("current_temp") is not None:
            current_temps.append(weatherapi["current_temp"])
        if weatherapi.get("current_feels") is not None:
            current_feels.append(weatherapi["current_feels"])
        sources_used.append("WeatherAPI")

    if not max_temps and not current_temps:
        return (
            "⚠️ <b>خطا در دریافت اطلاعات آب‌وهوا</b>\n"
            "متأسفانه هیچ‌کدام از منابع در دسترس پاسخ ندادند. لطفاً بعداً بررسی کنید."
        )

    temp_max = round(mean(max_temps), 1) if max_temps else None
    temp_min = round(mean(min_temps), 1) if min_temps else None
    humidity = round(mean(humidities)) if humidities else None

    current_temp = round(mean(current_temps), 1) if current_temps else None
    current_feel = round(mean(current_feels), 1) if current_feels else None
    current_humidity = round(mean(current_humidities)) if current_humidities else None
    current_wind = round(mean(current_winds)) if current_winds else None

    # وضعیت کلی هوا از منبع اصلی (Open-Meteo) در صورت وجود
    if open_meteo:
        condition_text, condition_emoji = describe_weather_code(open_meteo["weather_code"])
        wind = open_meteo["wind"]
    elif wttr:
        condition_text, condition_emoji = wttr["condition"], "🌡️"
        wind = wttr.get("wind_kmph")
    elif weatherapi:
        condition_text, condition_emoji = weatherapi["condition"], "🌡️"
        wind = weatherapi.get("wind_kmph")
    else:
        condition_text, condition_emoji = "نامشخص", "🌡️"
        wind = current_wind

    # هشدار اختلاف قابل توجه بین منابع (صحت‌سنجی متقابل)
    disagreement_note = ""
    if len(max_temps) > 1 and (max(max_temps) - min(max_temps) > 4):
        disagreement_note = (
            "\n⚠️ <i>توجه: اختلاف محسوسی بین پیش‌بینی منابع مختلف مشاهده شد؛ "
            "عدد نمایش‌داده‌شده میانگین منابع است.</i>\n"
        )

    aqi_text, aqi_emoji = describe_aqi(aqi)

    # --- تحلیل کوتاه خودکار ---
    ref_temp_max = temp_max if temp_max is not None else current_temp
    ref_temp_min = temp_min if temp_min is not None else current_temp
    if ref_temp_max is not None and ref_temp_max >= 35:
        analysis = "هوای امروز بسیار گرم است؛ از فعالیت طولانی زیر آفتاب در ساعات میانی روز خودداری کنید."
    elif ref_temp_max is not None and ref_temp_max >= 28:
        analysis = "روز گرمی در پیش است؛ نوشیدن آب کافی و استفاده از ضدآفتاب توصیه می‌شود."
    elif ref_temp_min is not None and ref_temp_min <= 5:
        analysis = "هوا نسبتاً سرد است؛ پوشیدن لباس گرم به‌ویژه در ساعات ابتدایی و انتهایی روز فراموش نشود."
    elif "باران" in condition_text or "rain" in condition_text.lower():
        analysis = "احتمال بارش وجود دارد؛ همراه داشتن چتر خالی از لطف نیست."
    else:
        analysis = "شرایط جوی امروز نسبتاً معتدل و مناسب برای فعالیت‌های روزمره است."

    report = (
        f"🌆 <b>گزارش آب‌وهوای تهران</b>\n"
        f"🗓️ {date_str}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{condition_emoji} <b>وضعیت کلی:</b> {condition_text}\n"
    )

    if current_temp is not None:
        feels_part = f"   (حس‌شده: {current_feel}°C)" if current_feel is not None else ""
        report += f"🌡️ <b>دمای لحظه‌ای:</b> {current_temp}°C{feels_part}\n"

    if temp_min is not None and temp_max is not None:
        report += f"📉📈 <b>دمای امروز:</b> حداقل {temp_min}°C   |   حداکثر {temp_max}°C\n"

    show_humidity = humidity if humidity is not None else current_humidity
    if show_humidity is not None:
        report += f"💧 <b>رطوبت:</b> {show_humidity}%\n"

    show_wind = wind if wind else current_wind
    if show_wind:
        report += f"🍃 <b>سرعت باد:</b> {round(show_wind)} km/h\n"

    report += f"{aqi_emoji} <b>کیفیت هوا (AQI):</b> {aqi if aqi is not None else 'نامشخص'} ({aqi_text})\n"

    if yesterday_diff is not None:
        if abs(yesterday_diff) < 0.5:
            report += "🔁 <b>نسبت به دیروز:</b> تقریباً مشابه دیروز\n"
        elif yesterday_diff > 0:
            report += f"🔺 <b>نسبت به دیروز:</b> {abs(yesterday_diff)}°C گرم‌تر\n"
        else:
            report += f"🔻 <b>نسبت به دیروز:</b> {abs(yesterday_diff)}°C خنک‌تر\n"

    report += disagreement_note
    report += (
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📝 <b>تحلیل کوتاه:</b>\n{analysis}"
    )

    if forecast_3day and len(forecast_3day) > 1:
        weekday_short = {
            0: "دوشنبه", 1: "سه‌شنبه", 2: "چهارشنبه", 3: "پنجشنبه",
            4: "جمعه", 5: "شنبه", 6: "یکشنبه",
        }
        forecast_lines = []
        for day in forecast_3day[1:]:  # روز اول = امروز، از فردا به بعد نشان بده
            d = datetime.strptime(day["date"], "%Y-%m-%d")
            _, emoji = describe_weather_code(day["weather_code"])
            forecast_lines.append(
                f"  {emoji} {weekday_short[d.weekday()]}: {round(day['temp_min'])}° تا {round(day['temp_max'])}°C"
            )
        report += (
            f"\n━━━━━━━━━━━━━━━━━━\n"
            f"🔮 <b>پیش‌بینی روزهای آینده:</b>\n" + "\n".join(forecast_lines)
        )

    logger.info(f"منابع استفاده‌شده در این گزارش: {', '.join(sources_used) or '—'}")
    return report


def extract_current_temp(open_meteo, wttr, weatherapi, met_norway):
    """میانگین دمای لحظه‌ای از همهٔ منابع در دسترس (برای تاریخچه/هشدار)."""
    temps = []
    for src in (open_meteo, wttr, weatherapi, met_norway):
        if src and src.get("current_temp") is not None:
            temps.append(src["current_temp"])
    return round(mean(temps), 1) if temps else None


# ---------------------------------------------------------------------------
# هشدار فوری شرایط خطرناک (مستقل از زمان‌بندی معمول گزارش)
# ---------------------------------------------------------------------------
# state بین اجراها، برای جلوگیری از ارسال چندبارهٔ هشدار مشابه در یک روز
_alert_state = {"date": None, "sent_types": set()}


async def check_and_send_alerts(app: Client, temp_max, temp_min, aqi):
    if not ENABLE_ALERTS:
        return

    today_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    if _alert_state["date"] != today_str:
        _alert_state["date"] = today_str
        _alert_state["sent_types"] = set()

    alerts = []
    if temp_max is not None and temp_max >= HEAT_ALERT_C and "heat" not in _alert_state["sent_types"]:
        alerts.append((
            "heat",
            f"🔥 <b>هشدار گرمای شدید</b>\n"
            f"دمای امروز تهران به {round(temp_max)}°C می‌رسد. از قرارگیری طولانی زیر "
            f"آفتاب و فعالیت سنگین در ساعات میانی روز خودداری کنید و آب کافی بنوشید."
        ))
    if temp_min is not None and temp_min <= COLD_ALERT_C and "cold" not in _alert_state["sent_types"]:
        alerts.append((
            "cold",
            f"🥶 <b>هشدار سرمای شدید</b>\n"
            f"دمای تهران تا {round(temp_min)}°C کاهش می‌یابد. لباس گرم و مراقبت از یخ‌زدگی سطح معابر را جدی بگیرید."
        ))
    if aqi is not None and aqi >= AQI_ALERT_THRESHOLD and "aqi" not in _alert_state["sent_types"]:
        alerts.append((
            "aqi",
            f"🟣 <b>هشدار آلودگی هوا</b>\n"
            f"شاخص کیفیت هوا (AQI) به {round(aqi)} رسیده که در سطح ناسالم قرار دارد. "
            f"فعالیت بدنی در فضای باز، به‌ویژه برای گروه‌های حساس، توصیه نمی‌شود."
        ))

    for alert_type, text in alerts:
        try:
            await app.send_message(CHANNEL_ID, text, parse_mode=ParseMode.HTML)
            _alert_state["sent_types"].add(alert_type)
            logger.info(f"هشدار فوری ارسال شد: {alert_type}")
        except Exception as e:
            logger.error(f"ارسال هشدار فوری ناموفق بود ({alert_type}): {e}")


# ---------------------------------------------------------------------------
# منطق ارسال گزارش
# ---------------------------------------------------------------------------
async def send_daily_report(app: Client):
    logger.info("در حال دریافت اطلاعات آب‌وهوا از منابع مختلف...")
    async with aiohttp.ClientSession() as session:
        (
            open_meteo, aqi, wttr, weatherapi, met_norway,
            hourly_forecast, forecast_3day,
        ) = await asyncio.gather(
            fetch_open_meteo(session),
            fetch_air_quality(session),
            fetch_wttr(session),
            fetch_weatherapi(session),
            fetch_met_norway(session),
            fetch_hourly_forecast(session) if ENABLE_CHART else asyncio.sleep(0, result=None),
            fetch_3day_forecast(session),
        )

    current_temp = extract_current_temp(open_meteo, wttr, weatherapi, met_norway)
    yesterday_diff = get_yesterday_comparison(current_temp)

    report_text = build_report(
        open_meteo, aqi, wttr, weatherapi, met_norway,
        forecast_3day=forecast_3day, yesterday_diff=yesterday_diff,
    )

    # --- ارسال نمودار روند دما (در صورت فعال بودن) ---
    chart_path = None
    if ENABLE_CHART and hourly_forecast:
        chart_path = build_temperature_chart(hourly_forecast)

    if chart_path:
        try:
            await app.send_photo(
                CHANNEL_ID, chart_path,
                caption="📊 روند دمای امروز تهران", parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.error(f"ارسال نمودار ناموفق بود (متن گزارش همچنان ارسال می‌شود): {e}")
        finally:
            if os.path.exists(chart_path):
                os.remove(chart_path)

    try:
        await app.send_message(CHANNEL_ID, report_text, parse_mode=ParseMode.HTML)
        logger.info("گزارش با موفقیت ارسال شد.")
    except Exception as e:
        logger.error(f"ارسال گزارش به کانال ناموفق بود: {e}")

    # --- ذخیرهٔ تاریخچه برای مقایسه‌های بعدی ---
    temp_max = open_meteo["temp_max"] if open_meteo else None
    temp_min = open_meteo["temp_min"] if open_meteo else None
    save_history(current_temp, temp_max, temp_min, aqi)

    # --- بررسی و ارسال هشدار فوری در صورت شرایط خطرناک ---
    await check_and_send_alerts(app, temp_max, temp_min, aqi)


# ---------------------------------------------------------------------------
# راه‌اندازی کلاینت و زمان‌بند
# ---------------------------------------------------------------------------
app = Client(
    "weather_selfbot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    in_memory=True,  # روی Railway نیازی به ذخیره فایل session نیست
)


async def main():
    init_history_db()

    async with app:
        me = await app.get_me()
        logger.info(f"با اکانت «{me.first_name}» با موفقیت وارد شدیم.")

        scheduler = AsyncIOScheduler(timezone=TIMEZONE)

        if REPORT_MODE == "daily":
            scheduler.add_job(
                send_daily_report,
                trigger=CronTrigger(hour=REPORT_HOUR, minute=REPORT_MINUTE),
                args=[app],
                id="weather_report",
            )
            logger.info(
                f"زمان‌بند فعال شد (حالت daily). گزارش هر روز ساعت "
                f"{REPORT_HOUR:02d}:{REPORT_MINUTE:02d} (به وقت تهران) ارسال خواهد شد."
            )
        else:
            scheduler.add_job(
                send_daily_report,
                trigger=IntervalTrigger(hours=REPORT_INTERVAL_HOURS),
                args=[app],
                id="weather_report",
                next_run_time=datetime.now(TIMEZONE),  # اولین اجرا بلافاصله
            )
            logger.info(
                f"زمان‌بند فعال شد (حالت interval). گزارش هر "
                f"{REPORT_INTERVAL_HOURS} ساعت یک‌بار ارسال خواهد شد."
            )

        scheduler.start()

        if RUN_ON_START and REPORT_MODE == "daily":
            # در حالت interval، اولین اجرا خودش بلافاصله انجام می‌شود (نیازی به این پرچم نیست)
            logger.info("RUN_ON_START فعال است؛ ارسال گزارش تستی...")
            await send_daily_report(app)

        # ربات را تا ابد در حال اجرا نگه می‌دارد
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())

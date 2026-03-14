import os
import json
import logging
import requests

logger = logging.getLogger(__name__)

def send_alert(message: str, level: str = "ERROR", context: dict = None):
    """
    Send an alert notification via Telegram or Log.
    PF_ALERT_CHANNEL: telegram | log (default log)
    """
    channel = os.environ.get("PF_ALERT_CHANNEL", "log").lower()
    
    # Enrich message with context if provided
    full_message = f"[{level}] {message}"
    if context:
        full_message += f"\nContext: {json.dumps(context, indent=2)}"

    if channel == "telegram":
        token = os.environ.get("PF_TELEGRAM_BOT_TOKEN")
        chat_id = os.environ.get("PF_TELEGRAM_CHAT_ID")
        
        if token and chat_id:
            try:
                url = f"https://api.telegram.org/bot{token}/sendMessage"
                payload = {
                    "chat_id": chat_id,
                    "text": full_message,
                    "parse_mode": "HTML" if "</b>" in full_message else None
                }
                response = requests.post(url, json=payload, timeout=10)
                response.raise_for_status()
                logger.info("Telegram alert sent successfully.")
                return True
            except Exception as e:
                logger.error(f"Failed to send Telegram alert: {e}")
                # Fallback to log
        else:
            logger.warning("Telegram credentials missing (PF_TELEGRAM_BOT_TOKEN/PF_TELEGRAM_CHAT_ID).")

    # Fallback to Log
    if level == "ERROR":
        logger.error(full_message)
    elif level == "WARNING":
        logger.warning(full_message)
    else:
        logger.info(full_message)
    
    return True

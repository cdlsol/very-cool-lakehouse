from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()
LOCAL_SPARK = os.environ.get("LOCAL_SPARK")

def get_current_calmonth() -> tuple[str, str, str, str]:
    today = datetime.now().date()
    calmonth = today.strftime("%Y%m%d")
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")

    return calmonth, year, month, day
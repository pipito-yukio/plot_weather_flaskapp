import enum
from datetime import date, datetime, timedelta
from typing import List, Dict

"""
日付処理ユーティリティ
"""

# デフォルトの日付フォーマット (ISO8601形式)
FMT_ISO8601 = "%Y-%m-%d"
FMT_DATETIME: str = '%Y-%m-%d %H:%M:%S'
FMT_DATETIME_HM: str = '%Y-%m-%d %H:%M'

ADD_TIME_00_00_00: str = " 00:00:00"
FMT_JP_DATE: str = "%Y年%m月%d日"
FMT_JP_DATE_WITH_WEEK: str = "{} ({})"

DICT_DAY_WEEK_JP: Dict[str, str] = {
    "Sun": "日", "Mon": "月", "Tue": "火", "Wed": "水", "Thu": "木", "Fri": "金", "Sat": '土'
}

# 日本語の曜日
JP_WEEK_DAY_NAMES: List[str] = ["月", "火", "水", "木", "金", "土", "日"]


class DateCompEnum(enum.Enum):
    """ 日付比較結果 """
    EQ = 0
    GT = 1
    LT = -1


def datetimeToJpDate(curc_datetime: datetime) -> str:
    """
    datetimeを日本語日付に変換する
    param: cur_datetime
    :return: 日本語日付
    """
    return curc_datetime.strftime(FMT_JP_DATE)


def datetimeToJpDateWithWeek(cur_datetime: datetime) -> str:
    """
    日本語の曜日を含む日付を返却する
    (例) 2022-09-09 -> 2022-09-09 (金)
    :param cur_datetime:日付
    :return: 日本語の曜日を含む日付
    """
    s_date: str = cur_datetime.strftime(FMT_JP_DATE)
    idx_week: int = cur_datetime.weekday()
    return FMT_JP_DATE_WITH_WEEK.format(s_date, JP_WEEK_DAY_NAMES[idx_week])


def strDateToDatetimeTime000000(s_date: str) -> datetime:
    """
    日付文字列の "00:00:00"のdatetimeブジェクトを返却する
    :param s_date: 日付文字列
    :return: datetimeブジェクト
    """
    return datetime.strptime(s_date + ADD_TIME_00_00_00, FMT_DATETIME)


def addDayToString(s_date: str, add_days=1, fmt_date=FMT_ISO8601) -> str:
    """
    指定された日付文字列に指定された日数を加減算する
    :param s_date: 日付文字列
    :param add_days: 加算(n)または減算(-n)する日数
    :param fmt_date: デフォルト ISO8601形式
    :return: 加減算した日付文字列
    """
    dt = datetime.strptime(s_date, fmt_date)
    dt += timedelta(days=add_days)
    s_next = dt.strftime(fmt_date)
    return s_next


def calcEndOfMonth(s_year_month: str) -> int:
    """
    年月(文字列)の末日を計算する
    :param s_year_month: 年月(文字列, "-"区切り)
    :return: 末日
    """
    parts = s_year_month.split("-")
    val_year, val_month = int(parts[0]), int(parts[1])
    if val_month == 12:
        val_year += 1
        val_month = 1
    else:
        val_month += 1
    # 月末日の翌月の1日
    next_year_month = date(val_year, val_month, 1)
    # 月末日の計算: 次の月-1日
    result = next_year_month - timedelta(days=1)
    return result.day


def nextYearMonth(s_year_month: str) -> str:
    """
    年月文字列の次の月を計算する
    :param s_year_month: 年月文字列
    :return: 翌年月叉は翌年月日
    :raise ValueError:
    """
    date_parts: List[str] = s_year_month.split('-')
    date_parts_size = len(date_parts)
    if date_parts_size < 2 or date_parts_size > 3:
        raise ValueError

    year, month = int(date_parts[0]), int(date_parts[1])
    month += 1
    if month > 12:
        year += 1
        month = 1
    if date_parts_size == 2:
        result = f"{year:04}-{month:02}"
    else:
        day = int(date_parts[2])
        result = f"{year:04}-{month:02}-{day:02}"
    return result


def toPreviousYearMonth(s_year_month: str) -> str:
    """
    1年前の年月を取得する
    :param s_year_month: 妥当性チェック済みの年月文字列 "YYYY-MM"
    :return: 1年前の年月
    """
    s_year, s_month = s_year_month.split('-')
    # 1年前
    prev_year: int = int(s_year) - 1
    return f"{prev_year}-{s_month}"


def check_str_date(s_date, fmt_date=FMT_ISO8601) -> bool:
    """
    日付文字列チェック
    :param s_date: 日付文字列
    :param fmt_date: デフォルト ISO8601形式
    :return: 日付文字列ならTrue, それ以外はFalse
    """
    try:
        datetime.strptime(s_date, fmt_date)
        return True
    except ValueError:
        return False


def check_str_time(s_time: str, has_second: bool = True) -> bool:
    """
    時刻文字列チェック ('時:分' | '時:分:秒' ※秒精度)
    :param s_time:
    :param has_second: 秒までの精度か, デフォルトTrue
    :return: 妥当ならTrue, それ以外はFalse
    """
    if len(s_time) == 0:
        return False

    # 時刻はコロン区切り
    times: List = s_time.split(':')
    times_size = len(times)
    if times_size < 2 or times_size > 3:
        return False

    # 秒ありなら3分割, 秒なしなら2分割
    if (has_second and times_size != 3) or (not has_second and times_size != 2):
        return False

    # 本日でチェックする
    today: date = date.today()
    s_today: str = today.isoformat()
    check_datetime: str = f"{s_today} {s_time}"
    fmt_datetime: str
    if has_second:
        # 時分秒
        fmt_datetime = FMT_DATETIME
    else:
        # 時分まで
        fmt_datetime = FMT_DATETIME_HM
    try:
        datetime.strptime(check_datetime, fmt_datetime)
        return True
    except ValueError:
        return False


def dateCompare(s_date1: str, s_date2: str) -> DateCompEnum:
    """
    s_date1(小さい想定の日付文字列) と s_date2(大きい想定の日付文字列)を比較する
    :param s_date1: 小さい想定の日付文字列
    :param s_date2: 大きい想定の日付文字列
    :return: s_date2が大きい場合 DateCompare.GT, 小さい場合 DateCompare.LT, 等しい場合 DateCompare.EQ
    """
    d1 = datetime.strptime(s_date1, FMT_ISO8601)
    d2 = datetime.strptime(s_date2, FMT_ISO8601)
    if d1 == d2:
        return DateCompEnum.EQ
    elif d1 < d2:
        return DateCompEnum.GT
    else:
        return DateCompEnum.LT


def diffInDays(s_date1: str, s_date2: str) -> int:
    """
    2つの日付の差分(日数)を求める
    :param s_date1: 小さい想定の日付文字列
    :param s_date2: 大きい想定の日付文字列
    :return: 差分(日数)
    """
    d1 = datetime.strptime(s_date1, FMT_ISO8601)
    d2 = datetime.strptime(s_date2, FMT_ISO8601)
    # Differential value is datetime.timedelta
    diff_days: timedelta = d2 - d1
    return diff_days.days


def makeDateTextWithJpWeekday(iso_date: str, has_month: bool = False) -> str:
    """
    X軸の日付ラベル文字列を生成する\n
    [形式] '日 (曜日)' | '月/日 (曜日)' ※日は前ゼロ
    :param iso_date: ISO8601 日付文字列
    :param has_month: 月を表示
    :return: 日付ラベル文字列
    """
    val_date: datetime = datetime.strptime(iso_date, FMT_ISO8601)
    weekday_name = JP_WEEK_DAY_NAMES[val_date.weekday()]
    if not has_month:
        return f"{val_date.day} ({weekday_name})"
    else:
        return f"{val_date.month}/{val_date.day:#02d} ({weekday_name})"

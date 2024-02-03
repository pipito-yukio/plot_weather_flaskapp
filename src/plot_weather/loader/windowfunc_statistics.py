import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from io import StringIO
from typing import List, Tuple, Optional, Dict

import numpy as np
import pandas as pd
from psycopg2.extensions import connection, cursor

from .dataframeloader import COL_TIME, COL_TEMP_OUT
from plot_weather.util.date_util import (
    FMT_DATETIME_HM, addDayToString
)

"""
指定された日付(1日分)の外気温の最低気温と最高気温を取得(DICT)する
[使用箇所]
気象データビューワーAndroidアプリの最新データ取得画面に追加する
[前提条件]
 (1) 1日の最低気温、最高気温は複数回出現する可能性がある ※それぞれ1つとは限らない
 (2) 同一時間に最低気温と最高気温が出現することも有りうる
[取得条件]
  複数有る場合は直近1件とする (ソートはDESC)
"""


def _temp_out_to_string_io(
        tuple_list: List[Tuple[str, float]]) -> StringIO:
    str_buffer = StringIO()
    str_buffer.write('"measurement_time","temp_out"' + "\n")
    for (m_time, temp_out) in tuple_list:
        line = f'"{m_time}",{temp_out}\n'
        str_buffer.write(line)

    # StringIO need Set first position
    str_buffer.seek(0)
    return str_buffer


def _make_temp_out(data_one: pd.DataFrame) -> Dict:
    # https://pandas.pydata.org/docs/reference/api/pandas.Timestamp.to_pydatetime.html
    # pandas.Timestamp.to_pydatetime
    pd_timestamp_ser: pd.Series = data_one[COL_TIME]
    # FutureWarning: Calling float on a single element Series is deprecated and will raise a TypeError in the future.
    # Use float(ser.iloc[0]) instead
    pd_timestamp: pd.Timestamp = pd_timestamp_ser.iloc[0]
    py_datetime: datetime = pd_timestamp.to_pydatetime()
    # 時刻は "時:分"までとする
    s_datetime: str = py_datetime.strftime(FMT_DATETIME_HM)
    # 日付部分を除く
    time_hm: str = s_datetime.split(" ")[1]
    temp_out: float = float(data_one[COL_TEMP_OUT].iloc[0])
    return asdict(TempOut(appear_time=time_hm, temper=temp_out))


@dataclass
class TempOut:
    appear_time: Optional[str]
    temper: Optional[float]


class TempOutStatistics:
    _QUERY: str = """
SELECT
  to_char(measurement_time,'YYYY-MM-DD HH24:MI') as measurement_time,
  temp_out
FROM
  weather.t_weather
WHERE
  did=(SELECT id FROM weather.t_device WHERE name = %(name)s)
AND (
  measurement_time >= %(from_date)s AND measurement_time < %(next_date)s
)
ORDER BY measurement_time DESC
"""

    def __init__(self, conn: connection,
                 logger: Optional[logging.Logger] = None, is_debug_out: bool = False):
        self.conn: connection = conn
        self.logger: Optional[logging.Logger] = logger
        self.is_debug_out: bool = is_debug_out

    def _get_find_datas(self, dev_name: str, from_date: str) -> Optional[StringIO]:
        next_date: str = addDayToString(from_date)
        params: Dict = {
            "name": dev_name, "from_date": from_date, "next_date": next_date
        }
        if self.is_debug_out:
            self.logger.debug(f"params: {params}")

        curr: cursor
        with self.conn.cursor() as curr:
            curr.execute(self._QUERY, params)
            rows: List[Tuple[str, float]] = curr.fetchall()
            record_size: int = len(rows)
            if self.is_debug_out:
                self.logger.debug(f"rows: {record_size}")
        if record_size > 0:
            return _temp_out_to_string_io(rows)
        return None

    def get_statistics(self, device_name: str, find_date: str) -> Tuple[Dict, Dict]:
        csv_buff: Optional[StringIO] = self._get_find_datas(device_name, find_date)
        if csv_buff is None:
            none_out: TempOut = TempOut(appear_time=None, temper=None)
            return asdict(none_out), asdict(none_out)

        df: pd.DataFrame = pd.read_csv(csv_buff, parse_dates=[COL_TIME])
        df.index = df[COL_TIME]
        temp_out_ser: pd.Series = df[COL_TEMP_OUT]
        val_min_temp_out: np.float64 = temp_out_ser.min()
        val_max_temp_out: np.float64 = temp_out_ser.max()
        if self.is_debug_out:
            self.logger.debug(
                f"min_temp_out: {val_min_temp_out}, max_temp_out: {val_max_temp_out}"
            )
        # type(min_ser): <class 'pandas.core.series.Series'>
        df_min_all: pd.DataFrame = df[temp_out_ser <= val_min_temp_out]
        df_max_all: pd.DataFrame = df[temp_out_ser >= val_max_temp_out]
        if self.is_debug_out:
            self.logger.debug(f"df_min_all: {df_min_all}")
            self.logger.debug(f"df_max_all: {df_max_all}")
        # 最初のレコードのみ取得 == 最新データ
        min_first: pd.DataFrame = df_min_all.head(n=1)
        max_first: pd.DataFrame = df_max_all.head(n=1)
        min_temp_out: Dict[TempOut] = _make_temp_out(min_first)
        max_temp_out: Dict[TempOut] = _make_temp_out(max_first)
        return min_temp_out, max_temp_out

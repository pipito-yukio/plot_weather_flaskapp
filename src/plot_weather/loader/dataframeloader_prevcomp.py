import logging
from io import StringIO
from typing import List, Optional, Tuple

from psycopg2.extensions import connection

import pandas as pd
from pandas.core.frame import DataFrame

from .dataframeloader import (
    COL_TIME, COL_TEMP_OUT, COL_HUMID, COL_PRESSURE
)
from plot_weather.dao.weatherdao_prevcomp import WeatherPrevCompDao

from plot_weather.util.date_util import toPreviousYearMonth

"""
前年月比較データロードモジュール (測定時刻,外気温,室内湿度,気圧)
※室内気温は未使用
"""

# 取得カラム: 測定時刻,外気温,湿度,気圧
HEADER: str = f'"{COL_TIME}","{COL_TEMP_OUT}","{COL_HUMID}","{COL_PRESSURE}"'


def _csvToStringIO(
        tuple_list: List[Tuple[str, float, float, float]]) -> StringIO:
    str_buffer = StringIO()
    str_buffer.write(HEADER + "\n")

    for (m_time, temp_out, humid, pressure) in tuple_list:
        line = f'"{m_time}",{temp_out},{humid},{pressure}\n'
        str_buffer.write(line)

    # StringIO need Set first position
    str_buffer.seek(0)
    return str_buffer


def _load_dataframe(
        dao: WeatherPrevCompDao, device_name: str, year_month: str,
        logger: Optional[logging.Logger] = None, log_debug: bool = False
) -> Tuple[int, Optional[pd.DataFrame]]:
    """
    指定された検索年月の観測データのDataFrameを取得 ※室内気温を除く
    :param dao: WeatherPrevCompDao
    :param device_name: デバイス名
    :param year_month: 今年の年月
    :param logger: アプリロガー
    :param log_debug: デバック出力可否 default False
    :return: レコード有り(件数, DataFrame), レコード無し(0, None)
    """
    data_list: List[Tuple[str, float, float, float]] = dao.getMonthData(
        device_name, year_month
    )
    rec_count: int = len(data_list)
    if rec_count == 0:
        return rec_count, None

    csv_buffer: StringIO = _csvToStringIO(data_list)
    df: pd.DataFrame = pd.read_csv(csv_buffer, header=0, parse_dates=[COL_TIME])
    df.index = df[COL_TIME]
    if logger is not None and log_debug:
        logger.debug(f"{df}")
    return rec_count, df


def loadPrevCompDataFrames(
        conn: connection, device_name: str, year_month,
        logger: Optional[logging.Logger] = None, logger_debug: bool = False
) -> Tuple[Optional[DataFrame], Optional[DataFrame]]:
    """
    指定された年月の気象データのDataFrameを取得する
    :param conn: psycopg2.connection
    :param device_name: デバイス名
    :param year_month: 検索年月
    :param logger: app_logger
    :param logger_debug: デバック出力可否 default False
    :return: レコードあり(今年DataFrame, 前年のDataFrame, 前年月)
     レコードなし (None, None)
    """
    dao = WeatherPrevCompDao(conn, logger=logger)
    try:
        rec_count: int
        df_curr: Optional[DataFrame]
        # 今年の年月テータ取得
        rec_count, df_curr = _load_dataframe(
            dao, device_name, year_month, logger=logger, log_debug=logger_debug)
        if rec_count == 0:
            return None, None

        # 前年の年月テータ取得
        # 前年計算
        prev_year_month: str = toPreviousYearMonth(year_month)
        rec_count, df_prev = _load_dataframe(
            dao, device_name, prev_year_month, logger=logger, log_debug=logger_debug)
        if rec_count > 0:
            return df_curr, df_prev
        else:
            return None, None
    except Exception as err:
        logger.warning(err)
        raise err

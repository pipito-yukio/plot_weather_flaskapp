import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd

from .plottercommon import COL_TIME, COL_TEMP_OUT


""" 統計情報計算モジュール"""


@dataclass
class TempOut:
    """ 外気温情報 """
    # 出現時刻
    appear_time: str
    # 外気温
    temper: float


@dataclass
class TempOutStat:
    """ 外気温統計情報 """
    # 測定日
    measurement_day: str
    # 平均外気温
    average_temper: float
    # 最低外気温情報
    min: TempOut
    # 最高外気温情報
    max: TempOut


def get_temp_out_stat(df: pd.DataFrame, logger: Optional[logging.Logger]) -> TempOutStat:
    """ 外気温の統計情報 ([最低気温|最高気温] の気温その出現時刻) を取得する """

    def get_measurement_time(measurement_time_ser: pd.Series) -> str:
        # https://pandas.pydata.org/docs/reference/api/pandas.Timestamp.to_pydatetime.html
        # pandas.Timestamp.to_pydatetime
        pd_timestamp: pd.Timestamp = measurement_time_ser.iloc[0]
        py_datetime: datetime = pd_timestamp.to_pydatetime()
        # 時刻部分は "時:分"までとする
        return py_datetime.strftime("%Y-%m-%d %H:%M")

    # 外気温列
    temp_out_ser: pd.Series = df[COL_TEMP_OUT]
    # 外気温列から最低・最高・平均気温を取得
    min_temper: np.float64 = temp_out_ser.min()
    max_temper: np.float64 = temp_out_ser.max()
    avg_temper: np.float64 = temp_out_ser.mean()
    # 全ての最低気温を取得する
    df_min_all: pd.DataFrame = df[temp_out_ser <= min_temper]
    # 全ての最高気温を取得する
    df_max_all: pd.DataFrame = df[temp_out_ser >= max_temper]
    if logger is not None:
        logger.debug(f"df_min_all:\n{df_min_all}")
        logger.debug(f"df_max_all:\n{df_max_all}")
    # それぞれ直近の１レコードのみ取得
    min_first: pd.DataFrame = df_min_all.head(n=1)
    max_first: pd.DataFrame = df_max_all.head(n=1)
    # 最低気温情報
    min_measurement_datetime: str = get_measurement_time(min_first[COL_TIME])
    #   測定日は先頭 10桁分(年月日)
    measurement_day: str = min_measurement_datetime[:10]
    #   出現時刻
    temp_out_min: TempOut = TempOut(
        min_measurement_datetime, float(min_first[COL_TEMP_OUT].iloc[0])
    )
    # 最高気温情報
    max_measurement_datetime: str = get_measurement_time(max_first[COL_TIME])
    temp_out_max: TempOut = TempOut(
        max_measurement_datetime, float(max_first[COL_TEMP_OUT].iloc[0])
    )
    return TempOutStat(
        measurement_day, average_temper=float(avg_temper),
        min=temp_out_min, max=temp_out_max
    )

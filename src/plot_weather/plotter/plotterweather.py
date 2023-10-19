import enum
import logging
from typing import Dict, List, Optional, Tuple, Union
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
from pandas.core.frame import DataFrame
from psycopg2.extensions import connection

import matplotlib.dates as mdates
from matplotlib import rcParams
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.pyplot import setp

from ..dao.weathercommon import PLOT_CONF
from ..dao.weatherdao import WeatherDao
from .plottercommon import (
    COL_TIME, COL_TEMP_OUT, COL_TEMP_IN, COL_HUMID, COL_PRESSURE,
    Y_LABEL_TEMP, Y_LABEL_HUMID, Y_LABEL_PRESSURE,
    convert_html_image_src
)
from ..util.date_util import (
    addDayToString, datetimeToJpDateWithWeek, strDateToDatetimeTime000000,
    FMT_ISO8601, FMT_DATETIME
)

""" 気象データ画像のbase64エンコードテキストデータを出力する """

# 日本語フォントの設定方法
#  日本語フォントとしてIPAexフォントがインストール済みのであると仮定した場合
# [A] 叉は [B] のどちらの方法でも指定された日本語フォントが反映される
# [A] https://matplotlib.org/3.1.0/gallery/text_labels_and_annotations/font_family_rc_sgskip.html
#    Configuring the font family
# rcParams['font.family'] = 'sans-serif'
# rcParams[font.sans-serif] = ['IPAexGothic'] <<-- リスト
# ~/[Application root]/plot_weather/dao/conf/plot_weather.json: PLOT_CONF
rcParams['font.family'] = PLOT_CONF['font.family']
font_family_font: str = 'font.' + PLOT_CONF['font.family']
rcParams[font_family_font] = PLOT_CONF['japanese.font']
# [B] "matplotlibrc" ファイルの 14,15,16行目に記載されている方法
# ## If you wish to change your default style, copy this file to one of the
# ## following locations:
# ##     Unix/Linux:
# ##            $HOME/.config/matplotlib/matplotlibrc
# ## ...無関係部分省略...
# ## and edit that copy.
# 下記(1),(2)のコメントアウトを外す ※オリジナルはコメントアウトされている
#  (1) font.family: sans-serif
#      "IPAexGothic" を先頭に追記する
#  (2) font.sans-serif: IPAexGothic, DejaVu Sans, ..., sans-serif

# クラフの軸ラベルフォントサイズ
LABEL_FONT_SIZE: int = 10
# グラフのグリッド線スタイル
GRID_STYLES: Dict[str, Union[str, float]] = {"linestyle": "--", "linewidth": 1.0}


class ImageDateType(enum.Enum):
    """ 日付データ型 """
    TODAY = 0  # 当日データ
    YEAR_MONTH = 1  # 年月データ
    RANGE = 2  # 期間データ: 当日を含む過去日(検索開始日)からN日後


class ParamKey(enum.Enum):
    TODAY = "today"
    YEAR_MONTH = "yearMonth"
    BEFORE_DAYS = "beforeDays"
    START_DAY = "startDay"
    PHONE_SIZE = "phoneSize"


class ImageDateParams(object):
    def __init__(self, imageDateType: ImageDateType = ImageDateType.TODAY):
        self.imageDateType = imageDateType
        self.typeParams: Dict[ImageDateType, Dict[ParamKey, str]] = {
            ImageDateType.TODAY: {ParamKey.TODAY: "", ParamKey.PHONE_SIZE: ""},
            ImageDateType.YEAR_MONTH: {ParamKey.YEAR_MONTH: ""},
            ImageDateType.RANGE: {
                ParamKey.START_DAY: "",
                ParamKey.BEFORE_DAYS: "",
                ParamKey.PHONE_SIZE: ""
            }
        }

    def getParam(self) -> Dict[ParamKey, str]:
        return self.typeParams[self.imageDateType]

    def setParam(self, param: Dict[ParamKey, str]):
        self.typeParams[self.imageDateType] = param

    def getImageDateType(self) -> ImageDateType:
        return self.imageDateType


def _to_japanese_date(s_date: str) -> str:
    """
    ISO8601日付文字列を日本語日付に変換
    :param s_date: ISO8601日付文字列
    :return 日本語日付
    """
    date_parts: List[str] = s_date.split("-")
    return f"{date_parts[0]}年{date_parts[1]}月{date_parts[2]}日"


def loadTodayDataFrame(
        dao: WeatherDao, device_name: str, today_iso8601: str,
        logger: Optional[Optional[logging.Logger]] = None, logger_debug: bool = False
) -> Tuple[int, Optional[pd.DataFrame], Optional[str], Optional[datetime], Optional[datetime]]:
    """
    当日の観測データのDataFrameを取得
    :param dao: WeatherDao
    :param device_name: デバイス名
    :param today_iso8601: 当日(最終登録日) ※ISO8601形式の文字列
    :param logger: app_logger
    :param logger_debug: デバック出力可否 default False
    :return: レコード有り(件数, DataFrame, タイトル(日付部分), 当日の最小値, 翌日の最小値)、
             レコード無し(0, None, None, None, None)
    """
    # dao return StringIO buffer(line'\n') on csv format with header
    rec_count: int
    csv_buffer: StringIO
    rec_count, csv_buffer = dao.getTodayData(device_name, today_iso8601, require_header=True)
    # 該当レコード無し
    if rec_count == 0:
        return rec_count, None, None, None, None

    df: pd.DataFrame = pd.read_csv(
        csv_buffer,
        header=0,
        parse_dates=[COL_TIME],
        names=[COL_TIME, COL_TEMP_OUT, COL_TEMP_IN, COL_HUMID, COL_PRESSURE]  # Use cols
    )
    if logger is not None and logger_debug:
        logger.debug(f"Before df:\n{df}")
        logger.debug(f"Before df.index:\n{df.index}")

    # 測定時刻をデータフレームのインデックスに設定
    #  df.index: RangeIndex(start=0, stop=70, step=1) ※行番号 (0..)
    #   drop=False: "measurement_time"列をDataFrameに残す
    #   inplace=True: オリジナルのDataFrameのインデックスを"measurement_time"列にする
    #   inplace=False: オリジナルのインデックスは更新されず、更新されたコピーが返却される
    #    df = df.set_index(COL_INDEX)  とする必要がある
    df.set_index(COL_TIME, drop=False, inplace=True)
    if logger is not None and logger_debug:
        logger.debug(f"After df:\n{df}")
        logger.debug(f"After df.index:\n{df.index}")

    # 先頭の測定日付(Pandas Timestamp) から Pythonのdatetimeに変換
    # https://pandas.pydata.org/pandas-docs/version/0.22/generated/pandas.Timestamp.to_datetime.html
    first_datetime = df.index[0].to_pydatetime()
    # タイトル: 日本語日付(曜日)
    title_date_part: str = datetimeToJpDateWithWeek(first_datetime)
    # 当日の日付文字列 ※一旦 dateオブジェクトに変換して"年月日"を取得
    first_date: str = first_datetime.date().isoformat()
    # 表示範囲：当日の "00:00:00" から
    today_min_date: datetime = strDateToDatetimeTime000000(first_date)
    # 翌日の "00:00:00" 迄
    nextday: str = addDayToString(first_date)
    next_min_date: datetime = strDateToDatetimeTime000000(nextday)
    return rec_count, df, title_date_part, today_min_date, next_min_date


def loadMonthDataFrame(
        dao: WeatherDao, device_name: str, year_month: str,
        logger: Optional[logging.Logger] = None, logger_debug: bool = False
) -> Tuple[int, Optional[pd.DataFrame], Optional[str]]:
    """
    指定された検索年月の観測データのDataFrameを取得
    :param dao: WeatherDao
    :param device_name: デバイス名
    :param year_month: 検索年月
    :param logger: app_logger
    :param logger_debug: デバック出力可否 default False
    :return: レコード有り(件数, DataFrame, タイトル(日付部分))、レコード無し(0, None, None)
    """
    rec_count: int
    csv_buffer: StringIO
    rec_count, csv_buffer = dao.getMonthData(device_name, year_month,
                                             require_header=True)
    if rec_count == 0:
        return rec_count, None, None

    df: pd.DataFrame = pd.read_csv(
        csv_buffer,
        header=0,
        parse_dates=[COL_TIME],
        names=[COL_TIME, COL_TEMP_OUT, COL_TEMP_IN, COL_HUMID, COL_PRESSURE]
    )
    if logger is not None and logger_debug:
        logger.debug(df)
    df.set_index(COL_TIME, drop=False, inplace=True)
    date_parts: List[str] = year_month.split("-")
    title_date_part = f"{date_parts[0]}年{date_parts[1]}月"
    return rec_count, df, title_date_part


def loadBeforeDaysRangeDataFrame(
        dao: WeatherDao, device_name: str,
        start_day: str,
        before_days: int,
        logger: Optional[logging.Logger] = None, logger_debug: bool = False
) -> Tuple[int, Optional[pd.DataFrame], Optional[str]]:
    """
    指定された期間の観測データのDataFrameを取得
    :param dao: WeatherDao
    :param device_name: デバイス名
    :param start_day: 検索開始年月日 ※ISO8601形式文字列
    :param before_days: N日 ※検索開始年月日からN日以前
    :param logger: app_logger
    :param logger_debug: デバック出力可否 default False
    :return: レコード有り(件数, DataFrame, タイトル(日付部分))、レコード無し(0, None, None)
    """
    start_datetime: datetime = datetime.strptime(start_day, FMT_ISO8601)
    from_datetime: datetime = start_datetime - timedelta(days=before_days)
    from_date: str = from_datetime.strftime(FMT_ISO8601)
    to_date: str = start_datetime.strftime(FMT_ISO8601)
    if logger is not None and logger_debug:
        logger.debug(f"from_date: {from_date}, to_date: {to_date}")
    rec_count: int
    csv_buffer: StringIO
    rec_count, csv_buffer = dao.getFromToRangeData(
        device_name, from_date, to_date, require_header=True)
    if rec_count == 0:
        return rec_count, None, None

    df: pd.DataFrame = pd.read_csv(
        csv_buffer,
        header=0,
        parse_dates=[COL_TIME],
        names=[COL_TIME, COL_TEMP_OUT, COL_TEMP_IN, COL_HUMID, COL_PRESSURE]
    )
    if logger is not None and logger_debug:
        logger.debug(df)
    df.set_index(COL_TIME, drop=False, inplace=True)

    from_date = _to_japanese_date(from_date)
    to_date = _to_japanese_date(to_date)
    title_date_part: str = f"{from_date} 〜 {to_date}"
    return rec_count, df, title_date_part


def _temperaturePlotting(
        ax: Axes, df: pd.DataFrame, title_date: str, label_font_size: int) -> None:
    """
    温度サブプロット(axes)にタイトル、軸・軸ラベルを設定し、
    DataFrameオプジェクトの外気温・室内気温データをプロットする
    :param ax:温度サブプロット(axes)
    :param df:DataFrameオプジェクト
    :param title_date: タイトル ※日付
    :param label_font_size: ラベルフォントサイズ
    """
    ax.plot(df[COL_TIME], df[COL_TEMP_OUT], color="blue", marker="", label="外気温")
    ax.plot(df[COL_TIME], df[COL_TEMP_IN], color="red", marker="", label="室内気温")
    ax.set_ylim(PLOT_CONF["ylim"]["temp"])
    ax.set_ylabel(Y_LABEL_TEMP, fontsize=label_font_size)
    ax.legend(loc="best")
    ax.set_title(f"気象データ：{title_date}")
    # Hide xlabel
    ax.label_outer()


def _humidPlotting(ax: Axes, df: pd.DataFrame, label_font_size) -> None:
    """
    湿度サブプロット(axes)に軸・軸ラベルを設定し、DataFrameオプジェクトの室内湿度データをプロットする
    :param ax:湿度サブプロット(axes)
    :param df:DataFrameオプジェクト
    :param label_font_size: ラベルフォントサイズ
    """
    ax.plot(df[COL_TIME], df[COL_HUMID], color="green", marker="")
    ax.set_ylim(ymin=0., ymax=100.)
    ax.set_ylabel(Y_LABEL_HUMID, fontsize=label_font_size)
    # Hide xlabel
    ax.label_outer()


def _pressurePlotting(ax: Axes, df: pd.DataFrame, label_font_size: int) -> None:
    """
    気圧サブプロット(axes)に軸・軸ラベルを設定し、DataFrameオプジェクトの気圧データをプロットする
    :param ax:気圧サブプロット(axes)
    :param df:DataFrameオプジェクト
    :param label_font_size: ラベルフォントサイズ
    """
    ax.plot(df[COL_TIME], df[COL_PRESSURE], color="fuchsia", marker="")
    ax.set_ylim(PLOT_CONF["ylim"]["pressure"])
    ax.set_ylabel(Y_LABEL_PRESSURE, fontsize=label_font_size)


def _axesPressureSettingWithBeforeDays(
        ax: Axes, start_day: str, before_days: int, x_date_tick_font_size: int) -> None:
    """
    気圧サブプロットの期間指定x軸ラベルを設定する
    :param ax:気圧サブプロット(axes)
    :param start_day: 検索日 ※本日以外にアプリから過去の任意の日付を指定可能
    :param before_days: 当日からＮ日前のＮ
    :param x_date_tick_font_size: 日付軸ラベルフォントサイズ
    """
    # datetimeオブジェタクトに変更
    start_datetime: datetime = datetime.strptime(start_day, FMT_ISO8601)
    # デフォルトでは最後の軸に対応する日付ラベルが表示されない
    # 次の日の 00:30 までラベルを表示するための日付計算
    next_day: datetime = start_datetime + timedelta(days=1)
    s_next_day: str = next_day.strftime("%Y-%m-%d 00:30:00")
    # datetimeオブジェクトに戻す
    next_day = datetime.strptime(s_next_day, FMT_DATETIME)
    ax.set_xlim(xmax=next_day)
    if before_days == 7:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
        ax.tick_params(axis='x', labelsize=x_date_tick_font_size - 1)
    else:
        # [1,2,3] day
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H"))
        ax.tick_params(axis='x', labelsize=x_date_tick_font_size - 1, labelrotation=45)


def generate_figure(phone_size: str = None, logger=None, logger_debug=False) -> Figure:
    """
    リクエスト端末に応じたサイズのプロット領域枠(Figure)を生成する
    :param phone_size: リクエスト端末がモバイルのイメージ描画情報 ※任意 (ブラウザの場合はNone)
    :param logger: app_logger
    :param logger_debug: デバック有効フラグ (デフォルト無効)
    :return: プロット領域枠(Figure)
    """
    if phone_size is not None and len(phone_size) > 8:
        sizes: List[str] = phone_size.split("x")
        width_pixel: int = int(sizes[0])
        height_pixel: int = int(sizes[1])
        density: float = float(sizes[2])
        # Androidスマホは pixel指定
        # https://matplotlib.org/stable/gallery/subplots_axes_and_figures/figure_size_units.html
        #   Figure size in pixel
        px: float = 1 / rcParams["figure.dpi"]  # pixel in inches
        # density=1.0 の10インチタブレットはちょうどいい
        # 画面の小さいスマホのdensityで割る ※densityが大きい端末だとグラフサイズが極端に小さくなる
        #  いまのところ Pixel-4a ではこれが一番綺麗に表示される
        px = px / (2.0 if density > 2.0 else density)
        fig_width_px: float = width_pixel * px
        fig_height_px: float = height_pixel * px
        if logger is not None and logger_debug:
            logger.debug(f"px: {px} / density : {density}")
            logger.debug(f"fig_width_px: {fig_width_px}, fig_height_px: {fig_height_px}")
        fig = Figure(figsize=(fig_width_px, fig_height_px), constrained_layout=True)
    else:
        # PCブラウザはinch指定でdpi=72
        fig = Figure(figsize=PLOT_CONF["figsize"]["pc"], constrained_layout=True)
    if logger is not None and logger_debug:
        logger.debug(f"fig: {fig}")
    return fig


def make_graph(df: DataFrame, image_params: ImageDateParams,
               phone_size: str = None, title_part: str = None,
               today_xmin: datetime = None, today_xmax: datetime = None,
               start_day: str = None, before_days: int = None,
               logger=None, logger_debug=False) -> Figure:
    """
    観測データのDataFrameからグラフを生成し描画領域を取得する
    :param df: 観測データをロードしたDataFrame
    :param image_params: 画像入力パラメータ
    :param phone_size: 端末のイメージ領域サイズ情報 ※任意 (PC BrowserでのリクエストはNone)
    :param title_part: タイトル部分文字列
    :param today_xmin: 当日データの最小値(x軸)(当日の"00:00:00") ※任意
    :param today_xmax: 当日データの最大値(x軸)(翌日の"00:00:00") ※任意
    :param start_day: 範囲データの検索開始日 ※任意
    :param before_days: 範囲データの検索開始日からN日前 ※任意
    :param logger: app_logger
    :param logger_debug: デバック有効フラグ (デフォルト無効)
    :return: 観測データをプロットした描画領域
    """
    # 図の生成
    fig: Figure = generate_figure(phone_size, logger=logger, logger_debug=logger_debug)
    # x軸を共有する3行1列のサブプロット生成
    ax_temp: Axes
    ax_humid: Axes
    ax_pressure: Axes
    (ax_temp, ax_humid, ax_pressure) = fig.subplots(nrows=3, ncols=1, sharex=True)
    for ax in [ax_temp, ax_humid, ax_pressure]:
        ax.grid(**GRID_STYLES)

    # 軸ラベルのフォントサイズを設定
    #  ラベルフォントサイズ, y軸ラベルフォントサイズ, x軸(日付)ラベルフォントサイズ
    label_font_size: int = PLOT_CONF["label.sizes"][0]
    y_tick_labels_font_size: int = PLOT_CONF["label.sizes"][1]
    date_tick_lables_font_size: int = PLOT_CONF["label.sizes"][2]
    for ax in [ax_temp, ax_humid, ax_pressure]:
        setp(ax.get_xticklabels(), fontsize=date_tick_lables_font_size)
        setp(ax.get_yticklabels(), fontsize=y_tick_labels_font_size)

    # サブプロットの設定
    # 1.外気温と室内気温
    _temperaturePlotting(ax_temp, df, title_part, label_font_size)
    # 2.室内湿度
    _humidPlotting(ax_humid, df, label_font_size)
    # 3.気圧
    if image_params.getImageDateType() == ImageDateType.TODAY:
        # 当日データx軸の範囲: 当日 00時 から 翌日 00時
        for ax in [ax_temp, ax_humid, ax_pressure]:
            ax.set_xlim(xmin=today_xmin, xmax=today_xmax)
        # 当日データのx軸フォーマット: 軸ラベルは時間 (00,03,06,09,12,15,18,21,翌日の00)
        ax_pressure.xaxis.set_major_formatter(mdates.DateFormatter("%H"))
    elif image_params.getImageDateType() == ImageDateType.YEAR_MONTH:
        # 年月指定データのx軸フォーマット設定: 軸は"月/日"
        ax_pressure.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
    else:
        # 期間データのx軸フォーマット設定
        _axesPressureSettingWithBeforeDays(
            ax_pressure, start_day, before_days, date_tick_lables_font_size
        )
    # 気圧データプロット
    _pressurePlotting(ax_pressure, df, label_font_size)
    return fig


def gen_plot_image(
        conn: connection, device_name: str, image_params: ImageDateParams, logger=None
) -> Tuple[int, Optional[str]]:
    """
    観測データの画像を生成する
    :param conn: connection
    :param device_name: デバイス名
    :param image_params: イメージ用入力パラメータ
    :param logger: app_logger
    :return: データがある場合は(データ件数, 画像(base64エンコード文字列))、ない場合は (0, None)
    """
    if logger is not None:
        logger_debug = (logger.getEffectiveLevel() <= logging.DEBUG)
    else:
        logger_debug = False

    dao = WeatherDao(conn, logger=logger)
    # for phone appli (ImageDateType.TODAY | ImageDateType.RANGE)
    phone_size: str = ""
    # for ImageDateType.TODAY
    today_xmin: Optional[datetime] = None
    today_xmax: Optional[datetime] = None
    # for ImageDateType.RANGE
    start_day: Optional[str] = None
    before_days: Optional[int] = None
    if image_params.getImageDateType() == ImageDateType.TODAY:
        param: Dict[ParamKey, str] = image_params.getParam()
        phone_size = param.get(ParamKey.PHONE_SIZE, "")
        # app_mainで当日(又は最新登録日)が設定される
        s_today: str = param.get(ParamKey.TODAY, "")
        if logger is not None and logger_debug:
            logger.debug(f"today: {s_today}, phone_size: {phone_size}")
        rec_count, df, title_date_part, today_xmin, today_xmax = loadTodayDataFrame(
            dao, device_name, s_today, logger=logger, logger_debug=logger_debug
        )
    elif image_params.getImageDateType() == ImageDateType.YEAR_MONTH:
        # 指定された年月データ
        param: Dict[ParamKey, str] = image_params.getParam()
        year_month: str = param.get(ParamKey.YEAR_MONTH, "")
        if logger is not None and logger_debug:
            logger.debug(f"year_month: {year_month}")
        rec_count, df, title_date_part = loadMonthDataFrame(
            dao, device_name, year_month=year_month, logger=logger, logger_debug=logger_debug
        )
    else:
        # 範囲指定データ
        param: Dict[ParamKey, str] = image_params.getParam()
        # [仕様変更] 検索開始日
        start_day = param.get(ParamKey.START_DAY, "")
        param_before_days = param.get(ParamKey.BEFORE_DAYS, "")
        phone_size = param.get(ParamKey.PHONE_SIZE, "")
        if logger is not None and logger_debug:
            logger.debug(f"start_day: {start_day}, before_days: {param_before_days}")
        before_days = int(param_before_days)
        rec_count, df, title_date_part = loadBeforeDaysRangeDataFrame(
            dao, device_name, start_day, before_days,
            logger=logger, logger_debug=logger_debug
        )
    # 件数チェック
    if rec_count == 0:
        return rec_count, None

    # グラフ生成
    fig: Figure = make_graph(
        df, image_params,
        phone_size=phone_size, title_part=title_date_part,
        today_xmin=today_xmin, today_xmax=today_xmax,
        start_day=start_day, before_days=before_days,
        logger=logger, logger_debug=logger_debug)
    # 画像をバイトストリームに溜め込みそれをbase64エンコードしてレスポンスとして返す
    img_src: str = convert_html_image_src(fig, logger=logger, logger_debug=logger_debug)
    # 件数と画像
    return rec_count, img_src

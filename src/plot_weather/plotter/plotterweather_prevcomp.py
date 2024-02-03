import logging
from datetime import datetime
from typing import Dict, List, Optional

from matplotlib import rcParams
import matplotlib.dates as mdates
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.patches import Patch

import numpy as np
from pandas.core.frame import DataFrame, Series

from plot_weather.loader.dataframeloader import (
    COL_TIME, COL_TEMP_OUT, COL_HUMID, COL_PRESSURE,
)
from .plottercommon import (
    PLOT_CONF, Y_LABEL_HUMID, Y_LABEL_PRESSURE,
    convert_html_image_src
)

""" 前年と比較した気象データ画像のbase64エンコードテキストデータを出力する """

rcParams["font.family"] = PLOT_CONF["font.families"]
# 可変長ゴシックフォント
rcParams["font.sans-serif"] = PLOT_CONF["sans-serif.font"][0]

COL_PREV_PLOT_TIME: str = 'prev_plot_measurement_time'

# 比較データのみのラベル
Y_LABEL_TEMP_OUT: str = '外気温 (℃)'
# タイトルフォーマット
FMT_MEASUREMENT_RANGE: str = "{} − {} データ比較"
# 平均値文字列
FMT_JP_YEAR_MONTH: str = "{year}年{month}月"
FMT_AVEG_TEXT: str = "{jp_year_month} 平均{type} {value:#.1f} {unit}"
DICT_AVE_TEMP: Dict = {'type': '気温', 'unit': '℃'}
DICT_AVEG_HUMID: Dict = {'type': '湿度', 'unit': '％'}
DICT_AVEG_PRESSURE: Dict = {'type': '気圧', 'unit': 'hPa'}
# 描画領域のグリッド線スタイル: Y方向のグリッド線のみ表示
GRID_STYLE: Dict = {'axis': 'y', 'linestyle': 'dashed', 'linewidth': 0.7,
                    'alpha': 0.75}
# 線カラー
# 'tab:blue'
CURR_COLOR: str = 'C0'
# 'tab:orange'
PREV_COLOR: str = 'C1'
# 平均線スタイル
AVEG_LINE_STYLE: Dict = {'linestyle': 'dashdot', 'linewidth': 1.}
CURR_AVEG_LINE_STYLE: Dict = {'color': CURR_COLOR, **AVEG_LINE_STYLE}
PREV_AVEG_LINE_STYLE: Dict = {'color': PREV_COLOR, **AVEG_LINE_STYLE}
# X軸のラベルスタイル
X_TICKS_STYLE: Dict = {'fontsize': 8, 'fontweight': 'heavy', 'rotation': 90}
# プロット領域のラベルスタイル
LABEL_STYLE: Dict = {'fontsize': 10, }
# 凡例スタイル
LEGEND_STYLE: Dict = {'fontsize': 10, }
# タイトルスタイル
TITLE_STYLE: Dict = {'fontsize': 11, }


def plusOneYear(prev_datetime: datetime) -> datetime:
    """
    前年のdatetimeオブジェクトに1年プラスしたdatetimeオブジェクトを取得する
    :param prev_datetime: 前年のdatetimeオブジェクト
    @return: 1年プラスしたdatetimeオブジェクト
    """
    next_val: datetime = datetime(prev_datetime.year + 1,
                                  prev_datetime.month,
                                  prev_datetime.day,
                                  prev_datetime.hour,
                                  prev_datetime.minute,
                                  prev_datetime.second
                                  )
    return next_val


def makeLegendLabel(s_year_month: str) -> str:
    """
    凡例用ラベル生成
    :param s_year_month: 年月文字列
    @return: 凡例用ラベル
    """
    parts: List[str] = s_year_month.split('-')
    return FMT_JP_YEAR_MONTH.format(year=parts[0], month=parts[1])


def makeAvePatch(plot_label, f_ave: float, s_color: str, dict_ave: Dict) -> Patch:
    """
    平均値パッチ生成
    :param plot_label:
    :param f_ave:
    :param s_color
    :param dict_ave:
    @return:
    """

    def makeAvegText(jp_year_month: str, value: float, data_dict: Dict) -> str:
        """
        平均値用の文字列生成
        :param jp_year_month: 日本語年月文字列
        :param value: 平均値
        :param data_dict: データ型ごとの置換用辞書オブジェクト
        :return: 平均値用の文字列
        """
        data_dict['jp_year_month'] = jp_year_month
        data_dict['value'] = value
        return FMT_AVEG_TEXT.format(**data_dict)

    s_ave = makeAvegText(plot_label, f_ave, dict_ave)
    return Patch(color=s_color, label=s_ave)


def setYLimWithAxes(plot_axes: Axes,
                    curr_ser: Series, prev_ser: Series,
                    curr_temp_ser: Series, prev_temp_ser: Series) -> None:
    """
    各データの最大値・最小値を設定する
    :param plot_axes: プロット領域
    :param curr_ser: 最新データ
    :param prev_ser: 前年データ
    :param curr_temp_ser: 最新温度データ
    :param prev_temp_ser: 前年温度データ
    """
    val_min: float = np.min([curr_ser.min(), prev_ser.min()])
    val_max: float = np.max([curr_temp_ser.max(), prev_temp_ser.max()])
    val_min = np.floor(val_min / 10.) * 10.
    val_max = np.ceil(val_max / 10.) * 10.
    plot_axes.set_ylim(val_min, val_max)


def _temperature_plotting(
        ax_temp: Axes,
        df_curr: DataFrame, df_prev: DataFrame,
        curr_temp_ser: Series, prev_temp_ser: Series,
        main_title: str, curr_plot_label: str, prev_plot_label: str) -> None:
    """
    外気温領域のプロット
    :param ax_temp:外気温サブプロット(axes)
    :param df_curr:今年の年月DataFrame
    :param df_prev:前年の年月DataFrame
    :param curr_temp_ser: 現在の年月外気温データ
    :param prev_temp_ser: 前年の年月外気温データ
    :param main_title: タイトル
    :param curr_plot_label: 今年ラベル
    :param prev_plot_label: 前年ラベル
    """
    # 最低・最高
    setYLimWithAxes(ax_temp, curr_temp_ser, prev_temp_ser, curr_temp_ser, prev_temp_ser)
    # 最新年月の外気温
    ax_temp.plot(df_curr[COL_TIME], curr_temp_ser, color=CURR_COLOR, marker="")
    val_ave = curr_temp_ser.mean()
    curr_patch = makeAvePatch(curr_plot_label, val_ave, CURR_COLOR, DICT_AVE_TEMP)
    ax_temp.axhline(val_ave, **CURR_AVEG_LINE_STYLE)
    # 前年月の外気温
    ax_temp.plot(df_prev[COL_PREV_PLOT_TIME], prev_temp_ser, color=PREV_COLOR, marker="")
    val_ave = prev_temp_ser.mean()
    prev_patch = makeAvePatch(prev_plot_label, val_ave, PREV_COLOR, DICT_AVE_TEMP)
    ax_temp.axhline(val_ave, **PREV_AVEG_LINE_STYLE)
    ax_temp.set_ylabel(Y_LABEL_TEMP_OUT, **LABEL_STYLE)
    # 凡例
    ax_temp.legend(handles=[curr_patch, prev_patch], **LEGEND_STYLE)
    ax_temp.set_title(main_title, **TITLE_STYLE)
    # Hide xlabel
    ax_temp.label_outer()


def _humid_plotting(ax_humid: Axes,
                    df_curr: DataFrame, df_prev: DataFrame,
                    curr_humid_ser: Series, prev_humid_ser: Series,
                    curr_plot_label: str, prev_plot_label: str) -> None:
    """
    湿度サブプロット(axes)に軸・軸ラベルを設定し、DataFrameオプジェクトの室内湿度データをプロットする
    :param ax_humid:湿度サブプロット(axes)
    :param df_curr:今年の年月DataFrame
    :param df_prev:前年の年月DataFrame
    :param curr_humid_ser: 現在の室内湿度データ
    :param prev_humid_ser: 前年の室内湿度データ
    :param curr_plot_label: 今年ラベル
    :param prev_plot_label: 前年ラベル
    """
    ax_humid.set_ylim(ymin=0., ymax=100.)
    # 最新年月
    ax_humid.plot(df_curr[COL_TIME], curr_humid_ser, color=CURR_COLOR, marker="")
    val_ave = curr_humid_ser.mean()
    curr_patch = makeAvePatch(curr_plot_label, val_ave, CURR_COLOR, DICT_AVEG_HUMID)
    ax_humid.axhline(val_ave, **CURR_AVEG_LINE_STYLE)
    # 前年月
    ax_humid.plot(df_prev[COL_PREV_PLOT_TIME], prev_humid_ser, color=PREV_COLOR, marker="")
    val_ave = prev_humid_ser.mean()
    prev_patch = makeAvePatch(prev_plot_label, val_ave, PREV_COLOR, DICT_AVEG_HUMID)
    ax_humid.axhline(val_ave, **PREV_AVEG_LINE_STYLE)
    ax_humid.set_ylabel(Y_LABEL_HUMID, **LABEL_STYLE)
    # 凡例
    ax_humid.legend(handles=[curr_patch, prev_patch], **LEGEND_STYLE)
    # Hide xlabel
    ax_humid.label_outer()


def _pressure_plotting(
        ax_pressure: Axes,
        df_curr: DataFrame, df_prev: DataFrame,
        curr_pressure_ser: Series, prev_pressure_ser: Series,
        curr_plot_label: str, prev_plot_label: str) -> None:
    """
    気圧サブプロット(axes)に軸・軸ラベルを設定し、DataFrameオプジェクトの気圧データをプロットする
    :param ax_pressure:気圧サブプロット(axes)
    :param df_curr:今年の年月DataFrame
    :param df_prev:前年の年月DataFrame
    :param curr_pressure_ser: 現在の気圧データ
    :param prev_pressure_ser: 前年の気圧データ
    :param curr_plot_label: 今年ラベル
    :param prev_plot_label: 前年ラベル
    """
    ax_pressure.set_ylim(PLOT_CONF["ylim"]["pressure"])
    # 最新年月
    ax_pressure.plot(df_curr[COL_TIME], curr_pressure_ser, color=CURR_COLOR, marker="")
    val_ave = curr_pressure_ser.mean()
    curr_patch = makeAvePatch(curr_plot_label, val_ave, CURR_COLOR, DICT_AVEG_PRESSURE)
    ax_pressure.axhline(val_ave, **CURR_AVEG_LINE_STYLE)
    # 前年月
    ax_pressure.plot(df_prev[COL_PREV_PLOT_TIME], prev_pressure_ser, color=PREV_COLOR, marker="")
    val_ave = prev_pressure_ser.mean()
    prev_patch = makeAvePatch(prev_plot_label, val_ave, PREV_COLOR, DICT_AVEG_PRESSURE)
    ax_pressure.axhline(val_ave, **PREV_AVEG_LINE_STYLE)
    ax_pressure.set_ylabel(Y_LABEL_PRESSURE, **LABEL_STYLE)
    # 凡例
    ax_pressure.legend(handles=[curr_patch, prev_patch], **LEGEND_STYLE)
    # X軸ラベル
    ax_pressure.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))


def make_graph(
        df_curr: DataFrame, df_prev: DataFrame,
        year_month: str, prev_year_month: str,
        logger: Optional[logging.Logger] = None,
        log_debug: bool = False
) -> Figure:
    """
    観測データのDataFrameからグラフを生成し描画領域を取得する
    :param df_curr: 今年の観測データをロードしたDataFrame
    :param df_prev: 前年の観測データをロードしたDataFrame
    :param year_month: 検索対象年月
    :param prev_year_month: 前年年月
    :param logger: app_logger
    :param log_debug: DEBUG出力するかどうか ※デフォルト False
    :return: 観測データをプロットした描画領域
    """
    # 凡例用ラベル
    # 今年年月
    curr_plot_label = makeLegendLabel(year_month)
    # 前年年月
    prev_plot_label = makeLegendLabel(prev_year_month)
    # タイトル
    title: str = FMT_MEASUREMENT_RANGE.format(curr_plot_label, prev_plot_label)

    # (1) 外気温データ(今年・前年)
    curr_temp_ser: Series = df_curr[COL_TEMP_OUT]
    prev_temp_ser: Series = df_prev[COL_TEMP_OUT]
    # (2) 湿度データ(今年・前年)
    curr_humid_ser: Series = df_curr[COL_HUMID]
    prev_humid_ser: Series = df_prev[COL_HUMID]
    # (3) 気圧データ(今年・前年)
    curr_pressure_ser: Series = df_curr[COL_PRESSURE]
    prev_pressure_ser: Series = df_prev[COL_PRESSURE]
    # 前年データをX軸にプロットするために測定時刻列にを1年プラスする
    df_prev[COL_PREV_PLOT_TIME] = df_prev[COL_TIME].apply(plusOneYear)

    fig: Figure
    ax_temp: Axes
    ax_humid: Axes
    ax_pressure: Axes
    # PCブラウザはinch指定でdpi=72
    fig = Figure(figsize=PLOT_CONF["figsize"]["pc"], constrained_layout=True)
    if logger is not None and log_debug:
        logger.debug(f"fig: {fig}")
    # x軸を共有する3行1列のサブプロット生成
    (ax_temp, ax_humid, ax_pressure) = fig.subplots(nrows=3, ncols=1, sharex=True)
    # Y方向のグリッド線のみ表示
    for ax in [ax_temp, ax_humid, ax_pressure]:
        ax.grid(**GRID_STYLE)

    # (1) 外気温領域のプロット
    _temperature_plotting(ax_temp,
                          df_curr, df_prev, curr_temp_ser, prev_temp_ser,
                          title, curr_plot_label, prev_plot_label)
    # (2) 湿度領域のプロット
    _humid_plotting(ax_humid,
                    df_curr, df_prev, curr_humid_ser, prev_humid_ser,
                    curr_plot_label, prev_plot_label)
    # (3) 気圧領域のプロット
    _pressure_plotting(ax_pressure,
                       df_curr, df_prev, curr_pressure_ser, prev_pressure_ser,
                       curr_plot_label, prev_plot_label)
    return fig


def gen_plot_image(
        df_curr: DataFrame, df_prev: DataFrame, year_month: str, logger=None
) -> str:
    """
    比較年月用の観測データの画像を生成する
    :param df_curr: 今年の年月データ
    :param df_prev: 前年の年月データ
    :param year_month: 今年の年月
    :param logger: app_logger
    :return: 比較年月データ画像(base64エンコード文字列)
    """
    log_debug: bool
    if logger is not None:
        log_debug = (logger.getEffectiveLevel() <= logging.DEBUG)
    else:
        log_debug = False

    # DataFrameの先頭から開始日を取得
    prev_year_month: str
    dt_first: datetime = df_prev.index[0].to_pydatetime()
    prev_year_month: str = dt_first.strftime("%Y-%m")
    if logger is not None and log_debug:
        logger.debug(f"prev_year_month: {prev_year_month}")

    # グラフ生成
    fig: Figure = make_graph(
        df_curr, df_prev, year_month, prev_year_month,
        logger=logger, log_debug=log_debug
    )
    # 画像をバイトストリームに溜め込みそれをbase64エンコードしてレスポンスとして返す
    img_src: str = convert_html_image_src(fig, logger=logger, log_debug=log_debug)
    return img_src

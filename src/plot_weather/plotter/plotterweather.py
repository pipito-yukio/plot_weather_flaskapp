import enum
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

from pandas.core.frame import DataFrame

import matplotlib.dates as mdates
from matplotlib import rcParams
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.legend import Legend
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from matplotlib.text import Text
from matplotlib.pyplot import setp

from .plottercommon import (
    PLOT_CONF, Y_LABEL_TEMP, Y_LABEL_HUMID, Y_LABEL_PRESSURE,
    convert_html_image_src
)
from plot_weather.loader.pandas_statistics import TempOutStat, get_temp_out_stat
from plot_weather.loader.dataframeloader import (
    COL_TIME, COL_TEMP_OUT, COL_TEMP_IN, COL_HUMID, COL_PRESSURE,
)
from plot_weather.util.date_util import (
    FMT_ISO8601, FMT_DATETIME, JP_WEEK_DAY_NAMES
)

""" 気象データ画像のbase64エンコードテキストデータを出力する """

# 日本語フォントの設定方法
#  日本語フォントとしてIPAexフォントがインストール済みのであると仮定した場合
# [A] 叉は [B] のどちらの方法でも指定された日本語フォントが反映される
# [A] https://matplotlib.org/3.1.0/gallery/text_labels_and_annotations/fdfont_family_rc_sgskip.html
#    Configuring the font family
# rcParams['font.family'] = 'sans-serif'
# rcParams[font.sans-serif] = ['IPAexGothic'] <<-- リスト
# ~/[Application root]/plot_weather/dao/conf/plot_weather.json: PLOT_CONF
rcParams["font.family"] = PLOT_CONF["font.families"]
# 可変長ゴシックフォント
rcParams["font.sans-serif"] = PLOT_CONF["sans-serif.font"][0]
# 固変定ゴシックフォント: 統計情報で使用
rcParams["font.monospace"] = PLOT_CONF['monospace.font'][0]
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
GRID_STYLES: Dict[str, Union[str, float]] = {"linestyle": "dashed", "linewidth": 1.0}
# 外気温統計情報のカラー定数定義
COLOR_MIN_TEMPER: str = "darkcyan"
COLOR_MAX_TEMPER: str = "orange"
COLOR_AVG_TEMPER: str = "red"


class PlotDateType(enum.Enum):
    """ 日付データ型 """
    TODAY = 0  # 当日データ
    YEAR_MONTH = 1  # 年月データ
    RANGE = 2  # 期間データ: 当日を含む過去日(検索開始日)からN日後


@dataclass
class PlotParam:
    # 画像プロット日付データ型 ※必須
    plote_date_type: PlotDateType
    # 検索開始日 (ISO8601形式) ※必須
    start_date: str
    # 検索終了日 (ISO8601形式): 期間データの場合は必須
    end_date: Optional[str]
    # 検索開始日のN日前: 期間データの場合は必須
    before_days: Optional[int]


def _make_title(
    plot_date_type: PlotDateType, start_date: str, end_date: Optional[str] = None
) -> str:
    def date_title(s_date: str, add_weekday: bool = False) -> str:
        date_parts: List[str] = s_date.split("-")
        title: str = f"{date_parts[0]}年{date_parts[1]}月{date_parts[2]}日"
        if add_weekday:
            dt: datetime = datetime.strptime(s_date, FMT_ISO8601)
            weekday_name: str = JP_WEEK_DAY_NAMES[dt.weekday()]
            title += f" ({weekday_name})"
        return title

    def month_title(s_date: str) -> str:
        date_parts: List[str] = s_date.split("-")
        return f"{date_parts[0]} 年 {date_parts[1]} 月"

    date_part: str
    if plot_date_type == PlotDateType.TODAY:
        # 当日データの場合は末尾に曜日出力
        date_part = date_title(start_date, add_weekday=True)
    elif plot_date_type == PlotDateType.YEAR_MONTH:
        # 年月データ
        date_part = month_title(start_date)
    else:
        # 開始日付〜終了日付
        date_part = f"{date_title(start_date)} 〜 {date_title(end_date)}"
    return date_part


def _set_x_axis_format(
        ax: Axes, plot_param: PlotParam, tick_lables_font_size: int
) -> None:
    """
    X軸のフォーマットとX軸ラベルのフォントを設定する
    :param ax 下段プロット領域
    :param plot_param: PlotParam
    :param tick_lables_font_size: X軸ラベルフォントサイズ
    """
    plot_date_type: PlotDateType = plot_param.plote_date_type
    start_date: str = plot_param.start_date
    if plot_date_type == PlotDateType.TODAY:
        # 表示範囲：当日の "00:00:00" から
        dt_min: datetime = datetime.strptime(start_date, FMT_ISO8601)
        # 翌日の "00:00:00" 迄
        dt_max: datetime = dt_min + timedelta(days=1)
        # 当日データx軸の範囲: 当日 00時 から 翌日 00時
        ax.set_xlim(xmin=dt_min, xmax=dt_max)
        # 当日データのx軸フォーマット: 軸ラベルは時間 (00,03,06,09,12,15,18,21,翌日の00)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H"))
    elif plot_date_type == PlotDateType.YEAR_MONTH:
        # 年月指定データのx軸フォーマット設定: 軸は"月/日"
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
    else:
        # 期間データのx軸フォーマット設定
        # datetimeオブジェタクトに変更
        dt_min: datetime = datetime.strptime(start_date, FMT_ISO8601)
        # デフォルトでは最後の軸に対応する日付ラベルが表示されない
        # 次の日の 00:30 までラベルを表示するための日付計算
        end_datetime: datetime = datetime.strptime(plot_param.end_date, FMT_ISO8601)
        end_datetime += timedelta(days=1)
        s_next_datetime: str = end_datetime.strftime(f"{FMT_ISO8601} 00:30:00")
        # datetimeオブジェクトに戻す
        dt_max = datetime.strptime(s_next_datetime, FMT_DATETIME)
        ax.set_xlim(xmin=dt_min, xmax=dt_max)
        if plot_param.before_days == 7:
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
            ax.tick_params(axis='x', labelsize=(tick_lables_font_size - 1))
        else:
            # [1,2,3] day
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H"))
            ax.tick_params(
                axis='x', labelsize=(tick_lables_font_size - 1),
                labelrotation=45
            )


def _temperature_plotting(
        ax: Axes, df: DataFrame, title_date: str,
        plot_date_type: PlotDateType, temp_out_stat: TempOutStat
) -> None:
    """
    温度サブプロット(axes)にタイトル、軸・軸ラベルを設定し、
    DataFrameオプジェクトの外気温・室内気温データをプロットする
    :param ax:温度サブプロット(axes)
    :param df:DataFrameオプジェクト
    :param title_date: タイトル ※日付
    :param plot_date_type: PlotDateType
    :param temp_out_stat: 外気温統計情報
    """
    def make_patch(
            label: str, appear_time: Optional[str], temper: float, s_color: str
    ) -> Patch:
        """ 指定されたラベルと外気温統計の凡例を生成 """
        if appear_time is not None:
            patch_label: str = f"{label} {temper:4.1f}℃ [{appear_time}]"
        else:
            # 出現時刻がない場合は平均値
            patch_label: str = f"{label} {temper:4.1f}℃"
        return Patch(color=s_color, label=patch_label)

    def plot_horizontal_line(
            axes: Axes, temper: float, s_color: str, linestyle: Optional[str] = None
    ):
        """ 指定された統計情報の外気温の横線を生成する """
        line_style: str = "dashed" if linestyle is None else linestyle
        axes.axhline(temper, color=s_color, linestyle=line_style, linewidth=1.)

    # 外気温・室内気温のプロット
    #  Y軸ラベルフォント
    label_font_size: int = PLOT_CONF["label.sizes"][0]
    # 凡例フォント
    legend_font_size: int = PLOT_CONF["legend-fontsize"]
    # {list: 1} [<matplotlib.lines.Line2D object at オブジェクトアドレス>]
    plot_temp_out_list: List[Line2D] = ax.plot(df[COL_TIME], df[COL_TEMP_OUT],
                                               color="blue", marker="", label="外気温")
    plot_temp_in_list: List[Line2D] = ax.plot(df[COL_TIME], df[COL_TEMP_IN],
                                              color="red", marker="", label="室内気温")
    ax.set_ylim(PLOT_CONF["ylim"]["temp"])
    ax.set_ylabel(Y_LABEL_TEMP, fontsize=label_font_size)
    ax.set_title(f"気象データ：{title_date}")
    # Hide xlabel
    ax.label_outer()
    # 外気温・室内気温の凡例: 常に左上端
    #  当日なら凡例のPatchに日付部分がないのでカラムを1行に2列とする, 期間なら 2行1列
    first_legend: Legend = ax.legend(
        handles=[plot_temp_out_list[0], plot_temp_in_list[0]],
        ncol=(2 if plot_date_type == plot_date_type.TODAY else 1),
        loc="upper left"
    )
    text: Text
    for text in first_legend.get_texts():
        # フォントサイズ設定
        text.set_fontsize(str(legend_font_size))
    ax.add_artist(first_legend)

    # 外気温統計情報を追加
    # 凡例に追加する最低・最高・平均気温の統計情報(Patch)を生成する
    # 出現時刻は日付データ型に応じて
    min_appear_time: str
    max_appear_time: str
    if plot_date_type == PlotDateType.TODAY:
        # 当日データの場合: 時分秒
        min_appear_time = temp_out_stat.min.appear_time[11:]
        max_appear_time = temp_out_stat.max.appear_time[11:]
    else:
        # 当日データ以外の場合: 年月日 + 時分秒
        min_appear_time = temp_out_stat.min.appear_time
        max_appear_time = temp_out_stat.max.appear_time
    mim_temper_patch: Patch = make_patch(
        "最低", min_appear_time, temp_out_stat.min.temper, COLOR_MIN_TEMPER
    )
    max_temper_patch: Patch = make_patch(
        "最高", max_appear_time, temp_out_stat.max.temper, COLOR_MAX_TEMPER
    )
    avg_temper_patch: Patch = make_patch(
        "平均", None, temp_out_stat.average_temper, COLOR_AVG_TEMPER
    )
    # 最低気温の横線
    plot_horizontal_line(ax, temp_out_stat.min.temper, COLOR_MIN_TEMPER)
    # 最高気温の横線
    plot_horizontal_line(ax, temp_out_stat.max.temper, COLOR_MAX_TEMPER)
    # 平均気温の横線
    #  最低気温と最高気温の差が既定値以下なら平均線を出力しない ※線が接近して非常に見づらい
    appear_threthold: float = PLOT_CONF["appear_avg_line.threthold_diff_temper"]
    diff_temper: float = abs(temp_out_stat.max.temper - temp_out_stat.min.temper)
    if diff_temper > appear_threthold:
        # しきい値より大きかったら出力
        plot_horizontal_line(
            ax, temp_out_stat.average_temper, COLOR_AVG_TEMPER, linestyle="dashdot"
        )
    # Create TempOutStat legend
    stat_legend: Legend = ax.legend(
        handles=[mim_temper_patch, max_temper_patch, avg_temper_patch],
        title="外気温統計",
        loc="best"
    )
    # 統計情報は日本語固定フォントを設定
    for text in stat_legend.get_texts():
        text.set_fontfamily("monospace")
        text.set_fontsize(str(legend_font_size))


def gen_figure(phone_size: str = None, logger=None, log_debug=False) -> Figure:
    """
    リクエスト端末に応じたサイズのプロット領域枠(Figure)を生成する
    :param phone_size: リクエスト端末がモバイルのイメージ描画情報 ※任意 (ブラウザの場合はNone)
    :param logger: app_logger
    :param log_debug: デバック出力フラグ (デフォルト無効)
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
        if logger is not None and log_debug:
            logger.debug(f"px: {px} / density : {density}")
            logger.debug(f"fig_width_px: {fig_width_px}, fig_height_px: {fig_height_px}")
        fig = Figure(figsize=(fig_width_px, fig_height_px), constrained_layout=True)
    else:
        # PCブラウザはinch指定でdpi=72
        fig = Figure(figsize=PLOT_CONF["figsize"]["pc"], constrained_layout=True)
    if logger is not None and log_debug:
        logger.debug(f"fig: {fig}")
    return fig


def make_graph(
        df: DataFrame,
        plot_param: PlotParam,
        phone_image_size: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        log_debug: bool = False
) -> Figure:
    """
    観測データのDataFrameからグラフを生成し描画領域を取得する
    :param df DataFrame ※必須
    :param plot_param: PlotParam ※必須
    :param phone_image_size: スマホの場合は表示領域サイズ情報
    :param logger: app_logger
    :param log_debug: DEBUG出力するかどうか ※デフォルト False
    :return: 観測データをプロットした描画領域
    """
    # 図の生成
    fig: Figure = gen_figure(phone_image_size, logger=logger, log_debug=log_debug)
    # x軸を共有する3行1列のサブプロット生成
    ax_temp: Axes
    ax_humid: Axes
    ax_pressure: Axes
    (ax_temp, ax_humid, ax_pressure) = fig.subplots(
        nrows=3, ncols=1, sharex=True,
        gridspec_kw={'height_ratios': PLOT_CONF["axes_height_ratio"]}
    )
    for ax in [ax_temp, ax_humid, ax_pressure]:
        ax.grid(**GRID_STYLES)

    # 軸ラベルのフォントサイズを設定
    #  ラベルフォントサイズ, y軸ラベルフォントサイズ, x軸(日付)ラベルフォントサイズ
    label_font_size: int = PLOT_CONF["label.sizes"][0]
    y_tick_labels_font_size: int = PLOT_CONF["label.sizes"][1]
    x_tick_lables_font_size: int = PLOT_CONF["label.sizes"][2]
    # 気圧プロット領域
    setp(ax_pressure.get_xticklabels(), fontsize=x_tick_lables_font_size)
    # 全プロット領域
    for ax in [ax_temp, ax_humid, ax_pressure]:
        setp(ax.get_yticklabels(), fontsize=y_tick_labels_font_size)

    # サブプロットの設定
    # 外気温統計情報を取得する
    temp_out_stat: TempOutStat = get_temp_out_stat(df)
    if logger is not None and log_debug:
        logger.debug(temp_out_stat)
    # タイトルの日付部分
    title_date: str = _make_title(
        plot_param.plote_date_type, plot_param.start_date, plot_param.end_date
    )
    # 1.外気温と室内気温、外気温統計情報
    _temperature_plotting(
        ax_temp, df, title_date, plot_param.plote_date_type, temp_out_stat
    )
    # 2.室内湿度
    ax_humid.plot(df[COL_TIME], df[COL_HUMID], color="green", marker="")
    ax_humid.set_ylim(ymin=0., ymax=100.)
    ax_humid.set_ylabel(Y_LABEL_HUMID, fontsize=label_font_size)
    # Hide xlabel
    ax.label_outer()
    # 3.気圧
    # X軸のフォーマット
    _set_x_axis_format(ax_pressure, plot_param, x_tick_lables_font_size)
    # 気圧データプロット
    ax_pressure.plot(df[COL_TIME], df[COL_PRESSURE], color="fuchsia", marker="")
    ax_pressure.set_ylim(PLOT_CONF["ylim"]["pressure"])
    ax_pressure.set_ylabel(Y_LABEL_PRESSURE, fontsize=label_font_size)
    return fig


def gen_plot_image(
        df: DataFrame,
        plot_param: PlotParam,
        phone_image_size: Optional[str] = None,
        logger: Optional[logging.Logger] = None
) -> str:
    """
    観測データの画像を生成する
    :param df DataFrame ※必須
    :param plot_param: PlotParam ※必須
    :param phone_image_size: スマホの場合は表示領域サイズ情報
    :param logger: app_logger
    :return: 画像(base64エンコード文字列)
    """
    log_debug: bool
    if logger is not None:
        log_debug = (logger.getEffectiveLevel() <= logging.DEBUG)
    else:
        log_debug = False

    # グラフ生成
    fig: Figure = make_graph(
        df, plot_param, phone_image_size=phone_image_size,
        logger=logger, log_debug=log_debug
    )
    # 画像をバイトストリームに溜め込みそれをbase64エンコードしてレスポンスとして返す
    return convert_html_image_src(fig, logger=logger, log_debug=log_debug)

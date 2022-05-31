import base64
from io import BytesIO

from matplotlib import rcParams
from matplotlib.dates import DateFormatter
from matplotlib.figure import Figure
from matplotlib.pyplot import setp

from ..dao.weathercommon import PLOT_CONF, WEATHER_CONF
from ..dao.weatherdbwithpandas import PLOT_WEATHER_IDX_COLUMN, WeatherPandas
from ..util.dateutil import (
    addDayString,
    datetimeToJpDateWithWeek,
    strDateToDatetimeTime000000,
)

""" 気象データ画像のbase64エンコードテキストデータを出力する """

# 日本語フォント設定
# https://matplotlib.org/3.1.0/gallery/text_labels_and_annotations/font_family_rc_sgskip.html
rcParams["font.family"] = PLOT_CONF["font.family"]


def gen_plotimage(conn, year_month=None, logger=None):
    wpd = WeatherPandas(conn, logger=logger)
    if year_month is None:
        # 本日データ
        df = wpd.getTodayDataFrame(
            WEATHER_CONF["DEVICE_NAME"], today=WEATHER_CONF["TODAY"]
        )
        # タイムスタンプをデータフレームのインデックスに設定
        df.index = df[PLOT_WEATHER_IDX_COLUMN]
        # 先頭の測定日付(Pandas Timestamp) から Pythonのdatetimeに変換
        # https://pandas.pydata.org/pandas-docs/version/0.22/generated/pandas.Timestamp.to_datetime.html
        first_datetime = df.index[0].to_pydatetime()
        # 当日の日付文字列 ※一旦 date()オブジェクトに変換して"年月日"を取得
        s_first_date = first_datetime.date().isoformat()
        # 表示範囲：当日の "00:00:00" から
        x_day_min = strDateToDatetimeTime000000(s_first_date)
        # 翌日の "00:00:00" 迄
        s_nextday = addDayString(s_first_date)
        x_day_max = strDateToDatetimeTime000000(s_nextday)
        # タイトル用の日本語日付(曜日)
        s_title_date = datetimeToJpDateWithWeek(first_datetime)
    else:
        # 指定された年月データ
        df = wpd.getMonthDataFrame(WEATHER_CONF["DEVICE_NAME"], year_month)
        # タイムスタンプをデータフレームのインデックスに設定
        df.index = df[PLOT_WEATHER_IDX_COLUMN]
        # タイトル用の日本語日付(曜日)
        splited = year_month.split("-")
        s_title_date = f"{splited[0]}年{splited[1]}月"
    # データフレームをDEBUGレベルでログに出力
    if logger is not None:
        logger.debug(df)

    # https://matplotlib.org/stable/api/figure_api.html?highlight=figure#module-matplotlib.figure
    fig = Figure(figsize=PLOT_CONF["figsize"]["pc"])
    label_fontsize, ticklabel_fontsize, ticklable_date_fontsize = tuple(
        PLOT_CONF["label.sizes"]
    )
    grid_styles = {"linestyle": "- -", "linewidth": 1.0}
    # PC用
    # TypeError: subplots() got an unexpected keyword argument 'constrained_layout'
    (ax_temp, ax_humid, ax_pressure) = fig.subplots(3, 1, sharex=True)

    # サブプロット間の間隔を変更する
    # Figure(..., constrained_layout=True) と subplots_adjust()は同時に設定できない
    # UserWarning: This figure was using constrained_layout,
    #  but that is incompatible with subplots_adjust and/or tight_layout; disabling constrained_layout.
    fig.subplots_adjust(wspace=0.1, hspace=0.1)
    # 軸ラベルなどのフォントサイズを設定
    for ax in [ax_temp, ax_humid, ax_pressure]:
        setp(ax.get_xticklabels(), fontsize=ticklable_date_fontsize)
        setp(ax.get_yticklabels(), fontsize=ticklabel_fontsize)

    if year_month is None:
        # 1日データx軸の範囲: 当日 00時 から 翌日 00時
        for ax in [ax_temp, ax_humid, ax_pressure]:
            ax.set_xlim([x_day_min, x_day_max])

    # temp_out and temp_in
    ax_temp.plot(
        df[PLOT_WEATHER_IDX_COLUMN],
        df["temp_out"],
        color="blue",
        marker="",
        label="外気温",
    )
    ax_temp.plot(
        df[PLOT_WEATHER_IDX_COLUMN], df["temp_in"], color="red", marker="", label="室内気温"
    )
    ax_temp.set_ylim(PLOT_CONF["ylim"]["temp"])
    ax_temp.set_ylabel("気温 (℃)", fontsize=label_fontsize)
    ax_temp.legend(loc="upper right")
    ax_temp.set_title("気象データ：{}".format(s_title_date))
    # Hide xlabel
    ax_temp.label_outer()
    ax_temp.grid(grid_styles)

    # humid
    ax_humid.plot(df[PLOT_WEATHER_IDX_COLUMN], df["humid"], color="green", marker="")
    ax_humid.set_ylim([0, 100])
    ax_humid.set_ylabel("室内湿度 (％)", fontsize=label_fontsize)
    # Hide xlabel
    ax_humid.label_outer()
    ax_humid.grid(grid_styles)

    # pressure
    if year_month is None:
        # 当日データなので軸ラベルは時間 (00,03,06,09,12,15,18,21,翌日の00)
        ax_pressure.xaxis.set_major_formatter(DateFormatter("%H"))
    else:
        # 指定年月していなので軸は"月/日"
        ax_pressure.xaxis.set_major_formatter(DateFormatter("%m/%d"))

    ax_pressure.plot(
        df[PLOT_WEATHER_IDX_COLUMN], df["pressure"], color="fuchsia", marker=""
    )
    ax_pressure.set_ylim(PLOT_CONF["ylim"]["pressure"])
    ax_pressure.set_ylabel("hPa", fontsize=label_fontsize)
    ax_pressure.grid(grid_styles)

    # 画像をバイトストリームに溜め込みそれをbase64エンコードしてレスポンスとして返す
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    data = base64.b64encode(buf.getbuffer()).decode("ascii")
    img_src = "data:image/png;base64," + data
    return img_src

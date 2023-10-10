import base64
from io import BytesIO
from matplotlib.figure import Figure

# フィールド定義
# pandas.DataFrameのインデックス列
COL_TIME: str = 'measurement_time'
COL_TEMP_IN: str = 'temp_in'
COL_TEMP_OUT: str = 'temp_out'
COL_HUMID: str = "humid"
COL_PRESSURE: str = 'pressure'
# 共通ラベル
Y_LABEL_HUMID: str = '室内湿度 (％)'
Y_LABEL_PRESSURE: str = '気　圧 (hPa)'
# 個別
Y_LABEL_TEMP: str = '気温 (℃)'
Y_LABEL_TEMP_OUT: str = '外気温 (℃)'


def convert_html_image_src(fig: Figure, logger=None, logger_debug=False) -> str:
    """
    プロット(Figure)オブジェクトのbase64エンコードを取得する
    :param fig: Figure
    :param logger: app_logger
    :param logger_debug: デバック出力可否
    :return: 画像のbase64エンコード文字列
    """
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    data = base64.b64encode(buf.getbuffer()).decode("ascii")
    if logger is not None and logger_debug:
        logger.debug(f"data.len: {len(data)}")
    return "data:image/png;base64," + data

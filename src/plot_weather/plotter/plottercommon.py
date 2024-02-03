import base64
import os
from io import BytesIO
from matplotlib.figure import Figure

import plot_weather.util.file_util as fu

""" 画像プロットに必要な共通定数定義 """

# プロット設定ファイル
_base_dir: str = os.path.abspath(os.path.dirname(__file__))
_conf_path: str = os.path.join(_base_dir, "conf")
PLOT_CONF = fu.read_json(os.path.join(_conf_path, "plot_weather.json"))

# フィールド定義
# pandas.DataFrameのインデックス列
# 共通ラベル
Y_LABEL_HUMID: str = '室内湿度 (％)'
Y_LABEL_PRESSURE: str = '気　圧 (hPa)'
# 個別
Y_LABEL_TEMP: str = '気温 (℃)'
Y_LABEL_TEMP_OUT: str = '外気温 (℃)'



def convert_html_image_src(fig: Figure, logger=None, log_debug=False) -> str:
    """
    プロット(Figure)オブジェクトのbase64エンコードを取得する
    :param fig: Figure
    :param logger: app_logger
    :param log_debug: デバック出力可否
    :return: 画像のbase64エンコード文字列
    """
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    data = base64.b64encode(buf.getbuffer()).decode("ascii")
    if logger is not None and log_debug:
        logger.debug(f"data.len: {len(data)}")
    return "data:image/png;base64," + data

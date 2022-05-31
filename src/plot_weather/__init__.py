import os
import uuid

from flask import Flask

from plot_weather.log import logsetting
from plot_weather.util.image_util import image_to_base64encoded

BAD_REQUEST_IMAGE_DATA = None
INTERNAL_SERVER_ERROR_IMAGE_DATA = None

app = Flask(__name__)
# ロガーを本アプリ用のものに設定する
app_logger = logsetting.get_logger("app_main")
app.config.from_object("plot_weather.config")
# HTMLテンプレートに使うメッセージキーをapp.configに読み込み
app.config.from_pyfile(os.path.join(".", "messages/messages.conf"), silent=False)
# リクエストヘッダに設定するキーをapp.configに読み込み
app.config.from_pyfile(os.path.join(".", "messages/requestkeys.conf"), silent=False)
# セッション用の秘密キー
app.secret_key = uuid.uuid4().bytes
# Strip newline
app.jinja_env.lstrip_blocks = True
app.jinja_env.trim_blocks = True

# サーバホストとセッションのドメインが一致しないとブラウザにセッションIDが設定されない
IP_HOST = os.environ.get("IP_HOST", "localhost:5000")
has_prod = os.environ.get("ENV") == "production"
if has_prod:
    # Production mode
    SERVER_HOST = IP_HOST + ":8080"
else:
    SERVER_HOST = IP_HOST + ":5000"

app.config["SERVER_NAME"] = SERVER_HOST
app.config["APPLICATION_ROOT"] = "/plot_weather"
# use flask jsonify with japanese message
app.config["JSON_AS_ASCII"] = False
app_logger.debug("app.secret_key: {}".format(app.secret_key))
app_logger.debug("{}".format(app.config))
# "BAD REQUEST"用画像のbase64エンコード文字列ファイル
curr_dir = os.path.dirname(__file__)
cotent_path = os.path.join(curr_dir, "static", "content")
file_bad_request = os.path.join(cotent_path, "BadRequest_png_base64encoded.txt")
BAD_REQUEST_IMAGE_DATA = image_to_base64encoded(file_bad_request)
# "Internal Server Error"用画像のbase64エンコード文字列ファイル
file_internal_error = os.path.join(
    cotent_path, "InternalServerError_png_base64encoded.txt"
)
INTERNAL_SERVER_ERROR_IMAGE_DATA = image_to_base64encoded(file_internal_error)

# Application main program
from plot_weather.views import app_main

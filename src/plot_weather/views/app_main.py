from flask import abort, g, jsonify, render_template
from plot_weather import app, app_logger
from plot_weather.dao.weathercommon import WEATHER_CONF
from plot_weather.dao.weatherdao import WeatherDao, weather_db
from plot_weather.db import sqlite3db
from plot_weather.db.sqlite3conv import DateFormatError, strdate2timestamp
from plot_weather.plotter.plotterweather import gen_plotimage
from werkzeug.exceptions import BadRequest

APP_ROOT = app.config["APPLICATION_ROOT"]
CODE_BAD_REQUEST = 400
MSG_TITLE_SUFFIX = app.config["TITLE_SUFFIX"]
MSG_STR_TODAY = app.config["STR_TODAY"]


def get_dbconn():
    db = getattr(g, "_dbconn", None)
    if db is None:
        db = g._dbconn = sqlite3db.get_connection(
            weather_db, read_only=True, logger=app_logger
        )
    return db


@app.errorhandler(BadRequest)
def incorrect_access(ex):
    return "Bad request !", CODE_BAD_REQUEST


@app.teardown_appcontext
def close_conn(exception):
    db = getattr(g, "_dbconn", None)
    app_logger.debug(f"db:{db}")
    if db is not None:
        db.close()


@app.route(APP_ROOT, methods=["GET"])
def index():
    """本日データ表示画面 (初回リクエストのみ)

    :return: 本日データ表示HTMLページ (matplotlibでプロットした画像含む)
    """
    try:
        conn = get_dbconn()
        # 年月日リスト取得
        dao = WeatherDao(conn, logger=app_logger)
        yearMonthList = dao.getGroupbyMonths(
            device_name=WEATHER_CONF["DEVICE_NAME"],
            start_date=WEATHER_CONF["STA_YEARMONTH"],
        )
        app_logger.debug("yearMonthList:{}".format(yearMonthList))
        # 本日データプロット画像取得
        imgBase64Encoded = gen_plotimage(conn, logger=app_logger)
    except Exception as exp:
        app_logger.error(exp)
        return abort(501)

    strToday = app.config.get("STR_TODAY")
    titleSuffix = app.config.get("TITLE_SUFFIX")
    defaultMainTitle = strToday + titleSuffix
    return render_template(
        "showplotweather.html",
        ip_host=app.config["SERVER_NAME"],
        app_root_url=APP_ROOT,
        path_get_today="/gettoday",
        path_get_month="/getmonth/",
        str_today=strToday,
        title_suffix=titleSuffix,
        info_today_update_interval=app.config.get("INFO_TODAY_UPDATE_INTERVAL"),
        default_main_title=defaultMainTitle,
        year_month_list=yearMonthList,
        img_src=imgBase64Encoded,
    )


@app.route("/plot_weather/gettoday", methods=["GET"])
def getTodayImage():
    """本日データ取得リクエスト(2回以降) JavaScriptからのリクエスト想定

    :return: jSON形式(matplotlibでプロットした画像データ(形式: png)のbase64エンコード済み文字列)
         (出力内容) jSON('data:image/png;base64,... base64encoded data ...')
    """
    app_logger.debug("getTodayImage()")
    try:
        conn = get_dbconn()
        # 本日データプロット画像取得
        imgBase64Encoded = gen_plotimage(conn, year_month=None, logger=app_logger)
    except Exception as exp:
        app_logger.error(exp)
        return _create_error_response(501)

    return _create_image_response(imgBase64Encoded)


@app.route("/plot_weather/getmonth/<yearmonth>", methods=["GET"])
def getMonthImage(yearmonth):
    """要求された年月の月間データ取得

    :param yearmonth str: 年月 (例) 2022-01
    :return: jSON形式(matplotlibでプロットした画像データ(形式: png)のbase64エンコード済み文字列)
         (出力内容) jSON('data:image/png;base64,... base64encoded data ...')
    """
    app_logger.debug("yearmonth: {}".format(yearmonth))
    try:
        # リクエストパラメータの妥当性チェック: "YYYY-mm" + "-01"
        chk_yyyymmdd = yearmonth + "-01"
        # 日付チェック(YYYY-mm-dd): 日付不正の場合例外スロー
        strdate2timestamp(chk_yyyymmdd, raise_error=True)
        conn = get_dbconn()
        # 指定年月(year_month)データプロット画像取得
        imgBase64Encoded = gen_plotimage(conn, year_month=yearmonth, logger=app_logger)
    except DateFormatError as dfe:
        # BAD Request
        app_logger.warning(dfe)
        return _create_error_response(400)
    except Exception as exp:
        # ここにくるのはDBエラー・バグなど想定
        app_logger.error(exp)
        return _create_error_response(501)

    return _create_image_response(imgBase64Encoded)


@app.route("/plot_weather/getcurrenttimedata", methods=["GET"])
def getcurrenttimedata():
    """現在時刻での最新の気象データを取得する"""
    app_logger.debug("getcurrenttimedata()")
    try:
        conn = get_dbconn()
        # 現在時刻時点の最新の気象データ取得
        dao = WeatherDao(conn, logger=app_logger)
        (measurement_time, temp_out, temp_in, humid, pressure) = dao.getLastData(
            device_name=WEATHER_CONF["DEVICE_NAME"]
        )
    except Exception as exp:
        app_logger.error(exp)
        return _create_error_response(501)

    return _create_currtimedatae_response(
        measurement_time, temp_out, temp_in, humid, pressure
    )


def _create_image_response(img_src):
    resp_obj = {"status": "success"}
    resp_obj["data"] = {"img_src": img_src}
    return jsonify(resp_obj)


def _create_currtimedatae_response(mesurement_time, temp_out, temp_in, humid, pressure):
    resp_obj = {"status": "success"}
    resp_obj["data"] = {
        "measurement_time": mesurement_time,
        "temp_out": temp_out,
        "temp_in": temp_in,
        "humid": humid,
        "pressure": pressure,
    }
    return jsonify(resp_obj)


def _create_error_response(err_code):
    resp_obj = {"status": "error", "code": err_code}
    return jsonify(resp_obj)

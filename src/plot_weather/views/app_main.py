from datetime import date, datetime
from typing import Dict, List, Optional, Tuple, Union

from flask import (
    abort, g, jsonify, render_template, request, make_response, Response
)
from werkzeug.datastructures import Headers, MultiDict
from werkzeug.exceptions import (
    BadRequest, Forbidden, HTTPException, InternalServerError, NotFound
)

from pandas.core.frame import DataFrame

import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extensions import connection

from plot_weather import (BAD_REQUEST_IMAGE_DATA,
                          INTERNAL_SERVER_ERROR_IMAGE_DATA,
                          NO_IMAGE_DATA,
                          DebugOutRequest,
                          app, app_logger, app_logger_debug)
from plot_weather.dao.weatherdao import WeatherDao
from plot_weather.dao.weatherstatdao import TempOutStatDao
from plot_weather.dao.devicedao import DeviceDao, DeviceRecord
from plot_weather.db.sqlite3conv import DateFormatError, strdate2timestamp
from plot_weather.loader.dataframeloader import (
    loadTodayDataFrame, loadMonthDataFrame, loadBeforeDaysRangeDataFrame
)
from plot_weather.loader.dataframeloader_prevcomp import loadPrevCompDataFrames
from plot_weather.plotter.plotterweather import (
    gen_plot_image, PlotDateType, PlotParam
)
from plot_weather.plotter.plotterweather_prevcomp import (
    gen_plot_image as gen_comp_prev_plot_image
)
import plot_weather.util.date_util as date_util

APP_ROOT: str = app.config["APPLICATION_ROOT"]

# エラーメッセージの内容 ※messages.confで定義
MSG_REQUIRED: str = app.config["MSG_REQUIRED"]
MSG_INVALID: str = app.config["MSG_INVALID"]
MSG_NOT_FOUND: str = app.config["MSG_NOT_FOUND"]
# ヘッダー
# トークン ※携帯端末では必須, 一致 ※ない場合は不一致とみなす
# messages.conf で定義済み
# 端末サイズ情報 ※携帯端末では必須, 形式は 幅x高さx密度
MSG_PHONE_IMG: str = "phone image size"
REQUIRED_PHONE_IMG: str = f"401,{MSG_PHONE_IMG} {MSG_REQUIRED}"
INVALID_PHONE_IMG: str = f"402,{MSG_PHONE_IMG} {MSG_INVALID}"
# リクエストパラメータ
PARAM_DEVICE: str = "device_name"
PARAM_START_DAY: str = "start_day"
PARAM_BOFORE_DAYS: str = "before_days"
PARAM_YEAR_MONTH: str = "year_month"

# リクエストパラメータエラー時のコード: 421番台以降
# デバイス名: 必須, 長さチェック (1-20byte), 未登録
DEVICE_LENGTH: int = 20
#  デバイスリスト取得クリエスと以外の全てのリクエスト
REQUIRED_DEVICE: str = f"421,{PARAM_DEVICE} {MSG_REQUIRED}"
INVALIDD_DEVICE: str = f"422,{PARAM_DEVICE} {MSG_INVALID}"
DEVICE_NOT_FOUND: str = f"423,{PARAM_DEVICE} {MSG_NOT_FOUND}"
# 期間指定画像取得リクエスト
#  (1)検索開始日["start_day"]: 任意 ※未指定ならシステム日付を検索開始日とする
#     日付形式(ISO8601: YYYY-mm-dd), 10文字一致
INVALID_START_DAY: str = f"431,{PARAM_START_DAY} {MSG_INVALID}"
#  (2)検索開始日から N日前 (1,2,3,7日): 必須
REQUIRED_BOFORE_DAY: str = f"433,{PARAM_BOFORE_DAYS} {MSG_REQUIRED}"
INVALID_BOFORE_DAY: str = f"434,{PARAM_BOFORE_DAYS} {MSG_INVALID}"
# 月間指定画像取得リクエスト
#   年月: 必須, 形式(YYYY-mm), 7文字一致
REQUIRED_YEAR_MONTH: str = f"435,{PARAM_YEAR_MONTH} {MSG_REQUIRED}"
INVALID_YEAR_MONTH: str = f"436,{PARAM_YEAR_MONTH} {MSG_INVALID}"

# エラーメッセージを格納する辞書オブジェクト定義
MSG_DESCRIPTION: str = "error_message"
# 固定メッセージエラー辞書オブジェクト
ABORT_DICT_UNMATCH_TOKEN: Dict[str, str] = {
    MSG_DESCRIPTION: app.config["UNMATCH_TOKEN"]}
# 可変メッセージエラー辞書オブジェクト: ""部分を置き換える
ABORT_DICT_BLANK_MESSAGE: Dict[str, str] = {MSG_DESCRIPTION: ""}


def get_connection() -> connection:
    if 'db' not in g:
        conn_pool: SimpleConnectionPool = app.config["postgreSQL_pool"]
        g.db: connection = conn_pool.getconn()
        g.db.set_session(readonly=True, autocommit=True)
        if app_logger_debug:
            app_logger.debug(f"g.db:{g.db}")
    return g.db


@app.teardown_appcontext
def close_connection(exception=None) -> None:
    db: connection = g.pop('db', None)
    if app_logger_debug:
        app_logger.debug(f"db:{db}")
    if db is not None:
        app.config["postgreSQL_pool"].putconn(db)


@app.route(APP_ROOT, methods=["GET"])
def index() -> str:
    """本日データ表示画面 (初回リクエストのみ)

    :return: 本日データ表示HTMLページ (matplotlibでプロットした画像含む)
    """
    # 前回アプリ実行時のデバイス名がクッキーに存在するか
    device_in_cookie: str = request.cookies.get(PARAM_DEVICE)
    if app_logger_debug:
        app_logger.debug(
            f"{request.path}, cookie.device_name: {device_in_cookie}")

    try:
        conn: connection = get_connection()
        # センサーデバイスリスト取得
        device_dao: DeviceDao = DeviceDao(conn, logger=app_logger)
        devices: List[DeviceRecord] = device_dao.get_devices()
        device_dict_list: List[Dict[str, str]
                               ] = DeviceDao.to_dict_without_id(devices)
        if app_logger_debug:
            app_logger.debug(f"device_dict_list:{device_dict_list}")

        # 前回選択されたデバイス名存在したら関連する年月リストと当日画像をロードする
        ym_list: Optional[List[str]] = None
        prev_ym_list: Optional[List[str]] = None
        rec_count: Optional[int] = None
        img_base64_encoded: Optional[str] = None
        if device_in_cookie is not None:
            # 当日: ブラウザ版は開発環境利用も考慮し最終日付とする
            conn: connection = get_connection()
            dao: WeatherDao = WeatherDao(conn, logger=app_logger)
            last_register_day: Optional[str] = dao.getLastRegisterDay(
                device_in_cookie)
            today_date: str
            if last_register_day is not None:
                today_date = last_register_day
            else:
                today_date = date.today().strftime(date_util.FMT_ISO8601)
            # 年月リスト
            ym_list = dao.getGroupByMonths(device_in_cookie)
            prev_ym_list = dao.getPrevYearMonthList(device_in_cookie)
            # DataFrameの取得
            rec_count: int
            df: Optional[DataFrame]
            rec_count, df = loadTodayDataFrame(
                conn, device_in_cookie, today_date,
                logger=app_logger, logger_debug=app_logger_debug
            )
            if rec_count > 0:
                # 当日データのパラメータ生成
                plot_param: PlotParam = PlotParam(
                    plote_date_type=PlotDateType.TODAY,
                    start_date=today_date, end_date=None, before_days=None
                )
                img_base64_encoded: str = gen_plot_image(
                    df, plot_param, phone_image_size=None, logger=app_logger
                )
        if rec_count is None or rec_count == 0:
            # No image
            img_base64_encoded = NO_IMAGE_DATA

        return render_template(
            "showplotweather.html",
            info_today_update_interval=app.config.get(
                "INFO_TODAY_UPDATE_INTERVAL"),
            app_root_url=APP_ROOT,
            ip_host=app.config["SERVER_NAME"],
            path_get_today_image="/gettodayimage/",
            path_get_month_image="/getmonthimage/",
            path_get_comp_prevyear_image="/getcompprevyearimage/",
            path_get_ym_list="/getyearmonthlistwithdevice/",
            no_image_src=NO_IMAGE_DATA,
            default_radio='today',
            device_dict_list=device_dict_list,
            device_name=device_in_cookie if device_in_cookie is not None else '',
            ym_list=ym_list if ym_list is not None else [],
            prev_ym_list=prev_ym_list if prev_ym_list is not None else [],
            ym_list_loaded=True if ym_list is not None else False,
            rec_count=rec_count if rec_count is not None else 0,
            img_src=img_base64_encoded,
        )
    except Exception as exp:
        app_logger.error(exp)
        abort(InternalServerError.code,
              InternalServerError(original_exception=exp))


@app.route("/plot_weather/getyearmonthlistwithdevice/<device_name>", methods=["GET"])
def getYearMonthListWithDevice(device_name) -> Response:
    """要求されたデバイス名の年月リストと前年比較用年月リストを取得

    :param device_name str: デバイス名 (例) esp8266_1
    :return: JSON形式のレスポンス
    (出力例) {"data":{"ymList":[年月リスト],"prevYmList":[前年比較用年月リスト]'}, "status":...)
    """
    if app_logger_debug:
        app_logger.debug(f"{request.path}, device_name: {device_name}")

    try:
        conn: connection = get_connection()
        dao: WeatherDao = WeatherDao(conn, logger=app_logger)
        # 年月リスト取得
        ym_list: List[str] = dao.getGroupByMonths(device_name)
        # 前年比較用年月リスト
        prev_ym_list: List[str] = dao.getPrevYearMonthList(device_name)
        result: Dict = {
            "status": "success",
            "data": {"ymList": ym_list, "prevYmList": prev_ym_list}
        }
        resp: Response = _make_respose(result, 200)
        # デバイス名をクッキーにセット
        # https://flask.palletsprojects.com/en/3.0.x/config/
        #  PERMANENT_SESSION_LIFETIME: Default: timedelta(days=31) (2678400 seconds)
        resp.set_cookie(PARAM_DEVICE, device_name)
        return resp
    except psycopg2.Error as db_err:
        app_logger.error(db_err)
        abort(InternalServerError.code, _set_errormessage(f"559,{db_err}"))
    except Exception as exp:
        app_logger.error(exp)
        return _createErrorImageResponse(InternalServerError.code)


@app.route("/plot_weather/gettodayimage/<device_name>", methods=["GET"])
def getTodayImage(device_name: str) -> Response:
    """本日データ取得リクエスト JavaScriptからのリクエスト想定

    :param device_name: デバイス名 ※必須
    :return: JSON形式(matplotlibでプロットした画像データ(形式: png)のbase64エンコード済み文字列)
            (出力例) {"data":"image/png;base64,... base64encoded data ...", "rec_count": 件数}
    """
    if app_logger_debug:
        app_logger.debug(f"{request.path}, device_name: {device_name}")

    # デバイス名 ※必須
    try:
        conn: connection = get_connection()
        # 本日データプロット画像取得
        dao: WeatherDao = WeatherDao(conn, logger=app_logger)
        last_day: Optional[str] = dao.getLastRegisterDay(device_name)
        today_date: str
        if last_day is not None:
            today_date = last_day
        else:
            today_date = date.today().strftime(date_util.FMT_ISO8601)
        # DataFrameの取得
        rec_count: int
        df: Optional[DataFrame]
        rec_count, df = loadTodayDataFrame(
            conn, device_name, today_date,
            logger=app_logger, logger_debug=app_logger_debug
        )
        if rec_count > 0:
            # 当日データのパラメータ生成
            plot_param: PlotParam = PlotParam(
                plote_date_type=PlotDateType.TODAY,
                start_date=today_date, end_date=None, before_days=None
            )
            img_base64_encoded: str = gen_plot_image(
                df, plot_param, phone_image_size=None, logger=app_logger
            )
            return _createImageResponse(rec_count, img_base64_encoded)
        else:
            return _createImageResponse(0, None)
    except psycopg2.Error as db_err:
        app_logger.error(db_err)
        abort(InternalServerError.code, _set_errormessage(f"559,{db_err}"))
    except Exception as exp:
        app_logger.error(exp)
        return _createErrorImageResponse(InternalServerError.code)


@app.route("/plot_weather/getmonthimage/<device_name>/<year_month>", methods=["GET"])
def getMonthImage(device_name: str, year_month: str) -> Response:
    """要求された年月の月間データ取得

    :param device_name: デバイス名
    :param yearmonth: 年月 (例) 2022-01
    :return: JSON形式(matplotlibでプロットした画像データ)
    """
    if app_logger_debug:
        app_logger.debug(f"{request.path}, {device_name}, {year_month}")
    try:
        # リクエストパラメータの妥当性チェック: "YYYY-mm" + "-01"
        chk_yyyymmdd = year_month + "-01"
        # 日付チェック(YYYY-mm-dd): 日付不正の場合例外スロー
        strdate2timestamp(chk_yyyymmdd, raise_error=True)
        conn: connection = get_connection()
        # DataFrameの取得
        rec_count: int
        df: Optional[DataFrame]
        rec_count, df = loadMonthDataFrame(
            conn, device_name, year_month,
            logger=app_logger, logger_debug=app_logger_debug
        )
        if rec_count > 0:
            # 年月データのパラメータ生成
            start_date: str = f"{year_month}-01"
            plot_param: PlotParam = PlotParam(
                plote_date_type=PlotDateType.YEAR_MONTH,
                start_date=start_date, end_date=None, before_days=None
            )
            img_base64_encoded: str = gen_plot_image(
                df, plot_param, phone_image_size=None, logger=app_logger
            )
            return _createImageResponse(rec_count, img_base64_encoded)
        else:
            return _createImageResponse(0, None)
    except DateFormatError as dfe:
        # BAD Request
        app_logger.warning(dfe)
        return _createErrorImageResponse(BadRequest.code)
    except psycopg2.Error as db_err:
        # DBエラー
        app_logger.error(db_err)
        abort(InternalServerError.code, _set_errormessage(f"559,{db_err}"))
    except Exception as exp:
        # バグ, DBサーバーダウンなど想定
        app_logger.error(exp)
        return _createErrorImageResponse(InternalServerError.code)


@app.route("/plot_weather/getcompprevyearimage/<device_name>/<year_month>", methods=["GET"])
def getcompprevyearimage(device_name, year_month) -> Response:
    """要求された年月の前年比較月間データ取得

    :param device_name: デバイス名
    :param yearmonth: 年月 (例) 2022-01
    :return: JSON形式(matplotlibでプロットした画像データ)
    """
    if app_logger_debug:
        app_logger.debug(f"{request.path}, {device_name}, {year_month}")
    try:
        chk_yyyymmdd = year_month + "-01"
        strdate2timestamp(chk_yyyymmdd, raise_error=True)
        conn: connection = get_connection()
        # DataFrameの取得
        df_curr: Optional[DataFrame]
        df_prev: Optional[DataFrame]
        df_curr, df_prev = loadPrevCompDataFrames(
            conn, device_name, year_month,
            logger=app_logger, logger_debug=app_logger_debug
        )
        if df_curr is not None and df_prev is not None:
            img_base64_encoded: str = gen_comp_prev_plot_image(
                df_curr, df_prev, year_month, logger=app_logger
            )
            rec_count: int = df_curr.shape[0]
            return _createImageResponse(rec_count, img_base64_encoded)
        else:
            return _createImageResponse(0, None)
    except DateFormatError as dfe:
        # BAD Request
        app_logger.warning(dfe)
        return _createErrorImageResponse(BadRequest.code)
    except psycopg2.Error as db_err:
        # DBエラー
        app_logger.error(db_err)
        abort(InternalServerError.code, _set_errormessage(f"559,{db_err}"))
    except Exception as exp:
        # バグ, DBサーバーダウンなど想定
        app_logger.error(exp)
        return _createErrorImageResponse(InternalServerError.code)


@app.route("/plot_weather/getlastdataforphone", methods=["GET"])
def getLastDataForPhone() -> Response:
    """最新の気象データを取得する (スマートホン専用)
       [仕様変更] 2023-09-09
         (1) リクエストパラメータ追加
            device_name: デバイス名 ※必須
       [仕様変更] 2023-12-03
         (2) レスポンスに外気温の統計情報を追加する

    :param: request parameter: device_name="xxxxx"
    """
    if app_logger_debug:
        app_logger.debug(request.path)
        # Debug output request.headers or request.arg or both
        _debugOutRequestObj(request, debugout=DebugOutRequest.HEADERS)

    # トークン必須
    headers: Headers = request.headers
    if not _matchToken(headers):
        abort(Forbidden.code, ABORT_DICT_UNMATCH_TOKEN)

    # デバイス名必須
    device_name: str = _checkDeviceName(request.args)
    try:
        conn: connection = get_connection()
        # 現在時刻時点の最新の気象データ取得
        dao = WeatherDao(conn, logger=app_logger)
        rec_count: int
        row: Optional[Tuple[str, float, float, float, float]]
        # デバイス名に対応する最新のレコード取得
        row = dao.getLastData(device_name=device_name)
        if row:
            rec_count = 1
            measurement_time, temp_out, temp_in, humid, pressure = row
            # 検索日の外気温の統計情報を取得
            #   上記の測定時刻から検索日付を取得
            find_date: str = measurement_time[:10]
            temp_out_stat: TempOutStatDao = TempOutStatDao(
                conn, logger=app_logger, is_debug_out=app_logger_debug)
            min_temp: Dict
            max_temp: Dict
            min_temp, max_temp = temp_out_stat.get_statistics(
                device_name, find_date)
            if app_logger_debug:
                app_logger.debug(f"min_temp: {min_temp}, max_temp: {max_temp}")
            # 検索日の統計情報Dict
            stat_today_dict: Dict = _makeTempOutStatDict(min_temp, max_temp)
            # 検索日を追加
            stat_today_dict["measurement_date"] = find_date
            # 前日の外気温の統計情報を取得
            before_date: str = date_util.addDayToString(find_date, add_days=-1)
            min_temp, max_temp = temp_out_stat.get_statistics(
                device_name, before_date)
            if app_logger_debug:
                app_logger.debug(f"min_temp: {min_temp}, max_temp: {max_temp}")
            stat_before_dict: Dict = _makeTempOutStatDict(min_temp, max_temp)
            stat_before_dict["measurement_date"] = before_date
            return _responseLastDataForPhone(
                measurement_time, temp_out, temp_in, humid, pressure,
                rec_count, stat_today_dict=stat_today_dict, stat_before_dict=stat_before_dict)
        else:
            # デバイス名に対応するレコード無し
            rec_count = 0
            return _responseLastDataForPhone(
                None, None, None, None, None, rec_count,
                stat_today_dict=None, stat_before_dict=None
            )
    except psycopg2.Error as db_err:
        app_logger.error(db_err)
        abort(InternalServerError.code, _set_errormessage(f"559,{db_err}"))
    except Exception as exp:
        app_logger.error(exp)
        abort(InternalServerError.code, description=str(exp))


@app.route("/plot_weather/getfirstregisterdayforphone", methods=["GET"])
def getFirstRegisterDayForPhone() -> Response:
    """デバイスの観測データの初回登録日を取得する (スマートホン専用)
       [仕様追加] 2023-09-13

           :param: request parameter: device_name="xxxxx"
    """
    if app_logger_debug:
        app_logger.debug(request.path)
        # Debug output request.headers or request.arg or both
        _debugOutRequestObj(request, debugout=DebugOutRequest.HEADERS)

    # トークン必須
    headers: Headers = request.headers
    if not _matchToken(headers):
        abort(Forbidden.code, ABORT_DICT_UNMATCH_TOKEN)

    # デバイス名必須
    param_device_name: str = _checkDeviceName(request.args)
    try:
        conn: connection = get_connection()
        dao = WeatherDao(conn, logger=app_logger)
        # デバイス名に対応する初回登録日取得
        first_register_day: Optional[str] = dao.getFisrtRegisterDay(
            param_device_name)
        if app_logger_debug:
            app_logger.debug(
                f"first_register_day[{type(first_register_day)}]: {first_register_day}")
        if first_register_day:
            return _responseFirstRegisterDayForPhone(first_register_day, 1)
        else:
            # デバイス名に対応するレコード無し
            return _responseFirstRegisterDayForPhone(None, 0)
    except psycopg2.Error as db_err:
        app_logger.error(db_err)
        abort(InternalServerError.code, _set_errormessage(f"559,{db_err}"))
    except Exception as exp:
        app_logger.error(exp)
        abort(InternalServerError.code, description=str(exp))


@app.route("/plot_weather/gettodayimageforphone", methods=["GET"])
def getTodayImageForPhone() -> Response:
    """本日データ画像取得リクエスト (スマートホン専用)
       [仕様変更] 2023-09-09
         (1) リクエストパラメータ追加
            device_name: デバイス名 ※必須
         (2) レスポンスにレコード件数を追加 ※0件エラーの抑止

    :param: request parameter: device_name="xxxxx"
    :return: jSON形式(matplotlibでプロットした画像データ(形式: png)のbase64エンコード済み文字列)
         (出力内容) JSON('data:': 'img_src':'image/png;base64,... base64encoded data ...',
                         'rec_count':xxx)
    """
    if app_logger_debug:
        app_logger.debug(request.path)
        _debugOutRequestObj(request, debugout=DebugOutRequest.HEADERS)

    # トークン必須
    headers: Headers = request.headers
    if not _matchToken(headers):
        abort(Forbidden.code, ABORT_DICT_UNMATCH_TOKEN)

    # デバイス名必須
    device_name: str = _checkDeviceName(request.args)

    # 表示領域サイズ+密度は必須: 形式(横x縦x密度)
    str_img_size: str = _checkPhoneImageSize(headers)
    try:
        conn: connection = get_connection()
        # 当日はシステム日付
        today_date = date.today().strftime(date_util.FMT_ISO8601)
        # DataFrameの取得
        rec_count: int
        df: Optional[DataFrame]
        rec_count, df = loadTodayDataFrame(
            conn, device_name, today_date,
            logger=app_logger, logger_debug=app_logger_debug
        )
        if rec_count > 0:
            # 当日データのパラメータ生成
            plot_param: PlotParam = PlotParam(
                plote_date_type=PlotDateType.TODAY,
                start_date=today_date, end_date=None, before_days=None
            )
            img_base64_encoded: str = gen_plot_image(
                df, plot_param, phone_image_size=str_img_size, logger=app_logger
            )
            return _responseImageForPhone(rec_count, img_base64_encoded)
        else:
            return _responseImageForPhone(0, None)
    except psycopg2.Error as db_err:
        app_logger.error(db_err)
        abort(InternalServerError.code, _set_errormessage(f"559,{db_err}"))
    except Exception as exp:
        app_logger.error(exp)
        abort(InternalServerError.code, description=str(exp))


@app.route("/plot_weather/getbeforedaysimageforphone", methods=["GET"])
def getBeforeDateImageForPhone() -> Response:
    """過去経過日指定データ画像取得リクエスト (スマートホン専用)
       [仕様変更] 2023-09-09
         (1) リクエストパラメータ追加
            device_name: デバイス名 ※必須
            start_day: 検索開始日(iso8601形式) ※任意
         (2) レスポンスにレコード件数を追加 ※0件エラーの抑止

    :param: request parameter: ?device_name=xxxxx&start_day=2023-05-01&before_days=(2|3|7)
    :return: jSON形式(matplotlibでプロットした画像データ(形式: png)のbase64エンコード済み文字列)
         (出力内容) jSON('data:': 'img_src':'image/png;base64,... base64encoded data ...',
                         'rec_count':xxx)
    """
    if app_logger_debug:
        app_logger.debug(request.path)
        _debugOutRequestObj(request, debugout=DebugOutRequest.BOTH)

    # トークン必須
    headers = request.headers
    if not _matchToken(headers):
        abort(Forbidden.code, ABORT_DICT_UNMATCH_TOKEN)

    # デバイス名 ※必須チェック
    device_name: str = _checkDeviceName(request.args)
    # 検索開始日 ※任意、指定されている場合はISO8601形式チェック
    end_date: Optional[str] = _checkStartDay(request.args)
    if end_date is None:
        # 検索開始日がない場合は当日を設定
        end_date = date_util.getTodayIsoDate()
    # Check before_days query parameter
    before_days: int = _checkBeforeDays(request.args)

    # 表示領域サイズ+密度は必須: 形式(横x縦x密度)
    str_img_size: str = _checkPhoneImageSize(headers)
    try:
        conn: connection = get_connection()
        # DataFrameの取得
        rec_count: int
        df: Optional[DataFrame]
        rec_count, df = loadBeforeDaysRangeDataFrame(
            conn, device_name, end_date, before_days,
            logger=app_logger, logger_debug=True
        )
        if rec_count > 0:
            # DataFrameの先頭から開始日を取得
            dt_first: datetime = df.index[0].to_pydatetime()
            # 当日の日付文字列 ※一旦 dateオブジェクトに変換して"年月日"を取得
            first_date: str = dt_first.date().isoformat()
            # 検索終了日からN日前のデータ取得パラメータ生成
            plot_param: PlotParam = PlotParam(
                plote_date_type=PlotDateType.RANGE,
                start_date=first_date, end_date=end_date, before_days=before_days
            )
            img_base64_encoded: str = gen_plot_image(
                df, plot_param, phone_image_size=str_img_size, logger=app_logger
            )
            return _responseImageForPhone(rec_count, img_base64_encoded)
        else:
            return _responseImageForPhone(0, None)
    except psycopg2.Error as db_err:
        app_logger.error(db_err)
        abort(InternalServerError.code, _set_errormessage(f"559,{db_err}"))
    except Exception as exp:
        app_logger.error(exp)
        abort(InternalServerError.code, description=str(exp))


@app.route("/plot_weather/get_devices", methods=["GET"])
def getDevices() -> Response:
    """センサーディバイスリスト取得リクエスト

    :return: JSON形式(idを除くセンサーディバイスリスト)
         (出力内容) JSON({"data":{"devices":[...]}')
    """
    if app_logger_debug:
        app_logger.debug(request.path)

    devices_with_dict: List[Dict]
    try:
        conn: connection = get_connection()
        dao: DeviceDao = DeviceDao(conn, logger=app_logger)
        devices: List[DeviceRecord] = dao.get_devices()
        devices_with_dict = DeviceDao.to_dict_without_id(devices)
        resp_obj: Dict[str, Dict] = {
            "data": {"devices": devices_with_dict},
            "status": {"code": 0, "message": "OK"}
        }
        return _make_respose(resp_obj, 200)
    except psycopg2.Error as db_err:
        app_logger.error(db_err)
        abort(InternalServerError.code, _set_errormessage(f"559,{db_err}"))
    except Exception as exp:
        app_logger.error(exp)
        abort(InternalServerError.code, description=str(exp))


def _debugOutRequestObj(request, debugout=DebugOutRequest.ARGS) -> None:
    if debugout == DebugOutRequest.ARGS or debugout == DebugOutRequest.BOTH:
        app_logger.debug(f"reqeust.args: {request.args}")
    if debugout == DebugOutRequest.HEADERS or debugout == DebugOutRequest.BOTH:
        app_logger.debug(f"request.headers: {request.headers}")


def _matchToken(headers: Headers) -> bool:
    """トークン一致チェック
    :param headers: request header
    :return: if match token True, not False.
    """
    token_value: str = app.config.get("HEADER_REQUEST_PHONE_TOKEN_VALUE", "!")
    req_token_value: Optional[str] = headers.get(
        key=app.config.get("HEADER_REQUEST_PHONE_TOKEN_KEY", "!"),
        type=str,
        default=""
    )
    if req_token_value != token_value:
        app_logger.warning("Invalid request token!")
        return False
    return True


def _checkPhoneImageSize(headers: Headers) -> str:
    """
    ヘッダーに表示領域サイズ+密度([width]x[height]x[density])をつけてくる
    ※1.トークンチェックを通過しているのでセットされている前提で処理
    ※2.途中でエラー (Androidアプリ側のBUG) ならExceptionで補足されJSONでメッセージが返却される
    :param headers: request header
    :return: (imageWidth, imageHeight, density)
    """
    img_size: str = headers.get(
        app.config.get("HEADER_REQUEST_IMAGE_SIZE_KEY", ""), type=str, default=""
    )
    if app_logger_debug:
        app_logger.debug(f"Phone imgSize: {img_size}")
    if len(img_size) == 0:
        abort(BadRequest.code, _set_errormessage(REQUIRED_PHONE_IMG))

    sizes: List[str] = img_size.split("x")
    try:
        img_wd: int = int(sizes[0])
        img_ht: int = int(sizes[1])
        density: float = float(sizes[2])
        if app_logger_debug:
            app_logger.debug(
                f"imgWd: {img_wd}, imgHt: {img_ht}, density: {density}")
        return img_size
    except Exception as exp:
        # ログには例外メッセージ
        app_logger.warning(f"[phone image size] {exp}")
        abort(BadRequest.code, _set_errormessage(INVALID_PHONE_IMG))


def _checkBeforeDays(args: MultiDict) -> int:
    # QueryParameter: before_days in (1,2,3,7)
    # before_days = args.get("before_days", default=-1, type=int)
    # args.get(key): keyが無い場合も キーが有る場合で数値以外でも -1 となり必須チェックができない
    # before_days = args.pop("before_days"): TypeError: 'ImmutableMultiDict' objects are immutable
    if len(args.keys()) == 0 or PARAM_BOFORE_DAYS not in args.keys():
        abort(BadRequest.code, _set_errormessage(REQUIRED_BOFORE_DAY))

    before_days = args.get(PARAM_BOFORE_DAYS, default=-1, type=int)
    if before_days not in [1, 2, 3, 7]:
        abort(BadRequest.code, _set_errormessage(INVALID_BOFORE_DAY))

    return before_days


def _checkDeviceName(args: MultiDict) -> str:
    """デバイス名チェック
        パラメータなし: abort(BadRequest)
        該当レコードなし: abort(NotFound)
    return デバイス名    
    """
    # 必須チェック
    if len(args.keys()) == 0 or PARAM_DEVICE not in args.keys():
        abort(BadRequest.code, _set_errormessage(REQUIRED_DEVICE))

    # 長さチェック: 1 - 20
    param_device_name: str = args.get(PARAM_DEVICE, default="", type=str)
    chk_size: int = len(param_device_name)
    if chk_size < 1 or chk_size > DEVICE_LENGTH:
        abort(BadRequest.code, _set_errormessage(INVALIDD_DEVICE))

    # 存在チェック
    if app_logger_debug:
        app_logger.debug("requestParam.device_name: " + param_device_name)

    exists: bool = False
    try:
        conn: connection = get_connection()
        dao: DeviceDao = DeviceDao(conn, logger=app_logger)
        exists = dao.exists(param_device_name)
    except Exception as exp:
        app_logger.error(exp)
        abort(InternalServerError.code, description=str(exp))

    if exists is True:
        return param_device_name
    else:
        abort(BadRequest.code, _set_errormessage(DEVICE_NOT_FOUND))


def _checkStartDay(args: MultiDict) -> Optional[str]:
    """検索開始日の形式チェック
        パラメータなし: OK
        パラメータ有り: ISO8601形式チェック
    return 検索開始日 | None    
    """
    if len(args.keys()) == 0 or PARAM_START_DAY not in args.keys():
        return None

    # 形式チェック
    param_start_day: str = args.get(PARAM_START_DAY, default="", type=str)
    if app_logger_debug:
        app_logger.debug(f"start_day: {param_start_day}")
    valid: bool = date_util.check_str_date(param_start_day)
    if valid is True:
        return param_start_day
    else:
        # 不正パラメータ
        abort(BadRequest.code, _set_errormessage(INVALID_START_DAY))


def _createImageResponse(rec_count: int, img_src: Optional[str]) -> Response:
    """画像レスポンスを返却する (JavaScript用)"""
    resp_obj = {"status": "success",
                "data": {
                    "img_src": img_src,
                    "rec_count": rec_count
                }
                }
    return _make_respose(resp_obj, 200)


def _createErrorImageResponse(err_code) -> Response:
    """エラー画像レスポンスを返却する (JavaScript用)"""
    resp_obj = {"status": "error", "code": err_code}
    if err_code == BadRequest.code:
        resp_obj["data"] = {"img_src": BAD_REQUEST_IMAGE_DATA}
    elif err_code == InternalServerError.code:
        resp_obj["data"] = {"img_src": INTERNAL_SERVER_ERROR_IMAGE_DATA}
    return _make_respose(resp_obj, err_code)


def _responseLastDataForPhone(
        mesurement_time: Optional[str],
        temp_out: Optional[float],
        temp_in: Optional[float],
        humid: Optional[float],
        pressure: Optional[float],
        rec_count: int,
        stat_today_dict: Optional[Dict],
        stat_before_dict: Optional[Dict]
) -> Response:
    """気象データの最終レコードを返却する (スマホアプリ用)"""
    resp_obj: Dict[str, Dict[str, Union[str, float]]] = {
        "status":
            {"code": 0, "message": "OK"},
        "data": {
            "measurement_time": mesurement_time,
            "temp_out": temp_out,
            "temp_in": temp_in,
            "humid": humid,
            "pressure": pressure,
            "rec_count": rec_count,
            "temp_out_stat_today": stat_today_dict,
            "temp_out_stat_before": stat_before_dict
        }
    }
    return _make_respose(resp_obj, 200)


def _makeTempOutStatDict(minDict: Dict, maxDict: Dict) -> Dict:
    stat_dict: Dict = {
        "min": minDict, "max": maxDict
    }
    return stat_dict


def _responseFirstRegisterDayForPhone(
        first_day: Optional[str],
        rec_count: int
) -> Response:
    """気象データの初回登録日を返却する (スマホアプリ用)"""
    resp_obj: Dict[str, Dict[str, Union[str, int]]] = {
        "status":
            {"code": 0, "message": "OK"},
        "data": {
            "first_register_day": first_day,
            "rec_count": rec_count
        }
    }
    return _make_respose(resp_obj, 200)


def _responseImageForPhone(rec_count: int, img_src: str) -> Response:
    """Matplotlib生成画像を返却する (スマホアプリ用)
       [仕様変更] 2023-09-09
         レスポンスにレコード件数を追加 ※0件エラーの抑止
    """
    resp_obj: Dict[str, Dict[str, Union[int, str]]] = {
        "status": {"code": 0, "message": "OK"},
        "data": {
            "img_src": img_src,
            "rec_count": rec_count
        }
    }
    return _make_respose(resp_obj, 200)


def _set_errormessage(message: str) -> Dict:
    ABORT_DICT_BLANK_MESSAGE[MSG_DESCRIPTION] = message
    return ABORT_DICT_BLANK_MESSAGE


# Request parameter check error.
@app.errorhandler(BadRequest.code)
# Token error.
@app.errorhandler(Forbidden.code)
# Device not found.
@app.errorhandler(NotFound.code)
@app.errorhandler(InternalServerError.code)
def error_handler(error: HTTPException) -> Response:
    app_logger.warning(f"error_type:{type(error)}, {error}")
    # Bugfix: 2023-09-06
    err_msg: str
    if isinstance(error.description, dict):
        # アプリが呼び出すabort()の場合は辞書オブジェクト
        err_msg = error.description["error_message"]
    else:
        # Flaskが出す場合は HTTPException)
        err_msg = error.description
    resp_obj: Dict[str, Dict[str, Union[int, str]]] = {
        "status": {"code": error.code, "message": err_msg}
    }
    return _make_respose(resp_obj, error.code)


def _make_respose(resp_obj: Dict, resp_code: int) -> Response:
    response = make_response(jsonify(resp_obj), resp_code)
    response.headers["Content-Type"] = "application/json"
    return response

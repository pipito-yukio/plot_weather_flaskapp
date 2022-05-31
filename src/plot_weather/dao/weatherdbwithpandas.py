import pandas as pd

from ..util.dateutil import nextYearMonth

""" 気象データテーブルから Pandas DataFrameを生成するクラス """

PLOT_WEATHER_IDX_COLUMN = "measurement_time"


class WeatherPandas:
    """Not use did coloumn"""

    _QUERY_TODAY_DATA = """
    SELECT
       datetime(measurement_time, 'unixepoch', 'localtime') as measurement_time
       , temp_out, temp_in, humid, pressure
    FROM
       t_weather
    WHERE
       did=(SELECT id FROM t_device WHERE name=:device_name)
       AND
       measurement_time >= strftime('%s', date(:today), '-9 hours')
    ORDER BY did, measurement_time;
    """

    _QUERY_MONTH_DATA = """
    SELECT
       datetime(measurement_time, 'unixepoch', 'localtime') as measurement_time
       , temp_out, temp_in, humid, pressure
    FROM
       t_weather
    WHERE
       did=(SELECT id FROM t_device WHERE name=:device_name)
       AND (
         measurement_time >= strftime('%s', date(:day_start), '-9 hours')
         AND
         measurement_time < strftime('%s', date(:day_end), '-9 hours')
       )
    ORDER BY did, measurement_time;
    """

    def __init__(self, conn, logger=None):
        self.conn = conn
        self.logger = logger

    def getTodayDataFrame(self, device_name, today="now"):
        query_params = {"device_name": device_name, "today": today}
        if self.logger is not None:
            self.logger.debug(f"query_params: {query_params}")

        return pd.read_sql(
            self._QUERY_TODAY_DATA,
            self.conn,
            params=query_params,
            parse_dates=["measurement_time"],
        )

    def getMonthDataFrame(self, device_name, s_year_month):
        s_start = s_year_month + "-01"
        s_end_exclude = nextYearMonth(s_start)
        query_params = {
            "device_name": device_name,
            "day_start": s_start,
            "day_end": s_end_exclude,
        }
        if self.logger is not None:
            self.logger.debug(f"query_params: {query_params}")

        return pd.read_sql(
            self._QUERY_MONTH_DATA,
            self.conn,
            params=query_params,
            parse_dates=["measurement_time"],
        )

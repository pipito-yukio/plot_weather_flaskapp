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
       did=(SELECT id FROM t_device WHERE name='{}')
       AND
       measurement_time >= strftime('%s', date('now'), '-9 hours')
    ORDER BY did, measurement_time;
    """

    _QUERY_MONTH_DATA = """
    SELECT
       datetime(measurement_time, 'unixepoch', 'localtime') as measurement_time
       , temp_out, temp_in, humid, pressure
    FROM
       t_weather
    WHERE
       did=(SELECT id FROM t_device WHERE name='{}')
       AND (
         measurement_time >= strftime('%s', '{}', '-9 hours')
         AND
         measurement_time < strftime('%s', '{}', '-9 hours')
       )
    ORDER BY did, measurement_time;
    """

    def __init__(self, conn, logger=None):
        self.conn = conn
        self.logger = logger

    def getTodayDataFrame(self, device_name):
        sql = self._QUERY_TODAY_DATA.format(device_name)
        if self.logger is not None:
            self.logger.debug("SQL: {}".format(sql))
        return pd.read_sql(sql, self.conn, parse_dates=["measurement_time"])

    def getMonthDataFrame(self, device_name, s_year_month):
        s_start = s_year_month + "-01"
        s_end_exclude = nextYearMonth(s_start)
        sql = self._QUERY_MONTH_DATA.format(device_name, s_start, s_end_exclude)
        if self.logger is not None:
            self.logger.debug("SQL: {}".format(sql))
        return pd.read_sql(sql, self.conn, parse_dates=["measurement_time"])

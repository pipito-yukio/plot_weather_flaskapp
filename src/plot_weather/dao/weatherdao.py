import os

from ..db.sqlite3conv import strdate2timestamp
from ..util.dateutil import nextYearMonth

""" 気象データDAOクラス """

HEADER_WEATHER = '"did","measurement_time","temp_out","temp_in","humid","pressure"'
HEADER_DEVICE = '"id","name"'

my_home = os.environ.get("HOME", "/home/yukio")
weather_db = os.environ.get("PATH_WEATHER_DB", my_home + "/db/weather.db")


class WeatherDao:

    _QUERY_WEATHER_LASTREC = """
SELECT
  MAX(measurement_time)
  , strftime('%Y %m %d %H %M', measurement_time, 'unixepoch', 'localtime')
  , temp_out, temp_in, humid, pressure
FROM
  t_weather
WHERE
  did=(SELECT id FROM t_device WHERE name=?);
"""

    _QUERY_GROUPBY_DAYS = """
SELECT
  date(measurement_time, 'unixepoch', 'localtime') 
FROM
  t_weather
WHERE
  did=(SELECT id FROM t_device WHERE name=?)
  AND
  date(measurement_time, 'unixepoch', 'localtime') >= ?
GROUP BY date(measurement_time, 'unixepoch', 'localtime')
ORDER BY date(measurement_time, 'unixepoch', 'localtime');
    """

    _QUERY_GROUPBY_MONTHS = """
SELECT
  strftime('%Y-%m', measurement_time, 'unixepoch', 'localtime') 
FROM
  t_weather
WHERE
  did=(SELECT id FROM t_device WHERE name=?)
  AND
  strftime('%Y-%m', measurement_time, 'unixepoch', 'localtime') >= ?
  GROUP BY strftime('%Y-%m', measurement_time, 'unixepoch', 'localtime')
  ORDER BY strftime('%Y-%m', measurement_time, 'unixepoch', 'localtime') DESC;
"""

    _QUERY_TODAY_DATA = """
SELECT
   did, datetime(measurement_time, 'unixepoch', 'localtime')
   , temp_out, temp_in, humid, pressure
FROM
   t_weather
WHERE
   did=(SELECT id FROM t_device WHERE name=?)
   AND
   measurement_time >= strftime('%s', date('now'), '-9 hours')
ORDER BY did, measurement_time;
"""

    _QUERY_MONTH_DATA = """
SELECT
   did, datetime(measurement_time, 'unixepoch', 'localtime')
   , temp_out, temp_in, humid, pressure
FROM
   t_weather
WHERE
   did=(SELECT id FROM t_device WHERE name=?)
   AND (
     measurement_time >= strftime('%s', ?, '-9 hours')
     AND
     measurement_time < strftime('%s', ?, '-9 hours')
   )
ORDER BY did, measurement_time;
"""

    def __init__(self, conn, logger=None):
        self.conn = conn
        self.logger = logger

    def getLastData(self, device_name):
        """観測デバイスの最終レコードを取得する

        Args:
          device_name str: 観測デバイス名

        Returns:
          tuple: (measurement_time[%Y %m %d %H %M], temp_out, temp_in, humid, pressure)
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(self._QUERY_WEATHER_LASTREC, (device_name,))
            row = cursor.fetchone()
            if self.logger is not None:
                self.logger.debug("row: {}".format(row))
        finally:
            cursor.close()
        # remove first column: MAX(measurement_time)
        return row[1:]

    def _getDateGroupByList(self, qrouping_sql, device_name, start_date):
        """観測デバイスのグルーピングSQLに対応した日付リストを取得する

        Args:
            qrouping_sql str: グルーピングSQL
            device_name str: 観測デバイス名
            start_date str: 検索開始日付

        Returns:
          list: 文字列の日付 (年月 | 年月日)
        """
        # Check start_date
        strdate2timestamp(start_date)
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                qrouping_sql,
                (
                    device_name,
                    start_date,
                ),
            )
            # fetchall() return tuple list [(?,), (?,), ..., (?,)]
            tupledlist = cursor.fetchall()
            if self.logger is not None:
                self.logger.debug("tupledlist: {}".format(tupledlist))
            # tuple -> list
            result = [item for (item,) in tupledlist]
        finally:
            cursor.close()
        return result

    def getGroupbyDays(self, device_name, start_date):
        """観測デバイスの年月日にグルーピングしたリストを取得する

        Args:
            device_name str: 観測デバイス名
            start_date str: 検索開始年月日

        Returns:
            list[str]: 年月日リスト(%Y-%m-%d)
        """
        return self._getDateGroupByList(
            self._QUERY_GROUPBY_DAYS, device_name, start_date
        )

    def getGroupbyMonths(self, device_name, start_date):
        """観測デバイスの年月にグルーピングしたリストを取得する

        Args:
            device_name str: 観測デバイス名
            start_date str: 検索開始年月日

        Returns:
            list[str]: 降順の年月リスト(%Y-%m)
        """
        return self._getDateGroupByList(
            self._QUERY_GROUPBY_MONTHS, device_name, start_date
        )

    def _toWeatherCSVList(self, tupledlist, require_header=True):
        if require_header:
            result_list = [HEADER_WEATHER]
            for (did, m_time, temp_in, temp_out, humid, pressure) in tupledlist:
                result_list.append(
                    f'{did},"{m_time}",{temp_in},{temp_out},{humid},{pressure}'
                )
        else:
            result_list = [
                f'{did},"{m_time}",{temp_in},{temp_out},{humid},{pressure}'
                for (did, m_time, temp_in, temp_out, humid, pressure) in tupledlist
            ]
        return result_list

    def getTodayData(self, device_name, output_string=False, require_header=True):
        cursor = self.conn.cursor()
        try:
            cursor.execute(self._QUERY_TODAY_DATA, (device_name,))
            tupledlist = cursor.fetchall()
            if self.logger is not None:
                self.logger.debug("tupledlist size: {}".format(len(tupledlist)))
            # [tuple, ...] -> list
            csvList = self._toWeatherCSVList(tupledlist, require_header)
        finally:
            cursor.close()
        if output_string:
            return "\n".join(csvList)
        else:
            return csvList

    def getMonthData(
        self, device_name, s_year_month, output_string=False, require_header=True
    ):
        s_start = s_year_month + "-01"
        s_end_exclude = nextYearMonth(s_start)
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                self._QUERY_MONTH_DATA,
                (
                    device_name,
                    s_start,
                    s_end_exclude,
                ),
            )
            tupledlist = cursor.fetchall()
            if self.logger is not None:
                self.logger.debug("tupledlist size: {}".format(len(tupledlist)))
            # [tuple, ...] -> list
            csvList = self._toWeatherCSVList(tupledlist, require_header)
        finally:
            cursor.close()
        if output_string:
            return "\n".join(csvList)
        else:
            return csvList

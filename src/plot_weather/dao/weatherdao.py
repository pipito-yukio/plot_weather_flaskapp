import logging
from typing import List, Tuple, Optional, Dict

from psycopg2.extensions import connection

from plot_weather.util.date_util import addDayToString, nextYearMonth

""" 気象データDAOクラス """


class WeatherDao:
    _QUERY_LASTREC: str = """
SELECT
  to_char(measurement_time,'YYYY-MM-DD HH24:MI') as measurement_time
  , temp_out, temp_in, humid, pressure
FROM
  weather.t_weather tw INNER JOIN weather.t_device td ON tw.did = td.id
WHERE
  td.name=%(name)s
  AND
  measurement_time = (SELECT max(measurement_time) FROM weather.t_weather);
"""

    _QUERY_GROUPBY_MONTHS: str = """
SELECT
  to_char(measurement_time, 'YYYY-MM') as groupby_months
FROM
  weather.t_weather tw INNER JOIN weather.t_device td ON tw.did = td.id
WHERE
  td.name=%(name)s
  GROUP BY to_char(measurement_time, 'YYYY-MM')
  ORDER BY to_char(measurement_time, 'YYYY-MM') DESC;
"""

    _QUERY_RANGE_DATA: str = """
SELECT
   to_char(measurement_time,'YYYY-MM-DD HH24:MI') as measurement_time,
   temp_out, temp_in, humid, pressure
FROM
  weather.t_weather tw INNER JOIN weather.t_device td ON tw.did = td.id
WHERE
   td.name=%(name)s
   AND (
     measurement_time >= to_timestamp(%(from_date)s, 'YYYY-MM-DD HH24::MI:SS')
     AND
     measurement_time < to_timestamp(%(exclude_to_date)s, 'YYYY-MM-DD HH24:MI:SS')
   )
ORDER BY measurement_time;
"""

    _QUERY_FIRST_DATE_WITH_DEVICE: str = """
SELECT
   to_char(min(measurement_time), 'YYYY-MM-DD') as min_measurement_day
FROM 
  weather.t_weather tw INNER JOIN weather.t_device td ON tw.did = td.id
WHERE
   td.name=%(name)s;
"""

    _QUERY_PREV_YEAR_MONTH_LIST: str = """
WITH t_year_month AS(
  SELECT
    did ,to_char(measurement_time, 'YYYYMM') AS year_month
  FROM
    weather.t_weather tw INNER JOIN weather.t_device td ON tw.did = td.id
  WHERE
    td.name=%(name)s  
  GROUP BY did,year_month
)
SELECT
  curr.year_month as latest_year_month
FROM
  t_year_month curr
  INNER JOIN t_year_month prev ON curr.did = prev.did
WHERE
  to_number(curr.year_month, '999999') = to_number(prev.year_month, '999999') + 100
ORDER BY latest_year_month DESC;
"""

    _QUERY_LAST_DATE_WITH_DEVICE: str = """
SELECT
   to_char(max(measurement_time), 'YYYY-MM-DD') as min_measurement_day
FROM 
  weather.t_weather tw INNER JOIN weather.t_device td ON tw.did = td.id
WHERE
   td.name=%(name)s;
"""

    def __init__(self, conn: connection, logger: Optional[logging.Logger] = None):
        self.conn: connection = conn
        self.logger: Optional[logging.Logger] = logger
        self.logger_debug: bool = False
        if self.logger is not None:
            self.logger_debug = (self.logger.getEffectiveLevel() <= logging.DEBUG)

    def getLastData(self,
                    device_name: str
                    ) -> Optional[Tuple[str, float, float, float, float]]:
        """観測デバイスの最終レコードを取得する
        :param device_name: 観測デバイス名
        :return
          tuple: (measurement_time, temp_out, temp_in, humid, pressure)
          ただし観測デバイス名に対応するレコードがない場合は None
        """
        with self.conn.cursor() as cursor:
            cursor.execute(self._QUERY_LASTREC, {'name': device_name})
            row: Optional[Tuple[str, float, float, float, float]] = cursor.fetchone()
            if self.logger is not None and self.logger_debug:
                self.logger.debug("row: {}".format(row))

        return row

    def getGroupByMonths(self, device_name: str) -> List[str]:
        """観測デバイスのグルーピングSQLに対応した日付リストを取得する
        :param device_name: 観測デバイス名
        :return list: レコードが存在する場合は年月リスト、存在しない場合は空のリスト
        """
        if self.logger is not None and self.logger_debug:
            self.logger.debug(f"device: {device_name}")

        params: Dict[str, str] = {'name': device_name}
        with self.conn.cursor() as cursor:
            cursor.execute(self._QUERY_GROUPBY_MONTHS, params)
            # fetchall() return tuple list [(?,), (?,), ..., (?,)]
            tuple_list: List[Tuple[str]] = cursor.fetchall()
            if self.logger is not None and self.logger_debug:
                self.logger.debug("tuple_list: {}".format(tuple_list))
            # tuple -> list
            if len(tuple_list) > 0:
                result = [item for (item,) in tuple_list]
            else:
                result = []
        return result

    def getTodayData(self,
                     device_name: str, today_iso8601: str
                     ) -> List[Tuple[str, float, float, float, float]]:
        if self.logger is not None and self.logger_debug:
            self.logger.debug(f"device_name: {device_name}, today: {today_iso8601}")

        exclude_date: str = addDayToString(today_iso8601)
        params: Dict = {
            "name": device_name, "from_date": today_iso8601, "exclude_to_date": exclude_date
        }
        result: List[Tuple[str, float, float, float, float]]
        with self.conn.cursor() as cursor:
            cursor.execute(self._QUERY_RANGE_DATA, params)
            tuple_list = cursor.fetchall()
            rec_count: int = len(tuple_list)
            if self.logger is not None and self.logger_debug:
                self.logger.debug(f"tuple_list.size {rec_count}")
            if rec_count == 0:
                result = []
            else:
                result = [rec for rec in tuple_list]
        return result

    def getMonthData(self,
                     device_name: str, year_month: str
                     ) -> List[Tuple[str, float, float, float, float]]:
        from_date: str = year_month + "-01"
        exclude_date: str = nextYearMonth(from_date)
        if self.logger is not None and self.logger_debug:
            self.logger.debug(
                f"device_name: {device_name}, from: {from_date}, to_date: {exclude_date}"
            )
        params: Dict = {
            "name": device_name, "from_date": from_date, "exclude_to_date": exclude_date
        }
        result: List[Tuple[str, float, float, float, float]]
        with self.conn.cursor() as cursor:
            cursor.execute(self._QUERY_RANGE_DATA, params)
            tuple_list = cursor.fetchall()
            rec_count: int = len(tuple_list)
            if self.logger is not None and self.logger_debug:
                self.logger.debug(f"tuple_list.size {rec_count}")
            if rec_count == 0:
                result = []
            else:
                result = [rec for rec in tuple_list]
        return result

    def getFromToRangeData(self,
                           device_name: str,
                           from_date: str,
                           to_date: str,
                           ) -> List[Tuple[str, float, float, float, float]]:
        exclude_date: str = addDayToString(to_date)
        if self.logger is not None and self.logger_debug:
            self.logger.debug(
                f"device_name: {device_name}, from: {from_date}, to_date: {exclude_date}"
            )
        params: Dict = {
            "name": device_name, "from_date": from_date, "exclude_to_date": exclude_date
        }
        result: List[Tuple[str, float, float, float, float]]
        with self.conn.cursor() as cursor:
            cursor.execute(self._QUERY_RANGE_DATA, params)
            tuple_list = cursor.fetchall()
            rec_count: int = len(tuple_list)
            if self.logger is not None and self.logger_debug:
                self.logger.debug(f"tuple_list.size {rec_count}")
            if rec_count == 0:
                result = []
            else:
                result = [rec for rec in tuple_list]
        return result

    def getFirstRegisterDay(self, device_name: str) -> Optional[str]:
        """観測デバイスの初回登録日を取得する
        :param device_name: 観測デバイス名
        :return 存在する場合は初回登録日, 存在しない場合はNone
        """
        with self.conn.cursor() as cursor:
            cursor.execute(self._QUERY_FIRST_DATE_WITH_DEVICE, {'name': device_name})
            row = cursor.fetchone()
            if self.logger is not None and self.logger_debug:
                self.logger.debug("row: {}".format(row))

        if row is not None:
            return row[0]

        # レコードなし
        return None

    def getPrevYearMonthList(self, device_name: str) -> List[str]:
        """観測デバイスの前年度データが存在する年月リストを取得する
        :param device_name: 観測デバイス名
        :return 対応する観測データが存在する場合は降順の年月リスト(%Y-%m),
                存在しない場合は空のリスト
        """
        with self.conn.cursor() as cursor:
            cursor.execute(self._QUERY_PREV_YEAR_MONTH_LIST, {'name': device_name})
            # fetchall() return tuple list [(?,), (?,), ..., (?,)]
            tuple_list: List[Tuple[str]] = cursor.fetchall()
            if self.logger is not None and self.logger_debug:
                self.logger.debug("tuple_list: {}".format(tuple_list))
            # tuple -> list
            if len(tuple_list) > 0:
                # [('YYYYmm',), ...]
                val_list: List[str] = [item for (item,) in tuple_list]
                # 先頭4桁をハイフンで分割: ['YYYY-mm', ...]
                return [f"{ym[:4]}-{ym[4:]}" for ym in val_list]

        return []

    def getLastRegisterDay(self, device_name: str) -> Optional[str]:
        """観測デバイスの最終登録日を取得する
        :param device_name: 観測デバイス名
        :return 存在する場合は最終登録日, 存在しない場合はNone
        """
        with self.conn.cursor() as cursor:
            cursor.execute(self._QUERY_LAST_DATE_WITH_DEVICE, {'name': device_name})
            row = cursor.fetchone()
            if self.logger is not None and self.logger_debug:
                self.logger.debug("row: {}".format(row))

        if row is not None:
            return row[0]

        # レコードなし
        return None

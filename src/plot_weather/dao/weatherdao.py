import logging
from io import StringIO
from typing import List, Tuple, Optional, Dict
from psycopg2.extensions import connection
from ..util.date_util import addDayToString, nextYearMonth

""" 気象データDAOクラス """

HEADER_WEATHER: str = '"did","measurement_time","temp_out","temp_in","humid","pressure"'


def _csvToStringIO(
        tuple_list: List[Tuple[int, str, float, float, float, float]],
        require_header=True) -> StringIO:
    str_buffer = StringIO()
    if require_header:
        str_buffer.write(HEADER_WEATHER + "\n")

    for (did, m_time, temp_in, temp_out, humid, pressure) in tuple_list:
        line = f'{did},"{m_time}",{temp_in},{temp_out},{humid},{pressure}\n'
        str_buffer.write(line)

    # StringIO need Set first position
    str_buffer.seek(0)
    return str_buffer


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

    _QUERY_GROUPBY_DAYS: str = """
SELECT
  to_char(measurement_time, 'YYYY-MM-DD') as groupby_days
FROM
  weather.t_weather tw INNER JOIN weather.t_device td ON tw.did = td.id
WHERE
  td.name=%(name)s
  AND
  to_char(measurement_time, 'YYYY-MM-DD') >= %(start_date)s
GROUP BY to_char(measurement_time, 'YYYY-MM-DD')
ORDER BY to_char(measurement_time, 'YYYY-MM-DD');
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

    _QUERY_TODAY_DATA: str = """
SELECT
   did, to_char(measurement_time,'YYYY-MM-DD HH24:MI') as measurement_time
   , temp_out, temp_in, humid, pressure
FROM
  weather.t_weather tw INNER JOIN weather.t_device td ON tw.did = td.id
WHERE
   td.name=%(name)s
   AND
   measurement_time >= to_timestamp(%(today)s, 'YYYY-MM-DD HH24:MI:SS')
ORDER BY measurement_time;
"""

    _QUERY_RANGE_DATA: str = """
SELECT
   did, to_char(measurement_time,'YYYY-MM-DD HH24:MI') as measurement_time
   , temp_out, temp_in, humid, pressure
FROM
  weather.t_weather tw INNER JOIN weather.t_device td ON tw.did = td.id
WHERE
   td.name=%(name)s
   AND (
     measurement_time >= to_timestamp(%(from_date)s, 'YYYY-MM-DD HH24::MI:SS')
     AND
     measurement_time < to_timestamp(%(to_next_date)s, 'YYYY-MM-DD HH24:MI:SS')
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
        self.conn = conn
        self.logger = logger
        self.logger_debug: bool = False
        if self.logger is not None:
            self.logger_debug = (self.logger.getEffectiveLevel() <= logging.DEBUG)

    def getLastData(self,
                    device_name: str) -> Optional[Tuple[str, float, float, float, float]]:
        """観測デバイスの最終レコードを取得する
        :param device_name: 観測デバイス名
        :return
          tuple: (measurement_time[%Y %m %d %H %M], temp_out, temp_in, humid, pressure)
          ただし観測デバイス名に対応するレコードがない場合は None
        """
        with self.conn.cursor() as cursor:
            cursor.execute(self._QUERY_LASTREC, {'name': device_name})
            row = cursor.fetchone()
            if self.logger is not None and self.logger_debug:
                self.logger.debug("row: {}".format(row))

        return row

    def _getDateGroupByList(self,
                            qrouping_sql: str,
                            device_name: str,
                            start_date: str = None
                            ) -> List[str]:
        """観測デバイスのグルーピングSQLに対応した日付リストを取得する
        :param qrouping_sql:  グルーピングSQL
        :param device_name: 観測デバイス名
        :param start_date: 開始日付 ※任意 (日付グルーピングのみ指定される)
        :return list: [年月|年月日]リスト
        """
        if self.logger is not None and self.logger_debug:
            self.logger.debug(f"device: {device_name}, start_date: {start_date}")

        params: Dict[str, str] = {'name': device_name}
        if start_date is not None:
            params['start_date'] = start_date
        with self.conn.cursor() as cursor:
            cursor.execute(qrouping_sql, params)
            # fetchall() return tuple list [(?,), (?,), ..., (?,)]
            tuple_list: List[Tuple[str]] = cursor.fetchall()
            if self.logger is not None and self.logger_debug:
                self.logger.debug("tuple_list: {}".format(tuple_list))
            # tuple -> list
            result = [item for (item,) in tuple_list]

        return result

    def getGroupByDays(self, device_name: str, start_date: str) -> List[str]:
        """観測デバイスの年月日にグルーピングしたリストを取得する
        :param device_name: 観測デバイス名
        :param start_date: 取得開始日 ※必須
        :return
            list[str]: 年月日リスト(%Y-%m-%d)
        """
        return self._getDateGroupByList(self._QUERY_GROUPBY_DAYS,
                                        device_name, start_date=start_date)

    def getGroupByMonths(self, device_name: str) -> List[str]:
        """観測デバイスの年月にグルーピングしたリストを取得する
        :param device_name: 観測デバイス名
        :return
                    list[str]: 降順の年月リスト(%Y-%m)
        """
        return self._getDateGroupByList(self._QUERY_GROUPBY_MONTHS, device_name)

    def getTodayData(self,
                     device_name: str,
                     today_iso8601: str,
                     require_header: bool = True) -> Tuple[int, Optional[StringIO]]:
        if self.logger is not None and self.logger_debug:
            self.logger.debug("device_name: {}, today: {}".format(device_name, today_iso8601))

        with self.conn.cursor() as cursor:
            cursor.execute(self._QUERY_TODAY_DATA, {'name': device_name, 'today': today_iso8601})
            tuple_list = cursor.fetchall()
            rec_count: int = len(tuple_list)
            if self.logger is not None and self.logger_debug:
                self.logger.debug(f"tuple_list.size: {rec_count}")

        if rec_count == 0:
            return 0, None
        return rec_count, _csvToStringIO(tuple_list, require_header)

    def getMonthData(self,
                     device_name: str,
                     s_year_month: str,
                     require_header: bool = True) -> Tuple[int, Optional[StringIO]]:
        s_start = s_year_month + "-01"
        s_end_exclude = nextYearMonth(s_start)
        if self.logger is not None and self.logger_debug:
            self.logger.debug("device_name: {}, from_date: {}, to_next_date: {}".format(
                device_name, s_start, s_end_exclude))

        with self.conn.cursor() as cursor:
            cursor.execute(self._QUERY_RANGE_DATA, {
                'name': device_name,
                'from_date': s_start,
                'to_next_date': s_end_exclude,
            }
                           )
            tuple_list = cursor.fetchall()
            rec_count: int = len(tuple_list)
            if self.logger is not None and self.logger_debug:
                self.logger.debug(f"tuple_list.size {rec_count}")

        if rec_count == 0:
            return 0, None
        return rec_count, _csvToStringIO(tuple_list, require_header)

    def getFromToRangeData(self,
                           device_name: str,
                           from_date: str,
                           to_date: str,
                           require_header: bool = True) -> Tuple[int, Optional[StringIO]]:
        s_end_exclude: str = addDayToString(to_date)
        if self.logger is not None and self.logger_debug:
            self.logger.debug("device_name: {}, from_date: {}, to_next_date: {}".format(
                device_name, from_date, s_end_exclude))

        with self.conn.cursor() as cursor:
            cursor.execute(self._QUERY_RANGE_DATA, {
                'name': device_name,
                'from_date': from_date,
                'to_next_date': s_end_exclude,
            }
                           )
            tuple_list = cursor.fetchall()
            rec_count: int = len(tuple_list)
            if self.logger is not None and self.logger_debug:
                self.logger.debug(f"tuple_list.size {rec_count}")

        if rec_count == 0:
            return 0, None
        return rec_count, _csvToStringIO(tuple_list, require_header)

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

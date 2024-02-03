import logging
from typing import Dict, List, Optional, Tuple
from psycopg2.extensions import connection
from ..util.date_util import nextYearMonth

"""
前年比較年月データ取得DAOクラス
[比較対象] 外気温, 気圧
"""


class WeatherPrevCompDao:
    _QUERY: str = """
SELECT
   to_char(measurement_time,'YYYY-MM-DD HH24:MI') as measurement_time,
   temp_out, humid, pressure
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

    def __init__(self, conn: connection, logger: Optional[logging.Logger] = None):
        self.conn: connection = conn
        self.logger: Optional[logging.Logger] = logger
        self.logger_debug: bool = False
        if self.logger is not None:
            self.logger_debug = (self.logger.getEffectiveLevel() <= logging.DEBUG)

    def getMonthData(self,
                     device_name: str, year_month: str
                     ) -> List[Tuple[str, float, float, float]]:
        from_date: str = year_month + "-01"
        exclude_date: str = nextYearMonth(from_date)
        if self.logger is not None and self.logger_debug:
            self.logger.debug(
                f"device_name: {device_name}, from: {from_date}, to_date: {exclude_date}"
            )
        params: Dict = {
            "name": device_name, "from_date": from_date, "exclude_to_date": exclude_date
        }
        result: List[Tuple[str, float, float, float]]
        with self.conn.cursor() as cursor:
            cursor.execute(self._QUERY, params)
            tuple_list = cursor.fetchall()
            rec_count: int = len(tuple_list)
            if self.logger is not None and self.logger_debug:
                self.logger.debug(f"tuple_list.size {rec_count}")
            if rec_count == 0:
                result = []
            else:
                result = [rec for rec in tuple_list]
        return result

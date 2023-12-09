import logging
from io import StringIO
from typing import List, Tuple, Optional
from psycopg2.extensions import connection
from ..util.date_util import nextYearMonth

""" 前年比較年月データ取得DAOクラス """

# 取得カラム: 測定時刻,外気温,湿度,気圧
HEADER_WEATHER: str = '"measurement_time","temp_out","humid","pressure"'


def _csvToStringIO(
        tuple_list: List[Tuple[str, float, float, float]],
        require_header=True) -> StringIO:
    str_buffer = StringIO()
    if require_header:
        str_buffer.write(HEADER_WEATHER + "\n")

    for (m_time, temp_out, humid, pressure) in tuple_list:
        line = f'"{m_time}",{temp_out},{humid},{pressure}\n'
        str_buffer.write(line)

    # StringIO need Set first position
    str_buffer.seek(0)
    return str_buffer


class WeatherPrevCompDao:
    _QUERY_RANGE_DATA: str = """
SELECT
   measurement_time, temp_out, humid, pressure
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

    def __init__(self, conn: connection, logger: Optional[logging.Logger] = None):
        self.conn = conn
        self.logger = logger
        self.logger_debug: bool = False
        if self.logger is not None:
            self.logger_debug = (self.logger.getEffectiveLevel() <= logging.DEBUG)

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

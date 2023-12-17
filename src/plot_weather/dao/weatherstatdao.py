import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple
from psycopg2.extensions import connection, cursor

""" 気象データの外気温統計取得DAOクラス """

FMT_ISO8601_DATE: str = "%Y-%m-%d"


@dataclass
class TempOut:
    appear_time: Optional[str]
    temper: Optional[float]


class TempOutStatDao:
    _QUERY: str = """
WITH find_records AS (
  SELECT
    date(measurement_time),
    measurement_time,
    temp_out,
    CASE
      WHEN temp_out <= MIN(temp_out) OVER (PARTITION BY date(measurement_time)) 
      THEN temp_out
    END AS min_temp_out,
    CASE
      WHEN temp_out >= MAX(temp_out) OVER (PARTITION BY date(measurement_time)) 
      THEN temp_out
    END AS max_temp_out
  FROM
    weather.t_weather tw INNER JOIN weather.t_device td ON tw.did = td.id
  WHERE
    td.name = %(name)s
    AND (
      measurement_time >= %(from_date)s AND measurement_time < %(exclude_to_date)s
    )
  -- Sort by recent data
  ORDER BY measurement_time DESC
),
min_temp_out_record AS (
  --Recent minimum temperature first record
  SELECT
    -- Only time data required
    to_char(measurement_time,'HH24:MI') as measurement_time,
    min_temp_out AS temp_out
  FROM
    find_records
  WHERE
    min_temp_out IS NOT NULL
    LIMIT 1
),
max_temp_out_record AS (
  --Recent maximum temperature first record
  SELECT
    -- Only time data required
    to_char(measurement_time,'HH24:MI') as measurement_time,
    max_temp_out AS temp_out
  FROM
    find_records
  WHERE
    max_temp_out IS NOT NULL
    LIMIT 1
)
-- first maximum temperature, second maximum temperature 2record
SELECT * FROM min_temp_out_record
UNION ALL
SELECT * FROM max_temp_out_record
;
"""

    def __init__(self, conn: connection,
                 logger: Optional[logging.Logger] = None, is_debug_out: bool = False):
        self.conn: connection = conn
        self.logger: Optional[logging.Logger] = logger
        self.is_debug_out: bool = is_debug_out

    def get_statistics(self, device_name: str, from_date: str) -> List[Dict]:
        dt: datetime = datetime.strptime(from_date, FMT_ISO8601_DATE)
        dt += timedelta(days=1)
        exclude_to_date: str = dt.strftime(FMT_ISO8601_DATE)
        params: Dict = {
            "name": device_name, "from_date": from_date, "exclude_to_date": exclude_to_date
        }
        if self.logger is not None and self.is_debug_out:
            self.logger.debug(f"params: {params}")

        result: List[Dict] = []
        curr: cursor
        with self.conn.cursor() as curr:
            curr.execute(self._QUERY, params)
            rows: List[Tuple[str, float]] = curr.fetchall()
            record_size: int = len(rows)
            if self.logger is not None and self.is_debug_out:
                self.logger.debug(f"rows.size: {record_size}")
        # 取得結果は2レコードか、レコードなし
        if record_size == 2:
            for row in rows:
                temp_out: TempOut = TempOut(row[0], row[1])
                result.append(asdict(temp_out))
            return result
        else:
            return result

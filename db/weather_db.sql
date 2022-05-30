CREATE TABLE IF NOT EXISTS t_device(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS t_weather(
    did INTEGER NOT NULL,
    measurement_time INTEGER NOT NULL,
    temp_out real,
    temp_in real,
    humid real,
    pressure real,
    PRIMARY KEY (did, measurement_time),
    FOREIGN KEY (did) REFERENCES t_devices (id) ON DELETE CASCADE
);


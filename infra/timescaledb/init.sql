CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Car telemetry hypertable
CREATE TABLE car_telemetry (
    time                  TIMESTAMPTZ NOT NULL,
    session_uid           TEXT NOT NULL,
    frame_id              INTEGER NOT NULL,
    car_index             SMALLINT NOT NULL,
    speed                 SMALLINT,
    throttle              REAL,
    steer                 REAL,
    brake                 REAL,
    clutch                SMALLINT,
    gear                  SMALLINT,
    engine_rpm            SMALLINT,
    drs                   BOOLEAN,
    rev_lights_percent    SMALLINT,
    brakes_temp_rl        SMALLINT,
    brakes_temp_rr        SMALLINT,
    brakes_temp_fl        SMALLINT,
    brakes_temp_fr        SMALLINT,
    tyres_surface_temp_rl SMALLINT,
    tyres_surface_temp_rr SMALLINT,
    tyres_surface_temp_fl SMALLINT,
    tyres_surface_temp_fr SMALLINT,
    tyres_inner_temp_rl   SMALLINT,
    tyres_inner_temp_rr   SMALLINT,
    tyres_inner_temp_fl   SMALLINT,
    tyres_inner_temp_fr   SMALLINT
);

SELECT create_hypertable('car_telemetry', 'time');

CREATE INDEX idx_car_telemetry_session
    ON car_telemetry (session_uid, car_index, time DESC);

-- Lap data hypertable
CREATE TABLE lap_data (
    time                 TIMESTAMPTZ NOT NULL,
    session_uid          TEXT NOT NULL,
    frame_id             INTEGER NOT NULL,
    car_index            SMALLINT NOT NULL,
    current_lap_time_ms  INTEGER,
    current_lap_num      SMALLINT,
    sector1_time_ms      INTEGER,
    sector2_time_ms      INTEGER,
    sector3_time_ms      INTEGER,
    lap_distance         REAL,
    total_distance       REAL,
    car_position         SMALLINT,
    current_lap_invalid  BOOLEAN,
    penalties            SMALLINT,
    num_pit_stops        SMALLINT,
    pit_status           SMALLINT,
    sector               SMALLINT,
    result_status        SMALLINT
);

SELECT create_hypertable('lap_data', 'time');

CREATE INDEX idx_lap_data_session
    ON lap_data (session_uid, car_index, time DESC);

-- Car status hypertable
CREATE TABLE car_status (
    time                    TIMESTAMPTZ NOT NULL,
    session_uid             TEXT NOT NULL,
    frame_id                INTEGER NOT NULL,
    car_index               SMALLINT NOT NULL,
    tyre_wear_rl            REAL,
    tyre_wear_rr            REAL,
    tyre_wear_fl            REAL,
    tyre_wear_fr            REAL,
    actual_tyre_compound    SMALLINT,
    visual_tyre_compound    SMALLINT,
    tyres_age_laps          SMALLINT,
    fuel_in_tank            REAL,
    fuel_capacity           REAL,
    fuel_remaining_laps     REAL,
    ers_store_energy        REAL,
    ers_deploy_mode         SMALLINT,
    ers_harvested_lap_mguk  REAL,
    ers_harvested_lap_mguh  REAL,
    ers_deployed_lap        REAL
);

SELECT create_hypertable('car_status', 'time');

CREATE INDEX idx_car_status_session
    ON car_status (session_uid, car_index, time DESC);

-- Participants hypertable
CREATE TABLE participants (
    time                  TIMESTAMPTZ NOT NULL,
    session_uid           TEXT NOT NULL,
    frame_id              INTEGER NOT NULL,
    car_index             SMALLINT NOT NULL,
    name                  TEXT,
    team                  SMALLINT,
    race_number           SMALLINT,
    nationality           SMALLINT,
    is_ai_controlled      BOOLEAN,
    driver                SMALLINT,
    platform              SMALLINT,
    is_my_team            BOOLEAN,
    is_telemetry_public   BOOLEAN
);

SELECT create_hypertable('participants', 'time');

CREATE INDEX idx_participants_session
    ON participants (session_uid, car_index, time DESC);

-- Session hypertable
CREATE TABLE session (
    time                        TIMESTAMPTZ NOT NULL,
    session_uid                 TEXT NOT NULL,
    frame_id                    INTEGER NOT NULL,
    car_index                   SMALLINT NOT NULL,
    track                       SMALLINT,
    session_type                SMALLINT,
    weather                     SMALLINT,
    track_temperature           SMALLINT,
    air_temperature             SMALLINT,
    total_laps                  SMALLINT,
    track_length                INTEGER,
    session_time_left           INTEGER,
    session_duration            INTEGER,
    safety_car_status           SMALLINT,
    pit_speed_limit             SMALLINT,
    formula                     SMALLINT,
    game_paused                 BOOLEAN,
    pit_stop_window_ideal_lap   SMALLINT,
    pit_stop_window_latest_lap  SMALLINT,
    pit_stop_rejoin_position    SMALLINT
);

SELECT create_hypertable('session', 'time');

CREATE INDEX idx_session_session
    ON session (session_uid, time DESC);

-- Session history hypertable
CREATE TABLE session_history (
    time                    TIMESTAMPTZ NOT NULL,
    session_uid             TEXT NOT NULL,
    frame_id                INTEGER NOT NULL,
    car_index               SMALLINT NOT NULL,
    num_laps                SMALLINT,
    num_tyre_stints         SMALLINT,
    best_lap_time_lap_num   SMALLINT,
    best_sector1_lap_num    SMALLINT,
    best_sector2_lap_num    SMALLINT,
    best_sector3_lap_num    SMALLINT,
    latest_lap_time_ms      INTEGER,
    latest_sector1_time_ms  INTEGER,
    latest_sector2_time_ms  INTEGER,
    latest_sector3_time_ms  INTEGER,
    latest_lap_valid        BOOLEAN
);

SELECT create_hypertable('session_history', 'time');

CREATE INDEX idx_session_history_session
    ON session_history (session_uid, car_index, time DESC);

-- Weather forecast hypertable
CREATE TABLE IF NOT EXISTS weather_forecast (
    time                     TIMESTAMPTZ NOT NULL,
    session_uid              TEXT NOT NULL,
    frame_id                 INTEGER NOT NULL,
    car_index                SMALLINT NOT NULL,
    forecast_session_type    SMALLINT NOT NULL,
    time_offset              SMALLINT NOT NULL,
    weather                  SMALLINT NOT NULL,
    track_temperature        SMALLINT NOT NULL,
    track_temperature_change SMALLINT NOT NULL,
    air_temperature          SMALLINT NOT NULL,
    air_temperature_change   SMALLINT NOT NULL,
    rain_percentage          SMALLINT NOT NULL,
    forecast_accuracy        SMALLINT NOT NULL,
    sample_index             SMALLINT NOT NULL
);

SELECT create_hypertable('weather_forecast', 'time');

CREATE INDEX idx_weather_forecast_session
    ON weather_forecast (session_uid, time DESC);

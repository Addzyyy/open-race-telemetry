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

create table public.spots (
  spot_id serial not null,
  spot_name character varying(255) not null,
  latitude numeric(10, 7) not null,
  longitude numeric(10, 7) not null,
  timezone character varying(64) not null,
  constraint spots_pkey primary key (spot_id),
  constraint spots_spot_name_key unique (spot_name)
) TABLESPACE pg_default;



create table public.users (
  user_id uuid not null default gen_random_uuid (),
  name character varying(255) not null,
  email character varying(255) not null,
  password_hash character varying(255) not null,
  surf_level character varying(50) null,
  goofy_regular_stance character varying(10) null,
  preferred_wave_direction character varying(20) null,
  bio text null,
  profile_picture_url character varying(255) null,
  registration_timestamp timestamp with time zone null default now(),
  last_login_timestamp timestamp with time zone null,
  constraint users_pkey primary key (user_id),
  constraint users_email_key unique (email)
) TABLESPACE pg_default;



create table public.forecasts (
  forecast_id serial not null,
  spot_id integer not null,
  timestamp_utc timestamp with time zone not null,
  wave_height_sg numeric(5, 2) null,
  wave_direction_sg numeric(6, 2) null,
  wave_period_sg numeric(5, 2) null,
  swell_height_sg numeric(5, 2) null,
  swell_direction_sg numeric(6, 2) null,
  swell_period_sg numeric(5, 2) null,
  secondary_swell_height_sg numeric(5, 2) null,
  secondary_swell_direction_sg numeric(6, 2) null,
  secondary_swell_period_sg numeric(5, 2) null,
  wind_speed_sg numeric(5, 2) null,
  wind_direction_sg numeric(6, 2) null,
  water_temperature_sg numeric(5, 2) null,
  air_temperature_sg numeric(5, 2) null,
  current_speed_sg numeric(5, 2) null,
  current_direction_sg numeric(6, 2) null,
  sea_level_sg numeric(5, 2) null,
  constraint forecasts_pkey primary key (forecast_id),
  constraint uq_forecast_spot_timestamp unique (spot_id, timestamp_utc),
  constraint fk_spot foreign KEY (spot_id) references spots (spot_id)
) TABLESPACE pg_default;



create table public.level_spot_preferences (
  level_preference_id serial not null,
  spot_id integer not null,
  surf_level character varying(50) not null,
  min_wave_height numeric(5, 2) null,
  max_wave_height numeric(5, 2) null,
  ideal_wave_height numeric(5, 2) null,
  min_wave_period numeric(5, 2) null,
  max_wave_period numeric(5, 2) null,
  ideal_wave_period numeric(5, 2) null,
  min_swell_height numeric(5, 2) null,
  max_swell_height numeric(5, 2) null,
  ideal_swell_height numeric(5, 2) null,
  min_swell_period numeric(5, 2) null,
  max_swell_period numeric(5, 2) null,
  ideal_swell_period numeric(5, 2) null,
  preferred_wave_direction character varying(20) null,
  preferred_swell_direction character varying(20) null,
  ideal_tide_type character varying(10) null,
  min_sea_level numeric(5, 2) null,
  max_sea_level numeric(5, 2) null,
  ideal_sea_level numeric(5, 2) null,
  min_wind_speed numeric(5, 2) null,
  max_wind_speed numeric(5, 2) null,
  ideal_wind_speed numeric(5, 2) null,
  preferred_wind_direction character varying(20) null,
  ideal_water_temperature numeric(5, 2) null,
  ideal_air_temperature numeric(5, 2) null,
  constraint level_spot_preferences_pkey primary key (level_preference_id),
  constraint uq_level_spot_pref unique (spot_id, surf_level),
  constraint fk_spot_level_pref foreign KEY (spot_id) references spots (spot_id)
) TABLESPACE pg_default;



create table public.model_spot_preferences (
  model_preference_id serial not null,
  user_id uuid not null,
  spot_id integer not null,
  min_wave_height numeric(5, 2) null,
  max_wave_height numeric(5, 2) null,
  ideal_wave_height numeric(5, 2) null,
  min_wave_period numeric(5, 2) null,
  max_wave_period numeric(5, 2) null,
  ideal_wave_period numeric(5, 2) null,
  min_swell_height numeric(5, 2) null,
  max_swell_height numeric(5, 2) null,
  ideal_swell_height numeric(5, 2) null,
  min_swell_period numeric(5, 2) null,
  max_swell_period numeric(5, 2) null,
  ideal_swell_period numeric(5, 2) null,
  preferred_wave_direction character varying(20) null,
  preferred_swell_direction character varying(20) null,
  ideal_tide_type character varying(10) null,
  min_sea_level numeric(5, 2) null,
  max_sea_level numeric(5, 2) null,
  ideal_sea_level numeric(5, 2) null,
  min_wind_speed numeric(5, 2) null,
  max_wind_speed numeric(5, 2) null,
  ideal_wind_speed numeric(5, 2) null,
  preferred_wind_direction character varying(20) null,
  ideal_water_temperature numeric(5, 2) null,
  ideal_air_temperature numeric(5, 2) null,
  ideal_current_speed numeric(5, 2) null,
  constraint model_spot_preferences_pkey primary key (model_preference_id),
  constraint uq_model_spot_pref unique (user_id, spot_id),
  constraint fk_spot_model_pref_spot foreign KEY (spot_id) references spots (spot_id),
  constraint fk_spot_model_pref_user foreign KEY (user_id) references users (user_id)
) TABLESPACE pg_default;

create table public.surf_ratings (
  rating_id serial not null,
  user_id uuid not null,
  spot_id integer not null,
  rating_value integer not null,
  comments text null,
  session_date date not null,
  session_start_time time with time zone null,
  session_end_time time with time zone null,
  constraint surf_ratings_pkey primary key (rating_id),
  constraint fk_spot_rating foreign KEY (spot_id) references spots (spot_id),
  constraint fk_user_rating foreign KEY (user_id) references users (user_id)
) TABLESPACE pg_default;


create table public.rating_conditions_snapshot (
  snapshot_id serial not null,
  rating_id integer not null,
  timestamp_utc timestamp with time zone not null,
  wave_height_sg numeric(5, 2) null,
  wave_direction_sg numeric(6, 2) null,
  wave_period_sg numeric(5, 2) null,
  swell_height_sg numeric(5, 2) null,
  swell_direction_sg numeric(6, 2) null,
  swell_period_sg numeric(5, 2) null,
  secondary_swell_height_sg numeric(5, 2) null,
  secondary_swell_direction_sg numeric(6, 2) null,
  secondary_swell_period_sg numeric(5, 2) null,
  wind_speed_sg numeric(5, 2) null,
  wind_direction_sg numeric(6, 2) null,
  water_temperature_sg numeric(5, 2) null,
  air_temperature_sg numeric(5, 2) null,
  current_speed_sg numeric(5, 2) null,
  current_direction_sg numeric(6, 2) null,
  sea_level_sg numeric(5, 2) null,
  tide_type character varying(10) null,
  tide_height numeric(5, 3) null,
  constraint rating_conditions_snapshot_pkey primary key (snapshot_id),
  constraint rating_conditions_snapshot_rating_id_key unique (rating_id),
  constraint fk_rating_snapshot foreign KEY (rating_id) references surf_ratings (rating_id)
) TABLESPACE pg_default;







create table public.tides_forecast (
  tide_id serial not null,
  spot_id integer not null,
  timestamp_utc timestamp with time zone not null,
  tide_type character varying(10) not null,
  height numeric(5, 2) not null,
  constraint tides_forecast_pkey primary key (tide_id),
  constraint uq_tide_spot_timestamp_type unique (spot_id, timestamp_utc, tide_type),
  constraint fk_spot_tide foreign KEY (spot_id) references spots (spot_id)
) TABLESPACE pg_default;




create table public.user_recommendation_presets (
  preset_id serial not null,
  user_id uuid not null,
  preset_name character varying(255) not null,
  spot_ids integer[] not null,
  weekdays integer[] not null default '{}'::integer[],
  start_time time without time zone not null,
  end_time time without time zone not null,
  is_default boolean null default false,
  is_active boolean null default true,
  updated_at timestamp with time zone null default CURRENT_TIMESTAMP,

  constraint user_recommendation_presets_pkey primary key (preset_id),
  constraint user_recommendation_presets_user_id_fkey foreign KEY (user_id) references users (user_id) on delete CASCADE
) TABLESPACE pg_default;

create unique INDEX IF not exists unique_default_preset_per_user_if_true on public.user_recommendation_presets using btree (user_id, is_default) TABLESPACE pg_default
where
  (is_default = true);



create table public.user_spot_preferences (
  user_preference_id serial not null,
  user_id uuid not null,
  spot_id integer not null,
  is_active boolean not null default true,
  min_wave_height numeric(5, 2) null,
  max_wave_height numeric(5, 2) null,
  ideal_wave_height numeric(5, 2) null,
  min_wave_period numeric(5, 2) null,
  max_wave_period numeric(5, 2) null,
  ideal_wave_period numeric(5, 2) null,
  min_swell_height numeric(5, 2) null,
  max_swell_height numeric(5, 2) null,
  ideal_swell_height numeric(5, 2) null,
  min_swell_period numeric(5, 2) null,
  max_swell_period numeric(5, 2) null,
  ideal_swell_period numeric(5, 2) null,
  preferred_wave_direction character varying(20) null,
  preferred_swell_direction character varying(20) null,
  ideal_tide_type character varying(10) null,
  min_sea_level numeric(5, 2) null,
  max_sea_level numeric(5, 2) null,
  ideal_sea_level numeric(5, 2) null,
  min_wind_speed numeric(5, 2) null,
  max_wind_speed numeric(5, 2) null,
  ideal_wind_speed numeric(5, 2) null,
  preferred_wind_direction character varying(20) null,
  ideal_water_temperature numeric(5, 2) null,
  ideal_air_temperature numeric(5, 2) null,
  constraint user_spot_preferences_pkey primary key (user_preference_id),
  constraint uq_user_spot_pref unique (user_id, spot_id),
  constraint fk_spot_pref foreign KEY (spot_id) references spots (spot_id),
  constraint fk_user_pref foreign KEY (user_id) references users (user_id)
) TABLESPACE pg_default;





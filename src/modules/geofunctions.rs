const NORMALIZATION_FACTOR: f64 = 2i32.pow(26) as f64; // 2^26

const MIN_LATITUDE: f64 = -85.05112878;
const MAX_LATITUDE: f64 = 85.05112878;
const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;

const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

const EARTH_RADIUS: f64 = 6372797.560856;

pub fn get_distance(x_lon: f64, x_lat: f64, y_lon: f64, y_lat: f64) -> f64 {
    let x_lat = x_lat.to_radians();
    let y_lat = y_lat.to_radians();
    let delta_lon = (y_lon - x_lon).to_radians();
    let delta_lat = y_lat - x_lat;

    let a = (delta_lat/2.0).sin().powi(2) + x_lat.cos() * y_lat.cos() * (delta_lon/2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0-a).sqrt());
    EARTH_RADIUS * c
}

pub fn location_to_score(latitude: f64, longitude: f64) -> f64 {
    let normalized_latitude = (NORMALIZATION_FACTOR * (latitude - MIN_LATITUDE) / LATITUDE_RANGE) as u32;
    let normalized_longitude = (NORMALIZATION_FACTOR * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE) as u32;
    interleave(normalized_latitude, normalized_longitude) as f64
}

pub fn score_to_location(score: f64) -> (f64, f64) {
    let y = score as u64 >> 1;
    let x = score as u64;
    let grid_latitude_number = compact_u64_to_u32(x);
    let grid_longitude_number = compact_u64_to_u32(y);

    let grid_latitude_min = MIN_LATITUDE + LATITUDE_RANGE * (grid_latitude_number as f64 / (NORMALIZATION_FACTOR));
    let grid_latitude_max = MIN_LATITUDE + LATITUDE_RANGE * ((grid_latitude_number + 1) as f64 / (NORMALIZATION_FACTOR));
    let grid_longitude_min = MIN_LONGITUDE + LONGITUDE_RANGE * (grid_longitude_number as f64 / (NORMALIZATION_FACTOR));
    let grid_longitude_max = MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_longitude_number + 1) as f64 / (NORMALIZATION_FACTOR));

    let latitude = (grid_latitude_min + grid_latitude_max) / 2.0;
    let longitude = (grid_longitude_min + grid_longitude_max) / 2.0;
    (latitude, longitude)
}

fn interleave(x: u32, y: u32) -> u64 {
    let x = spread_u32_to_u64(x);
    let y = spread_u32_to_u64(y);
    let y_shifted = y << 1;
    x | y_shifted
}

fn spread_u32_to_u64(x: u32) -> u64 {
    let mut v = x as u64;
    v = (v | (v << 16)) & 0x0000FFFF0000FFFF;
    v = (v | (v << 8))  & 0x00FF00FF00FF00FF;
    v = (v | (v << 4))  & 0x0F0F0F0F0F0F0F0F;
    v = (v | (v << 2))  & 0x3333333333333333;
    v = (v | (v << 1))  & 0x5555555555555555;
    v
}

fn compact_u64_to_u32(mut v: u64) -> u32 {
    v = v & 0x5555555555555555;
    v = (v | (v >> 1)) & 0x3333333333333333;
    v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F;
    v = (v | (v >> 4)) & 0x00FF00FF00FF00FF;
    v = (v | (v >> 8)) & 0x0000FFFF0000FFFF;
    v = (v | (v >> 16)) & 0x00000000FFFFFFFF;

    v as u32
}
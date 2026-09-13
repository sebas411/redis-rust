use std::vec;

use super::*;

impl ClientHandler {
    pub(super) async fn execute_geospatial_command(
        &mut self,
        command: &str,
        args: Vec<RedisValue>,
    ) -> Result<Vec<u8>> {
        let response = match command {
            "GEOADD" => {
                if args.len() != 5 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'GEOADD' command".to_string(),
                    )
                    .encode()
                } else {
                    let key = args[1].get_string()?;
                    let longitude = args[2].get_string()?;
                    let latitude = args[3].get_string()?;
                    let name = args[4].get_string()?;

                    let longitude = longitude.parse::<f64>()?;
                    let latitude = latitude.parse::<f64>()?;

                    if longitude < -180.0
                        || longitude > 180.0
                        || latitude < -85.05112878
                        || latitude > 85.05112878
                    {
                        RedisValue::Error(format!(
                            "ERR invalid longitude,latitude pair {:.6},{}",
                            longitude, latitude
                        ))
                        .encode()
                    } else {
                        let entry =
                            SortedSetEntry::new(&name, location_to_score(latitude, longitude));
                        let mut db = self.db.write().await;
                        match db.get_mut(&key) {
                            Some(record) => {
                                if let DbRecord::SortedSet(record) = record {
                                    record.insert(entry);
                                }
                            }
                            None => {
                                let mut record = SortedSetRecord::new();
                                record.insert(entry);
                                db.insert(key, DbRecord::SortedSet(record));
                            }
                        }
                        RedisValue::Int(1).encode()
                    }
                }
            }
            "GEOPOS" => {
                if args.len() < 3 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'GEOPOS' command".to_string(),
                    )
                    .encode()
                } else {
                    let key = args[1].get_string()?;
                    let mut responses = vec![];
                    for member in &args[2..] {
                        let member = member.get_string()?;
                        let db = self.db.read().await;
                        match db.get(&key) {
                            Some(DbRecord::SortedSet(set)) => {
                                if let Some(entry) = set.get(&member) {
                                    let score = entry.get_score();
                                    let (latitude, longitude) = score_to_location(score);
                                    responses.push(RedisValue::array_from_string_vec(vec![
                                        &format!("{}", longitude),
                                        &format!("{}", latitude),
                                    ]));
                                } else {
                                    responses.push(RedisValue::NullArray);
                                }
                            }
                            _ => responses.push(RedisValue::NullArray),
                        }
                    }
                    RedisValue::Array(responses).encode()
                }
            }
            "GEODIST" => {
                if args.len() != 4 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'GEODIST' command".to_string(),
                    )
                    .encode()
                } else {
                    let key = args[1].get_string()?;
                    let x_member = args[2].get_string()?;
                    let y_member = args[3].get_string()?;
                    let db = self.db.read().await;
                    match db.get(&key) {
                        Some(DbRecord::SortedSet(record)) => {
                            if let Some(x_entry) = record.get(&x_member)
                                && let Some(y_entry) = record.get(&y_member)
                            {
                                let (x_lat, x_lon) = score_to_location(x_entry.get_score());
                                let (y_lat, y_lon) = score_to_location(y_entry.get_score());
                                let distance = get_distance(x_lon, x_lat, y_lon, y_lat);
                                RedisValue::String(format!("{}", distance).as_bytes().to_vec()).encode()
                            } else {
                                RedisValue::String(vec![]).encode()
                            }
                        }
                        _ => RedisValue::String(vec![]).encode(),
                    }
                }
            }
            "GEOSEARCH" => {
                if args.len() != 8 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'GEOSEARCH' command".to_string(),
                    )
                    .encode()
                } else {
                    if args[2].get_string()?.to_uppercase() != "FROMLONLAT"
                        || args[5].get_string()?.to_uppercase() != "BYRADIUS"
                    {
                        return Ok(RedisValue::Error(
                            "Err 'GEOSEARCH' command only supports FROMLONLAT and BYRADIUS options"
                                .to_string(),
                        )
                        .encode());
                    }
                    let key = args[1].get_string()?;
                    let longitude = args[3].get_string()?.parse()?;
                    let latitude = args[4].get_string()?.parse()?;

                    let mut distance = args[6].get_string()?.parse::<f64>()?;
                    let unit = args[7].get_string()?;
                    match unit.as_str() {
                        "m" => (),
                        "km" => distance *= 1000.0,
                        "mi" => distance *= 1609.34,
                        "ft" => distance /= 3.28084,
                        "yd" => distance /= 1.09391,
                        _ => {
                            return Ok(RedisValue::Error(
                                "Err distance unit not supported".to_string(),
                            )
                            .encode());
                        }
                    }

                    let mut responses = vec![];
                    let db = self.db.read().await;
                    match db.get(&key) {
                        Some(DbRecord::SortedSet(record)) => {
                            for entry in record {
                                let entry_score = entry.get_score();
                                let (entry_lat, entry_lon) = score_to_location(entry_score);
                                let entry_distance =
                                    get_distance(longitude, latitude, entry_lon, entry_lat);
                                if entry_distance <= distance {
                                    responses
                                        .push(RedisValue::String(entry.get_value().as_bytes().to_vec()));
                                }
                            }
                        }
                        _ => (),
                    }
                    RedisValue::Array(responses).encode()
                }
            }
            _ => unreachable!("command routed to the wrong handler: {command}"),
        };
        Ok(response)
    }
}

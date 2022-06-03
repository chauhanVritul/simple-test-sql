use html_escape::decode_html_entities;
use mysql::{Pool, Opts, prelude::*, Row};
use serde_json::{json, Value, Map};
fn main() {
    let base = "mysql";
    let username = "analytics";
    let password = "19d229e00a89adbc711603549f92f3a2c4fa6d4e35f73fafc57f70c0a3a6a5d31d585eeb5b2d8765c432b1f5c5fc9831e92b4b8ae94e250b805b337a436dbb8f";
    let url = "muspell-db.wiise.azure.net";
    let db_name = "analyticsdb";

    let pool = Pool::new(
        Opts::from_url(
            format!(
                "{}://{}:{}@{}/{}",
                base,
                username,
                password,
                url,
                db_name
            )
            .as_str(),
        )
        .unwrap()
    )
    .expect("fail to create sql pool");

    let mut conn = pool.get_conn().unwrap();
    let mut result: Vec<Map<String, Value>> = Vec::new();

    let query = format!(
        "select \
        va.server_time as 'Timestamp', \
        en.name as 'Screen Name', \
        visit.custom_dimension_2 as 'Launch Type', \
        visit.custom_dimension_3 as 'User Name', \
        visit.custom_dimension_4 as visit_params, \
        va.custom_dimension_1 as 'MPI ID', \
        va.custom_dimension_2 as 'Source Software ID', \
        va.custom_dimension_3 as action_params \
      from \
        matomo_log_visit as visit \
        LEFT JOIN matomo_log_link_visit_action as va on va.idvisit = visit.idvisit \
        LEFT JOIN matomo_log_action as en on va.idaction_name = en.idaction \
      where \
        va.idsite = 6 \
        and visit.custom_dimension_2='epic' \
        and en.type = 4 \
      order by \
        va.server_time desc;"
    );

    conn.query(query.as_str())
        .unwrap()
        .into_iter()
        .for_each(|row: Row| {
            let mut res_map = Map::<String, Value>::new();
            (0..row.len()).for_each(|i| match row.columns()[i].name_str().to_string().as_str() {
                "visit_params" | "action_params" => {
                    let encoded_data: String = match row.get(i).unwrap_or_default() {
                        Some(s) => s,
                        None => String::new(),
                    };
                    let data = decode_html_entities(&encoded_data);
                    let js: Map<String, Value> = serde_json::from_str(&data).unwrap_or_default();
                    res_map.extend(js);
                }
                col_name => {
                    let encoded_data: String = match row.get(i).unwrap_or_default() {
                        Some(s) => s,
                        None => String::new(),
                    };
                    let data = decode_html_entities(&encoded_data);
                    res_map.insert(col_name.to_string(), json!(data));
                }
            });
            result.push(res_map);
        });

    println!("{:?}", result);

}

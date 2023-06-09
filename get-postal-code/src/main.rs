use std::env;

use aws_sdk_dynamodb::{types::AttributeValue, Client};
use lambda_http::aws_lambda_events::serde_json;
use lambda_http::{run, service_fn, Body, Error, Request, RequestExt, Response};

#[derive(serde::Deserialize, serde::Serialize, std::fmt::Debug)]
struct Address {
    prefecture: String,
    city: String,
    town: String,
    prefecture_kana: String,
    city_kana: String,
    town_kana: String,
}

#[derive(serde::Deserialize, serde::Serialize, std::fmt::Debug)]
struct ResponseData {
    code: String,
    data: Vec<Address>,
}

async fn function_handler(client: &Client, event: Request) -> Result<Response<Body>, Error> {
    let table_name = env::var("POSTAL_CODE_TABLE").expect("POSTAL_CODE_TABLE not set");

    let query_map = event.query_string_parameters();
    tracing::info!(query_map = ?query_map, "query");

    let mut code: Option<String> = None;
    let mut address: Option<Address> = None;
    if let Some(postal_code) = query_map.first("postal_code") {
        let postal_code = postal_code_normalize(postal_code);
        tracing::info!(postal_code = ?postal_code, "Postal code");

        let item = client
            .get_item()
            .table_name(table_name)
            .key("postal_code", AttributeValue::S(postal_code.to_string()))
            .send()
            .await?;

        if let Some(record) = item.item() {
            let prefecture = record.get("prefecture").unwrap().as_s().unwrap();
            let city = record.get("city").unwrap().as_s().unwrap();
            let town = record.get("town").unwrap().as_s().unwrap();
            let prefecture_kana = record.get("prefecture_kana").unwrap().as_s().unwrap();
            let city_kana = record.get("city_kana").unwrap().as_s().unwrap();
            let town_kana = record.get("town_kana").unwrap().as_s().unwrap();

            address = Some(Address {
                prefecture: prefecture.to_string(),
                city: city.to_string(),
                town: town.to_string(),
                prefecture_kana: prefecture_kana.to_string(),
                city_kana: city_kana.to_string(),
                town_kana: town_kana.to_string(),
            });
        }

        code = Some(postal_code);
    }

    let data = ResponseData {
        code: code.unwrap_or("".to_string()),
        data: match address {
            Some(address) => vec![address],
            None => vec![],
        },
    };

    let body = serde_json::to_string(&data).map_err(Box::new)?;

    let res = Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .body(body.into())
        .map_err(Box::new)?;

    Ok(res)
}

fn postal_code_normalize(postal_code: &str) -> String {
    let hankaku: String = postal_code.chars().map(zenkaku_to_hankaku).collect();

    hankaku.replace("-", "")
}

/// 全角英数記号を半角英数記号に変換
fn zenkaku_to_hankaku(c: char) -> char {
    match c {
        // half ascii code
        '\u{0020}'..='\u{007E}' => c,
        // FullWidth
        // '！'..='～' = '\u{FF01}'..='\u{FF5E}'
        '\u{FF01}'..='\u{FF5E}' => char_from_u32(c as u32 - 0xFF01 + 0x21, c),
        // space
        '\u{2002}'..='\u{200B}' => ' ',
        '\u{3000}' | '\u{FEFF}' => ' ',
        // others
        _ => c,
    }
}

/// u32からcharに変換
fn char_from_u32(i: u32, def: char) -> char {
    char::from_u32(i).unwrap_or(def)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .without_time()
        .init();

    let client = Client::new(&aws_config::load_from_env().await);

    run(service_fn(|event| async {
        function_handler(&client, event).await
    }))
    .await
}

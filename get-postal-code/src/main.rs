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
    // 環境変数から郵便番号が保存されたDynamoDBのテーブル名を取得
    let table_name = env::var("POSTAL_CODE_TABLE").expect("POSTAL_CODE_TABLE not set");

    // API Gatewayから渡されたパスパラメータを取得
    let path_parameters = event.path_parameters();
    tracing::info!(path_parameters = ?path_parameters, "query");

    let mut code: Option<String> = None;
    let mut address: Option<Address> = None;
    //パスパラメータから検索する郵便番号を取得
    if let Some(postal_code) = path_parameters.first("postalCode") {
        //郵便番号入力値を正規化
        let postal_code = postal_code_normalize(postal_code);
        tracing::info!(postal_code = ?postal_code, "Postal code");

        //郵便番号をキーにしてDynamoDBから住所情報を取得
        let item = client
            .get_item()
            .table_name(table_name)
            .key("postal_code", AttributeValue::S(postal_code.to_string()))
            .send()
            .await?;

        //DynamoDBから住所情報を取得出来たら、レスポンスに住所情報をセット
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

        //検索に使用した入力値をレスポンスにセット
        code = Some(postal_code);
    }

    // 返却用のデータを作成
    let data = ResponseData {
        code: code.unwrap_or("".to_string()),
        data: match address {
            Some(address) => vec![address],
            None => vec![],
        },
    };

    // データ構造を返却用のJSON文字列に変換
    let body = serde_json::to_string(&data).map_err(Box::new)?;

    // レスポンス作成
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

mod ken_all;
mod postal_code_record;

use std::collections::HashMap;
use std::env;

use aws_sdk_dynamodb::types::{PutRequest, WriteRequest};
use aws_sdk_dynamodb::{types::AttributeValue, Client};
use lambda_runtime::{run, service_fn, Error, LambdaEvent};

use crate::postal_code_record::PostalCodeRecord;

// コンテンツ全体に対するハッシュ値を保存するキー (national_local_government_codeと絶対に被らない適当な文字列であればよい)
const HASH_ITEM_KEY: &str = "#hash#";

#[derive(serde::Deserialize, serde::Serialize, std::fmt::Debug)]
struct ResponseData {
    code: usize,
    count: usize,
    message: String,
}

async fn function_handler(
    client: &Client,
    _event: LambdaEvent<serde_json::Value>,
) -> Result<serde_json::Value, Error> {
    tracing::info!("Start handler");

    // 環境変数から必要なDynamoDBのテーブル名を取得
    let table_name = env::var("POSTAL_CODE_TABLE").expect("POSTAL_CODE_TABLE not set");
    let hash_table_name = env::var("HASH_TABLE").expect("POSTAL_CODE_TABLE not set");

    tracing::info!("start ken_all");
    let ken_all_data = crate::ken_all::ken_all_records();
    tracing::info!("end ken_all");

    tracing::info!("grouping ken_all");
    //住所データを郵便番号でグルーピングします
    let mut postal_code_to_records = HashMap::<String, Vec<PostalCodeRecord>>::new();
    for (_, records) in ken_all_data.grouped_postal_code_record_list {
        for record in records {
            //郵便番号をキーにして、同じ郵便番号を持つデータを配列にまとめます
            postal_code_to_records
                .entry(record.postal_code.clone())
                .or_default()
                .push(record);
        }
    }
    tracing::info!("end grouping ken_all");

    tracing::info!("marge ken_all");
    //同じ郵便番号を持つデータの情報を統合します
    let mut result = Vec::<PostalCodeRecord>::new();
    for (_, mut records) in postal_code_to_records {
        // データが一つしなかったら
        if records.len() == 1 {
            //そのまま結果リストにデータを移します
            result.push(records.remove(0));
        } else {
            //複数データを持っている場合は、異なる値を持つ部分をクリアします
            let mut item = records.remove(0);
            for other in &records {
                // 内容が異なれば
                if item.town != "" && item.town != other.town {
                    item.town = "".to_string();
                    item.town_kana = "".to_string();
                }
                if item.city != "" && item.city != other.city {
                    item.city = "".to_string();
                    item.city_kana = "".to_string();
                }
                if item.prefecture != "" && item.prefecture != other.prefecture {
                    item.prefecture = "".to_string();
                    item.prefecture_kana = "".to_string();
                }
            }
            //内容をチェックした後のデータを結果リストに映します
            result.push(item);
        }
    }
    tracing::info!("end marge ken_all");

    // dynamoDBから取得したハッシュ値をキャッシュする
    let mut cache = HashMap::<String, String>::new();

    // 全体の変更検知のためにハッシュ値を取得
    let contents_changed = is_hash_change(
        client,
        hash_table_name.clone(),
        &mut cache,
        HASH_ITEM_KEY.to_string(),
        &ken_all_data.all_contents_hash,
    )
    .await?;

    let mut count = 0;
    //コンテンツに変更がある場合は
    if contents_changed {
        // DynamoDBにデータを書き込みます
        let mut requests = Vec::<WriteRequest>::new();

        for record in result {
            //対象のレコードは変更があったレコードか？
            let changed = is_hash_change(
                client,
                hash_table_name.clone(),
                &mut cache,
                record.national_local_government_code.clone(),
                ken_all_data
                    .national_local_government_code_to_hash
                    .get(&record.national_local_government_code)
                    .unwrap(),
            )
            .await?;

            // 変更のあったレコードなら
            if changed {
                // DynamoDBに住所情報を書き込む
                let put_request = PutRequest::builder()
                    .item("postal_code", AttributeValue::S(record.postal_code))
                    .item("prefecture", AttributeValue::S(record.prefecture))
                    .item("prefecture_kana", AttributeValue::S(record.prefecture_kana))
                    .item("city", AttributeValue::S(record.city))
                    .item("city_kana", AttributeValue::S(record.city_kana))
                    .item("town", AttributeValue::S(record.town))
                    .item("town_kana", AttributeValue::S(record.town_kana))
                    .build();

                let req = WriteRequest::builder().put_request(put_request).build();

                requests.push(req);

                // 大きなデータを送信するとエラーになるため適当な数ごとにバッチリクエストを実行する
                if requests.len() == 25 {
                    count += requests.len();

                    send_batch_write_item(client, table_name.to_owned(), requests.clone()).await?;

                    requests.clear();
                }
            }
        }

        if requests.len() > 0 {
            count += requests.len();

            send_batch_write_item(client, table_name.to_owned(), requests.clone()).await?;

            requests.clear();
        }

        // 最後にハッシュ値をDynamoDBに書き込む
        let put_request = PutRequest::builder()
            .item("id", AttributeValue::S(HASH_ITEM_KEY.to_string()))
            .item("hash", AttributeValue::S(ken_all_data.all_contents_hash))
            .build();
        let req = WriteRequest::builder().put_request(put_request).build();
        requests.push(req);

        for (id, hash) in ken_all_data.national_local_government_code_to_hash {
            let put_request = PutRequest::builder()
                .item("id", AttributeValue::S(id))
                .item("hash", AttributeValue::S(hash))
                .build();
            let req = WriteRequest::builder().put_request(put_request).build();
            requests.push(req);

            // 大きなデータを送信するとエラーになるため適当な数ごとにバッチリクエストを実行する
            if requests.len() == 25 {
                send_batch_write_item(client, hash_table_name.to_owned(), requests.clone()).await?;

                requests.clear();
            }
        }

        if requests.len() > 0 {
            send_batch_write_item(client, hash_table_name.to_owned(), requests).await?;
        }
    }

    let response_data = ResponseData {
        code: 0,
        count: count,
        message: "".to_string(),
    };

    Ok(serde_json::json!(response_data))
}

// batch_write_itemのリクエストを送信する
async fn send_batch_write_item(
    client: &Client,
    table_name: String,
    requests: Vec<WriteRequest>,
) -> Result<(), Error> {
    client
        .batch_write_item()
        .request_items(table_name, requests)
        .send()
        .await?;

    Ok(())
}

async fn is_hash_change(
    client: &Client,
    table_name: String,
    cache: &mut HashMap<String, String>,
    id: String,
    hash: &String,
) -> Result<bool, Error> {
    let cache_item = cache.get(&id);

    //すでにDynamoDBから取得していたら、取得済みの値と比較
    let changed = if let Some(cached_hash) = cache_item {
        cached_hash != hash
    } else {
        //まだハッシュ値を持っていなければ、DynamoDBから取得
        let hash_item = client
            .get_item()
            .table_name(table_name)
            .key("id", AttributeValue::S(id.clone()))
            .send()
            .await?;

        if let Some(item) = hash_item.item() {
            let contents_hash = item.get("hash").unwrap().as_s().unwrap();
            //取得したハッシュ値をキャッシュに保存
            cache.insert(id, contents_hash.to_owned());

            // 計算した結果と取得したハッシュを比較して異なっていたら変更されていると判定
            contents_hash != hash
        } else {
            //DynamoDB上に項目がなかったため、空文字をキャッシュに保存
            cache.insert(id, "".to_string());
            //DynamoDBに項目がない場合は変更があったと判定
            true
        }
    };

    return Ok(changed);
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .without_time()
        .init();
    tracing::info!("Initializing lambda function");

    let client = Client::new(&aws_config::load_from_env().await);
    tracing::info!(client = ?client, "Created DynamoDB");

    let func = service_fn(|event| function_handler(&client, event));
    run(func).await
}

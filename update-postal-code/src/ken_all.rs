use base64::{engine::general_purpose, Engine as _};
use digest::FixedOutputReset;
use regex::Regex;
use reqwest::blocking::Response;
use std::{
    collections::HashMap,
    io::{Cursor, Read, Seek},
};

use encoding_rs::SHIFT_JIS;
use sha2::{Digest, Sha256};
use zip::ZipArchive;

use crate::postal_code_record::{self, PostalCodeRecord};

pub struct KenAllData {
    pub all_contents_hash: String,
    pub grouped_postal_code_record_list: HashMap<String, Vec<PostalCodeRecord>>,
    pub national_local_government_code_to_hash: HashMap<String, String>,
}

pub fn ken_all_records() -> KenAllData {
    //let filepath = std::path::Path::new("ken_all.zip");
    //let reader = std::fs::File::open(filepath).expect("Unable to open the file");

    //ken_all.zipをダウンロードする
    let mut response = download_ken_all_zip().expect("Unable to download the zip file");
    let reader =
        response_to_cursor(&mut response).expect("Unable to convert the response to cursor");

    //Zipファイルからファイルを一つ取り出して、その内容を文字列にして取得する
    let contents =
        zip_to_file_contents(reader).expect("Unable to convert the zip to file contents");

    // CSVファイルをパース
    let mut reader = csv::Reader::from_reader(contents.as_bytes());

    // CSVのそれぞれの行を分割しながら、リストに格納する
    let mut postal_code_record_list = Vec::<postal_code_record::PostalCodeRecord>::new();
    for result in reader.records() {
        let record = result.expect("Unable to read the record");
        //レコードの長さが不正であれば処理をしない
        if record.len() < 15 {
            //TODO ログ出力
            continue;
        }

        let national_local_government_code = record.get(0).unwrap(); //全国地方公共団体コード 半角数字
        let postal_code = record.get(2).unwrap(); //郵便番号 半角数字
        let prefecture_kana = record.get(3).unwrap(); //都道府県名カナ 半角カタカナ
        let city_kana = record.get(4).unwrap(); //市区町村名カナ 半角カタカナ
        let town_kana = record.get(5).unwrap(); //町域名カナ 半角カタカナ
        let prefecture = record.get(6).unwrap(); //都道府県名
        let city = record.get(7).unwrap(); //市区町村名
        let town = record.get(8).unwrap(); //町域名
        let is_一つの町域が二つ以上の郵便番号で表示される = record.get(9).unwrap() == "1";
        let is_小字毎に番地が起番されている町域 = record.get(10).unwrap() == "1";
        let is_丁目を有する町域 = record.get(11).unwrap() == "1";
        let is_一つの郵便番号で二以上の町域を表す = record.get(12).unwrap() == "1";

        let is_change = i32::from_str_radix(record.get(13).unwrap(), 10).unwrap(); //更新の表示　「0」は変更なし、「1」は変更あり、「2」廃止（廃止データのみ使用）

        //現状では使用していないのでコメントアウト
        //let change_reason = i32::from_str_radix(record.get(14).unwrap(), 10).unwrap(); //変更理由

        let postal_code_record = postal_code_record::PostalCodeRecord::new(
            national_local_government_code.to_owned(),
            postal_code.to_owned(),
            prefecture_kana.to_owned(),
            city_kana.to_owned(),
            town_kana.to_owned(),
            prefecture.to_owned(),
            city.to_owned(),
            town.to_owned(),
            is_一つの町域が二つ以上の郵便番号で表示される,
            is_小字毎に番地が起番されている町域,
            is_丁目を有する町域,
            is_一つの郵便番号で二以上の町域を表す,
            match is_change {
                0 => postal_code_record::Changed::NoChange,
                1 => postal_code_record::Changed::Changed,
                2 => postal_code_record::Changed::Deleted,
                _ => panic!("Unknown Changed value: {}", is_change),
            },
        );
        postal_code_record_list.push(postal_code_record);
    }

    //townとtown_kanaに対して、正規化を行う
    record_normalize(&mut postal_code_record_list);

    // ハッシュを撮る前準備として安定した並び順にするために
    // 住所レコードリストをnational_local_government_codeとpostal_codeでソートする
    postal_code_record_list.sort_by(|a, b| {
        let national_local_government_code_cmp = a
            .national_local_government_code
            .cmp(&b.national_local_government_code);
        if national_local_government_code_cmp != std::cmp::Ordering::Equal {
            return national_local_government_code_cmp;
        }

        a.postal_code.cmp(&b.postal_code)
    });

    // 住所レコードリストをnational_local_government_codeでグルーピングする
    let mut grouped_postal_code_record_list =
        HashMap::<String, Vec<postal_code_record::PostalCodeRecord>>::new();
    for record in postal_code_record_list {
        grouped_postal_code_record_list
            .entry(record.national_local_government_code.clone())
            .or_insert_with(|| Vec::new())
            .push(record);
    }

    //national_local_government_codeとハッシュ値のペアを格納するリスト
    let mut national_local_government_code_to_hash = HashMap::<String, String>::new();

    //コンテンツ全体に対するハッシュ計算用インスタンス
    let mut all_content_hasher = Sha256::new();
    //national_local_government_codeごとのハッシュ計算用インスタンス
    let mut national_local_government_code_hasher = Sha256::new();
    for (national_local_government_code, records) in &grouped_postal_code_record_list {
        for record in records {
            //コンテンツの内容をハッシュに反映
            record.hasher_add(&mut all_content_hasher);
            record.hasher_add(&mut national_local_government_code_hasher);
        }
        let hash = national_local_government_code_hasher.finalize_fixed_reset();
        // ハッシュ値をbase64に変換
        let encoded_hash = general_purpose::STANDARD_NO_PAD.encode(hash);

        // マップに保存
        national_local_government_code_to_hash
            .insert(national_local_government_code.to_owned(), encoded_hash);
    }

    let hash = all_content_hasher.finalize();
    // ハッシュ値をbase64に変換
    let encoded_hash = general_purpose::STANDARD_NO_PAD.encode(hash);

    return KenAllData {
        all_contents_hash: encoded_hash,
        grouped_postal_code_record_list: grouped_postal_code_record_list,
        national_local_government_code_to_hash: national_local_government_code_to_hash,
    };
}

fn download_ken_all_zip() -> Option<Response> {
    let url = "https://www.post.japanpost.jp/zipcode/dl/kogaki/zip/ken_all.zip";

    match reqwest::blocking::get(url) {
        Ok(response) => {
            if response.status() == reqwest::StatusCode::OK {
                return Some(response);
            } else {
                //TODO ログ出力
                return None;
            }
        }
        Err(e) => {
            //TODO ログ出力
            return None;
        }
    }
}

fn response_to_cursor(response: &mut Response) -> Option<Cursor<Vec<u8>>> {
    let mut buf = Vec::new();
    match response.copy_to(&mut buf) {
        Ok(_) => {
            let mut cursor = Cursor::new(buf);
            cursor.seek(std::io::SeekFrom::Start(0)).unwrap();

            return Some(cursor);
        }
        Err(e) => {
            //TODO ログ出力
            return None;
        }
    }
}

fn zip_to_file_contents<R: Read + std::io::Seek>(cursor: R) -> Option<String> {
    let mut archive = ZipArchive::new(cursor).expect("Unable to open the zip file");

    // Zipファイルに一つだけファイルが含まれているはずなので、そのファイルを取得
    let mut zip_file = archive.by_index(0).expect("Unable to open the file");

    // Zipファイルの内容を解凍してバイト配列に格納
    let mut contents = Vec::new();
    zip_file
        .read_to_end(&mut contents)
        .expect("Unable to read the file");

    //文字コードがShift-JISになっているので、UTF-8に変換
    let (decoded_contents, _, _) = SHIFT_JIS.decode(&contents);

    return Some(decoded_contents.to_string());
}

fn record_normalize(postal_code_record_list: &mut Vec<postal_code_record::PostalCodeRecord>) {
    //（...）にマッチに正規表現
    let zenkaku_bracket_regexp = Regex::new(r"（.*?）").unwrap();
    let hankaku_bracket_regexp = Regex::new(r"\(.*?\)").unwrap();

    //分割行を統合中であればtrueになる
    let mut integrated = false;
    for record in postal_code_record_list.iter_mut() {
        if !integrated {
            // townが「以下に掲載がない場合」であれば不要な情報なのでクリア
            if record.town == "以下に掲載がない場合" {
                record.town_kana = "".to_string();
                record.town = "".to_string();
            }
            // townに「の次に番地が来る場合」が含まれていれば、不要な情報なのでクリア
            if record.town.contains("の次に番地が来る場合") {
                record.town_kana = "".to_string();
                record.town = "".to_string();
            }
            // townが「一円」と完全一致せず、「一円」を含むであれば、不要な情報なのでクリア
            if record.town != "一円" && record.town.contains("一円") {
                record.town_kana = "".to_string();
                record.town = "".to_string();
            }

            //TODO: 地割に関する処理を行っていないに関する正規化処理は行っていない
            // カンマ区切りで複数の町名が入っている、
            // ～ で越中畑６４地割～越中畑６６地割のような町名がある

            // townとtown_kanaに含まれる（から）までの文字列をすべて削除する
            record.town_kana = hankaku_bracket_regexp
                .replace_all(&record.town_kana, "")
                .to_string();
            record.town = zenkaku_bracket_regexp
                .replace_all(&record.town, "")
                .to_string();

            // townが「（」を含んでいるなら
            if let Some(start_index) = record.town.find("（") {
                //開きカッコより後ろのの文字列を削除する
                record.town = record.town[..start_index].to_string();
                //半角のtownに対しても同じ処理を適用する
                if let Some(hankaku_start_index) = record.town_kana.find("(") {
                    //開きカッコより後ろのの文字列を削除する
                    record.town_kana = record.town_kana[..hankaku_start_index].to_string();
                }

                //閉じカッコを含まない場合は、複数行にデータがまたがっているため閉じカッコが見つかるまで統合する
                integrated = true;
            }
        } else {
            //分割された行の統合中であれば不要な行なので統合フラグを立てておく
            record.integrated = true;

            // 閉じカッコを含んでいるか？
            if record.town.contains("）") {
                //統合処理終了
                integrated = false;
            }
        }
    }

    //Vecの中から統合されたレコードを削除する
    postal_code_record_list.retain(|record| !record.integrated);
}

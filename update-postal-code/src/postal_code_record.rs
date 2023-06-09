use std::hash::{Hash, Hasher};

use digest::Update;
use sha2::Sha256;

#[derive(Debug)]
pub enum Changed {
    NoChange,
    Changed,
    Deleted,
}

#[derive(Debug)]
pub struct PostalCodeRecord {
    pub national_local_government_code: String,
    pub postal_code: String,
    pub prefecture_kana: String,
    pub city_kana: String,
    pub town_kana: String,
    pub prefecture: String,
    pub city: String,
    pub town: String,
    pub is_一つの町域が二つ以上の郵便番号で表示される: bool,
    pub is_小字毎に番地が起番されている町域: bool,
    pub is_丁目を有する町域: bool,
    pub is_一つの郵便番号で二以上の町域を表す: bool,
    pub is_change: Changed,
    pub integrated: bool,
}

impl PostalCodeRecord {
    pub fn new(
        national_local_government_code: String,
        postal_code: String,
        prefecture_kana: String,
        city_kana: String,
        town_kana: String,
        prefecture: String,
        city: String,
        town: String,
        is_一つの町域が二つ以上の郵便番号で表示される: bool,
        is_小字毎に番地が起番されている町域: bool,
        is_丁目を有する町域: bool,
        is_一つの郵便番号で二以上の町域を表す: bool,
        is_change: Changed,
    ) -> PostalCodeRecord {
        PostalCodeRecord {
            national_local_government_code,
            postal_code,
            prefecture_kana,
            city_kana,
            town_kana,
            prefecture,
            city,
            town,
            is_一つの町域が二つ以上の郵便番号で表示される,
            is_小字毎に番地が起番されている町域,
            is_丁目を有する町域,
            is_一つの郵便番号で二以上の町域を表す,
            is_change,
            integrated: false,
        }
    }

    pub fn hasher_add(&self, hasher: &mut Sha256) {
        hasher.update(self.national_local_government_code.as_bytes());
        hasher.update(self.postal_code.as_bytes());
        hasher.update(self.prefecture_kana.as_bytes());
        hasher.update(self.city_kana.as_bytes());
        hasher.update(self.town_kana.as_bytes());
        hasher.update(self.prefecture.as_bytes());
        hasher.update(self.city.as_bytes());
        hasher.update(self.town.as_bytes());
    }

    #[allow(dead_code)]
    pub fn to_csv_record(&self) -> String {
        let separator = ",";
        //フィールドの内容をカンマ区切りの文字列に変換する。文字列は全てダブルクオーテーションで囲む
        let mut record = String::new();
        record.push_str(&format!("\"{}\"", self.national_local_government_code));
        record.push_str(separator);

        record.push_str(&format!("\"{}\"", self.postal_code));
        record.push_str(separator);

        record.push_str(&format!("\"{}\"", self.prefecture_kana));
        record.push_str(separator);

        record.push_str(&format!("\"{}\"", self.city_kana));
        record.push_str(separator);

        record.push_str(&format!("\"{}\"", self.town_kana));
        record.push_str(separator);

        record.push_str(&format!("\"{}\"", self.prefecture));
        record.push_str(separator);

        record.push_str(&format!("\"{}\"", self.city));
        record.push_str(separator);

        record.push_str(&format!("\"{}\"", self.town));
        record.push_str(separator);

        record.push_str(&format!(
            "\"{}\"",
            self.is_一つの町域が二つ以上の郵便番号で表示される
        ));
        record.push_str(separator);

        record.push_str(&format!("\"{}\"", self.is_小字毎に番地が起番されている町域));
        record.push_str(separator);

        record.push_str(&format!("\"{}\"", self.is_丁目を有する町域));
        record.push_str(separator);

        record.push_str(&format!(
            "\"{}\"",
            self.is_一つの郵便番号で二以上の町域を表す
        ));
        record.push_str(separator);

        return record;
    }
}

# README.md

環境構築

```sql
CREATE TABLE dataset001.item001 (
id STRING,
name STRING,
ownerId STRING
);

CREATE TABLE dataset001.user001 (
id STRING,
name STRING
);

CREATE TABLE dataset001.result001 (
itemId STRING,
itemName STRING,
ownerId STRING,
ownerName STRING
);

INSERT INTO dataset001.item001 (id, name, ownerId) VALUES ("0", "item00", "0");
INSERT INTO dataset001.item001 (id, name, ownerId) VALUES ("1", "item01", "1");
INSERT INTO dataset001.item001 (id, name, ownerId) VALUES ("2", "item02", "0");
INSERT INTO dataset001.item001 (id, name, ownerId) VALUES ("3", "item03", "1");
INSERT INTO dataset001.item001 (id, name, ownerId) VALUES ("4", "item04", "0");

INSERT INTO dataset001.user001 (id, name) VALUES ("0", "Admin");
INSERT INTO dataset001.user001 (id, name) VALUES ("1", "User");
```

テンプレートクラス

- src/main/java/com/google/cloud/teleport/templates/BigQueryToBigQuery.java

テンプレートのコンパイル、GCSへのアップロード

```
mvn compile exec:java \
-Dexec.mainClass=com.google.cloud.teleport.templates.BigQueryToBigQuery \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=dataflow-sandbox-261206 \
--stagingLocation=gs://dataflow-sandbox-001/staging \
--tempLocation=gs://dataflow-sandbox-001/temp \
--templateLocation=gs://dataflow-sandbox-001/templates/BigQueryToBigQuery.json \
--runner=DataflowRunner"
```

BigQueryの出力先テーブルのスキーマ定義をGCSへアップロード
- gs://dataflow-sandbox-001/result001_schema.json

dataflowジョブの実行

- outputTable : 出力先テーブル名
- JSONPath : 出力先テーブルのスキーマ定義jsonが格納されているGCSのパス
- bigQueryLoadingTemporaryDirectory : BigQueryへのデータロードで使用する一時ディレクトリのGCSパス

```
gcloud dataflow jobs run test06 --gcs-location=gs://dataflow-sandbox-001/templates/BigQueryToBigQuery.json --region=asia-northeast1 --parameters="outputTable=dataflow-sandbox-261206:dataset001.result001,JSONPath=gs://dataflow-sandbox-001/result001_schema.json,bigQueryLoadingTemporaryDirectory=gs://dataflow-sandbox-001/temp"
```

```
mvn compile exec:java \
-Dexec.mainClass=com.google.cloud.teleport.templates.BigQueryRunQuery \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=dataflow-sandbox-261206 \
--stagingLocation=gs://dataflow-sandbox-001/staging \
--tempLocation=gs://dataflow-sandbox-001/temp \
--templateLocation=gs://dataflow-sandbox-001/templates/BigQueryRunQuery.json \
--runner=DataflowRunner"
```

```
gcloud dataflow jobs run test09 --gcs-location=gs://dataflow-sandbox-001/templates/BigQueryRunQuery.json --region=asia-northeast1 --parameters="bigQueryLoadingTemporaryDirectory=gs://dataflow-sandbox-001/temp"
```
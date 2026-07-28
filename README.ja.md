<!-- i18n: language-switcher -->
[English](README.md) | [日本語](README.ja.md)

# S3 Tables コネクタ

S3 Tables 用のネイティブ Irodori Table コネクタ拡張。

このクレートは、Irodori 拡張マーケットプレイスで使用されるコネクタのメタデータ、ネイティブ ABI エクスポート、およびドライバー実装をパッケージ化しています。

## コネクタ

- 拡張 ID: `irodori.s3-tables`
- エンジン ID: `s3Tables`
- ワイヤプロトコル: `lakehouse`
- デフォルトポート: `443`
- ネイティブ ABI: `irodori.connector.native.v1`
- ドライバー連携: `あり`
- マーケットプレイス公開範囲: `公開`
- パッケージバージョン: `0.1.3`

このパッケージはコネクタのメタデータとネイティブドライバーを直接使用し、デスクトップアダプターのソーススナップショットは不要です。

コネクタメタデータは `connector.config.json` と `irodori.extension.json` にあります。
Rust クレートは `src/lib.rs` からネイティブ ABI をエクスポートし、共有 JSON/バッファヘルパーに `irodori-connector-abi` を使用し、コネクタの動作は `src/driver.rs` に保持しています。

## 接続メタデータ

- エンドポイントモード: `cloudResource`, `customEndpoint`, `connectionString`
- トランスポートモード: `direct`, `sshTunnel`, `socks5Proxy`, `httpConnectProxy`, `proxyChain`
- TLS 対応: `あり`
- デフォルトで TLS 必須: `あり`
- カスタムドライバーオプション: `あり`

### エンドポイントフィールド

| フィールド | ラベル | 型 | 必須 |
| --- | --- | --- | --- |
| `region` | AWS リージョン | `string` | 必須 |
| `catalogType` | カタログタイプ | `string` | 必須 |
| `tableBucketArn` | テーブルバケット ARN | `string` | 必須 |
| `namespace` | ネームスペース | `string` | 任意 |
| `tableName` | テーブル | `string` | 任意 |
| `accessModel` | アクセスモデル | `string` | 任意 |
| `credentialVending` | クレデンシャルベンディング | `boolean` | 任意 |
| `endpoint` | カスタムエンドポイント | `uri` | 任意 |

## 認証

コネクタはこれらの認証モードを公開しており、クライアントは適切な認証情報フィールドを表示できます。
ドライバー固有またはプロバイダー固有の値は必要に応じて `options` 経由で渡すことも可能です。

| 認証方法 | ラベル | 種類 | 秘密情報の用途 |
| --- | --- | --- | --- |
| `connectionString` | 接続文字列 / DSN | `connectionString` | なし |
| `awsDefaultCredentialsChain` | AWS デフォルト認証情報チェーン | `iam` | なし |
| `awsSigV4` | AWS SigV4 | `iam` | `token` |
| `awsProfile` | AWS 共有設定プロファイル | `iam` | なし |
| `awsSso` | AWS IAM Identity Center / SSO | `iam` | `token` |
| `webIdentity` | AWS ウェブアイデンティティ | `iam` | `token` |
| `awsAssumeRole` | AWS STS ロール引き受け | `iam` | `token` |
| `sessionToken` | AWS セッショントークン | `token` | `token` |
| `customDriverOptions` | カスタムドライバーオプション | `custom` | `password`, `token`, `privateKey`, `privateKeyPassphrase` |

## ネイティブ ABI 呼び出し

| メソッド | レスポンス |
| --- | --- |
| `health` | コネクタのヘルス、エンジン ID、ABI バージョン、ドライバー状態を返します。 |
| `describe` | 埋め込みマニフェストとコネクタ設定を返します。 |
| `manifest` | 生の `irodori.extension.json` を返します。 |
| `config` | 生の `connector.config.json` を返します。 |
| `connect` | ネイティブコネクタ接続を開き、検証します。 |
| `query` | コネクタクエリを実行し、構造化された行または JSON 結果を返します。 |
| `metadata` | スキーマ、テーブル、カラム、インデックス、コレクション、または同等のメタデータを読み取ります。 |
| `close` | キャッシュされたネイティブ接続を閉じて削除します。 |

## 開発

このチェックアウト内のすべての拡張クレートは `../target` を共有しているため、依存関係は兄弟リポジトリ間で一度だけコンパイルされます。

```sh
make check
make build
```

リリースパッケージはプラットフォーム固有のネイティブアーティファクトを `dist/native` に配置します。

## ライセンス

0BSD。ほぼあらゆる目的でこのプロジェクトを使用、コピー、修正、配布できます。
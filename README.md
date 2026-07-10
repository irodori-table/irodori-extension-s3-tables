# S3 Tables Connector

Native Irodori Table connector extension for S3 Tables.

This crate packages the connector metadata, native ABI exports, and driver implementation used by the Irodori extension marketplace.

## Connector

- Extension ID: `irodori.s3-tables`
- Engine ID: `s3Tables`
- Wire protocol: `lakehouse`
- Default port: `443`
- Native ABI: `irodori.connector.native.v1`
- Driver linked: `yes`
- Marketplace visibility: `public`
- Package version: `0.1.3`

The package uses the connector metadata and native driver directly; no desktop adapter source snapshot is required.

Connector metadata lives in `connector.config.json` and `irodori.extension.json`.
The Rust crate exports the native ABI from `src/lib.rs`, uses `irodori-connector-abi` for shared JSON/buffer helpers, and keeps connector behavior in `src/driver.rs`.

## Connection Metadata

- Endpoint modes: `cloudResource`, `customEndpoint`, `connectionString`
- Transport modes: `direct`, `sshTunnel`, `socks5Proxy`, `httpConnectProxy`, `proxyChain`
- TLS supported: `yes`
- TLS required by default: `yes`
- Custom driver options: `yes`

### Endpoint Fields

| Field | Label | Type | Required |
| --- | --- | --- | --- |
| `region` | AWS region | `string` | yes |
| `catalogType` | Catalog type | `string` | yes |
| `tableBucketArn` | Table bucket ARN | `string` | yes |
| `namespace` | Namespace | `string` | no |
| `tableName` | Table | `string` | no |
| `accessModel` | Access model | `string` | no |
| `credentialVending` | Credential vending | `boolean` | no |
| `endpoint` | Custom endpoint | `uri` | no |

## Authentication

The connector advertises these authentication modes so clients can render the right credential fields. Driver-specific or provider-specific values can still be passed through `options` when needed.

| Auth method | Label | Kind | Secret purposes |
| --- | --- | --- | --- |
| `connectionString` | Connection string / DSN | `connectionString` | none |
| `awsDefaultCredentialsChain` | AWS default credential chain | `iam` | none |
| `awsSigV4` | AWS SigV4 | `iam` | `token` |
| `awsProfile` | AWS shared config profile | `iam` | none |
| `awsSso` | AWS IAM Identity Center / SSO | `iam` | `token` |
| `webIdentity` | AWS web identity | `iam` | `token` |
| `awsAssumeRole` | AWS STS assume role | `iam` | `token` |
| `sessionToken` | AWS session token | `token` | `token` |
| `customDriverOptions` | Custom driver options | `custom` | `password`, `token`, `privateKey`, `privateKeyPassphrase` |

## Native ABI Calls

| Method | Response |
| --- | --- |
| `health` | Returns connector health, engine id, ABI version, and driver status. |
| `describe` | Returns the embedded manifest and connector config. |
| `manifest` | Returns raw `irodori.extension.json`. |
| `config` | Returns raw `connector.config.json`. |
| `connect` | Opens and validates a native connector connection. |
| `query` | Runs a connector query and returns structured rows or JSON results. |
| `metadata` | Reads schemas, tables, columns, indexes, collections, or equivalent metadata. |
| `close` | Closes and removes a cached native connection. |

## Development

All extension crates in this checkout share `../target` so dependencies compile once across sibling repositories.

```sh
make check
make build
```

Release packages place platform-specific native artifacts under `dist/native`.

## License

0BSD. You can use, copy, modify, and distribute this project for almost any purpose.

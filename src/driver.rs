use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};

use serde_json::{json, Map, Value};

use crate::abi::{self, IrodoriConnectorBuffer};
use crate::{ABI_VERSION, CONFIG_JSON, DRIVER_LINKED, ENGINE, MANIFEST_JSON};

static CONNECTIONS: OnceLock<Mutex<HashMap<String, LakehouseConnection>>> = OnceLock::new();

struct LakehouseConnection {
    conn: duckdb::Connection,
    redaction_values: Vec<String>,
}

#[derive(Default)]
struct ObjectMeta {
    schema: String,
    name: String,
    kind: String,
    columns: Vec<Value>,
}

type QueryRows = Vec<Vec<Value>>;
type QueryOutput = (Vec<String>, QueryRows, bool);

fn connections() -> &'static Mutex<HashMap<String, LakehouseConnection>> {
    CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn call_json(request: IrodoriConnectorBuffer) -> IrodoriConnectorBuffer {
    let request = match abi::parse_request(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let method = match abi::request_method(request.as_ref()) {
        Ok(method) => method,
        Err(response) => return response,
    };

    match method {
        "health" | "ping" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        ])),
        "describe" | "capabilities" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
            (
                "manifest".to_string(),
                serde_json::from_str(MANIFEST_JSON).unwrap_or(Value::Null),
            ),
            (
                "config".to_string(),
                serde_json::from_str(CONFIG_JSON).unwrap_or(Value::Null),
            ),
        ])),
        "manifest" => abi::owned_buffer(MANIFEST_JSON.to_string()),
        "config" => abi::owned_buffer(CONFIG_JSON.to_string()),
        "connect" => connect(request.as_ref().expect("connect has request")),
        "query" => query(request.as_ref().expect("query has request")),
        "metadata" => metadata(request.as_ref().expect("metadata has request")),
        "close" => close(request.as_ref().expect("close has request")),
        other => abi::error(
            "connector.unknownMethod",
            format!("unknown connector method: {other}"),
        ),
    }
}

fn connect(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let conn = match duckdb::Connection::open_in_memory() {
        Ok(conn) => conn,
        Err(err) => return abi::error("connector.connectFailed", format!("connect failed: {err}")),
    };
    let redaction_values = redaction_values(request);
    if let Err(err) = configure_connection(&conn, request) {
        return abi::error("connector.connectFailed", redact(&redaction_values, &err));
    }
    let server_version = conn
        .query_row("select version()", [], |row| row.get::<_, String>(0))
        .unwrap_or_else(|_| "DuckDB lakehouse runtime".to_string());
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    guard.insert(
        connection_id.clone(),
        LakehouseConnection {
            conn,
            redaction_values,
        },
    );
    abi::ok(Map::from_iter([
        ("engine".to_string(), Value::String(ENGINE.to_string())),
        ("connectionId".to_string(), Value::String(connection_id)),
        ("serverVersion".to_string(), Value::String(server_version)),
        ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
    ]))
}

fn query(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let Some(sql) = abi::string_field(request, "sql")
        .or_else(|| abi::string_field(request, "query"))
        .or_else(|| abi::string_field(request, "statement"))
    else {
        return abi::error(
            "connector.invalidRequest",
            "query requires a string sql, query, or statement field.",
        );
    };
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let Some(connection) = guard.get_mut(&connection_id) else {
        return abi::error(
            "connector.connectionNotFound",
            format!("no open connection: {connection_id}"),
        );
    };
    match run_query(&connection.conn, sql, abi::max_rows(request)) {
        Ok((columns, rows, truncated)) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            (
                "columns".to_string(),
                Value::Array(columns.into_iter().map(Value::String).collect()),
            ),
            (
                "rows".to_string(),
                Value::Array(rows.into_iter().map(Value::Array).collect()),
            ),
            ("truncated".to_string(), Value::Bool(truncated)),
        ])),
        Err(err) => abi::error(
            "connector.queryFailed",
            redact(&connection.redaction_values, &err),
        ),
    }
}

fn metadata(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let Some(connection) = guard.get_mut(&connection_id) else {
        return abi::error(
            "connector.connectionNotFound",
            format!("no open connection: {connection_id}"),
        );
    };
    match load_metadata(&connection.conn) {
        Ok(metadata) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            ("metadata".to_string(), metadata),
        ])),
        Err(err) => abi::error(
            "connector.metadataFailed",
            redact(&connection.redaction_values, &err),
        ),
    }
}

fn close(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let closed = match connections().lock() {
        Ok(mut guard) => guard.remove(&connection_id).is_some(),
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    abi::ok(Map::from_iter([
        ("connectionId".to_string(), Value::String(connection_id)),
        ("closed".to_string(), Value::Bool(closed)),
    ]))
}

fn configure_connection(conn: &duckdb::Connection, request: &Value) -> Result<(), String> {
    apply_settings(conn, request)?;
    let Some(path) = option_string(
        request,
        &[
            "tablePath",
            "path",
            "location",
            "uri",
            "url",
            "connectionString",
        ],
    )
    .or_else(|| abi::profile_field(request, "database").map(str::to_string)) else {
        return Ok(());
    };
    let view = clean_identifier(
        &option_string(request, &["table", "tableName", "view", "viewName"])
            .unwrap_or_else(|| "lakehouse_table".to_string()),
    );
    let escaped_path = sql_string(&path);
    let sql = match ENGINE {
        "deltaLake" => {
            load_extension(conn, "httpfs", false)?;
            load_extension(conn, "delta", true)?;
            format!("create or replace view {view} as select * from delta_scan({escaped_path})")
        }
        "iceberg" | "s3Tables" => {
            load_extension(conn, "httpfs", false)?;
            load_extension(conn, "iceberg", true)?;
            format!("create or replace view {view} as select * from iceberg_scan({escaped_path})")
        }
        "hudi" | "hive" => {
            load_extension(conn, "httpfs", false)?;
            let pattern = parquet_pattern(&path);
            format!(
                "create or replace view {view} as select * from read_parquet({}, hive_partitioning=true, union_by_name=true)",
                sql_string(&pattern)
            )
        }
        _ => return Ok(()),
    };
    conn.execute_batch(&sql)
        .map_err(|err| format!("lakehouse table view creation failed: {err}"))?;
    Ok(())
}

/// The desktop connection form gives this engine two credential boxes labelled
/// "AWS profile / access key" and "Secret / session token", so a profile filled
/// in through the UI arrives with `user`/`password` rather than the explicit
/// option names. `password` is unambiguous — it is the secret access key.
/// `user` is not, so disambiguate it by shape: an access key id is 20 uppercase
/// alphanumerics beginning with `A` (`AKIA…` long-term, `ASIA…` temporary).
/// Anything else is a profile name and is not a credential.
fn looks_like_access_key_id(value: &str) -> bool {
    value.len() == 20
        && value.starts_with('A')
        && value
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

fn form_access_key_id(request: &Value) -> Option<String> {
    option_string(request, &["user", "username"]).filter(|value| looks_like_access_key_id(value))
}

/// Resolve each DuckDB S3 setting from the first candidate key that is present.
/// The explicit option names come first, so a profile that carries both an
/// explicit `accessKeyId` and a leftover `user` uses the explicit one.
fn setting_value(request: &Value, setting: &str, fields: &[&str]) -> Option<String> {
    option_string(request, fields).or_else(|| match setting {
        "s3_access_key_id" => form_access_key_id(request),
        "s3_secret_access_key" => option_string(request, &["password"]),
        _ => None,
    })
}

fn apply_settings(conn: &duckdb::Connection, request: &Value) -> Result<(), String> {
    for statement in secret_statements(request) {
        conn.execute_batch(&statement)
            .map_err(|err| format!("DuckDB credential setup failed: {err}"))?;
    }
    for (setting, fields) in [
        ("s3_region", &["s3Region", "region"][..]),
        ("s3_endpoint", &["s3Endpoint"][..]),
        ("s3_url_style", &["s3UrlStyle"][..]),
        ("s3_access_key_id", &["s3AccessKeyId", "accessKeyId"][..]),
        (
            "s3_secret_access_key",
            &["s3SecretAccessKey", "secretAccessKey"][..],
        ),
        ("s3_session_token", &["s3SessionToken", "sessionToken"][..]),
    ] {
        if let Some(value) = setting_value(request, setting, fields) {
            let sql = format!("set {setting} = {}", sql_string(&value));
            conn.execute_batch(&sql)
                .map_err(|err| format!("DuckDB setting {setting} failed: {err}"))?;
        }
    }
    Ok(())
}

fn load_extension(
    conn: &duckdb::Connection,
    extension: &str,
    required: bool,
) -> Result<(), String> {
    let install = format!("install {extension};");
    let load = format!("load {extension};");
    let install_result = conn.execute_batch(&install);
    let load_result = conn.execute_batch(&load);
    if required {
        load_result
            .or(install_result)
            .map_err(|err| format!("DuckDB extension {extension} unavailable: {err}"))?;
    }
    Ok(())
}

fn run_query(conn: &duckdb::Connection, sql: &str, cap: usize) -> Result<QueryOutput, String> {
    let lead = sql.trim_start().to_ascii_lowercase();
    let is_query = [
        "select", "with", "show", "pragma", "explain", "describe", "values", "table", "call",
    ]
    .iter()
    .any(|keyword| lead.starts_with(keyword));
    if !is_query {
        conn.execute(sql, [])
            .map_err(|err| format!("query failed: {err}"))?;
        return Ok((Vec::new(), Vec::new(), false));
    }

    let mut stmt = conn
        .prepare(sql)
        .map_err(|err| format!("query failed: {err}"))?;
    let mut duck_rows = stmt
        .query([])
        .map_err(|err| format!("query failed: {err}"))?;
    let columns = duck_rows
        .as_ref()
        .map(|stmt| {
            stmt.column_names()
                .iter()
                .map(|column| column.to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let column_count = columns.len();
    let mut rows = Vec::new();
    let mut truncated = false;
    while let Some(row) = duck_rows
        .next()
        .map_err(|err| format!("query failed: {err}"))?
    {
        if rows.len() >= cap {
            truncated = true;
            break;
        }
        rows.push(
            (0..column_count)
                .map(|index| cell_to_json(row, index))
                .collect(),
        );
    }
    Ok((columns, rows, truncated))
}

fn load_metadata(conn: &duckdb::Connection) -> Result<Value, String> {
    let mut objects = BTreeMap::<(String, String), ObjectMeta>::new();
    let mut stmt = conn
        .prepare(
            "select table_schema, table_name, table_type \
             from information_schema.tables \
             where table_schema not in ('information_schema', 'pg_catalog') \
             order by table_schema, table_name",
        )
        .map_err(|err| format!("metadata objects failed: {err}"))?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|err| format!("metadata objects failed: {err}"))?;
    for row in rows {
        let (schema, name, table_type) =
            row.map_err(|err| format!("metadata objects failed: {err}"))?;
        objects.insert(
            (schema.clone(), name.clone()),
            ObjectMeta {
                schema,
                name,
                kind: if table_type.eq_ignore_ascii_case("VIEW") {
                    "view".to_string()
                } else {
                    "table".to_string()
                },
                columns: Vec::new(),
            },
        );
    }

    let mut stmt = conn
        .prepare(
            "select table_schema, table_name, column_name, data_type, is_nullable, ordinal_position \
             from information_schema.columns \
             where table_schema not in ('information_schema', 'pg_catalog') \
             order by table_schema, table_name, ordinal_position",
        )
        .map_err(|err| format!("metadata columns failed: {err}"))?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, i32>(5)?,
            ))
        })
        .map_err(|err| format!("metadata columns failed: {err}"))?;
    for row in rows {
        let (schema, table, name, data_type, nullable, ordinal) =
            row.map_err(|err| format!("metadata columns failed: {err}"))?;
        if let Some(object) = objects.get_mut(&(schema, table)) {
            object.columns.push(json!({
                "name": name,
                "dataType": data_type,
                "nullable": nullable.eq_ignore_ascii_case("YES"),
                "ordinal": ordinal
            }));
        }
    }

    let mut schemas = BTreeMap::<String, Vec<Value>>::new();
    for object in objects.into_values() {
        schemas
            .entry(object.schema.clone())
            .or_default()
            .push(json!({
                "schema": object.schema,
                "name": object.name,
                "kind": object.kind,
                "columns": object.columns,
                "indexes": [],
                "primaryKey": [],
                "foreignKeys": []
            }));
    }
    Ok(json!({
        "schemas": schemas
            .into_iter()
            .map(|(name, objects)| json!({ "name": name, "objects": objects }))
            .collect::<Vec<_>>()
    }))
}

fn cell_to_json(row: &duckdb::Row, index: usize) -> Value {
    use duckdb::types::Value as DuckValue;
    match row.get::<usize, DuckValue>(index) {
        Ok(DuckValue::Null) => Value::Null,
        Ok(DuckValue::Boolean(value)) => Value::Bool(value),
        Ok(DuckValue::TinyInt(value)) => json!(value),
        Ok(DuckValue::SmallInt(value)) => json!(value),
        Ok(DuckValue::Int(value)) => json!(value),
        Ok(DuckValue::BigInt(value)) => json!(value),
        Ok(DuckValue::UTinyInt(value)) => json!(value),
        Ok(DuckValue::USmallInt(value)) => json!(value),
        Ok(DuckValue::UInt(value)) => json!(value),
        Ok(DuckValue::UBigInt(value)) => json!(value),
        Ok(DuckValue::Float(value)) => json!(value as f64),
        Ok(DuckValue::Double(value)) => json!(value),
        Ok(DuckValue::Text(value)) => Value::String(value),
        Ok(DuckValue::Blob(value)) => Value::String(format!("\\x{}", hex_encode(&value))),
        Ok(other) => Value::String(format!("{other:?}")),
        Err(_) => Value::Null,
    }
}

fn parquet_pattern(path: &str) -> String {
    if path.contains('*') || path.ends_with(".parquet") {
        path.to_string()
    } else {
        format!("{}/**/*.parquet", path.trim_end_matches('/'))
    }
}

fn clean_identifier(value: &str) -> String {
    let mut out = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if out.is_empty() {
        out = "lakehouse_table".to_string();
    }
    if out.chars().next().is_some_and(|ch| ch.is_ascii_digit()) {
        out.insert(0, '_');
    }
    out
}

/// The object-store credentials a profile implies, as DuckDB `CREATE SECRET`
/// statements.
///
/// The connector used to forward a handful of `SET s3_*` settings, which covers
/// exactly one of the credential sources `connector.config.json` declares:
/// a static access key. DuckDB's secret manager is the surface for the rest —
/// the AWS credential chain (and with it SSO and web identity), and the Azure
/// providers.
///
/// Pure on purpose. The statements are the whole behaviour worth testing, and
/// asserting on them needs no DuckDB, no network, and no credentials.
fn secret_statements(request: &Value) -> Vec<String> {
    let mut statements = Vec::new();
    if let Some(s3) = s3_secret(request) {
        statements.push(s3);
    }
    if let Some(azure) = azure_secret(request) {
        statements.push(azure);
    }
    statements
}

/// `CREATE SECRET` for S3-compatible storage, or `None` when the profile names
/// no S3 credential source and DuckDB's own defaults should stand.
fn s3_secret(request: &Value) -> Option<String> {
    let mut params: Vec<(&str, String)> = Vec::new();

    let key_id = option_string(
        request,
        &["s3AccessKeyId", "accessKeyId", "awsAccessKeyId", "user"],
    )
    .filter(|value| looks_like_access_key_id(value));
    let secret = option_string(
        request,
        &[
            "s3SecretAccessKey",
            "secretAccessKey",
            "awsSecretAccessKey",
            "password",
        ],
    );
    let session_token = option_string(
        request,
        &["s3SessionToken", "sessionToken", "awsSessionToken"],
    );
    let profile = option_string(request, &["awsProfile", "profile"]);
    let chain = option_string(request, &["awsCredentialChain", "credentialChain"]);
    let use_chain = option_string(request, &["awsUseCredentialChain", "useCredentialChain"])
        .is_some_and(|value| matches!(value.trim(), "1" | "true" | "yes"));

    // A static key pair is an explicit instruction; anything else that names a
    // credential source falls through to the chain, which is what carries SSO,
    // web identity, ECS/IMDS, and a profile's own `role_arn`.
    match (key_id, secret) {
        (Some(key_id), Some(secret)) => {
            params.push(("PROVIDER", "config".to_string()));
            params.push(("KEY_ID", sql_string(&key_id)));
            params.push(("SECRET", sql_string(&secret)));
            if let Some(token) = session_token {
                params.push(("SESSION_TOKEN", sql_string(&token)));
            }
        }
        _ if use_chain || profile.is_some() || chain.is_some() => {
            params.push(("PROVIDER", "credential_chain".to_string()));
            if let Some(chain) = chain {
                params.push(("CHAIN", sql_string(&chain)));
            }
            if let Some(profile) = profile {
                params.push(("PROFILE", sql_string(&profile)));
            }
        }
        _ => return None,
    }

    for (option, key) in [
        (["s3Region", "region", "awsRegion"].as_slice(), "REGION"),
        (["s3Endpoint", "endpoint"].as_slice(), "ENDPOINT"),
        (["s3UrlStyle", "urlStyle"].as_slice(), "URL_STYLE"),
    ] {
        if let Some(value) = option_string(request, option) {
            params.push((key, sql_string(&value)));
        }
    }

    Some(create_secret("irodori_s3", "s3", &params))
}

/// `CREATE SECRET` for Azure Blob / ADLS, or `None` when the profile names no
/// Azure credential source.
fn azure_secret(request: &Value) -> Option<String> {
    let mut params = azure_provider_params(request)?;
    if let Some(account) = option_string(request, &["azureAccountName", "accountName"]) {
        params.push(("ACCOUNT_NAME", sql_string(&account)));
    }
    Some(create_secret("irodori_azure", "azure", &params))
}

/// Which Azure provider the profile is asking for, and its parameters.
///
/// Split out so each branch can answer for itself; a profile that names an
/// Azure credential source but cannot complete it (a service principal with
/// neither a secret nor a certificate) answers `None` rather than emitting a
/// secret that cannot authenticate — that would displace whatever DuckDB could
/// otherwise have used and fail later and less clearly.
fn azure_provider_params(request: &Value) -> Option<Vec<(&'static str, String)>> {
    // A SAS token is delivered as a connection string; DuckDB's config provider
    // is the only one that accepts one.
    if let Some(connection_string) = option_string(
        request,
        &["azureConnectionString", "azureSasToken", "sasToken"],
    ) {
        return Some(vec![
            ("PROVIDER", "config".to_string()),
            ("CONNECTION_STRING", sql_string(&connection_string)),
        ]);
    }

    let tenant_id = option_string(request, &["azureTenantId", "tenantId"]);
    let client_id = option_string(request, &["azureClientId", "clientId"]);
    if let (Some(tenant_id), Some(client_id)) = (tenant_id, client_id) {
        // Secret or certificate — a service principal authenticates with one or
        // the other, never neither.
        let credential = match (
            option_string(request, &["azureClientSecret", "clientSecret"]),
            option_string(
                request,
                &["azureClientCertificatePath", "clientCertificatePath"],
            ),
        ) {
            (Some(secret), _) => ("CLIENT_SECRET", sql_string(&secret)),
            (None, Some(path)) => ("CLIENT_CERTIFICATE_PATH", sql_string(&path)),
            (None, None) => return None,
        };
        return Some(vec![
            ("PROVIDER", "service_principal".to_string()),
            ("TENANT_ID", sql_string(&tenant_id)),
            ("CLIENT_ID", sql_string(&client_id)),
            credential,
        ]);
    }

    // `cli`, `managed_identity`, `env`, … — this is what carries Azure AD
    // interactive login and managed identity.
    let chain = option_string(request, &["azureCredentialChain", "azureChain"])?;
    Some(vec![
        ("PROVIDER", "credential_chain".to_string()),
        ("CHAIN", sql_string(&chain)),
    ])
}

fn create_secret(name: &str, kind: &str, params: &[(&str, String)]) -> String {
    let body = params
        .iter()
        .map(|(key, value)| format!("    {key} {value}"))
        .collect::<Vec<_>>()
        .join(",\n");
    format!("create or replace secret {name} (\n    TYPE {kind},\n{body}\n);")
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn option_string(request: &Value, fields: &[&str]) -> Option<String> {
    request_containers(request)
        .into_iter()
        .find_map(|container| {
            fields.iter().find_map(|field| {
                container
                    .get(*field)
                    .map(|value| match value {
                        Value::String(value) => value.clone(),
                        Value::Number(value) => value.to_string(),
                        Value::Bool(value) => value.to_string(),
                        _ => String::new(),
                    })
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
        })
}

fn request_containers(request: &Value) -> Vec<&Value> {
    [
        Some(request),
        request.get("profile"),
        request.get("options"),
        request.get("auth"),
        request.get("secrets"),
        request
            .get("profile")
            .and_then(|profile| profile.get("options")),
        request
            .get("profile")
            .and_then(|profile| profile.get("auth")),
        request
            .get("profile")
            .and_then(|profile| profile.get("secrets")),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn redaction_values(request: &Value) -> Vec<String> {
    let mut values = Vec::new();
    for field in [
        "password",
        "token",
        "accessKeyId",
        "secretAccessKey",
        "s3AccessKeyId",
        "s3SecretAccessKey",
        "sessionToken",
        "s3SessionToken",
    ] {
        if let Some(value) = option_string(request, &[field]) {
            if !values.iter().any(|existing| existing == &value) {
                values.push(value);
            }
        }
    }
    values
}

fn redact(values: &[String], message: &str) -> String {
    values.iter().fold(message.to_string(), |message, secret| {
        if secret.is_empty() {
            message
        } else {
            message.replace(secret, "****")
        }
    })
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_safe_view_names_and_sql_strings() {
        assert_eq!(clean_identifier("1 a-b"), "_1_a_b");
        assert_eq!(sql_string("s3://bucket/a'b"), "'s3://bucket/a''b'");
        assert_eq!(
            parquet_pattern("s3://bucket/table"),
            "s3://bucket/table/**/*.parquet"
        );
    }

    #[test]
    fn takes_s3_credentials_from_the_connection_form_fields() {
        // The connection form labels `user`/`password` "AWS profile / access
        // key" and "Secret / session token", so this is the shape a profile
        // filled in through the UI arrives as.
        let request = json!({
            "profile": {
                "user": "AKIAIOSFODNN7EXAMPLE",
                "password": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
            }
        });
        assert_eq!(
            setting_value(
                &request,
                "s3_access_key_id",
                &["s3AccessKeyId", "accessKeyId"]
            )
            .as_deref(),
            Some("AKIAIOSFODNN7EXAMPLE")
        );
        assert_eq!(
            setting_value(
                &request,
                "s3_secret_access_key",
                &["s3SecretAccessKey", "secretAccessKey"]
            )
            .as_deref(),
            Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
        );
    }

    #[test]
    fn explicit_s3_options_win_over_the_form_fields() {
        let request = json!({
            "profile": {
                "user": "AKIAIOSFODNN7EXAMPLE",
                "password": "from-the-form",
                "options": {
                    "accessKeyId": "AKIAEXPLICITEXPLICIT",
                    "secretAccessKey": "explicit-secret"
                }
            }
        });
        assert_eq!(
            setting_value(
                &request,
                "s3_access_key_id",
                &["s3AccessKeyId", "accessKeyId"]
            )
            .as_deref(),
            Some("AKIAEXPLICITEXPLICIT")
        );
        assert_eq!(
            setting_value(
                &request,
                "s3_secret_access_key",
                &["s3SecretAccessKey", "secretAccessKey"]
            )
            .as_deref(),
            Some("explicit-secret")
        );
    }

    #[test]
    fn a_user_that_is_not_an_access_key_id_is_not_treated_as_a_credential() {
        let request = json!({ "profile": { "user": "staging", "password": "secret" } });
        assert_eq!(
            setting_value(
                &request,
                "s3_access_key_id",
                &["s3AccessKeyId", "accessKeyId"]
            ),
            None
        );
        assert!(looks_like_access_key_id("ASIAIOSFODNN7EXAMPLE"));
        assert!(!looks_like_access_key_id("staging"));
    }

    fn request(options: Value) -> Value {
        json!({ "profile": { "options": options } })
    }

    #[test]
    fn a_profile_with_no_credential_source_emits_no_secret() {
        // DuckDB's own defaults should stand; emitting an empty secret would
        // override them with nothing.
        assert!(secret_statements(&request(json!({}))).is_empty());
        assert!(secret_statements(&request(json!({ "region": "ap-northeast-1" }))).is_empty());
    }

    #[test]
    fn a_static_key_pair_becomes_a_config_secret() {
        let statements = secret_statements(&request(json!({
            "accessKeyId": "AKIAIOSFODNN7EXAMPLE",
            "secretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "sessionToken": "FQoGZXIvYXdzEBYa",
            "region": "ap-northeast-1"
        })));
        assert_eq!(statements.len(), 1);
        let sql = &statements[0];
        assert!(sql.contains("TYPE s3"), "{sql}");
        assert!(sql.contains("PROVIDER config"), "{sql}");
        assert!(sql.contains("KEY_ID 'AKIAIOSFODNN7EXAMPLE'"), "{sql}");
        assert!(sql.contains("SESSION_TOKEN 'FQoGZXIvYXdzEBYa'"), "{sql}");
        assert!(sql.contains("REGION 'ap-northeast-1'"), "{sql}");
    }

    #[test]
    fn a_profile_name_becomes_a_credential_chain_secret() {
        // This is the path that carries SSO, web identity, ECS/IMDS, and a
        // profile's own role_arn — none of which a `SET s3_*` can express.
        let statements = secret_statements(&request(json!({ "awsProfile": "analytics" })));
        assert_eq!(statements.len(), 1);
        let sql = &statements[0];
        assert!(sql.contains("PROVIDER credential_chain"), "{sql}");
        assert!(sql.contains("PROFILE 'analytics'"), "{sql}");
    }

    #[test]
    fn an_explicit_chain_is_passed_through() {
        let statements = secret_statements(&request(json!({
            "awsCredentialChain": "sso;env;instance"
        })));
        assert!(
            statements[0].contains("CHAIN 'sso;env;instance'"),
            "{:?}",
            statements
        );
    }

    #[test]
    fn a_static_key_pair_wins_over_the_chain() {
        // Explicit credentials are an instruction, not a hint.
        let statements = secret_statements(&request(json!({
            "accessKeyId": "AKIAIOSFODNN7EXAMPLE",
            "secretAccessKey": "secret",
            "awsProfile": "analytics"
        })));
        assert!(
            statements[0].contains("PROVIDER config"),
            "{:?}",
            statements
        );
        assert!(
            !statements[0].contains("credential_chain"),
            "{:?}",
            statements
        );
    }

    #[test]
    fn a_user_that_is_not_an_access_key_id_is_not_a_credential() {
        // The form overloads `user` for both a profile name and an access key
        // id; only the access-key shape may become KEY_ID.
        let statements = secret_statements(&request(json!({
            "user": "analytics", "password": "secret"
        })));
        assert!(statements.is_empty(), "{statements:?}");
        assert!(looks_like_access_key_id("ASIAIOSFODNN7EXAMPLE"));
        assert!(!looks_like_access_key_id("analytics"));
    }

    #[test]
    fn an_azure_service_principal_becomes_a_service_principal_secret() {
        let statements = secret_statements(&request(json!({
            "azureTenantId": "tenant",
            "azureClientId": "client",
            "azureClientSecret": "shh",
            "azureAccountName": "lakehouse"
        })));
        let sql = statements.last().expect("an azure secret");
        assert!(sql.contains("TYPE azure"), "{sql}");
        assert!(sql.contains("PROVIDER service_principal"), "{sql}");
        assert!(sql.contains("CLIENT_SECRET 'shh'"), "{sql}");
        assert!(sql.contains("ACCOUNT_NAME 'lakehouse'"), "{sql}");
    }

    #[test]
    fn an_azure_service_principal_can_authenticate_with_a_certificate() {
        let statements = secret_statements(&request(json!({
            "azureTenantId": "tenant",
            "azureClientId": "client",
            "azureClientCertificatePath": "/etc/ssl/sp.pem"
        })));
        let sql = statements.last().expect("an azure secret");
        assert!(
            sql.contains("CLIENT_CERTIFICATE_PATH '/etc/ssl/sp.pem'"),
            "{sql}"
        );
        assert!(!sql.contains("CLIENT_SECRET"), "{sql}");
    }

    #[test]
    fn an_azure_service_principal_without_a_credential_emits_nothing() {
        // Emitting a secret that cannot authenticate would replace whatever
        // DuckDB could otherwise have used, and fail later and less clearly.
        let statements = secret_statements(&request(json!({
            "azureTenantId": "tenant", "azureClientId": "client"
        })));
        assert!(statements.is_empty(), "{statements:?}");
    }

    #[test]
    fn an_azure_sas_token_becomes_a_connection_string_secret() {
        let statements = secret_statements(&request(json!({
            "azureSasToken": "BlobEndpoint=https://x.blob.core.windows.net/;SharedAccessSignature=sv=2022"
        })));
        let sql = statements.last().expect("an azure secret");
        assert!(sql.contains("PROVIDER config"), "{sql}");
        assert!(sql.contains("SharedAccessSignature=sv=2022"), "{sql}");
    }

    #[test]
    fn an_azure_chain_carries_managed_identity_and_cli_login() {
        let statements = secret_statements(&request(json!({
            "azureCredentialChain": "managed_identity;cli"
        })));
        let sql = statements.last().expect("an azure secret");
        assert!(sql.contains("PROVIDER credential_chain"), "{sql}");
        assert!(sql.contains("CHAIN 'managed_identity;cli'"), "{sql}");
    }

    #[test]
    fn s3_and_azure_credentials_coexist() {
        // A table can live in one store and its catalog in another.
        let statements = secret_statements(&request(json!({
            "awsProfile": "analytics",
            "azureCredentialChain": "cli"
        })));
        assert_eq!(statements.len(), 2, "{statements:?}");
        assert!(statements[0].contains("TYPE s3"));
        assert!(statements[1].contains("TYPE azure"));
    }

    #[test]
    fn secret_values_are_quoted_against_injection() {
        let statements = secret_statements(&request(json!({
            "accessKeyId": "AKIAIOSFODNN7EXAMPLE",
            "secretAccessKey": "it's a secret"
        })));
        assert!(
            statements[0].contains("SECRET 'it''s a secret'"),
            "{:?}",
            statements
        );
    }
}

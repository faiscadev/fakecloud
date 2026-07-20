//! AppSync CloudFormation provisioning: `AWS::AppSync::GraphQLApi`, `DataSource`
//! and `Resolver`. Each is written through to the `appsync` service state as the
//! same camelCase wire object the direct `CreateGraphqlApi` / `CreateDataSource`
//! / `CreateResolver` handlers store, so a CFN-created resource reads back on the
//! `Get*` ops and persists through the `appsync` snapshot hook (survives a
//! restart -- #1766 class).
//!
//! DataSources and Resolvers are stored nested under their API, so their physical
//! ids encode the parent api id (`<apiId>|<name>` / `<apiId>|<type>|<field>`) to
//! keep delete / `Fn::GetAtt` self-contained.

use serde_json::{json, Value};

use super::{cfn_props_to_camel, ProvisionResult, ResourceDefinition, ResourceProvisioner};

impl ResourceProvisioner {
    // ---------------------------------------------------------- GraphQLApi

    pub(super) fn create_appsync_graphql_api(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let region = &self.region;
        let account = &self.account_id;
        let api_id = gen_api_id();
        let arn = format!("arn:aws:appsync:{region}:{account}:apis/{api_id}");
        let graphql_url = format!("https://{api_id}.appsync-api.{region}.amazonaws.com/graphql");
        let realtime_url =
            format!("wss://{api_id}.appsync-realtime-api.{region}.amazonaws.com/graphql");
        let graphql_dns = format!("{api_id}.appsync-api.{region}.amazonaws.com");
        let realtime_dns = format!("{api_id}.appsync-realtime-api.{region}.amazonaws.com");

        let mut api = match cfn_props_to_camel(props, &[]) {
            Value::Object(m) => m,
            _ => serde_json::Map::new(),
        };
        api.remove("tags");
        api.insert("apiId".to_string(), json!(api_id));
        api.insert("arn".to_string(), json!(arn.clone()));
        api.insert("owner".to_string(), json!(account));
        api.insert(
            "uris".to_string(),
            json!({ "GRAPHQL": graphql_url, "REALTIME": realtime_url }),
        );
        api.insert(
            "dns".to_string(),
            json!({ "GRAPHQL": graphql_dns, "REALTIME": realtime_dns }),
        );
        api.entry("apiType").or_insert(json!("GRAPHQL"));
        api.entry("visibility").or_insert(json!("GLOBAL"));
        api.entry("xrayEnabled").or_insert(json!(false));

        let mut guard = self.appsync_state.write();
        let data = guard.get_or_create(account);
        if data.graphql_apis.contains_key(&api_id) {
            return Err(format!("GraphQL API {api_id} already exists"));
        }
        let tags = string_tag_map(props.get("Tags"));
        if !tags.is_empty() {
            data.tags.insert(api_id.clone(), tags.clone());
            data.tags.insert(arn.clone(), tags);
        }
        data.graphql_apis.insert(api_id.clone(), Value::Object(api));

        Ok(ProvisionResult::new(api_id.clone())
            .with("ApiId", api_id)
            .with("Arn", arn)
            .with("GraphQLUrl", graphql_url)
            .with("GraphQLDns", graphql_dns)
            .with("RealtimeUrl", realtime_url)
            .with("RealtimeDns", realtime_dns))
    }

    pub(super) fn delete_appsync_graphql_api(&self, physical_id: &str) -> Result<(), String> {
        let mut guard = self.appsync_state.write();
        let data = guard.get_or_create(&self.account_id);
        if let Some(api) = data.graphql_apis.remove(physical_id) {
            if let Some(arn) = api.get("arn").and_then(Value::as_str) {
                data.tags.remove(arn);
            }
        }
        data.tags.remove(physical_id);
        data.data_sources.remove(physical_id);
        data.resolvers.remove(physical_id);
        Ok(())
    }

    pub(super) fn get_att_appsync_graphql_api(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let guard = self.appsync_state.read();
        let data = guard.get(&self.account_id)?;
        let api = data.graphql_apis.get(physical_id)?;
        match attribute {
            "ApiId" => Some(physical_id.to_string()),
            "Arn" => api.get("arn").and_then(Value::as_str).map(str::to_string),
            "GraphQLUrl" => uri(api, "GRAPHQL"),
            "RealtimeUrl" => uri(api, "REALTIME"),
            "GraphQLDns" => dns(api, "GRAPHQL"),
            "RealtimeDns" => dns(api, "REALTIME"),
            _ => None,
        }
    }

    // ---------------------------------------------------------- DataSource

    pub(super) fn create_appsync_data_source(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(Value::as_str)
            .ok_or("AWS::AppSync::DataSource requires ApiId")?
            .to_string();
        let name = props
            .get("Name")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| resource.logical_id.clone());
        let region = &self.region;
        let account = &self.account_id;
        let arn = format!("arn:aws:appsync:{region}:{account}:apis/{api_id}/datasources/{name}");

        let mut ds = match cfn_props_to_camel(props, &[]) {
            Value::Object(m) => m,
            _ => serde_json::Map::new(),
        };
        ds.remove("apiId");
        ds.insert("name".to_string(), json!(name));
        ds.insert("dataSourceArn".to_string(), json!(arn.clone()));

        let mut guard = self.appsync_state.write();
        let data = guard.get_or_create(account);
        if !data.graphql_apis.contains_key(&api_id) {
            return Err(format!("GraphQL API {api_id} not yet provisioned"));
        }
        data.data_sources
            .entry(api_id.clone())
            .or_default()
            .insert(name.clone(), Value::Object(ds));

        Ok(ProvisionResult::new(format!("{api_id}|{name}"))
            .with("DataSourceArn", arn)
            .with("Name", name))
    }

    pub(super) fn delete_appsync_data_source(&self, physical_id: &str) -> Result<(), String> {
        let Some((api_id, name)) = physical_id.split_once('|') else {
            return Ok(());
        };
        let mut guard = self.appsync_state.write();
        let data = guard.get_or_create(&self.account_id);
        if let Some(m) = data.data_sources.get_mut(api_id) {
            m.remove(name);
        }
        Ok(())
    }

    pub(super) fn get_att_appsync_data_source(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let (api_id, name) = physical_id.split_once('|')?;
        let guard = self.appsync_state.read();
        let data = guard.get(&self.account_id)?;
        let ds = data.data_sources.get(api_id)?.get(name)?;
        match attribute {
            "DataSourceArn" => ds
                .get("dataSourceArn")
                .and_then(Value::as_str)
                .map(str::to_string),
            "Name" => Some(name.to_string()),
            _ => None,
        }
    }

    // ------------------------------------------------------------ Resolver

    pub(super) fn create_appsync_resolver(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(Value::as_str)
            .ok_or("AWS::AppSync::Resolver requires ApiId")?
            .to_string();
        let type_name = props
            .get("TypeName")
            .and_then(Value::as_str)
            .ok_or("AWS::AppSync::Resolver requires TypeName")?
            .to_string();
        let field = props
            .get("FieldName")
            .and_then(Value::as_str)
            .ok_or("AWS::AppSync::Resolver requires FieldName")?
            .to_string();
        let region = &self.region;
        let account = &self.account_id;
        let arn = format!(
            "arn:aws:appsync:{region}:{account}:apis/{api_id}/types/{type_name}/resolvers/{field}"
        );

        let mut r = match cfn_props_to_camel(props, &[]) {
            Value::Object(m) => m,
            _ => serde_json::Map::new(),
        };
        r.remove("apiId");
        r.insert("typeName".to_string(), json!(type_name));
        r.insert("fieldName".to_string(), json!(field));
        r.insert("resolverArn".to_string(), json!(arn.clone()));
        r.entry("kind").or_insert(json!("UNIT"));

        let mut guard = self.appsync_state.write();
        let data = guard.get_or_create(account);
        if !data.graphql_apis.contains_key(&api_id) {
            return Err(format!("GraphQL API {api_id} not yet provisioned"));
        }
        let key = format!("{type_name}::{field}");
        data.resolvers
            .entry(api_id.clone())
            .or_default()
            .insert(key, Value::Object(r));

        Ok(
            ProvisionResult::new(format!("{api_id}|{type_name}|{field}"))
                .with("ResolverArn", arn)
                .with("TypeName", type_name)
                .with("FieldName", field),
        )
    }

    pub(super) fn delete_appsync_resolver(&self, physical_id: &str) -> Result<(), String> {
        let parts: Vec<&str> = physical_id.split('|').collect();
        if parts.len() != 3 {
            return Ok(());
        }
        let (api_id, type_name, field) = (parts[0], parts[1], parts[2]);
        let mut guard = self.appsync_state.write();
        let data = guard.get_or_create(&self.account_id);
        if let Some(m) = data.resolvers.get_mut(api_id) {
            m.remove(&format!("{type_name}::{field}"));
        }
        Ok(())
    }

    pub(super) fn get_att_appsync_resolver(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let parts: Vec<&str> = physical_id.split('|').collect();
        if parts.len() != 3 {
            return None;
        }
        let (api_id, type_name, field) = (parts[0], parts[1], parts[2]);
        let guard = self.appsync_state.read();
        let data = guard.get(&self.account_id)?;
        let r = data
            .resolvers
            .get(api_id)?
            .get(&format!("{type_name}::{field}"))?;
        match attribute {
            "ResolverArn" => r
                .get("resolverArn")
                .and_then(Value::as_str)
                .map(str::to_string),
            "TypeName" => Some(type_name.to_string()),
            "FieldName" => Some(field.to_string()),
            _ => None,
        }
    }
}

fn uri(api: &Value, key: &str) -> Option<String> {
    api.get("uris")?
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn dns(api: &Value, key: &str) -> Option<String> {
    api.get("dns")?
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// Convert CFN `Tags` (`[{Key,Value}]`) into the AppSync `key -> value` tag map.
fn string_tag_map(value: Option<&Value>) -> std::collections::BTreeMap<String, String> {
    let mut out = std::collections::BTreeMap::new();
    if let Some(arr) = value.and_then(Value::as_array) {
        for t in arr {
            if let (Some(k), Some(v)) = (
                t.get("Key").and_then(Value::as_str),
                t.get("Value").and_then(Value::as_str),
            ) {
                out.insert(k.to_string(), v.to_string());
            }
        }
    }
    out
}

/// Mint a 26-char AppSync API id from the base32 alphabet `a-z234567`.
fn gen_api_id() -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut out = String::with_capacity(26);
    while out.len() < 26 {
        for b in uuid::Uuid::new_v4().as_bytes() {
            out.push(ALPHABET[(*b as usize) % ALPHABET.len()] as char);
            if out.len() == 26 {
                break;
            }
        }
    }
    out
}

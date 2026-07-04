// Handler bodies for `S3TablesService`, `include!`d into `service.rs` so the
// free helpers there share this module scope. Split out only to keep the
// routing table and per-operation logic in separate files.

impl S3TablesService {
    // ------------------------------------------------------------------
    // Table buckets
    // ------------------------------------------------------------------

    fn create_table_bucket(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let name = str_field(&body, "name").ok_or_else(|| bad_request("name is required."))?;
        let arn = table_bucket_arn(&req.region, &req.account_id, &name);
        let now = Utc::now();
        let encryption = body
            .get("encryptionConfiguration")
            .cloned()
            .unwrap_or_else(|| json!({ "sseAlgorithm": "AES256" }));
        let storage_class = body
            .get("storageClassConfiguration")
            .cloned()
            .unwrap_or_else(|| json!({ "storageClass": "STANDARD" }));
        let tags = parse_tags(&body, "tags");

        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.table_buckets.contains_key(&arn) {
            return Err(conflict(format!("A table bucket named {name} already exists.")));
        }
        let record = TableBucketRecord {
            arn: arn.clone(),
            name,
            owner_account_id: req.account_id.clone(),
            created_at: now,
            table_bucket_id: gen_id(),
            bucket_type: "CUSTOMER".into(),
            encryption: Some(encryption),
            maintenance: default_bucket_maintenance(),
            metrics_id: None,
            policy: None,
            replication: None,
            storage_class: Some(storage_class),
            tags,
            namespaces: Default::default(),
            tables: Default::default(),
        };
        st.table_buckets.insert(arn.clone(), record);
        Ok(ok(json!({ "arn": arn })))
    }

    fn list_table_buckets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let prefix = query(req, "prefix");
        let type_filter = query(req, "type");
        let accounts = self.state.read();
        let mut items: Vec<&TableBucketRecord> = accounts
            .get(&req.account_id)
            .map(|s| s.table_buckets.values().collect())
            .unwrap_or_default();
        items.retain(|b| prefix.as_deref().is_none_or(|p| b.name.starts_with(p)));
        items.retain(|b| type_filter.as_deref().is_none_or(|t| b.bucket_type == t));
        items.sort_by(|a, b| a.name.cmp(&b.name));

        let (start, end, next) = paginate(
            items.len(),
            query(req, "continuationToken").as_deref(),
            query_usize(req, "maxBuckets"),
        );
        let page: Vec<Value> = items[start..end].iter().map(|b| bucket_summary(b)).collect();
        let mut out = Map::new();
        out.insert("tableBuckets".into(), Value::Array(page));
        if let Some(t) = next {
            out.insert("continuationToken".into(), json!(t));
        }
        Ok(ok(Value::Object(out)))
    }

    fn get_table_bucket(&self, req: &AwsRequest, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let b = accounts
            .get(&req.account_id)
            .and_then(|s| s.table_buckets.get(arn))
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        Ok(ok(json!({
            "arn": b.arn,
            "name": b.name,
            "ownerAccountId": b.owner_account_id,
            "createdAt": ts(b.created_at),
            "tableBucketId": b.table_bucket_id,
            "type": b.bucket_type,
        })))
    }

    fn delete_table_bucket(
        &self,
        req: &AwsRequest,
        arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        let b = st
            .table_buckets
            .get(arn)
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        if !b.namespaces.is_empty() || !b.tables.is_empty() {
            return Err(conflict(format!(
                "The table bucket {arn} is not empty."
            )));
        }
        st.table_buckets.remove(arn);
        Ok(empty(204))
    }

    // ------------------------------------------------------------------
    // Bucket sub-resources (encryption / policy / storage class / metrics)
    // ------------------------------------------------------------------

    fn get_bucket_sub(
        &self,
        req: &AwsRequest,
        arn: &str,
        kind: SubKind,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let b = accounts
            .get(&req.account_id)
            .and_then(|s| s.table_buckets.get(arn))
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        match kind {
            SubKind::Encryption => {
                let cfg = b
                    .encryption
                    .clone()
                    .ok_or_else(|| not_found("No encryption configuration exists."))?;
                Ok(ok(json!({ "encryptionConfiguration": cfg })))
            }
            SubKind::Policy => {
                let p = b
                    .policy
                    .clone()
                    .ok_or_else(|| not_found("The specified table bucket has no policy."))?;
                Ok(ok(json!({ "resourcePolicy": p })))
            }
            SubKind::StorageClass => {
                let cfg = b
                    .storage_class
                    .clone()
                    .unwrap_or_else(|| json!({ "storageClass": "STANDARD" }));
                Ok(ok(json!({ "storageClassConfiguration": cfg })))
            }
            SubKind::Metrics => unreachable!("metrics has a dedicated handler"),
        }
    }

    fn put_bucket_sub(
        &self,
        req: &AwsRequest,
        arn: &str,
        kind: SubKind,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| not_found("The specified table bucket does not exist."))?;
        let b = st
            .table_buckets
            .get_mut(arn)
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        match kind {
            SubKind::Encryption => {
                let cfg = body
                    .get("encryptionConfiguration")
                    .cloned()
                    .ok_or_else(|| bad_request("encryptionConfiguration is required."))?;
                b.encryption = Some(cfg);
            }
            SubKind::Policy => {
                let p = str_field(&body, "resourcePolicy")
                    .ok_or_else(|| bad_request("resourcePolicy is required."))?;
                b.policy = Some(p);
            }
            SubKind::StorageClass => {
                let cfg = body
                    .get("storageClassConfiguration")
                    .cloned()
                    .ok_or_else(|| bad_request("storageClassConfiguration is required."))?;
                b.storage_class = Some(cfg);
            }
            SubKind::Metrics => unreachable!("metrics has a dedicated handler"),
        }
        Ok(empty(200))
    }

    fn delete_bucket_sub(
        &self,
        req: &AwsRequest,
        arn: &str,
        kind: SubKind,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| not_found("The specified table bucket does not exist."))?;
        let b = st
            .table_buckets
            .get_mut(arn)
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        match kind {
            SubKind::Encryption => b.encryption = None,
            SubKind::Policy => b.policy = None,
            SubKind::Metrics => b.metrics_id = None,
            SubKind::StorageClass => b.storage_class = None,
        }
        Ok(empty(204))
    }

    fn get_bucket_maintenance(
        &self,
        req: &AwsRequest,
        arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let b = accounts
            .get(&req.account_id)
            .and_then(|s| s.table_buckets.get(arn))
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        let cfg: Map<String, Value> = b
            .maintenance
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Ok(ok(json!({
            "tableBucketARN": b.arn,
            "configuration": Value::Object(cfg),
        })))
    }

    fn put_bucket_maintenance(
        &self,
        req: &AwsRequest,
        arn: &str,
        ty: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let value = body
            .get("value")
            .cloned()
            .ok_or_else(|| bad_request("value is required."))?;
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| not_found("The specified table bucket does not exist."))?;
        let b = st
            .table_buckets
            .get_mut(arn)
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        b.maintenance.insert(ty.to_string(), value);
        Ok(empty(204))
    }

    fn get_bucket_metrics(
        &self,
        req: &AwsRequest,
        arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let b = accounts
            .get(&req.account_id)
            .and_then(|s| s.table_buckets.get(arn))
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        let id = b
            .metrics_id
            .clone()
            .ok_or_else(|| not_found("No metrics configuration exists."))?;
        Ok(ok(json!({ "tableBucketARN": b.arn, "id": id })))
    }

    fn put_bucket_metrics(
        &self,
        req: &AwsRequest,
        arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| not_found("The specified table bucket does not exist."))?;
        let b = st
            .table_buckets
            .get_mut(arn)
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        b.metrics_id = Some(gen_id());
        Ok(empty(204))
    }

    // ------------------------------------------------------------------
    // Bucket replication (query form)
    // ------------------------------------------------------------------

    fn get_bucket_replication(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn =
            query(req, "tableBucketARN").ok_or_else(|| bad_request("tableBucketARN is required."))?;
        let accounts = self.state.read();
        let b = accounts
            .get(&req.account_id)
            .and_then(|s| s.table_buckets.get(&arn))
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        let repl = b
            .replication
            .as_ref()
            .ok_or_else(|| not_found("No replication configuration exists."))?;
        Ok(ok(json!({
            "versionToken": repl.version_token,
            "configuration": repl.configuration,
        })))
    }

    fn put_bucket_replication(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn =
            query(req, "tableBucketARN").ok_or_else(|| bad_request("tableBucketARN is required."))?;
        let body = req_body(req);
        let cfg = body
            .get("configuration")
            .cloned()
            .ok_or_else(|| bad_request("configuration is required."))?;
        let token = gen_token();
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| not_found("The specified table bucket does not exist."))?;
        let b = st
            .table_buckets
            .get_mut(&arn)
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        b.replication = Some(VersionedConfig {
            version_token: token.clone(),
            configuration: cfg,
        });
        Ok(ok(json!({ "versionToken": token, "status": "ENABLED" })))
    }

    fn delete_bucket_replication(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn =
            query(req, "tableBucketARN").ok_or_else(|| bad_request("tableBucketARN is required."))?;
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| not_found("The specified table bucket does not exist."))?;
        let b = st
            .table_buckets
            .get_mut(&arn)
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        b.replication = None;
        Ok(empty(204))
    }

    // ------------------------------------------------------------------
    // Namespaces
    // ------------------------------------------------------------------

    fn create_namespace(
        &self,
        req: &AwsRequest,
        arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let name = body
            .get("namespace")
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .ok_or_else(|| bad_request("namespace is required."))?;
        let now = Utc::now();
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| not_found("The specified table bucket does not exist."))?;
        let b = st
            .table_buckets
            .get_mut(arn)
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        if b.namespaces.contains_key(&name) {
            return Err(conflict(format!("The namespace {name} already exists.")));
        }
        b.namespaces.insert(
            name.clone(),
            NamespaceRecord {
                name: name.clone(),
                created_at: now,
                created_by: req.account_id.clone(),
                owner_account_id: req.account_id.clone(),
                namespace_id: gen_id(),
            },
        );
        Ok(ok(json!({
            "tableBucketARN": arn,
            "namespace": [name],
        })))
    }

    fn list_namespaces(
        &self,
        req: &AwsRequest,
        arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let prefix = query(req, "prefix");
        let accounts = self.state.read();
        let b = accounts
            .get(&req.account_id)
            .and_then(|s| s.table_buckets.get(arn))
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        let mut items: Vec<&NamespaceRecord> = b.namespaces.values().collect();
        items.retain(|n| prefix.as_deref().is_none_or(|p| n.name.starts_with(p)));
        items.sort_by(|a, c| a.name.cmp(&c.name));

        let (start, end, next) = paginate(
            items.len(),
            query(req, "continuationToken").as_deref(),
            query_usize(req, "maxNamespaces"),
        );
        let page: Vec<Value> = items[start..end]
            .iter()
            .map(|n| namespace_summary(n, &b.table_bucket_id))
            .collect();
        let mut out = Map::new();
        out.insert("namespaces".into(), Value::Array(page));
        if let Some(t) = next {
            out.insert("continuationToken".into(), json!(t));
        }
        Ok(ok(Value::Object(out)))
    }

    fn get_namespace(
        &self,
        req: &AwsRequest,
        arn: &str,
        ns: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let b = accounts
            .get(&req.account_id)
            .and_then(|s| s.table_buckets.get(arn))
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        let n = b
            .namespaces
            .get(ns)
            .ok_or_else(|| not_found(format!("The namespace {ns} does not exist.")))?;
        Ok(ok(json!({
            "namespace": [n.name],
            "createdAt": ts(n.created_at),
            "createdBy": n.created_by,
            "ownerAccountId": n.owner_account_id,
            "namespaceId": n.namespace_id,
            "tableBucketId": b.table_bucket_id,
        })))
    }

    fn delete_namespace(
        &self,
        req: &AwsRequest,
        arn: &str,
        ns: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| not_found("The specified table bucket does not exist."))?;
        let b = st
            .table_buckets
            .get_mut(arn)
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        if !b.namespaces.contains_key(ns) {
            return Err(not_found(format!("The namespace {ns} does not exist.")));
        }
        if b.tables.values().any(|t| t.namespace == ns) {
            return Err(conflict(format!("The namespace {ns} is not empty.")));
        }
        b.namespaces.remove(ns);
        Ok(empty(204))
    }

    // ------------------------------------------------------------------
    // Tables
    // ------------------------------------------------------------------

    fn create_table(
        &self,
        req: &AwsRequest,
        arn: &str,
        ns: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let name = str_field(&body, "name").ok_or_else(|| bad_request("name is required."))?;
        let format = str_field(&body, "format").ok_or_else(|| bad_request("format is required."))?;
        let encryption = body
            .get("encryptionConfiguration")
            .cloned()
            .unwrap_or_else(|| json!({ "sseAlgorithm": "AES256" }));
        let storage_class = body
            .get("storageClassConfiguration")
            .cloned()
            .unwrap_or_else(|| json!({ "storageClass": "STANDARD" }));
        let tags = parse_tags(&body, "tags");
        let now = Utc::now();

        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| not_found("The specified table bucket does not exist."))?;
        let b = st
            .table_buckets
            .get_mut(arn)
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        let namespace_id = b
            .namespaces
            .get(ns)
            .ok_or_else(|| not_found(format!("The namespace {ns} does not exist.")))?
            .namespace_id
            .clone();
        if b.tables.values().any(|t| t.namespace == ns && t.name == name) {
            return Err(conflict(format!("A table named {name} already exists.")));
        }
        let table_id = gen_id();
        let tarn = table_arn(&b.arn, &table_id);
        let warehouse = warehouse_location(&table_id);
        let token = gen_token();
        let record = TableRecord {
            table_id: table_id.clone(),
            name,
            namespace: ns.to_string(),
            arn: tarn.clone(),
            // `TableType` enum value is lowercase `customer`/`aws` on the wire.
            table_type: "customer".into(),
            format,
            created_at: now,
            // A freshly created table has never been modified: `modifiedAt`
            // equals `createdAt` and `modifiedBy` is empty (omitted on the
            // wire) until an actual modification sets it.
            modified_at: now,
            created_by: req.account_id.clone(),
            modified_by: String::new(),
            owner_account_id: req.account_id.clone(),
            version_token: token.clone(),
            // No Iceberg metadata pointer until the client writes one (via
            // CreateTable `metadata` or UpdateTableMetadataLocation).
            metadata_location: None,
            warehouse_location: warehouse,
            managed_by_service: None,
            namespace_id,
            encryption: Some(encryption),
            maintenance: default_table_maintenance(),
            policy: None,
            record_expiration: None,
            replication: None,
            storage_class: Some(storage_class),
            tags,
        };
        b.tables.insert(table_id, record);
        Ok(ok(json!({ "tableARN": tarn, "versionToken": token })))
    }

    fn list_tables(&self, req: &AwsRequest, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let ns_filter = query(req, "namespace");
        let prefix = query(req, "prefix");
        let accounts = self.state.read();
        let b = accounts
            .get(&req.account_id)
            .and_then(|s| s.table_buckets.get(arn))
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        let mut items: Vec<&TableRecord> = b.tables.values().collect();
        items.retain(|t| ns_filter.as_deref().is_none_or(|n| t.namespace == n));
        items.retain(|t| prefix.as_deref().is_none_or(|p| t.name.starts_with(p)));
        items.sort_by(|a, c| (a.namespace.as_str(), a.name.as_str()).cmp(&(c.namespace.as_str(), c.name.as_str())));

        let (start, end, next) = paginate(
            items.len(),
            query(req, "continuationToken").as_deref(),
            query_usize(req, "maxTables"),
        );
        let page: Vec<Value> = items[start..end]
            .iter()
            .map(|t| table_summary(t, &b.table_bucket_id))
            .collect();
        let mut out = Map::new();
        out.insert("tables".into(), Value::Array(page));
        if let Some(t) = next {
            out.insert("continuationToken".into(), json!(t));
        }
        Ok(ok(Value::Object(out)))
    }

    fn get_table(&self, req: &AwsRequest, l: &[String]) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);

        // Path form `GET /tables/{arn}/{namespace}/{name}` (used by the AWS SDK
        // / terraform provider). The canonical query form is handled below.
        if let [arn, ns, name] = l {
            let b = st
                .and_then(|s| s.table_buckets.get(arn))
                .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
            let t = b
                .tables
                .values()
                .find(|t| &t.namespace == ns && &t.name == name)
                .ok_or_else(|| not_found(format!("The table {name} does not exist.")))?;
            return Ok(ok(table_to_get_value(t, &b.table_bucket_id)));
        }

        // Resolve either by tableArn or by (tableBucketARN, namespace, name).
        let (bucket, table): (&TableBucketRecord, &TableRecord) = if let Some(tarn) =
            query(req, "tableArn")
        {
            let barn = bucket_arn_of_table(&tarn)
                .ok_or_else(|| bad_request("Invalid tableArn."))?;
            let tid = table_id_of_arn(&tarn).unwrap_or("");
            let b = st
                .and_then(|s| s.table_buckets.get(barn))
                .ok_or_else(|| not_found(format!("The table {tarn} does not exist.")))?;
            let t = b
                .tables
                .get(tid)
                .ok_or_else(|| not_found(format!("The table {tarn} does not exist.")))?;
            (b, t)
        } else {
            let barn = query(req, "tableBucketARN")
                .ok_or_else(|| bad_request("Either tableArn or tableBucketARN/namespace/name is required."))?;
            let ns = query(req, "namespace")
                .ok_or_else(|| bad_request("namespace is required."))?;
            let name = query(req, "name").ok_or_else(|| bad_request("name is required."))?;
            let b = st
                .and_then(|s| s.table_buckets.get(&barn))
                .ok_or_else(|| not_found(format!("The table bucket {barn} does not exist.")))?;
            let t = b
                .tables
                .values()
                .find(|t| t.namespace == ns && t.name == name)
                .ok_or_else(|| not_found(format!("The table {name} does not exist.")))?;
            (b, t)
        };
        Ok(ok(table_to_get_value(table, &bucket.table_bucket_id)))
    }

    fn delete_table(
        &self,
        req: &AwsRequest,
        arn: &str,
        ns: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let want_token = query(req, "versionToken");
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| not_found("The specified table bucket does not exist."))?;
        let b = st
            .table_buckets
            .get_mut(arn)
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        let tid = b
            .tables
            .values()
            .find(|t| t.namespace == ns && t.name == name)
            .map(|t| t.table_id.clone())
            .ok_or_else(|| not_found(format!("The table {name} does not exist.")))?;
        if let Some(tok) = &want_token {
            if b.tables[&tid].version_token != *tok {
                return Err(conflict("The provided versionToken does not match."));
            }
        }
        b.tables.remove(&tid);
        Ok(empty(204))
    }

    fn rename_table(
        &self,
        req: &AwsRequest,
        arn: &str,
        ns: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let new_ns = str_field(&body, "newNamespaceName");
        let new_name = str_field(&body, "newName");
        let want_token = str_field(&body, "versionToken");
        if new_ns.is_none() && new_name.is_none() {
            return Err(bad_request("Either newNamespaceName or newName must be specified."));
        }
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| not_found("The specified table bucket does not exist."))?;
        let b = st
            .table_buckets
            .get_mut(arn)
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        let tid = b
            .tables
            .values()
            .find(|t| t.namespace == ns && t.name == name)
            .map(|t| t.table_id.clone())
            .ok_or_else(|| not_found(format!("The table {name} does not exist.")))?;
        // On a namespace move the table adopts the target namespace's id, so
        // resolve it here (validating the target exists) before the table
        // borrow.
        let dest_namespace_id = if let Some(dest_ns) = &new_ns {
            Some(
                b.namespaces
                    .get(dest_ns)
                    .ok_or_else(|| not_found(format!("The namespace {dest_ns} does not exist.")))?
                    .namespace_id
                    .clone(),
            )
        } else {
            None
        };
        let target_ns = new_ns.clone().unwrap_or_else(|| ns.to_string());
        let target_name = new_name.clone().unwrap_or_else(|| name.to_string());
        if b
            .tables
            .values()
            .any(|t| t.table_id != tid && t.namespace == target_ns && t.name == target_name)
        {
            return Err(conflict(format!("A table named {target_name} already exists.")));
        }
        let t = b.tables.get_mut(&tid).expect("table id resolved above");
        if let Some(tok) = &want_token {
            if t.version_token != *tok {
                return Err(conflict("The provided versionToken does not match."));
            }
        }
        if let Some(n) = new_ns {
            t.namespace = n;
            if let Some(nid) = dest_namespace_id {
                t.namespace_id = nid;
            }
        }
        if let Some(n) = new_name {
            t.name = n;
        }
        t.modified_at = Utc::now();
        t.modified_by = req.account_id.clone();
        t.version_token = gen_token();
        Ok(empty(204))
    }

    fn update_table_metadata(
        &self,
        req: &AwsRequest,
        arn: &str,
        ns: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let want_token = str_field(&body, "versionToken")
            .ok_or_else(|| bad_request("versionToken is required."))?;
        let metadata = str_field(&body, "metadataLocation")
            .ok_or_else(|| bad_request("metadataLocation is required."))?;
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| not_found("The specified table bucket does not exist."))?;
        let b = st
            .table_buckets
            .get_mut(arn)
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        let tid = b
            .tables
            .values()
            .find(|t| t.namespace == ns && t.name == name)
            .map(|t| t.table_id.clone())
            .ok_or_else(|| not_found(format!("The table {name} does not exist.")))?;
        let t = b.tables.get_mut(&tid).expect("table id resolved above");
        if t.version_token != want_token {
            return Err(conflict("The provided versionToken does not match."));
        }
        t.metadata_location = Some(metadata.clone());
        t.modified_at = Utc::now();
        t.modified_by = req.account_id.clone();
        t.version_token = gen_token();
        Ok(ok(json!({
            "name": t.name,
            "tableARN": t.arn,
            "namespace": [t.namespace],
            "versionToken": t.version_token,
            "metadataLocation": metadata,
        })))
    }

    fn get_table_metadata(
        &self,
        req: &AwsRequest,
        arn: &str,
        ns: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let t = self.find_table_read(&accounts, &req.account_id, arn, ns, name)?;
        let mut m = Map::new();
        m.insert("versionToken".into(), json!(t.version_token));
        if let Some(loc) = &t.metadata_location {
            m.insert("metadataLocation".into(), json!(loc));
        }
        m.insert("warehouseLocation".into(), json!(t.warehouse_location));
        Ok(ok(Value::Object(m)))
    }

    // ------------------------------------------------------------------
    // Table sub-resources
    // ------------------------------------------------------------------

    fn get_table_sub(
        &self,
        req: &AwsRequest,
        arn: &str,
        ns: &str,
        name: &str,
        kind: SubKind,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let t = self.find_table_read(&accounts, &req.account_id, arn, ns, name)?;
        match kind {
            SubKind::Encryption => {
                let cfg = t
                    .encryption
                    .clone()
                    .ok_or_else(|| not_found("No encryption configuration exists."))?;
                Ok(ok(json!({ "encryptionConfiguration": cfg })))
            }
            SubKind::StorageClass => {
                let cfg = t
                    .storage_class
                    .clone()
                    .unwrap_or_else(|| json!({ "storageClass": "STANDARD" }));
                Ok(ok(json!({ "storageClassConfiguration": cfg })))
            }
            SubKind::Policy => {
                let p = t
                    .policy
                    .clone()
                    .ok_or_else(|| not_found("The specified table has no policy."))?;
                Ok(ok(json!({ "resourcePolicy": p })))
            }
            SubKind::Metrics => unreachable!("tables have no metrics sub-resource"),
        }
    }

    fn put_table_sub(
        &self,
        req: &AwsRequest,
        arn: &str,
        ns: &str,
        name: &str,
        kind: SubKind,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let mut accounts = self.state.write();
        let t = self.find_table_write(&mut accounts, &req.account_id, arn, ns, name)?;
        match kind {
            SubKind::Policy => {
                let p = str_field(&body, "resourcePolicy")
                    .ok_or_else(|| bad_request("resourcePolicy is required."))?;
                t.policy = Some(p);
            }
            _ => unreachable!("only policy is put via put_table_sub"),
        }
        Ok(empty(200))
    }

    fn delete_table_sub(
        &self,
        req: &AwsRequest,
        arn: &str,
        ns: &str,
        name: &str,
        kind: SubKind,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let t = self.find_table_write(&mut accounts, &req.account_id, arn, ns, name)?;
        match kind {
            SubKind::Policy => t.policy = None,
            _ => unreachable!("only policy is deleted via delete_table_sub"),
        }
        Ok(empty(204))
    }

    fn get_table_maintenance(
        &self,
        req: &AwsRequest,
        arn: &str,
        ns: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let t = self.find_table_read(&accounts, &req.account_id, arn, ns, name)?;
        let cfg: Map<String, Value> = t
            .maintenance
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Ok(ok(json!({
            "tableARN": t.arn,
            "configuration": Value::Object(cfg),
        })))
    }

    fn put_table_maintenance(
        &self,
        req: &AwsRequest,
        arn: &str,
        ns: &str,
        name: &str,
        ty: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let value = body
            .get("value")
            .cloned()
            .ok_or_else(|| bad_request("value is required."))?;
        let mut accounts = self.state.write();
        let t = self.find_table_write(&mut accounts, &req.account_id, arn, ns, name)?;
        t.maintenance.insert(ty.to_string(), value);
        Ok(empty(204))
    }

    fn get_table_maintenance_job(
        &self,
        req: &AwsRequest,
        arn: &str,
        ns: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let t = self.find_table_read(&accounts, &req.account_id, arn, ns, name)?;
        // No real background compaction runs; jobs settle to an idle status.
        let mut status = Map::new();
        for job in [
            "ICEBERG_COMPACTION",
            "ICEBERG_SNAPSHOT_MANAGEMENT",
            "ICEBERG_UNREFERENCED_FILE_REMOVAL",
        ] {
            status.insert(job.to_string(), json!({ "status": "NOT_YET_RUN" }));
        }
        Ok(ok(json!({ "tableARN": t.arn, "status": Value::Object(status) })))
    }

    // ------------------------------------------------------------------
    // Table replication (query form)
    // ------------------------------------------------------------------

    fn get_table_replication(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let tarn = query(req, "tableArn").ok_or_else(|| bad_request("tableArn is required."))?;
        let accounts = self.state.read();
        let t = self.find_table_by_arn_read(&accounts, &req.account_id, &tarn)?;
        let repl = t
            .replication
            .as_ref()
            .ok_or_else(|| not_found("No replication configuration exists."))?;
        Ok(ok(json!({
            "versionToken": repl.version_token,
            "configuration": repl.configuration,
        })))
    }

    fn put_table_replication(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let tarn = query(req, "tableArn").ok_or_else(|| bad_request("tableArn is required."))?;
        let body = req_body(req);
        let cfg = body
            .get("configuration")
            .cloned()
            .ok_or_else(|| bad_request("configuration is required."))?;
        let token = gen_token();
        let mut accounts = self.state.write();
        let t = self.find_table_by_arn_write(&mut accounts, &req.account_id, &tarn)?;
        t.replication = Some(VersionedConfig {
            version_token: token.clone(),
            configuration: cfg,
        });
        Ok(ok(json!({ "versionToken": token, "status": "ENABLED" })))
    }

    fn delete_table_replication(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let tarn = query(req, "tableArn").ok_or_else(|| bad_request("tableArn is required."))?;
        let mut accounts = self.state.write();
        let t = self.find_table_by_arn_write(&mut accounts, &req.account_id, &tarn)?;
        t.replication = None;
        Ok(empty(204))
    }

    fn get_table_replication_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let tarn = query(req, "tableArn").ok_or_else(|| bad_request("tableArn is required."))?;
        let accounts = self.state.read();
        let t = self.find_table_by_arn_read(&accounts, &req.account_id, &tarn)?;
        let repl = t
            .replication
            .as_ref()
            .ok_or_else(|| not_found("No replication configuration exists."))?;
        // Synthesize a settled destination status per configured destination.
        let dests: Vec<Value> = repl
            .configuration
            .get("rules")
            .and_then(|v| v.as_array())
            .map(|rules| {
                rules
                    .iter()
                    .flat_map(|r| r.get("destinations").and_then(|d| d.as_array()).cloned().unwrap_or_default())
                    .filter_map(|d| {
                        d.get("destinationTableBucketARN")
                            .and_then(|v| v.as_str())
                            .map(|arn| {
                                json!({
                                    "replicationStatus": "COMPLETED",
                                    "destinationTableBucketArn": arn,
                                })
                            })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(ok(json!({
            "sourceTableArn": t.arn,
            "destinations": dests,
        })))
    }

    // ------------------------------------------------------------------
    // Record expiration (query form)
    // ------------------------------------------------------------------

    fn get_record_expiration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let tarn = query(req, "tableArn").ok_or_else(|| bad_request("tableArn is required."))?;
        let accounts = self.state.read();
        let t = self.find_table_by_arn_read(&accounts, &req.account_id, &tarn)?;
        let cfg = t
            .record_expiration
            .clone()
            .unwrap_or_else(|| json!({ "status": "DISABLED" }));
        Ok(ok(json!({ "configuration": cfg })))
    }

    fn put_record_expiration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let tarn = query(req, "tableArn").ok_or_else(|| bad_request("tableArn is required."))?;
        let body = req_body(req);
        let value = body
            .get("value")
            .cloned()
            .ok_or_else(|| bad_request("value is required."))?;
        let mut accounts = self.state.write();
        let t = self.find_table_by_arn_write(&mut accounts, &req.account_id, &tarn)?;
        t.record_expiration = Some(value);
        Ok(empty(204))
    }

    fn get_record_expiration_job(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let tarn = query(req, "tableArn").ok_or_else(|| bad_request("tableArn is required."))?;
        let accounts = self.state.read();
        let _t = self.find_table_by_arn_read(&accounts, &req.account_id, &tarn)?;
        Ok(ok(json!({ "status": "NOT_YET_RUN" })))
    }

    // ------------------------------------------------------------------
    // Tagging
    // ------------------------------------------------------------------

    fn list_tags(&self, req: &AwsRequest, resource_arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let tags = self.resource_tags(&accounts, &req.account_id, resource_arn)?;
        Ok(ok(json!({ "tags": tags_value(&tags) })))
    }

    fn tag_resource(
        &self,
        req: &AwsRequest,
        resource_arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let tags = parse_tags(&body, "tags");
        let mut accounts = self.state.write();
        let target = self.resource_tags_mut(&mut accounts, &req.account_id, resource_arn)?;
        target.extend(tags);
        Ok(empty(200))
    }

    fn untag_resource(
        &self,
        req: &AwsRequest,
        resource_arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        // The `tagKeys` `@httpQuery` list serialises as repeated `tagKeys=`
        // pairs (`?tagKeys=env&tagKeys=team`). `req.query_params` is a map that
        // collapses repeated keys to the last value, so parse every occurrence
        // out of the raw query string instead, percent-decoding each value.
        let mut keys: Vec<String> = req
            .raw_query
            .split('&')
            .filter_map(|pair| pair.strip_prefix("tagKeys="))
            .map(decode)
            .collect();
        // Fallback for a single comma-joined value (some clients collapse the
        // list into one param).
        if keys.len() == 1 && keys[0].contains(',') {
            keys = keys[0].split(',').map(str::to_string).collect();
        }
        let mut accounts = self.state.write();
        let target = self.resource_tags_mut(&mut accounts, &req.account_id, resource_arn)?;
        for k in &keys {
            target.remove(k);
        }
        Ok(empty(204))
    }

    // ------------------------------------------------------------------
    // Shared lookup helpers
    // ------------------------------------------------------------------

    fn find_table_read<'a>(
        &self,
        accounts: &'a fakecloud_core::multi_account::MultiAccountState<crate::state::S3TablesState>,
        account_id: &str,
        arn: &str,
        ns: &str,
        name: &str,
    ) -> Result<&'a TableRecord, AwsServiceError> {
        let b = accounts
            .get(account_id)
            .and_then(|s| s.table_buckets.get(arn))
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        b.tables
            .values()
            .find(|t| t.namespace == ns && t.name == name)
            .ok_or_else(|| not_found(format!("The table {name} does not exist.")))
    }

    fn find_table_write<'a>(
        &self,
        accounts: &'a mut fakecloud_core::multi_account::MultiAccountState<
            crate::state::S3TablesState,
        >,
        account_id: &str,
        arn: &str,
        ns: &str,
        name: &str,
    ) -> Result<&'a mut TableRecord, AwsServiceError> {
        let b = accounts
            .get_mut(account_id)
            .and_then(|s| s.table_buckets.get_mut(arn))
            .ok_or_else(|| not_found(format!("The table bucket {arn} does not exist.")))?;
        let tid = b
            .tables
            .values()
            .find(|t| t.namespace == ns && t.name == name)
            .map(|t| t.table_id.clone())
            .ok_or_else(|| not_found(format!("The table {name} does not exist.")))?;
        Ok(b.tables.get_mut(&tid).expect("table id resolved above"))
    }

    fn find_table_by_arn_read<'a>(
        &self,
        accounts: &'a fakecloud_core::multi_account::MultiAccountState<crate::state::S3TablesState>,
        account_id: &str,
        tarn: &str,
    ) -> Result<&'a TableRecord, AwsServiceError> {
        let barn =
            bucket_arn_of_table(tarn).ok_or_else(|| bad_request("Invalid tableArn."))?;
        let tid = table_id_of_arn(tarn).unwrap_or("");
        accounts
            .get(account_id)
            .and_then(|s| s.table_buckets.get(barn))
            .and_then(|b| b.tables.get(tid))
            .ok_or_else(|| not_found(format!("The table {tarn} does not exist.")))
    }

    fn find_table_by_arn_write<'a>(
        &self,
        accounts: &'a mut fakecloud_core::multi_account::MultiAccountState<
            crate::state::S3TablesState,
        >,
        account_id: &str,
        tarn: &str,
    ) -> Result<&'a mut TableRecord, AwsServiceError> {
        let barn = bucket_arn_of_table(tarn)
            .ok_or_else(|| bad_request("Invalid tableArn."))?
            .to_string();
        let tid = table_id_of_arn(tarn).unwrap_or("").to_string();
        accounts
            .get_mut(account_id)
            .and_then(|s| s.table_buckets.get_mut(&barn))
            .and_then(|b| b.tables.get_mut(&tid))
            .ok_or_else(|| not_found(format!("The table {tarn} does not exist.")))
    }

    /// Read the tag map for a bucket or table identified by ARN.
    fn resource_tags(
        &self,
        accounts: &fakecloud_core::multi_account::MultiAccountState<crate::state::S3TablesState>,
        account_id: &str,
        resource_arn: &str,
    ) -> Result<TagMap, AwsServiceError> {
        let st = accounts
            .get(account_id)
            .ok_or_else(|| not_found(format!("The resource {resource_arn} does not exist.")))?;
        if let Some(barn) = bucket_arn_of_table(resource_arn) {
            let tid = table_id_of_arn(resource_arn).unwrap_or("");
            let t = st
                .table_buckets
                .get(barn)
                .and_then(|b| b.tables.get(tid))
                .ok_or_else(|| not_found(format!("The resource {resource_arn} does not exist.")))?;
            Ok(t.tags.clone())
        } else {
            let b = st
                .table_buckets
                .get(resource_arn)
                .ok_or_else(|| not_found(format!("The resource {resource_arn} does not exist.")))?;
            Ok(b.tags.clone())
        }
    }

    /// Mutable tag map for a bucket or table identified by ARN.
    fn resource_tags_mut<'a>(
        &self,
        accounts: &'a mut fakecloud_core::multi_account::MultiAccountState<
            crate::state::S3TablesState,
        >,
        account_id: &str,
        resource_arn: &str,
    ) -> Result<&'a mut TagMap, AwsServiceError> {
        let st = accounts
            .get_mut(account_id)
            .ok_or_else(|| not_found(format!("The resource {resource_arn} does not exist.")))?;
        if let Some(barn) = bucket_arn_of_table(resource_arn) {
            let barn = barn.to_string();
            let tid = table_id_of_arn(resource_arn).unwrap_or("").to_string();
            let t = st
                .table_buckets
                .get_mut(&barn)
                .and_then(|b| b.tables.get_mut(&tid))
                .ok_or_else(|| not_found(format!("The resource {resource_arn} does not exist.")))?;
            Ok(&mut t.tags)
        } else {
            let b = st
                .table_buckets
                .get_mut(resource_arn)
                .ok_or_else(|| not_found(format!("The resource {resource_arn} does not exist.")))?;
            Ok(&mut b.tags)
        }
    }
}

/// Build the full `GetTableResponse` value.
fn table_to_get_value(t: &TableRecord, table_bucket_id: &str) -> Value {
    let mut m = Map::new();
    m.insert("name".into(), json!(t.name));
    m.insert("type".into(), json!(t.table_type));
    m.insert("tableARN".into(), json!(t.arn));
    m.insert("namespace".into(), json!([t.namespace]));
    m.insert("namespaceId".into(), json!(t.namespace_id));
    m.insert("versionToken".into(), json!(t.version_token));
    if let Some(loc) = &t.metadata_location {
        m.insert("metadataLocation".into(), json!(loc));
    }
    m.insert("warehouseLocation".into(), json!(t.warehouse_location));
    m.insert("createdAt".into(), ts(t.created_at));
    m.insert("createdBy".into(), json!(t.created_by));
    if let Some(mbs) = &t.managed_by_service {
        m.insert("managedByService".into(), json!(mbs));
    }
    m.insert("modifiedAt".into(), ts(t.modified_at));
    // `modifiedBy` is only present once the table has actually been modified.
    if !t.modified_by.is_empty() {
        m.insert("modifiedBy".into(), json!(t.modified_by));
    }
    m.insert("ownerAccountId".into(), json!(t.owner_account_id));
    m.insert("format".into(), json!(t.format));
    m.insert("tableBucketId".into(), json!(table_bucket_id));
    Value::Object(m)
}

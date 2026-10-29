use super::*;

#[test]
fn s3_condition_keys_emits_list_params() {
    let mut q = std::collections::HashMap::new();
    q.insert("prefix".to_string(), "logs/".to_string());
    q.insert("delimiter".to_string(), "/".to_string());
    q.insert("max-keys".to_string(), "100".to_string());
    let keys = s3_condition_keys("ListObjectsV2", &q, &HeaderMap::new());
    assert_eq!(keys.get("s3:prefix"), Some(&vec!["logs/".to_string()]));
    assert_eq!(keys.get("s3:delimiter"), Some(&vec!["/".to_string()]));
    assert_eq!(keys.get("s3:max-keys"), Some(&vec!["100".to_string()]));
}

#[test]
fn s3_condition_keys_emit_request_header_keys() {
    let mut h = HeaderMap::new();
    h.insert("x-amz-server-side-encryption", "aws:kms".parse().unwrap());
    h.insert(
        "x-amz-server-side-encryption-aws-kms-key-id",
        "arn:aws:kms:us-east-1:123456789012:key/k".parse().unwrap(),
    );
    h.insert("x-amz-storage-class", "STANDARD_IA".parse().unwrap());
    h.insert("x-amz-metadata-directive", "REPLACE".parse().unwrap());
    h.insert("x-amz-copy-source", "/src/key".parse().unwrap());
    h.insert(
        "x-amz-server-side-encryption-customer-algorithm",
        " ".parse().unwrap(),
    );
    let keys = s3_condition_keys("PutObject", &std::collections::HashMap::new(), &h);
    assert_eq!(
        keys["s3:x-amz-server-side-encryption"],
        vec!["aws:kms".to_string()]
    );
    assert_eq!(
        keys["s3:x-amz-server-side-encryption-aws-kms-key-id"],
        vec!["arn:aws:kms:us-east-1:123456789012:key/k".to_string()]
    );
    assert_eq!(
        keys["s3:x-amz-storage-class"],
        vec!["STANDARD_IA".to_string()]
    );
    assert_eq!(
        keys["s3:x-amz-metadata-directive"],
        vec!["REPLACE".to_string()]
    );
    assert_eq!(keys["s3:x-amz-copy-source"], vec!["/src/key".to_string()]);
    // A blank header names no value and is not emitted.
    assert!(!keys.contains_key("s3:x-amz-server-side-encryption-customer-algorithm"));
}

#[test]
fn s3_condition_keys_omits_absent_params() {
    let q = std::collections::HashMap::new();
    let keys = s3_condition_keys("ListObjectsV2", &q, &HeaderMap::new());
    assert!(keys.is_empty());
}

#[test]
fn s3_condition_keys_partial_params() {
    let mut q = std::collections::HashMap::new();
    q.insert("prefix".to_string(), "archive/".to_string());
    let keys = s3_condition_keys("ListObjects", &q, &HeaderMap::new());
    assert_eq!(keys.len(), 1);
    assert_eq!(keys.get("s3:prefix"), Some(&vec!["archive/".to_string()]));
}

#[test]
fn s3_condition_keys_empty_for_non_list_actions() {
    let mut q = std::collections::HashMap::new();
    q.insert("prefix".to_string(), "logs/".to_string());
    assert!(s3_condition_keys("GetObject", &q, &HeaderMap::new()).is_empty());
    assert!(s3_condition_keys("PutObject", &q, &HeaderMap::new()).is_empty());
    assert!(s3_condition_keys("ListBuckets", &q, &HeaderMap::new()).is_empty());
}

#[test]
fn valid_bucket_names() {
    assert!(is_valid_bucket_name("my-bucket"));
    assert!(is_valid_bucket_name("my.bucket.name"));
    assert!(is_valid_bucket_name("abc"));
    assert!(!is_valid_bucket_name("ab"));
    assert!(!is_valid_bucket_name("-bucket"));
    assert!(!is_valid_bucket_name("Bucket"));
    assert!(!is_valid_bucket_name("bucket-"));
    // Underscores are not permitted for general-purpose buckets.
    assert!(!is_valid_bucket_name("my_bucket"));
    // No two adjacent periods.
    assert!(!is_valid_bucket_name("my..bucket"));
    // Must not look like an IPv4 address.
    assert!(!is_valid_bucket_name("192.168.5.4"));
    assert!(is_valid_bucket_name("192.168.5.4.example"));
    // Reserved prefixes / suffixes.
    assert!(!is_valid_bucket_name("xn--bucket"));
    assert!(!is_valid_bucket_name("sthree-bucket"));
    assert!(!is_valid_bucket_name("my-bucket-s3alias"));
    assert!(!is_valid_bucket_name("my-bucket--ol-s3"));
}

#[test]
fn parse_delete_xml() {
    let xml =
        r#"<Delete><Object><Key>a.txt</Key></Object><Object><Key>b/c.txt</Key></Object></Delete>"#;
    let entries = parse_delete_objects_xml(xml);
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].key, "a.txt");
    assert!(entries[0].version_id.is_none());
    assert_eq!(entries[1].key, "b/c.txt");
}

#[test]
fn parse_delete_xml_with_version() {
    let xml = r#"<Delete><Object><Key>a.txt</Key><VersionId>v1</VersionId></Object></Delete>"#;
    let entries = parse_delete_objects_xml(xml);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, "a.txt");
    assert_eq!(entries[0].version_id.as_deref(), Some("v1"));
}

#[test]
fn parse_tags_xml() {
    let xml = r#"<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>"#;
    let tags = parse_tagging_xml(xml);
    assert_eq!(tags, vec![("env".to_string(), "prod".to_string())]);
}

#[test]
fn md5_hash() {
    let hash = compute_md5(b"hello");
    assert_eq!(hash, "5d41402abc4b2a76b9719d911017c592");
}

#[test]
fn test_etag_matches() {
    assert!(etag_matches("\"abc\"", "\"abc\""));
    assert!(etag_matches("abc", "\"abc\""));
    assert!(etag_matches("*", "\"abc\""));
    assert!(!etag_matches("\"xyz\"", "\"abc\""));
}

#[test]
fn test_event_matches() {
    assert!(event_matches("s3:ObjectCreated:Put", "s3:ObjectCreated:*"));
    assert!(event_matches("s3:ObjectCreated:Copy", "s3:ObjectCreated:*"));
    assert!(event_matches(
        "s3:ObjectRemoved:Delete",
        "s3:ObjectRemoved:*"
    ));
    assert!(!event_matches(
        "s3:ObjectRemoved:Delete",
        "s3:ObjectCreated:*"
    ));
    assert!(event_matches(
        "s3:ObjectCreated:Put",
        "s3:ObjectCreated:Put"
    ));
    assert!(event_matches("s3:ObjectCreated:Put", "s3:*"));
}

#[test]
fn test_parse_notification_config() {
    let xml = r#"<NotificationConfiguration>
        <QueueConfiguration>
            <Queue>arn:aws:sqs:us-east-1:123456789012:my-queue</Queue>
            <Event>s3:ObjectCreated:*</Event>
        </QueueConfiguration>
        <TopicConfiguration>
            <Topic>arn:aws:sns:us-east-1:123456789012:my-topic</Topic>
            <Event>s3:ObjectRemoved:*</Event>
        </TopicConfiguration>
    </NotificationConfiguration>"#;
    let targets = parse_notification_config(xml);
    assert_eq!(targets.len(), 2);
    assert_eq!(
        targets[0].arn,
        "arn:aws:sqs:us-east-1:123456789012:my-queue"
    );
    assert_eq!(targets[0].events, vec!["s3:ObjectCreated:*"]);
    assert_eq!(
        targets[1].arn,
        "arn:aws:sns:us-east-1:123456789012:my-topic"
    );
    assert_eq!(targets[1].events, vec!["s3:ObjectRemoved:*"]);
}

#[test]
fn test_parse_notification_config_lambda() {
    // Test CloudFunctionConfiguration (older format)
    let xml = r#"<NotificationConfiguration>
        <CloudFunctionConfiguration>
            <CloudFunction>arn:aws:lambda:us-east-1:123456789012:function:my-func</CloudFunction>
            <Event>s3:ObjectCreated:*</Event>
        </CloudFunctionConfiguration>
    </NotificationConfiguration>"#;
    let targets = parse_notification_config(xml);
    assert_eq!(targets.len(), 1);
    assert!(matches!(
        targets[0].target_type,
        NotificationTargetType::Lambda
    ));
    assert_eq!(
        targets[0].arn,
        "arn:aws:lambda:us-east-1:123456789012:function:my-func"
    );
    assert_eq!(targets[0].events, vec!["s3:ObjectCreated:*"]);
}

#[test]
fn test_parse_notification_config_lambda_new_format() {
    // Test LambdaFunctionConfiguration (newer format used by AWS SDK)
    let xml = r#"<NotificationConfiguration>
        <LambdaFunctionConfiguration>
            <Function>arn:aws:lambda:us-east-1:123456789012:function:my-func</Function>
            <Event>s3:ObjectCreated:Put</Event>
            <Event>s3:ObjectRemoved:*</Event>
        </LambdaFunctionConfiguration>
    </NotificationConfiguration>"#;
    let targets = parse_notification_config(xml);
    assert_eq!(targets.len(), 1);
    assert!(matches!(
        targets[0].target_type,
        NotificationTargetType::Lambda
    ));
    assert_eq!(
        targets[0].arn,
        "arn:aws:lambda:us-east-1:123456789012:function:my-func"
    );
    assert_eq!(
        targets[0].events,
        vec!["s3:ObjectCreated:Put", "s3:ObjectRemoved:*"]
    );
}

#[test]
fn test_parse_notification_config_all_types() {
    let xml = r#"<NotificationConfiguration>
        <QueueConfiguration>
            <Queue>arn:aws:sqs:us-east-1:123456789012:q</Queue>
            <Event>s3:ObjectCreated:*</Event>
        </QueueConfiguration>
        <TopicConfiguration>
            <Topic>arn:aws:sns:us-east-1:123456789012:t</Topic>
            <Event>s3:ObjectRemoved:*</Event>
        </TopicConfiguration>
        <LambdaFunctionConfiguration>
            <Function>arn:aws:lambda:us-east-1:123456789012:function:f</Function>
            <Event>s3:ObjectCreated:Put</Event>
        </LambdaFunctionConfiguration>
    </NotificationConfiguration>"#;
    let targets = parse_notification_config(xml);
    assert_eq!(targets.len(), 3);
    assert!(matches!(
        targets[0].target_type,
        NotificationTargetType::Sqs
    ));
    assert!(matches!(
        targets[1].target_type,
        NotificationTargetType::Sns
    ));
    assert!(matches!(
        targets[2].target_type,
        NotificationTargetType::Lambda
    ));
}

#[test]
fn test_parse_notification_config_with_filters() {
    let xml = r#"<NotificationConfiguration>
        <LambdaFunctionConfiguration>
            <Function>arn:aws:lambda:us-east-1:123456789012:function:my-func</Function>
            <Event>s3:ObjectCreated:*</Event>
            <Filter>
                <S3Key>
                    <FilterRule>
                        <Name>prefix</Name>
                        <Value>images/</Value>
                    </FilterRule>
                    <FilterRule>
                        <Name>suffix</Name>
                        <Value>.jpg</Value>
                    </FilterRule>
                </S3Key>
            </Filter>
        </LambdaFunctionConfiguration>
    </NotificationConfiguration>"#;
    let targets = parse_notification_config(xml);
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].prefix_filter, Some("images/".to_string()));
    assert_eq!(targets[0].suffix_filter, Some(".jpg".to_string()));
}

#[test]
fn test_parse_notification_config_no_filters() {
    let xml = r#"<NotificationConfiguration>
        <LambdaFunctionConfiguration>
            <Function>arn:aws:lambda:us-east-1:123456789012:function:my-func</Function>
            <Event>s3:ObjectCreated:*</Event>
        </LambdaFunctionConfiguration>
    </NotificationConfiguration>"#;
    let targets = parse_notification_config(xml);
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].prefix_filter, None);
    assert_eq!(targets[0].suffix_filter, None);
}

#[test]
fn test_key_matches_filters() {
    // No filters — everything matches
    assert!(key_matches_filters("anything", &None, &None));

    // Prefix only
    assert!(key_matches_filters(
        "images/photo.jpg",
        &Some("images/".to_string()),
        &None
    ));
    assert!(!key_matches_filters(
        "docs/file.txt",
        &Some("images/".to_string()),
        &None
    ));

    // Suffix only
    assert!(key_matches_filters(
        "images/photo.jpg",
        &None,
        &Some(".jpg".to_string())
    ));
    assert!(!key_matches_filters(
        "images/photo.png",
        &None,
        &Some(".jpg".to_string())
    ));

    // Both prefix and suffix
    assert!(key_matches_filters(
        "images/photo.jpg",
        &Some("images/".to_string()),
        &Some(".jpg".to_string())
    ));
    assert!(!key_matches_filters(
        "images/photo.png",
        &Some("images/".to_string()),
        &Some(".jpg".to_string())
    ));
    assert!(!key_matches_filters(
        "docs/photo.jpg",
        &Some("images/".to_string()),
        &Some(".jpg".to_string())
    ));
}

#[test]
fn test_parse_cors_config() {
    let xml = r#"<CORSConfiguration>
        <CORSRule>
            <AllowedOrigin>https://example.com</AllowedOrigin>
            <AllowedMethod>GET</AllowedMethod>
            <AllowedMethod>PUT</AllowedMethod>
            <AllowedHeader>*</AllowedHeader>
            <ExposeHeader>x-amz-request-id</ExposeHeader>
            <MaxAgeSeconds>3600</MaxAgeSeconds>
        </CORSRule>
    </CORSConfiguration>"#;
    let rules = parse_cors_config(xml);
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].allowed_origins, vec!["https://example.com"]);
    assert_eq!(rules[0].allowed_methods, vec!["GET", "PUT"]);
    assert_eq!(rules[0].allowed_headers, vec!["*"]);
    assert_eq!(rules[0].expose_headers, vec!["x-amz-request-id"]);
    assert_eq!(rules[0].max_age_seconds, Some(3600));
}

#[test]
fn parse_cors_config_trims_pretty_printed_values() {
    // `put_bucket_cors` trims before validating, so a pretty-printed config is
    // accepted and stored with its surrounding whitespace. If the parser kept
    // it, no method and no origin would ever match and the bucket would go
    // silently CORS-dead.
    let xml = "<CORSConfiguration>
        <CORSRule>
            <AllowedOrigin>
                https://example.com
            </AllowedOrigin>
            <AllowedMethod>
                GET
            </AllowedMethod>
            <ExposeHeader>
                x-amz-request-id
            </ExposeHeader>
            <MaxAgeSeconds>
                3600
            </MaxAgeSeconds>
        </CORSRule>
    </CORSConfiguration>";
    let rules = parse_cors_config(xml);
    assert_eq!(rules[0].allowed_origins, vec!["https://example.com"]);
    assert_eq!(rules[0].allowed_methods, vec!["GET"]);
    assert_eq!(rules[0].expose_headers, vec!["x-amz-request-id"]);
    // Untrimmed this fails to parse, and the preflight loses its max-age.
    assert_eq!(rules[0].max_age_seconds, Some(3600));
    assert!(find_cors_rule(&rules, "https://example.com", "GET", &[]).is_some());
}

#[test]
fn find_cors_rule_matches_methods_exactly() {
    let xml = r#"<CORSConfiguration>
        <CORSRule>
            <AllowedOrigin>https://example.com</AllowedOrigin>
            <AllowedMethod>GET</AllowedMethod>
        </CORSRule>
    </CORSConfiguration>"#;
    let rules = parse_cors_config(xml);
    // `put_bucket_cors` only ever stores the canonical uppercase verbs, and
    // `Access-Control-Allow-Methods` is echoed from this list, which browsers
    // compare case-sensitively — so no normalization happens here.
    assert_eq!(rules[0].allowed_methods, vec!["GET"]);
    assert!(find_cors_rule(&rules, "https://example.com", "GET", &[]).is_some());
    // A method outside AllowedMethods is still denied, and a disallowed origin
    // is denied regardless of method.
    assert!(find_cors_rule(&rules, "https://example.com", "DELETE", &[]).is_none());
    assert!(find_cors_rule(&rules, "https://evil.com", "GET", &[]).is_none());
}

#[test]
fn find_cors_rule_checks_requested_headers() {
    let xml = r#"<CORSConfiguration>
        <CORSRule>
            <AllowedOrigin>https://example.com</AllowedOrigin>
            <AllowedMethod>PUT</AllowedMethod>
            <AllowedHeader>x-amz-meta-foo</AllowedHeader>
        </CORSRule>
    </CORSConfiguration>"#;
    let rules = parse_cors_config(xml);
    let allowed = ["X-Amz-Meta-Foo".to_string()];
    let denied = ["authorization".to_string()];
    // A preflight declaring only covered headers passes; one declaring a header
    // the rule never allows is denied, rather than approved with an
    // allow-headers list that does not contain what was asked for.
    assert!(find_cors_rule(&rules, "https://example.com", "PUT", &allowed).is_some());
    assert!(find_cors_rule(&rules, "https://example.com", "PUT", &denied).is_none());

    // A `*` AllowedHeader covers anything.
    let wild = parse_cors_config(
        r#"<CORSConfiguration><CORSRule>
            <AllowedOrigin>https://example.com</AllowedOrigin>
            <AllowedMethod>PUT</AllowedMethod>
            <AllowedHeader>*</AllowedHeader>
        </CORSRule></CORSConfiguration>"#,
    );
    assert!(find_cors_rule(&wild, "https://example.com", "PUT", &denied).is_some());
}

#[test]
fn find_cors_rule_denies_requested_headers_when_rule_allows_none() {
    let xml = r#"<CORSConfiguration>
        <CORSRule>
            <AllowedOrigin>https://example.com</AllowedOrigin>
            <AllowedMethod>PUT</AllowedMethod>
        </CORSRule>
    </CORSConfiguration>"#;
    let rules = parse_cors_config(xml);
    // A rule with no AllowedHeader covers no header. Browsers send
    // `Access-Control-Request-Headers: content-type` for any non-simple
    // content type, so such a config denies those preflights — matching S3,
    // which is why AllowedHeaders belongs in any config that posts JSON.
    assert!(find_cors_rule(&rules, "https://example.com", "PUT", &[]).is_some());
    assert!(find_cors_rule(
        &rules,
        "https://example.com",
        "PUT",
        &["content-type".to_string()]
    )
    .is_none());
}

#[test]
fn parse_cors_config_ignores_commented_out_rules() {
    // The stored body is the raw text, so the request-time parser has to skip
    // comments exactly as validation does. If it did not, a commented-out rule
    // would pass validation as absent and then go live, handing an
    // allow-origin to an origin and method the caller never enabled.
    let xml = "<CORSConfiguration>\
        <!-- <CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>DELETE</AllowedMethod></CORSRule> -->\
        <CORSRule>\
            <AllowedOrigin>https://app.example.com</AllowedOrigin>\
            <AllowedMethod>GET</AllowedMethod>\
        </CORSRule>\
    </CORSConfiguration>";
    let rules = parse_cors_config(xml);
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].allowed_methods, vec!["GET"]);
    assert!(find_cors_rule(&rules, "https://evil.example", "DELETE", &[]).is_none());
}

#[test]
fn test_header_matches() {
    // AWS permits one `*` per AllowedHeader; names are case-insensitive.
    assert!(header_matches("x-amz-meta-foo", "x-amz-*"));
    assert!(header_matches("X-Amz-Meta-Foo", "x-amz-*"));
    assert!(header_matches("authorization", "*"));
    assert!(header_matches("content-type", "Content-Type"));
    assert!(!header_matches("authorization", "x-amz-*"));
    // A prefix shorter than the pattern must not match by wrapping.
    assert!(!header_matches("x-amz", "x-amz-*"));
}

#[test]
fn wildcard_matchers_handle_more_than_one_star() {
    // `PutBucketCors` rejects multi-wildcard values, but a config stored by an
    // earlier build or restored from a snapshot never passed that check, so the
    // matchers must still evaluate it correctly instead of silently matching
    // nothing and leaving the bucket CORS-dead.
    assert!(origin_matches(
        "https://a.b.example.com",
        "https://*.*.example.com"
    ));
    assert!(!origin_matches(
        "https://a.example.com",
        "https://*.*.example.com"
    ));
    assert!(header_matches("x-amz-meta-foo", "x-*-meta-*"));
    assert!(!header_matches("x-amz-meta-foo", "x-*-other-*"));

    // Segments must not overlap: the tail cannot reuse bytes the head consumed.
    assert!(!origin_matches("ab", "a*b*c"));
    assert!(origin_matches("abc", "a*b*c"));

    // A bare wildcard still matches anything, including empty.
    assert!(origin_matches("", "*"));
    assert!(origin_matches("https://example.com", "*"));
    // An exact pattern still requires the whole value.
    assert!(!origin_matches(
        "https://example.com.evil",
        "https://example.com"
    ));
    assert!(!origin_matches(
        "https://example.com.evil",
        "https://*.example.com"
    ));

    // Regression: a middle segment longer than what is left of the value used
    // to slice past the end and panic, taking down the request.
    assert!(!origin_matches(
        "https://x.com",
        "https://*verylongmiddle*.com"
    ));
    assert!(!header_matches("x-a", "x-*-custom-header-*"));
    assert!(!origin_matches("", "*a*"));
}

#[test]
fn find_cors_rule_matches_the_request_as_a_whole() {
    // A rule matches on origin, method AND requested headers together, first
    // match winning. A broad rule that allows no headers must not shadow a
    // later one written specifically to permit `content-type` — denying there
    // would refuse exactly what the bucket owner configured.
    let xml = "<CORSConfiguration>\
        <CORSRule>\
            <AllowedOrigin>*</AllowedOrigin>\
            <AllowedMethod>PUT</AllowedMethod>\
        </CORSRule>\
        <CORSRule>\
            <AllowedOrigin>https://app.example.com</AllowedOrigin>\
            <AllowedMethod>PUT</AllowedMethod>\
            <AllowedHeader>content-type</AllowedHeader>\
        </CORSRule>\
    </CORSConfiguration>";
    let rules = parse_cors_config(xml);
    let requested = ["content-type".to_string()];
    let matched = find_cors_rule(&rules, "https://app.example.com", "PUT", &requested)
        .expect("the second rule allows this header");
    assert_eq!(matched.allowed_headers, vec!["content-type"]);
    // With no headers declared, the broad first rule wins.
    let matched = find_cors_rule(&rules, "https://app.example.com", "PUT", &[]).unwrap();
    assert_eq!(matched.allowed_origins, vec!["*"]);
    // A header no rule covers is still denied.
    assert!(find_cors_rule(
        &rules,
        "https://app.example.com",
        "PUT",
        &["authorization".to_string()]
    )
    .is_none());
}

#[test]
fn a_rule_listing_both_concrete_and_wildcard_origins_matches_the_concrete_one() {
    // The echoed allow-origin comes from the entry that matched, not from "does
    // this rule mention `*` anywhere". Echoing `*` here would drop
    // allow-credentials for an origin the rule names explicitly.
    let rules = parse_cors_config(
        "<CORSConfiguration><CORSRule>\
         <AllowedOrigin>https://app.example.com</AllowedOrigin>\
         <AllowedOrigin>*</AllowedOrigin>\
         <AllowedMethod>GET</AllowedMethod>\
         </CORSRule></CORSConfiguration>",
    );
    let rule = find_cors_rule(&rules, "https://app.example.com", "GET", &[]).unwrap();
    let matched = rule
        .allowed_origins
        .iter()
        .find(|o| origin_matches("https://app.example.com", o))
        .unwrap();
    assert_eq!(matched, "https://app.example.com");
}

#[test]
fn empty_origin_never_matches_even_a_wildcard_rule() {
    // An Origin that is absent, blank, or sent on several lines arrives as "".
    // A bare `*` would otherwise match it and hand back an allow-origin for a
    // request that declared no usable origin at all.
    let rules = parse_cors_config(
        "<CORSConfiguration><CORSRule>\
         <AllowedOrigin>*</AllowedOrigin>\
         <AllowedMethod>GET</AllowedMethod>\
         </CORSRule></CORSConfiguration>",
    );
    assert!(find_cors_rule(&rules, "", "GET", &[]).is_none());
    assert!(find_cors_rule(&rules, "https://anything.example", "GET", &[]).is_some());
}

#[test]
fn wildcard_matchers_do_not_panic_on_non_ascii() {
    // Callers only pass ASCII today, but the matcher must not become a
    // request-killing panic if that ever changes.
    assert!(!origin_matches("aé", "ab*"));
    assert!(!origin_matches("héllo", "h*l*o!"));
    assert!(!header_matches("x-é", "x-a*"));
    assert!(origin_matches("héllo", "h*o"));
}

#[test]
fn empty_allowed_method_cannot_approve_a_method_less_preflight() {
    // A preflight with no Access-Control-Request-Method arrives as "". An empty
    // <AllowedMethod/> next to a real one must not match it and hand back an
    // allow-origin.
    let xml = "<CORSConfiguration><CORSRule>\
        <AllowedOrigin>https://example.com</AllowedOrigin>\
        <AllowedMethod>GET</AllowedMethod>\
        <AllowedMethod></AllowedMethod>\
        </CORSRule></CORSConfiguration>";
    let rules = parse_cors_config(xml);
    assert_eq!(rules[0].allowed_methods, vec!["GET"]);
    assert!(find_cors_rule(&rules, "https://example.com", "", &[]).is_none());
    assert!(find_cors_rule(&rules, "https://example.com", "GET", &[]).is_some());
}

#[test]
fn test_origin_matches() {
    assert!(origin_matches("https://example.com", "https://example.com"));
    assert!(origin_matches("https://example.com", "*"));
    assert!(origin_matches("https://foo.example.com", "*.example.com"));
    assert!(!origin_matches("https://evil.com", "https://example.com"));
    // AWS's own documented form puts the wildcard mid-pattern; a
    // leading-`*`-only matcher would leave such a bucket CORS-dead.
    assert!(origin_matches(
        "https://app.example.com",
        "https://*.example.com"
    ));
    assert!(!origin_matches(
        "http://app.example.com",
        "https://*.example.com"
    ));
    assert!(!origin_matches(
        "https://app.evil.com",
        "https://*.example.com"
    ));
    // Origins are case-sensitive, unlike header names.
    assert!(!origin_matches(
        "https://EXAMPLE.com",
        "https://example.com"
    ));
}

/// Regression: resolve_object with versionId="null" must match objects
/// whose version_id is either None or Some("null").
#[test]
fn resolve_null_version_matches_both_none_and_null_string() {
    use crate::state::S3Bucket;
    use bytes::Bytes;
    use chrono::Utc;

    let mut b = S3Bucket::new("test", "us-east-1", "owner");

    // Helper to create a minimal S3Object
    let make_obj = |key: &str, vid: Option<&str>| crate::state::S3Object {
        key: key.to_string(),
        body: crate::state::memory_body(Bytes::from_static(b"x")),
        content_type: "text/plain".to_string(),
        etag: "\"abc\"".to_string(),
        size: 1,
        last_modified: Utc::now(),
        storage_class: "STANDARD".to_string(),
        version_id: vid.map(|s| s.to_string()),
        ..Default::default()
    };

    // Object with version_id = Some("null") (pre-versioning migrated)
    let obj = make_obj("file.txt", Some("null"));
    b.objects.insert("file.txt".to_string(), obj.clone());
    b.object_versions.insert("file.txt".to_string(), vec![obj]);

    let null_str = "null".to_string();
    let result = resolve_object(&b, "file.txt", Some(&null_str));
    assert!(
        result.is_ok(),
        "versionId=null should match version_id=Some(\"null\")"
    );

    // Object with version_id = None (true pre-versioning)
    let obj2 = make_obj("file2.txt", None);
    b.objects.insert("file2.txt".to_string(), obj2.clone());
    b.object_versions
        .insert("file2.txt".to_string(), vec![obj2]);

    let result2 = resolve_object(&b, "file2.txt", Some(&null_str));
    assert!(
        result2.is_ok(),
        "versionId=null should match version_id=None"
    );
}

#[test]
fn test_parse_replication_rules() {
    let xml = r#"<ReplicationConfiguration>
        <Role>arn:aws:iam::role/replication</Role>
        <Rule>
            <Status>Enabled</Status>
            <Filter><Prefix>logs/</Prefix></Filter>
            <Destination><Bucket>arn:aws:s3:::dest-bucket</Bucket></Destination>
        </Rule>
        <Rule>
            <Status>Disabled</Status>
            <Filter><Prefix></Prefix></Filter>
            <Destination><Bucket>arn:aws:s3:::other-bucket</Bucket></Destination>
        </Rule>
    </ReplicationConfiguration>"#;

    let rules = parse_replication_rules(xml);
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].status, "Enabled");
    assert_eq!(rules[0].prefix, "logs/");
    assert_eq!(rules[0].dest_bucket, "dest-bucket");
    assert_eq!(rules[1].status, "Disabled");
    assert_eq!(rules[1].prefix, "");
    assert_eq!(rules[1].dest_bucket, "other-bucket");
}

#[test]
fn test_parse_normalized_replication_rules() {
    // First, normalize the XML like the server does
    let input_xml = r#"<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::123456789012:role/replication-role</Role><Rule><ID>replicate-all</ID><Status>Enabled</Status><Filter><Prefix></Prefix></Filter><Destination><Bucket>arn:aws:s3:::repl-dest</Bucket></Destination></Rule></ReplicationConfiguration>"#;
    let normalized = normalize_replication_xml(input_xml);
    eprintln!("Normalized XML: {normalized}");
    let rules = parse_replication_rules(&normalized);
    assert_eq!(rules.len(), 1, "Expected 1 rule, got {}", rules.len());
    assert_eq!(rules[0].status, "Enabled");
    assert_eq!(rules[0].dest_bucket, "repl-dest");
}

/// Source object for the replication tests: `test-key` in a versioned
/// `source` bucket whose replication config is `config`.
fn replication_source(config: &str, owner: &str) -> crate::state::S3Bucket {
    let mut src = crate::state::S3Bucket::new("source", "us-east-1", owner);
    src.versioning = Some("Enabled".to_string());
    src.replication_config = Some(config.to_string());
    let obj = S3Object {
        key: "test-key".to_string(),
        body: crate::state::memory_body(Bytes::from_static(b"hello")),
        content_type: "text/plain".to_string(),
        etag: "abc".to_string(),
        size: 5,
        last_modified: Utc::now(),
        storage_class: "STANDARD".to_string(),
        version_id: Some("v1".to_string()),
        acl_owner_id: Some(owner.to_string()),
        ..Default::default()
    };
    src.objects.insert("test-key".to_string(), obj);
    src
}

/// A versioning-enabled bucket: replication only writes into those.
fn versioned_bucket(name: &str, owner: &str) -> crate::state::S3Bucket {
    let mut b = crate::state::S3Bucket::new(name, "us-east-1", owner);
    b.versioning = Some("Enabled".to_string());
    b
}

fn replicate(
    accounts: &mut fakecloud_core::multi_account::MultiAccountState<crate::state::S3State>,
    source_account: &str,
) {
    replicate_with_mode(accounts, source_account, fakecloud_core::auth::IamMode::Off);
}

fn replicate_with_mode(
    accounts: &mut fakecloud_core::multi_account::MultiAccountState<crate::state::S3State>,
    source_account: &str,
    mode: fakecloud_core::auth::IamMode,
) {
    let store = fakecloud_persistence::MemoryS3Store::new();
    let effects = crate::side_effects::S3SideEffects::new(Arc::new(DeliveryBus::new()), None, mode);
    crate::service::replication::replicate_through_store(
        accounts,
        source_account,
        Some(&store),
        "source",
        "test-key",
        Some(&effects),
    );
}

#[test]
fn test_replicate_object() {
    let mut accounts =
        fakecloud_core::multi_account::MultiAccountState::<crate::state::S3State>::new(
            "123456789012",
            "us-east-1",
            "",
        );
    let state = accounts.get_or_create("123456789012");
    state.buckets.insert(
        "source".to_string(),
        replication_source(
            "<ReplicationConfiguration>\
             <Rule><Status>Enabled</Status>\
             <Filter><Prefix></Prefix></Filter>\
             <Destination><Bucket>arn:aws:s3:::destination</Bucket></Destination>\
             </Rule></ReplicationConfiguration>",
            "owner",
        ),
    );
    state.buckets.insert(
        "destination".to_string(),
        versioned_bucket("destination", "owner"),
    );

    replicate(&mut accounts, "123456789012");

    let state = accounts.get("123456789012").unwrap();
    let dest_obj = state
        .buckets
        .get("destination")
        .unwrap()
        .objects
        .get("test-key");
    assert!(dest_obj.is_some());
    assert_eq!(
        state.read_body(&dest_obj.unwrap().body).unwrap(),
        Bytes::from_static(b"hello")
    );
}

/// The replica is a new version in a versioning-enabled destination: it
/// keeps the source version's id, and the object already at the key (written
/// before versioning) is kept as the "null" version instead of vanishing. A
/// destination whose versioning is suspended (or was never enabled) cannot
/// take replicas: nothing is written there and the source version is marked
/// FAILED ("If you disable versioning on the destination bucket, replication
/// fails", S3 User Guide).
#[test]
fn replica_follows_destination_versioning() {
    use crate::state::S3Bucket;

    for versioning in [Some("Enabled"), Some("Suspended"), None] {
        let source_written = Utc::now() - chrono::Duration::hours(1);
        let mut accounts =
            fakecloud_core::multi_account::MultiAccountState::<crate::state::S3State>::new(
                "123456789012",
                "us-east-1",
                "",
            );
        let state = accounts.get_or_create("123456789012");
        let mut src = replication_source(
            "<ReplicationConfiguration>\
                 <Rule><Status>Enabled</Status>\
                 <Filter><Prefix></Prefix></Filter>\
                 <Destination><Bucket>arn:aws:s3:::destination</Bucket></Destination>\
                 </Rule></ReplicationConfiguration>",
            "owner",
        );
        src.objects.get_mut("test-key").unwrap().last_modified = source_written;
        state.buckets.insert("source".to_string(), src);
        let mut dest = S3Bucket::new("destination", "us-east-1", "owner");
        dest.objects.insert(
            "test-key".to_string(),
            S3Object {
                key: "test-key".to_string(),
                body: crate::state::memory_body(Bytes::from_static(b"old")),
                ..Default::default()
            },
        );
        dest.versioning = versioning.map(str::to_string);
        state.buckets.insert("destination".to_string(), dest);

        replicate(&mut accounts, "123456789012");

        let state = accounts.get("123456789012").unwrap();
        let dest = state.buckets.get("destination").unwrap();
        let current = &dest.objects["test-key"];
        let source_status = state.buckets["source"].objects["test-key"]
            .replication_status
            .clone();
        if versioning != Some("Enabled") {
            assert_eq!(source_status.as_deref(), Some("FAILED"), "{versioning:?}");
            assert_eq!(
                state.read_body(&current.body).unwrap(),
                Bytes::from_static(b"old"),
                "{versioning:?}"
            );
            assert!(!dest.object_versions.contains_key("test-key"));
            continue;
        }
        assert_eq!(source_status.as_deref(), Some("COMPLETED"));
        assert_eq!(
            state.read_body(&current.body).unwrap(),
            Bytes::from_static(b"hello")
        );
        assert_eq!(current.replication_status.as_deref(), Some("REPLICA"));
        // Created at replication time, after the source version.
        assert!(current.last_modified > source_written);
        // The replica keeps the source version's id.
        assert_eq!(current.version_id.as_deref(), Some("v1"));
        let versions = &dest.object_versions["test-key"];
        assert_eq!(versions.len(), 2, "{versions:?}");
        assert_eq!(versions[0].version_id.as_deref(), Some("null"));
        assert_eq!(
            state.read_body(&versions[0].body).unwrap(),
            Bytes::from_static(b"old")
        );
        assert_eq!(versions[1].version_id.as_deref(), Some("v1"));
    }
}

/// A replica of a source version with no retention takes the destination
/// bucket's default retention; one whose source version carries a retention
/// keeps that retention instead.
#[test]
fn replica_takes_destination_default_retention_only_without_its_own() {
    use crate::state::S3Bucket;

    let source_until = Utc::now() + chrono::Duration::days(90);
    for source_lock in [None, Some(("COMPLIANCE", source_until))] {
        let mut accounts =
            fakecloud_core::multi_account::MultiAccountState::<crate::state::S3State>::new(
                "123456789012",
                "us-east-1",
                "",
            );
        let state = accounts.get_or_create("123456789012");
        let mut src = replication_source(
            "<ReplicationConfiguration>\
             <Rule><Status>Enabled</Status>\
             <Filter><Prefix></Prefix></Filter>\
             <Destination><Bucket>arn:aws:s3:::destination</Bucket></Destination>\
             </Rule></ReplicationConfiguration>",
            "owner",
        );
        if let Some((mode, until)) = source_lock {
            let obj = src.objects.get_mut("test-key").unwrap();
            obj.lock_mode = Some(mode.to_string());
            obj.lock_retain_until = Some(until);
        }
        state.buckets.insert("source".to_string(), src);
        let mut dest = S3Bucket::new("destination", "us-east-1", "owner");
        dest.versioning = Some("Enabled".to_string());
        dest.object_lock_config = Some(
            "<ObjectLockConfiguration><Rule><DefaultRetention><Mode>GOVERNANCE</Mode>\
             <Days>1</Days></DefaultRetention></Rule></ObjectLockConfiguration>"
                .to_string(),
        );
        state.buckets.insert("destination".to_string(), dest);

        let before = Utc::now();
        replicate(&mut accounts, "123456789012");

        let state = accounts.get("123456789012").unwrap();
        let replica = &state.buckets["destination"].objects["test-key"];
        match source_lock {
            None => {
                assert_eq!(replica.lock_mode.as_deref(), Some("GOVERNANCE"));
                let until = replica.lock_retain_until.unwrap();
                assert!(until >= before + chrono::Duration::days(1));
                assert!(until <= Utc::now() + chrono::Duration::days(1));
            }
            Some((mode, until)) => {
                assert_eq!(replica.lock_mode.as_deref(), Some(mode));
                assert_eq!(replica.lock_retain_until, Some(until));
            }
        }
    }
}

/// Bucket names are global: a rule whose destination bucket lives in another
/// account replicates into that account's bucket, and
/// `AccessControlTranslation` `Owner=Destination` hands the replica to the
/// destination owner.
#[test]
fn replicate_into_bucket_owned_by_another_account() {
    let mut accounts =
        fakecloud_core::multi_account::MultiAccountState::<crate::state::S3State>::new(
            "111111111111",
            "us-east-1",
            "",
        );
    accounts.get_or_create("111111111111").buckets.insert(
        "source".to_string(),
        replication_source(
            "<ReplicationConfiguration><Rule><Status>Enabled</Status>\
             <Filter><Prefix></Prefix></Filter>\
             <Destination><Bucket>arn:aws:s3:::dest-b</Bucket>\
             <Account>222222222222</Account>\
             <AccessControlTranslation><Owner>Destination</Owner></AccessControlTranslation>\
             </Destination></Rule></ReplicationConfiguration>",
            "111111111111",
        ),
    );
    accounts.get_or_create("222222222222").buckets.insert(
        "dest-b".to_string(),
        versioned_bucket("dest-b", "222222222222"),
    );

    replicate(&mut accounts, "111111111111");

    let b = accounts.get("222222222222").unwrap();
    let replica = b.buckets["dest-b"]
        .objects
        .get("test-key")
        .expect("replica lands in the destination owner's bucket");
    assert_eq!(
        b.read_body(&replica.body).unwrap(),
        Bytes::from_static(b"hello")
    );
    assert_eq!(replica.acl_owner_id.as_deref(), Some("222222222222"));
    assert_eq!(
        replica.acl_grants[0].grantee_id.as_deref(),
        Some("222222222222")
    );
}

/// Without ownership translation the replica keeps the source object's owner.
#[test]
fn replicate_cross_account_keeps_source_owner_without_translation() {
    let mut accounts =
        fakecloud_core::multi_account::MultiAccountState::<crate::state::S3State>::new(
            "111111111111",
            "us-east-1",
            "",
        );
    accounts.get_or_create("111111111111").buckets.insert(
        "source".to_string(),
        replication_source(
            "<ReplicationConfiguration><Rule><Status>Enabled</Status>\
             <Filter><Prefix></Prefix></Filter>\
             <Destination><Bucket>arn:aws:s3:::dest-b</Bucket></Destination>\
             </Rule></ReplicationConfiguration>",
            "111111111111",
        ),
    );
    accounts.get_or_create("222222222222").buckets.insert(
        "dest-b".to_string(),
        versioned_bucket("dest-b", "222222222222"),
    );

    replicate(&mut accounts, "111111111111");

    let replica = &accounts.get("222222222222").unwrap().buckets["dest-b"].objects["test-key"];
    assert_eq!(replica.acl_owner_id.as_deref(), Some("111111111111"));
}

/// A `Destination.Account` that does not own the destination bucket fails
/// the replication: nothing is written.
#[test]
fn replicate_skips_when_destination_account_does_not_own_bucket() {
    let mut accounts =
        fakecloud_core::multi_account::MultiAccountState::<crate::state::S3State>::new(
            "111111111111",
            "us-east-1",
            "",
        );
    accounts.get_or_create("111111111111").buckets.insert(
        "source".to_string(),
        replication_source(
            "<ReplicationConfiguration><Rule><Status>Enabled</Status>\
             <Filter><Prefix></Prefix></Filter>\
             <Destination><Bucket>arn:aws:s3:::dest-b</Bucket>\
             <Account>333333333333</Account></Destination>\
             </Rule></ReplicationConfiguration>",
            "111111111111",
        ),
    );
    accounts.get_or_create("222222222222").buckets.insert(
        "dest-b".to_string(),
        versioned_bucket("dest-b", "222222222222"),
    );

    replicate(&mut accounts, "111111111111");

    assert!(accounts.get("222222222222").unwrap().buckets["dest-b"]
        .objects
        .is_empty());
    assert_eq!(
        accounts.get("111111111111").unwrap().buckets["source"].objects["test-key"]
            .replication_status
            .as_deref(),
        Some("FAILED")
    );
}

const CROSS_ACCOUNT_REPLICATION: &str = "<ReplicationConfiguration>\
     <Role>arn:aws:iam::111111111111:role/replication</Role>\
     <Rule><Status>Enabled</Status><Filter><Prefix></Prefix></Filter>\
     <Destination><Bucket>arn:aws:s3:::dest-b</Bucket></Destination>\
     </Rule></ReplicationConfiguration>";

/// Source bucket in 111111111111 replicating into `dest-b` in 222222222222,
/// whose bucket policy is `policy`.
fn cross_account_replication(
    policy: Option<&str>,
) -> fakecloud_core::multi_account::MultiAccountState<crate::state::S3State> {
    let mut accounts =
        fakecloud_core::multi_account::MultiAccountState::<crate::state::S3State>::new(
            "111111111111",
            "us-east-1",
            "",
        );
    accounts.get_or_create("111111111111").buckets.insert(
        "source".to_string(),
        replication_source(CROSS_ACCOUNT_REPLICATION, "111111111111"),
    );
    let mut dest = versioned_bucket("dest-b", "222222222222");
    dest.policy = policy.map(str::to_string);
    accounts
        .get_or_create("222222222222")
        .buckets
        .insert("dest-b".to_string(), dest);
    accounts
}

fn source_status(
    accounts: &fakecloud_core::multi_account::MultiAccountState<crate::state::S3State>,
) -> Option<String> {
    accounts.get("111111111111").unwrap().buckets["source"].objects["test-key"]
        .replication_status
        .clone()
}

/// Under strict IAM, a destination bucket in another account without a
/// policy granting the replication role refuses the replica, and the source
/// is marked FAILED.
#[test]
fn replicate_cross_account_denied_without_bucket_policy_under_strict() {
    let mut accounts = cross_account_replication(None);
    replicate_with_mode(
        &mut accounts,
        "111111111111",
        fakecloud_core::auth::IamMode::Strict,
    );
    assert!(accounts.get("222222222222").unwrap().buckets["dest-b"]
        .objects
        .is_empty());
    assert_eq!(source_status(&accounts).as_deref(), Some("FAILED"));
}

/// A destination policy granting `s3:ReplicateObject` to the replication role
/// lets the replica through; the source is COMPLETED and the replica REPLICA.
#[test]
fn replicate_cross_account_allowed_by_bucket_policy_under_strict() {
    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
        "Principal":{"AWS":"arn:aws:iam::111111111111:role/replication"},
        "Action":["s3:ReplicateObject","s3:ReplicateDelete"],
        "Resource":"arn:aws:s3:::dest-b/*"}]}"#;
    let mut accounts = cross_account_replication(Some(policy));
    replicate_with_mode(
        &mut accounts,
        "111111111111",
        fakecloud_core::auth::IamMode::Strict,
    );
    let replica = &accounts.get("222222222222").unwrap().buckets["dest-b"].objects["test-key"];
    assert_eq!(replica.replication_status.as_deref(), Some("REPLICA"));
    assert_eq!(source_status(&accounts).as_deref(), Some("COMPLETED"));
}

/// A policy naming another principal does not authorize the role.
#[test]
fn replicate_cross_account_denied_when_policy_names_other_principal() {
    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
        "Principal":{"AWS":"arn:aws:iam::111111111111:role/other"},
        "Action":"s3:ReplicateObject","Resource":"arn:aws:s3:::dest-b/*"}]}"#;
    let mut accounts = cross_account_replication(Some(policy));
    replicate_with_mode(
        &mut accounts,
        "111111111111",
        fakecloud_core::auth::IamMode::Strict,
    );
    assert_eq!(source_status(&accounts).as_deref(), Some("FAILED"));
}

/// Under soft IAM the refusal is only logged: the replica is written.
#[test]
fn replicate_cross_account_soft_mode_logs_and_replicates() {
    let mut accounts = cross_account_replication(None);
    replicate_with_mode(
        &mut accounts,
        "111111111111",
        fakecloud_core::auth::IamMode::Soft,
    );
    assert!(accounts.get("222222222222").unwrap().buckets["dest-b"]
        .objects
        .contains_key("test-key"));
    assert_eq!(source_status(&accounts).as_deref(), Some("COMPLETED"));
}

#[test]
fn cors_header_value_does_not_panic_on_unusual_input() {
    // Verify that CORS header value parsing doesn't panic even with unusual strings.
    // HeaderValue::from_str rejects non-visible-ASCII, so our unwrap_or_else fallback
    // must produce a valid (empty) header value instead of panicking.
    let valid_origin = "https://example.com";
    let result: Result<http::HeaderValue, _> = valid_origin.parse();
    assert!(result.is_ok());

    // Non-ASCII would fail .parse() for HeaderValue; verify fallback works
    let bad_origin = "https://ex\x01ample.com";
    let result: Result<http::HeaderValue, _> = bad_origin.parse();
    assert!(result.is_err());
    // Our production code uses unwrap_or_else to return empty HeaderValue
    let fallback = bad_origin
        .parse()
        .unwrap_or_else(|_| http::HeaderValue::from_static(""));
    assert_eq!(fallback, "");
}

// ────────────────────────────────────────────────────────────────
// Service-level tests for tags / multipart / config submodules.
//
// Each helper below builds an isolated S3Service with the in-memory
// store so the submodule handlers can be driven directly without a
// running Axum router.
// ────────────────────────────────────────────────────────────────

use crate::state::{S3Bucket, S3Object};
use bytes::Bytes;
use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsServiceError};
use http::{HeaderMap, Method, StatusCode};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub(super) fn make_service() -> S3Service {
    let state: SharedS3State = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    S3Service::new(state, Arc::new(DeliveryBus::new()))
}

pub(super) fn seed_bucket(svc: &S3Service, name: &str) {
    let mut mas = svc.state.write();
    let state = mas.default_mut();
    state
        .buckets
        .insert(name.to_string(), S3Bucket::new(name, "us-east-1", "owner"));
}

pub(super) fn seed_object(svc: &S3Service, bucket: &str, key: &str, body: &[u8]) {
    let mut mas = svc.state.write();
    let state = mas.default_mut();
    let b = state.buckets.get_mut(bucket).expect("bucket seeded");
    let mut obj = S3Object {
        key: key.to_string(),
        body: fakecloud_persistence::BodyRef::Memory(Bytes::copy_from_slice(body)),
        content_type: "application/octet-stream".to_string(),
        etag: format!("\"{}\"", compute_md5(body)),
        size: body.len() as u64,
        last_modified: chrono::Utc::now(),
        ..Default::default()
    };
    obj.metadata.insert("version".to_string(), "1".to_string());
    b.objects.insert(key.to_string(), obj);
}

pub(super) fn make_request(
    method: Method,
    path: &str,
    query: &[(&str, &str)],
    body: &[u8],
) -> AwsRequest {
    let segments: Vec<String> = path
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    let query_params: HashMap<String, String> = query
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    let raw_query = query
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    // Wire body_stream from the same bytes so streaming-only handlers
    // (put_object, upload_part) can consume it. Buffered handlers
    // (put_object_tagging, put_object_acl, …) read `body` directly
    // and ignore the stream.
    let stream_body =
        fakecloud_core::service::RequestBodyStream::from(Bytes::copy_from_slice(body));
    AwsRequest {
        service: "s3".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test-req".to_string(),
        headers: HeaderMap::new(),
        query_params,
        body: Bytes::copy_from_slice(body),
        body_stream: parking_lot::Mutex::new(Some(stream_body)),
        path_segments: segments,
        raw_path: path.to_string(),
        raw_query,
        method,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

/// The base64 `Content-MD5` header value for `body`.
fn content_md5(body: &[u8]) -> String {
    crate::checksum::checksum(crate::checksum::ChecksumAlgorithm::Md5, body)
}

fn assert_aws_err(
    result: Result<AwsResponse, AwsServiceError>,
    expect_code: &str,
) -> AwsServiceError {
    let err = match result {
        Ok(_) => panic!("expected error, got Ok response"),
        Err(e) => e,
    };
    match &err {
        AwsServiceError::AwsError { code, .. } => {
            assert_eq!(code, expect_code, "wrong error code");
        }
        other => panic!("expected AwsError, got {other:?}"),
    }
    err
}

// ── Tags (service/tags.rs) ───────────────────────────────────────

#[test]
fn get_object_tagging_on_object_returns_xml_tagset() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "k", b"hello");
    {
        let mut mas = svc.state.write();
        let obj = mas
            .default_mut()
            .buckets
            .get_mut("b")
            .unwrap()
            .objects
            .get_mut("k")
            .unwrap();
        obj.tags.insert("env".to_string(), "prod".to_string());
        obj.tags.insert("team".to_string(), "plat".to_string());
    }

    let req = make_request(Method::GET, "/b/k", &[("tagging", "")], b"");
    let resp = svc
        .get_object_tagging("123456789012", &req, "b", "k")
        .unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Tag><Key>env</Key><Value>prod</Value></Tag>"));
    assert!(body.contains("<Tag><Key>team</Key><Value>plat</Value></Tag>"));
}

#[test]
fn get_object_tagging_missing_bucket_errors() {
    let svc = make_service();
    let req = make_request(Method::GET, "/nope/k", &[("tagging", "")], b"");
    assert_aws_err(
        svc.get_object_tagging("123456789012", &req, "nope", "k"),
        "NoSuchBucket",
    );
}

#[test]
fn put_object_tagging_rejects_aws_prefixed_key() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "k", b"x");

    let xml =
        r#"<Tagging><TagSet><Tag><Key>aws:internal</Key><Value>v</Value></Tag></TagSet></Tagging>"#;
    let req = make_request(Method::PUT, "/b/k", &[("tagging", "")], xml.as_bytes());
    assert_aws_err(
        svc.put_object_tagging("123456789012", &req, "b", "k"),
        "InvalidTag",
    );
}

#[test]
fn put_object_tagging_rejects_too_many_tags() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "k", b"x");

    let mut xml = String::from("<Tagging><TagSet>");
    for i in 0..11 {
        xml.push_str(&format!("<Tag><Key>k{i}</Key><Value>v</Value></Tag>"));
    }
    xml.push_str("</TagSet></Tagging>");
    let req = make_request(Method::PUT, "/b/k", &[("tagging", "")], xml.as_bytes());
    assert_aws_err(
        svc.put_object_tagging("123456789012", &req, "b", "k"),
        "BadRequest",
    );
}

#[test]
fn put_object_tagging_on_missing_object_errors() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let xml = r#"<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>"#;
    let req = make_request(
        Method::PUT,
        "/b/missing",
        &[("tagging", "")],
        xml.as_bytes(),
    );
    assert_aws_err(
        svc.put_object_tagging("123456789012", &req, "b", "missing"),
        "NoSuchKey",
    );
}

#[test]
fn put_object_tagging_replaces_existing_tags() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "k", b"x");
    {
        let mut mas = svc.state.write();
        let obj = mas
            .default_mut()
            .buckets
            .get_mut("b")
            .unwrap()
            .objects
            .get_mut("k")
            .unwrap();
        obj.tags.insert("old".to_string(), "gone".to_string());
    }

    let xml = r#"<Tagging><TagSet><Tag><Key>new</Key><Value>here</Value></Tag></TagSet></Tagging>"#;
    let req = make_request(Method::PUT, "/b/k", &[("tagging", "")], xml.as_bytes());
    let resp = svc
        .put_object_tagging("123456789012", &req, "b", "k")
        .unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let __mas = svc.state.read();
    let state = __mas.default_ref();
    let tags = &state
        .buckets
        .get("b")
        .unwrap()
        .objects
        .get("k")
        .unwrap()
        .tags;
    assert_eq!(tags.get("new").map(String::as_str), Some("here"));
    assert!(!tags.contains_key("old"));
}

#[test]
fn delete_object_tagging_clears_tags() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "k", b"x");
    {
        let mut mas = svc.state.write();
        let obj = mas
            .default_mut()
            .buckets
            .get_mut("b")
            .unwrap()
            .objects
            .get_mut("k")
            .unwrap();
        obj.tags.insert("env".to_string(), "prod".to_string());
    }

    let resp = svc.delete_object_tagging("123456789012", "b", "k").unwrap();
    assert_eq!(resp.status, StatusCode::NO_CONTENT);
    let __mas = svc.state.read();
    let state = __mas.default_ref();
    assert!(state
        .buckets
        .get("b")
        .unwrap()
        .objects
        .get("k")
        .unwrap()
        .tags
        .is_empty());
}

#[test]
fn delete_object_tagging_missing_key_errors() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    assert_aws_err(
        svc.delete_object_tagging("123456789012", "b", "gone"),
        "NoSuchKey",
    );
}

// ── Multipart (service/multipart.rs) ─────────────────────────────

fn initiate_mpu(svc: &S3Service, bucket: &str, key: &str) -> String {
    let req = make_request(
        Method::POST,
        &format!("/{bucket}/{key}"),
        &[("uploads", "")],
        b"",
    );
    let resp = svc
        .create_multipart_upload("123456789012", &req, bucket, key)
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let start = body.find("<UploadId>").unwrap() + "<UploadId>".len();
    let end = body.find("</UploadId>").unwrap();
    body[start..end].to_string()
}

#[test]
fn create_multipart_upload_records_upload_in_state() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let upload_id = initiate_mpu(&svc, "b", "big.bin");
    let __mas = svc.state.read();
    let state = __mas.default_ref();
    assert!(state
        .buckets
        .get("b")
        .unwrap()
        .multipart_uploads
        .contains_key(&upload_id));
}

#[test]
fn create_multipart_upload_rejects_acl_and_grants_combo() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let mut req = make_request(Method::POST, "/b/k", &[("uploads", "")], b"");
    req.headers.insert("x-amz-acl", "private".parse().unwrap());
    req.headers
        .insert("x-amz-grant-read", "id=owner".parse().unwrap());
    assert_aws_err(
        svc.create_multipart_upload("123456789012", &req, "b", "k"),
        "InvalidRequest",
    );
}

#[test]
fn create_multipart_upload_missing_bucket_errors() {
    let svc = make_service();
    let req = make_request(Method::POST, "/ghost/k", &[("uploads", "")], b"");
    assert_aws_err(
        svc.create_multipart_upload("123456789012", &req, "ghost", "k"),
        "NoSuchBucket",
    );
}

#[tokio::test]
async fn upload_part_rejects_invalid_part_number() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let upload_id = initiate_mpu(&svc, "b", "k");

    // part_number < 1 is an out-of-range argument: 400 InvalidArgument, same
    // as the > 10000 case (previously masked as a 404 NoSuchUpload).
    let req = make_request(Method::PUT, "/b/k", &[("partNumber", "0")], b"body");
    let err = svc
        .upload_part("123456789012", &req, "b", "k", &upload_id, 0)
        .await
        .err()
        .expect("partNumber 0 must error");
    assert_eq!(err.code(), "InvalidArgument");
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);

    // part_number > 10000 returns InvalidArgument.
    let req2 = make_request(Method::PUT, "/b/k", &[("partNumber", "10001")], b"body");
    assert_aws_err(
        svc.upload_part("123456789012", &req2, "b", "k", &upload_id, 10_001)
            .await,
        "InvalidArgument",
    );
}

#[tokio::test]
async fn upload_part_missing_upload_errors() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(Method::PUT, "/b/k", &[("partNumber", "1")], b"body");
    assert_aws_err(
        svc.upload_part("123456789012", &req, "b", "k", "not-an-upload", 1)
            .await,
        "NoSuchUpload",
    );
}

// PutObject's disk write is wrapped in run_blocking_io (bug-audit
// 2026-06-20, 3.2). On a multi-threaded runtime that path uses
// block_in_place, which panics if mis-guarded; this exercises it end-to-end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_object_runs_on_multi_thread_runtime() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let body = b"streamed-under-block-in-place";
    let req = make_request(Method::PUT, "/b/k", &[], body);
    let resp = svc
        .put_object("123456789012", &req, "b", "k")
        .await
        .expect("put_object should succeed on a multi-thread runtime");
    assert_eq!(resp.status, StatusCode::OK);

    let get_req = make_request(Method::GET, "/b/k", &[], b"");
    let got = svc.get_object("123456789012", &get_req, "b", "k").unwrap();
    assert_eq!(got.body.expect_bytes(), body);
}

#[tokio::test]
async fn mpu_full_lifecycle_creates_object() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let upload_id = initiate_mpu(&svc, "b", "k");

    // Single-part upload — CompleteMultipartUpload's MIN_PART_SIZE check
    // only applies to non-last parts, so a single part of any size works.
    let part_body = b"hello";
    let req = make_request(Method::PUT, "/b/k", &[("partNumber", "1")], part_body);
    let resp = svc
        .upload_part("123456789012", &req, "b", "k", &upload_id, 1)
        .await
        .unwrap();
    let etag = resp
        .headers
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let complete_xml = format!(
        r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"#,
    );
    let complete_req = make_request(
        Method::POST,
        "/b/k",
        &[("uploadId", &upload_id)],
        complete_xml.as_bytes(),
    );
    let resp = svc
        .complete_multipart_upload("123456789012", &complete_req, "b", "k", &upload_id)
        .unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let __mas = svc.state.read();
    let state = __mas.default_ref();
    let bucket = state.buckets.get("b").unwrap();
    let obj = bucket.objects.get("k").expect("object materialized");
    assert_eq!(obj.size, part_body.len() as u64);
    assert!(!bucket.multipart_uploads.contains_key(&upload_id));
}

/// 1.18: a multipart object with an additional checksum must report a
/// COMPOSITE checksum (`base64(HASH(concat(part raw digests)))-N`), not
/// a checksum over the whole reassembled object.
#[tokio::test]
async fn mpu_complete_uses_composite_additional_checksum() {
    let svc = make_service();
    seed_bucket(&svc, "comp");

    // Initiate with a SHA256 additional checksum.
    let init_req = make_request(Method::POST, "/comp/k", &[("uploads", "")], b"");
    let mut init_req = init_req;
    init_req.headers.insert(
        "x-amz-checksum-algorithm",
        http::HeaderValue::from_static("SHA256"),
    );
    let resp = svc
        .create_multipart_upload("123456789012", &init_req, "comp", "k")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let start = body.find("<UploadId>").unwrap() + "<UploadId>".len();
    let end = body.find("</UploadId>").unwrap();
    let upload_id = body[start..end].to_string();

    let part1 = vec![b'a'; 5 * 1024 * 1024];
    let part2 = b"second-part".to_vec();
    let mut etags = Vec::new();
    let mut checksums = Vec::new();
    for (n, data) in [(1u32, &part1), (2u32, &part2)] {
        let req = make_request(
            Method::PUT,
            "/comp/k",
            &[("partNumber", &n.to_string())],
            data,
        );
        let r = svc
            .upload_part("123456789012", &req, "comp", "k", &upload_id, n as i64)
            .await
            .unwrap();
        etags.push(r.headers.get("etag").unwrap().to_str().unwrap().to_string());
        // UploadPart returns the part checksum in the upload's algorithm.
        checksums.push(
            r.headers
                .get("x-amz-checksum-sha256")
                .expect("part checksum header")
                .to_str()
                .unwrap()
                .to_string(),
        );
    }

    let complete_xml = format!(
        r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag><ChecksumSHA256>{}</ChecksumSHA256></Part><Part><PartNumber>2</PartNumber><ETag>{}</ETag><ChecksumSHA256>{}</ChecksumSHA256></Part></CompleteMultipartUpload>"#,
        etags[0], checksums[0], etags[1], checksums[1],
    );
    let complete_req = make_request(
        Method::POST,
        "/comp/k",
        &[("uploadId", &upload_id)],
        complete_xml.as_bytes(),
    );
    svc.complete_multipart_upload("123456789012", &complete_req, "comp", "k", &upload_id)
        .unwrap();

    let sha256 = crate::checksum::ChecksumAlgorithm::Sha256;
    let expected = crate::checksum::composite(
        sha256,
        &[
            crate::checksum::digest(sha256, &part1),
            crate::checksum::digest(sha256, &part2),
        ],
    )
    .unwrap();
    assert!(
        expected.ends_with("-2"),
        "composite checksum must carry -N suffix"
    );

    let __mas = svc.state.read();
    let state = __mas.default_ref();
    let obj = state.buckets.get("comp").unwrap().objects.get("k").unwrap();
    assert_eq!(
        obj.checksum_value.as_deref(),
        Some(expected.as_str()),
        "stored checksum must be the COMPOSITE checksum-of-checksums, not a whole-object checksum"
    );

    // Sanity: it must NOT equal a single checksum over the whole object.
    let mut whole = part1.clone();
    whole.extend_from_slice(&part2);
    let whole_ck = crate::checksum::checksum(sha256, &whole);
    assert_ne!(obj.checksum_value.as_deref(), Some(whole_ck.as_str()));
}

/// 1.18: composite checksum helper round-trips for every supported
/// additional-checksum algorithm.
#[test]
fn compute_composite_checksum_supports_all_algorithms() {
    for algo in crate::checksum::ChecksumAlgorithm::ALL {
        let p1 = crate::checksum::digest(algo, b"part-one");
        let p2 = crate::checksum::digest(algo, b"part-two");
        assert_eq!(p1.len(), algo.digest_len(), "{algo} raw digest");
        let composite = crate::checksum::composite(algo, &[p1, p2]).unwrap();
        assert!(composite.ends_with("-2"), "{algo} composite suffix");
    }
    assert!(crate::checksum::composite(crate::checksum::ChecksumAlgorithm::Sha256, &[]).is_none());
}

/// 1.19: DeleteObjects must reject >1000 keys with a 400 MalformedXML.
#[tokio::test]
async fn delete_objects_rejects_over_1000_keys() {
    let svc = make_service();
    seed_bucket(&svc, "bdel-limit");

    let mut xml = String::from("<Delete>");
    for i in 0..1001 {
        xml.push_str(&format!("<Object><Key>k{i}.txt</Key></Object>"));
    }
    xml.push_str("</Delete>");
    let req = make_request(
        Method::POST,
        "/bdel-limit",
        &[("delete", "")],
        xml.as_bytes(),
    );
    assert_aws_err(
        svc.delete_objects("123456789012", &req, "bdel-limit"),
        "MalformedXML",
    );
}

/// bug-hunt 2026-06-13, finding 4.1: CompleteMultipartUpload assembles
/// parts off-lock from a cloned snapshot. In disk mode a concurrent
/// UploadPart of the same part number rewrites the fixed `part-NNNNN.bin`
/// file *after* the snapshot, so the bytes read no longer match the
/// snapshot's recorded etag. Complete must fail closed (InvalidPart)
/// rather than assemble bytes that don't match the parts the client
/// committed. We reproduce the divergence directly by mutating the stored
/// part body so it no longer hashes to the recorded etag, then completing
/// with that (now-stale) etag — which passes the submitted-etag check but
/// must be caught by the body re-verification.
#[tokio::test]
async fn mpu_complete_fails_closed_when_part_bytes_diverge_from_etag() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let upload_id = initiate_mpu(&svc, "b", "k");

    let req = make_request(
        Method::PUT,
        "/b/k",
        &[("partNumber", "1")],
        b"original-bytes",
    );
    let resp = svc
        .upload_part("123456789012", &req, "b", "k", &upload_id, 1)
        .await
        .unwrap();
    let etag = resp
        .headers
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Simulate a concurrent same-part overwrite that lands after the upload
    // recorded its etag: the stored bytes change, the etag stays stale.
    {
        let mut mas = svc.state.write();
        let part = mas
            .default_mut()
            .buckets
            .get_mut("b")
            .unwrap()
            .multipart_uploads
            .get_mut(&upload_id)
            .unwrap()
            .parts
            .get_mut(&1u32)
            .unwrap();
        part.body =
            fakecloud_persistence::BodyRef::Memory(Bytes::from_static(b"DIFFERENT-tampered-bytes"));
    }

    let complete_xml = format!(
        r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"#,
    );
    let complete_req = make_request(
        Method::POST,
        "/b/k",
        &[("uploadId", &upload_id)],
        complete_xml.as_bytes(),
    );
    assert_aws_err(
        svc.complete_multipart_upload("123456789012", &complete_req, "b", "k", &upload_id),
        "InvalidPart",
    );
}

#[tokio::test]
async fn mpu_complete_rejects_small_non_last_part() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let upload_id = initiate_mpu(&svc, "b", "k");

    for n in 1..=2 {
        let body = format!("part{n}");
        let req = make_request(
            Method::PUT,
            "/b/k",
            &[("partNumber", &n.to_string())],
            body.as_bytes(),
        );
        svc.upload_part("123456789012", &req, "b", "k", &upload_id, n)
            .await
            .unwrap();
    }

    // Grab the etags from state.
    let (etag1, etag2) = {
        let __mas = svc.state.read();
        let state = __mas.default_ref();
        let parts = &state
            .buckets
            .get("b")
            .unwrap()
            .multipart_uploads
            .get(&upload_id)
            .unwrap()
            .parts;
        (
            parts.get(&1).unwrap().etag.clone(),
            parts.get(&2).unwrap().etag.clone(),
        )
    };

    let complete_xml = format!(
        r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"#,
    );
    let complete_req = make_request(
        Method::POST,
        "/b/k",
        &[("uploadId", &upload_id)],
        complete_xml.as_bytes(),
    );
    assert_aws_err(
        svc.complete_multipart_upload("123456789012", &complete_req, "b", "k", &upload_id),
        "EntityTooSmall",
    );
}

#[test]
fn abort_multipart_upload_removes_upload() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let upload_id = initiate_mpu(&svc, "b", "k");
    let resp = svc
        .abort_multipart_upload("123456789012", "b", "k", &upload_id)
        .unwrap();
    assert_eq!(resp.status, StatusCode::NO_CONTENT);
    let __mas = svc.state.read();
    let state = __mas.default_ref();
    assert!(!state
        .buckets
        .get("b")
        .unwrap()
        .multipart_uploads
        .contains_key(&upload_id));
}

#[test]
fn abort_multipart_upload_unknown_id_errors() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    assert_aws_err(
        svc.abort_multipart_upload("123456789012", "b", "k", "no-such"),
        "NoSuchUpload",
    );
}

#[test]
fn list_multipart_uploads_includes_all_in_flight() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let u1 = initiate_mpu(&svc, "b", "a");
    let u2 = initiate_mpu(&svc, "b", "b");
    let resp = svc
        .list_multipart_uploads("123456789012", "b", &std::collections::HashMap::new())
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains(&u1));
    assert!(body.contains(&u2));
}

#[tokio::test]
async fn list_parts_after_upload_returns_parts() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let upload_id = initiate_mpu(&svc, "b", "k");
    let req = make_request(Method::PUT, "/b/k", &[("partNumber", "1")], b"data");
    svc.upload_part("123456789012", &req, "b", "k", &upload_id, 1)
        .await
        .unwrap();

    let list_req = make_request(Method::GET, "/b/k", &[("uploadId", &upload_id)], b"");
    let resp = svc
        .list_parts("123456789012", &list_req, "b", "k", &upload_id)
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<PartNumber>1</PartNumber>"));
}

// ── Config (service/config.rs) ───────────────────────────────────

#[test]
fn bucket_encryption_put_get_delete_round_trip() {
    let svc = make_service();
    seed_bucket(&svc, "b");

    let xml = r#"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"#;
    let req = make_request(Method::PUT, "/b", &[("encryption", "")], xml.as_bytes());
    let resp = svc
        .put_bucket_encryption("123456789012", &req, "b")
        .unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Normalized body should include BucketKeyEnabled=false.
    let get = svc.get_bucket_encryption("123456789012", "b").unwrap();
    let body = std::str::from_utf8(get.body.expect_bytes()).unwrap();
    assert!(body.contains("AES256"));
    assert!(body.contains("<BucketKeyEnabled>false</BucketKeyEnabled>"));

    let del = svc.delete_bucket_encryption("123456789012", "b").unwrap();
    assert_eq!(del.status, StatusCode::NO_CONTENT);
    assert_aws_err(
        svc.get_bucket_encryption("123456789012", "b"),
        "ServerSideEncryptionConfigurationNotFoundError",
    );
}

#[test]
fn bucket_policy_rejects_malformed_json() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(Method::PUT, "/b", &[("policy", "")], b"not-json");
    assert_aws_err(
        svc.put_bucket_policy("123456789012", &req, "b"),
        "MalformedPolicy",
    );
}

#[test]
fn bucket_policy_put_get_delete_round_trip() {
    let svc = make_service();
    seed_bucket(&svc, "b");

    let body = br#"{"Version":"2012-10-17","Statement":[]}"#;
    let put_req = make_request(Method::PUT, "/b", &[("policy", "")], body);
    let resp = svc
        .put_bucket_policy("123456789012", &put_req, "b")
        .unwrap();
    assert_eq!(resp.status, StatusCode::NO_CONTENT);

    let get = svc.get_bucket_policy("123456789012", "b").unwrap();
    assert_eq!(get.body.expect_bytes(), body);

    let del = svc.delete_bucket_policy("123456789012", "b").unwrap();
    assert_eq!(del.status, StatusCode::NO_CONTENT);
    assert_aws_err(
        svc.get_bucket_policy("123456789012", "b"),
        "NoSuchBucketPolicy",
    );
}

#[test]
fn bucket_lifecycle_empty_rules_clears_config() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    {
        let mut __mas = svc.state.write();
        let state = __mas.default_mut();
        state.buckets.get_mut("b").unwrap().lifecycle_config = Some("placeholder".to_string());
    }
    let req = make_request(
        Method::PUT,
        "/b",
        &[("lifecycle", "")],
        b"<LifecycleConfiguration></LifecycleConfiguration>",
    );
    svc.put_bucket_lifecycle("123456789012", &req, "b").unwrap();
    let __mas = svc.state.read();
    let state = __mas.default_ref();
    assert!(state.buckets.get("b").unwrap().lifecycle_config.is_none());
}

#[test]
fn bucket_cors_put_get_delete_round_trip() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let xml = br#"<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*</AllowedOrigin></CORSRule></CORSConfiguration>"#;
    let req = make_request(Method::PUT, "/b", &[("cors", "")], xml);
    svc.put_bucket_cors("123456789012", &req, "b").unwrap();
    let got = svc.get_bucket_cors("123456789012", "b").unwrap();
    assert!(std::str::from_utf8(got.body.expect_bytes())
        .unwrap()
        .contains("CORSConfiguration"));
    svc.delete_bucket_cors("123456789012", "b").unwrap();
    assert_aws_err(
        svc.get_bucket_cors("123456789012", "b"),
        "NoSuchCORSConfiguration",
    );
}

#[test]
fn bucket_versioning_put_and_get() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(
        Method::PUT,
        "/b",
        &[("versioning", "")],
        b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
    );
    svc.put_bucket_versioning("123456789012", &req, "b")
        .unwrap();

    let __mas = svc.state.read();
    let state = __mas.default_ref();
    assert_eq!(
        state.buckets.get("b").unwrap().versioning.as_deref(),
        Some("Enabled")
    );
    drop(__mas);

    let resp = svc.get_bucket_versioning("123456789012", "b").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Status>Enabled</Status>"));
}

#[test]
fn bucket_tagging_put_get_delete_round_trip() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(
        Method::PUT,
        "/b",
        &[("tagging", "")],
        br#"<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>"#,
    );
    svc.put_bucket_tagging("123456789012", &req, "b").unwrap();
    let get_req = make_request(Method::GET, "/b", &[("tagging", "")], b"");
    let got = svc
        .get_bucket_tagging("123456789012", &get_req, "b")
        .unwrap();
    assert!(std::str::from_utf8(got.body.expect_bytes())
        .unwrap()
        .contains("<Key>env</Key>"));
    let del_req = make_request(Method::DELETE, "/b", &[("tagging", "")], b"");
    svc.delete_bucket_tagging("123456789012", &del_req, "b")
        .unwrap();
    assert_aws_err(
        svc.get_bucket_tagging("123456789012", &get_req, "b"),
        "NoSuchTagSet",
    );
}

#[test]
fn bucket_accelerate_rejects_invalid_status() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(
        Method::PUT,
        "/b",
        &[("accelerate", "")],
        b"<AccelerateConfiguration><Status>Bogus</Status></AccelerateConfiguration>",
    );
    assert_aws_err(
        svc.put_bucket_accelerate("123456789012", &req, "b"),
        "MalformedXML",
    );
}

#[test]
fn bucket_accelerate_enabled_is_persisted() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(
        Method::PUT,
        "/b",
        &[("accelerate", "")],
        b"<AccelerateConfiguration><Status>Enabled</Status></AccelerateConfiguration>",
    );
    svc.put_bucket_accelerate("123456789012", &req, "b")
        .unwrap();
    let got = svc.get_bucket_accelerate("123456789012", "b").unwrap();
    assert!(std::str::from_utf8(got.body.expect_bytes())
        .unwrap()
        .contains("<Status>Enabled</Status>"));
}

#[test]
fn public_access_block_put_get_delete_round_trip() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let body = br#"<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>"#;
    let req = make_request(Method::PUT, "/b", &[("publicAccessBlock", "")], body);
    svc.put_public_access_block("123456789012", &req, "b")
        .unwrap();
    let got = svc.get_public_access_block("123456789012", "b").unwrap();
    assert!(std::str::from_utf8(got.body.expect_bytes())
        .unwrap()
        .contains("<BlockPublicAcls>true</BlockPublicAcls>"));
    svc.delete_public_access_block("123456789012", "b").unwrap();
    assert_aws_err(
        svc.get_public_access_block("123456789012", "b"),
        "NoSuchPublicAccessBlockConfiguration",
    );
}

#[test]
fn block_public_policy_rejects_wildcard_principal_policy() {
    // PAB.BlockPublicPolicy=true must reject PutBucketPolicy that
    // grants Allow to Principal "*" — the canonical "public read"
    // shape — with AccessDenied.
    let svc = make_service();
    seed_bucket(&svc, "locked");

    let pab_body = br#"<PublicAccessBlockConfiguration><BlockPublicPolicy>true</BlockPublicPolicy></PublicAccessBlockConfiguration>"#;
    let req = make_request(
        Method::PUT,
        "/locked",
        &[("publicAccessBlock", "")],
        pab_body,
    );
    svc.put_public_access_block("123456789012", &req, "locked")
        .unwrap();

    let policy = br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::locked/*"}]}"#;
    let req = make_request(Method::PUT, "/locked", &[("policy", "")], policy);
    let err = match svc.put_bucket_policy("123456789012", &req, "locked") {
        Err(e) => e,
        Ok(_) => panic!("expected BlockPublicPolicy to reject public policy"),
    };
    assert_eq!(err.status(), StatusCode::FORBIDDEN);
}

#[test]
fn block_public_policy_allows_non_public_policy() {
    // A policy that names a specific AWS principal must not be
    // blocked even when BlockPublicPolicy is set.
    let svc = make_service();
    seed_bucket(&svc, "locked");

    let pab_body = br#"<PublicAccessBlockConfiguration><BlockPublicPolicy>true</BlockPublicPolicy></PublicAccessBlockConfiguration>"#;
    let req = make_request(
        Method::PUT,
        "/locked",
        &[("publicAccessBlock", "")],
        pab_body,
    );
    svc.put_public_access_block("123456789012", &req, "locked")
        .unwrap();

    let policy = br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:role/r"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::locked/*"}]}"#;
    let req = make_request(Method::PUT, "/locked", &[("policy", "")], policy);
    svc.put_bucket_policy("123456789012", &req, "locked")
        .expect("named-principal policy should be allowed");
}

#[test]
fn block_public_acls_rejects_canned_public_read() {
    // PAB.BlockPublicAcls=true must reject `x-amz-acl: public-read`
    // on PutBucketAcl, which adds an AllUsers READ grant.
    let svc = make_service();
    seed_bucket(&svc, "locked");

    let pab_body = br#"<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls></PublicAccessBlockConfiguration>"#;
    let req = make_request(
        Method::PUT,
        "/locked",
        &[("publicAccessBlock", "")],
        pab_body,
    );
    svc.put_public_access_block("123456789012", &req, "locked")
        .unwrap();

    let mut req = make_request(Method::PUT, "/locked", &[("acl", "")], &[]);
    req.headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    let err = match svc.put_bucket_acl("123456789012", &req, "locked") {
        Err(e) => e,
        Ok(_) => panic!("expected BlockPublicAcls to reject public-read canned ACL"),
    };
    assert_eq!(err.status(), StatusCode::FORBIDDEN);
}

#[test]
fn bucket_website_put_get_delete_round_trip() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(
        Method::PUT,
        "/b",
        &[("website", "")],
        b"<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument></WebsiteConfiguration>",
    );
    svc.put_bucket_website("123456789012", &req, "b").unwrap();
    let got = svc.get_bucket_website("123456789012", "b").unwrap();
    assert!(std::str::from_utf8(got.body.expect_bytes())
        .unwrap()
        .contains("<Suffix>index.html</Suffix>"));
    svc.delete_bucket_website("123456789012", "b").unwrap();
    assert_aws_err(
        svc.get_bucket_website("123456789012", "b"),
        "NoSuchWebsiteConfiguration",
    );
}

#[test]
fn bucket_replication_requires_existing_bucket() {
    let svc = make_service();
    let req = make_request(Method::PUT, "/nope", &[("replication", "")], b"<x/>");
    assert_aws_err(
        svc.put_bucket_replication("123456789012", &req, "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn bucket_ownership_controls_put_get_delete_round_trip() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(
        Method::PUT,
        "/b",
        &[("ownershipControls", "")],
        b"<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
    );
    svc.put_bucket_ownership_controls("123456789012", &req, "b")
        .unwrap();
    let got = svc
        .get_bucket_ownership_controls("123456789012", "b")
        .unwrap();
    assert!(std::str::from_utf8(got.body.expect_bytes())
        .unwrap()
        .contains("BucketOwnerEnforced"));
    svc.delete_bucket_ownership_controls("123456789012", "b")
        .unwrap();
    assert_aws_err(
        svc.get_bucket_ownership_controls("123456789012", "b"),
        "OwnershipControlsNotFoundError",
    );
}

// ── Error branch tests: object operations ──

#[test]
fn get_object_nonexistent_bucket() {
    // GetObject's Smithy `errors` declares NoSuchKey + InvalidObjectState;
    // missing-bucket collapses into NoSuchKey for strict conformance.
    let svc = make_service();
    let req = make_request(Method::GET, "/no-bucket/key", &[], b"");
    assert_aws_err(
        svc.get_object("123456789012", &req, "no-bucket", "key"),
        "NoSuchKey",
    );
}

#[tokio::test]
async fn get_object_blocked_by_pab_ignore_public_acls() {
    let svc = make_service();
    seed_bucket(&svc, "b");

    let mut put_req = make_request(Method::PUT, "/b/k", &[], b"hello");
    put_req
        .headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    svc.put_object("123456789012", &put_req, "b", "k")
        .await
        .unwrap();

    // Anonymous GET works before PAB is enabled.
    let get_req = make_request(Method::GET, "/b/k", &[], b"");
    svc.get_object("123456789012", &get_req, "b", "k").unwrap();

    // Enable PAB with IgnorePublicAcls=true.
    let pab_xml = br#"<PublicAccessBlockConfiguration><IgnorePublicAcls>true</IgnorePublicAcls></PublicAccessBlockConfiguration>"#;
    let pab_req = make_request(Method::PUT, "/b", &[("publicAccessBlock", "")], pab_xml);
    svc.put_public_access_block("123456789012", &pab_req, "b")
        .unwrap();

    // Anonymous GET now rejected.
    let get_req = make_request(Method::GET, "/b/k", &[], b"");
    assert_aws_err(
        svc.get_object("123456789012", &get_req, "b", "k"),
        "AccessDenied",
    );

    // Authenticated GET still allowed.
    let mut authed = make_request(Method::GET, "/b/k", &[], b"");
    authed.access_key_id = Some("AKIA-TEST".to_string());
    svc.get_object("123456789012", &authed, "b", "k").unwrap();
}

#[test]
fn get_object_nonexistent_key() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(Method::GET, "/b/missing", &[], b"");
    assert_aws_err(
        svc.get_object("123456789012", &req, "b", "missing"),
        "NoSuchKey",
    );
}

#[tokio::test]
async fn put_object_key_too_long() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let long_key = "x".repeat(1025);
    let req = make_request(Method::PUT, &format!("/b/{long_key}"), &[], b"data");
    assert_aws_err(
        svc.put_object("123456789012", &req, "b", &long_key).await,
        "KeyTooLongError",
    );
}

#[tokio::test]
async fn put_object_with_aws_tag_prefix() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let mut req = make_request(Method::PUT, "/b/tagged", &[], b"data");
    req.headers
        .insert("x-amz-tagging", "aws:reserved=nope".parse().unwrap());
    assert_aws_err(
        svc.put_object("123456789012", &req, "b", "tagged").await,
        "InvalidTag",
    );
}

#[tokio::test]
async fn put_object_acl_and_grant_conflict() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let mut req = make_request(Method::PUT, "/b/conflict", &[], b"data");
    req.headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    req.headers
        .insert("x-amz-grant-read", "id=abc123".parse().unwrap());
    assert_aws_err(
        svc.put_object("123456789012", &req, "b", "conflict").await,
        "InvalidRequest",
    );
}

#[test]
fn head_object_nonexistent_key() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(Method::HEAD, "/b/missing", &[], b"");
    // HeadObject's Smithy model declares only `NotFound` as the error
    // shape (com.amazonaws.s3#HeadObject -> errors: [NotFound]); HEAD
    // responses carry no body so SDKs read the code from
    // `x-amz-error-code`, which must match the declared shape.
    assert_aws_err(
        svc.head_object("123456789012", &req, "b", "missing"),
        "NotFound",
    );
}

#[test]
fn head_object_nonexistent_bucket() {
    let svc = make_service();
    let req = make_request(Method::HEAD, "/nope/key", &[], b"");
    assert_aws_err(
        svc.head_object("123456789012", &req, "nope", "key"),
        "NotFound",
    );
}

#[test]
fn delete_object_nonexistent_bucket() {
    let svc = make_service();
    let req = make_request(Method::DELETE, "/nope/key", &[], b"");
    assert_aws_err(
        svc.delete_object("123456789012", &req, "nope", "key"),
        "NoSuchBucket",
    );
}

#[test]
fn copy_object_source_not_found() {
    let svc = make_service();
    seed_bucket(&svc, "src-b");
    seed_bucket(&svc, "dst-b");
    let mut req = make_request(Method::PUT, "/dst-b/copied", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "src-b/nonexistent".parse().unwrap());
    assert_aws_err(
        svc.copy_object("123456789012", &req, "dst-b", "copied"),
        "NoSuchKey",
    );
}

#[test]
fn copy_object_source_bucket_not_found() {
    let svc = make_service();
    seed_bucket(&svc, "dst-b2");
    let mut req = make_request(Method::PUT, "/dst-b2/copied", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "nope-bucket/key".parse().unwrap());
    assert_aws_err(
        svc.copy_object("123456789012", &req, "dst-b2", "copied"),
        "NoSuchBucket",
    );
}

#[test]
fn list_objects_v2_nonexistent_bucket() {
    let svc = make_service();
    let req = make_request(Method::GET, "/nope", &[("list-type", "2")], b"");
    assert_aws_err(
        svc.list_objects_v2("123456789012", &req, "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn list_objects_v2_empty_continuation_token() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(
        Method::GET,
        "/b",
        &[("list-type", "2"), ("continuation-token", "")],
        b"",
    );
    assert_aws_err(
        svc.list_objects_v2("123456789012", &req, "b"),
        "InvalidArgument",
    );
}

#[test]
fn list_objects_v1_nonexistent_bucket() {
    let svc = make_service();
    let req = make_request(Method::GET, "/nope", &[], b"");
    assert_aws_err(
        svc.list_objects_v1("123456789012", &req, "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn list_object_versions_nonexistent_bucket() {
    let svc = make_service();
    let req = make_request(Method::GET, "/nope", &[("versions", "")], b"");
    assert_aws_err(
        svc.list_object_versions("123456789012", &req, "nope"),
        "NoSuchBucket",
    );
}

// ── Error branch tests: multipart operations ──

#[test]
fn create_multipart_nonexistent_bucket() {
    let svc = make_service();
    let req = make_request(Method::POST, "/nope/key", &[("uploads", "")], b"");
    assert_aws_err(
        svc.create_multipart_upload("123456789012", &req, "nope", "key"),
        "NoSuchBucket",
    );
}

#[tokio::test]
async fn upload_part_nonexistent_upload() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(
        Method::PUT,
        "/b/key",
        &[("uploadId", "bogus"), ("partNumber", "1")],
        b"data",
    );
    assert_aws_err(
        svc.upload_part("123456789012", &req, "b", "key", "bogus", 1)
            .await,
        "NoSuchUpload",
    );
}

#[test]
fn complete_multipart_nonexistent_upload() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(
        Method::POST,
        "/b/key",
        &[("uploadId", "bogus")],
        b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"abc\"</ETag></Part></CompleteMultipartUpload>",
    );
    assert_aws_err(
        svc.complete_multipart_upload("123456789012", &req, "b", "key", "bogus"),
        "NoSuchUpload",
    );
}

#[test]
fn abort_multipart_nonexistent_upload() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    assert_aws_err(
        svc.abort_multipart_upload("123456789012", "b", "key", "bogus"),
        "NoSuchUpload",
    );
}

#[test]
fn list_parts_nonexistent_upload() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(Method::GET, "/b/key", &[("uploadId", "bogus")], b"");
    assert_aws_err(
        svc.list_parts("123456789012", &req, "b", "key", "bogus"),
        "NoSuchUpload",
    );
}

// ── Error branch tests: config operations ──

#[test]
fn get_bucket_acl_nonexistent() {
    let svc = make_service();
    let req = make_request(Method::GET, "/nope", &[("acl", "")], b"");
    assert_aws_err(
        svc.get_bucket_acl("123456789012", &req, "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_bucket_versioning_nonexistent() {
    let svc = make_service();
    assert_aws_err(
        svc.get_bucket_versioning("123456789012", "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn put_bucket_versioning_nonexistent() {
    let svc = make_service();
    let req = make_request(
        Method::PUT,
        "/nope",
        &[("versioning", "")],
        b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
    );
    assert_aws_err(
        svc.put_bucket_versioning("123456789012", &req, "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_bucket_location_nonexistent() {
    let svc = make_service();
    assert_aws_err(
        svc.get_bucket_location("123456789012", "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_bucket_lifecycle_nonexistent() {
    let svc = make_service();
    assert_aws_err(
        svc.get_bucket_lifecycle("123456789012", "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_bucket_notification_nonexistent() {
    let svc = make_service();
    assert_aws_err(
        svc.get_bucket_notification("123456789012", "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_bucket_encryption_nonexistent() {
    let svc = make_service();
    assert_aws_err(
        svc.get_bucket_encryption("123456789012", "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_bucket_logging_nonexistent() {
    let svc = make_service();
    assert_aws_err(
        svc.get_bucket_logging("123456789012", "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_object_lock_nonexistent() {
    let svc = make_service();
    assert_aws_err(
        svc.get_object_lock_configuration("123456789012", "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_object_attributes_nonexistent_key() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(Method::GET, "/b/missing", &[("attributes", "")], b"");
    assert_aws_err(
        svc.get_object_attributes("123456789012", &req, "b", "missing"),
        "NoSuchKey",
    );
}

#[test]
fn get_object_attributes_nonexistent_bucket() {
    // GetObjectAttributes only declares NoSuchKey in its Smithy `errors`.
    let svc = make_service();
    let req = make_request(Method::GET, "/nope/key", &[("attributes", "")], b"");
    assert_aws_err(
        svc.get_object_attributes("123456789012", &req, "nope", "key"),
        "NoSuchKey",
    );
}

#[test]
fn restore_object_nonexistent_bucket() {
    let svc = make_service();
    let req = make_request(
        Method::POST,
        "/nope/key",
        &[("restore", "")],
        b"<RestoreRequest><Days>1</Days></RestoreRequest>",
    );
    assert_aws_err(
        svc.restore_object("123456789012", &req, "nope", "key"),
        "NoSuchBucket",
    );
}

#[test]
fn get_public_access_block_nonexistent() {
    let svc = make_service();
    assert_aws_err(
        svc.get_public_access_block("123456789012", "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_bucket_policy_nonexistent() {
    let svc = make_service();
    assert_aws_err(
        svc.get_bucket_policy("123456789012", "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_bucket_cors_nonexistent() {
    let svc = make_service();
    assert_aws_err(svc.get_bucket_cors("123456789012", "nope"), "NoSuchBucket");
}

#[test]
fn get_bucket_tagging_nonexistent() {
    let svc = make_service();
    let req = make_request(Method::GET, "/nope", &[("tagging", "")], b"");
    assert_aws_err(
        svc.get_bucket_tagging("123456789012", &req, "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_bucket_website_nonexistent() {
    let svc = make_service();
    assert_aws_err(
        svc.get_bucket_website("123456789012", "nope"),
        "NoSuchBucket",
    );
}

// ── Object lock (lock.rs - 0% coverage) ──

#[test]
fn put_and_get_object_retention() {
    let svc = make_service();
    seed_bucket(&svc, "lock-b");
    seed_object(&svc, "lock-b", "retained.txt", b"data");

    let body = b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>";
    let req = make_request(
        Method::PUT,
        "/lock-b/retained.txt",
        &[("retention", "")],
        body,
    );
    svc.put_object_retention("123456789012", &req, "lock-b", "retained.txt")
        .unwrap();

    let req = make_request(
        Method::GET,
        "/lock-b/retained.txt",
        &[("retention", "")],
        b"",
    );
    let resp = svc
        .get_object_retention("123456789012", &req, "lock-b", "retained.txt")
        .unwrap();
    let body_str = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body_str.contains("GOVERNANCE"));
}

#[test]
fn get_object_retention_nonexistent_bucket() {
    let svc = make_service();
    let req = make_request(Method::GET, "/nope/key", &[("retention", "")], b"");
    assert_aws_err(
        svc.get_object_retention("123456789012", &req, "nope", "key"),
        "NoSuchBucket",
    );
}

#[test]
fn get_object_retention_nonexistent_key() {
    let svc = make_service();
    seed_bucket(&svc, "lock-b2");
    let req = make_request(Method::GET, "/lock-b2/missing", &[("retention", "")], b"");
    assert_aws_err(
        svc.get_object_retention("123456789012", &req, "lock-b2", "missing"),
        "NoSuchKey",
    );
}

#[test]
fn put_and_get_object_legal_hold() {
    let svc = make_service();
    seed_bucket(&svc, "hold-b");
    seed_object(&svc, "hold-b", "held.txt", b"data");

    let body = b"<LegalHold><Status>ON</Status></LegalHold>";
    let req = make_request(Method::PUT, "/hold-b/held.txt", &[("legal-hold", "")], body);
    svc.put_object_legal_hold("123456789012", &req, "hold-b", "held.txt")
        .unwrap();

    let req = make_request(Method::GET, "/hold-b/held.txt", &[("legal-hold", "")], b"");
    let resp = svc
        .get_object_legal_hold("123456789012", &req, "hold-b", "held.txt")
        .unwrap();
    let body_str = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body_str.contains("ON"));
}

#[test]
fn get_object_legal_hold_nonexistent() {
    let svc = make_service();
    let req = make_request(Method::GET, "/nope/key", &[("legal-hold", "")], b"");
    assert_aws_err(
        svc.get_object_legal_hold("123456789012", &req, "nope", "key"),
        "NoSuchBucket",
    );
}

// ── Object ACL (acl.rs - 0% coverage) ──

#[test]
fn get_object_acl_default() {
    let svc = make_service();
    seed_bucket(&svc, "acl-b");
    seed_object(&svc, "acl-b", "file.txt", b"data");

    let req = make_request(Method::GET, "/acl-b/file.txt", &[("acl", "")], b"");
    let resp = svc
        .get_object_acl("123456789012", &req, "acl-b", "file.txt")
        .unwrap();
    let body_str = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body_str.contains("AccessControlPolicy"));
}

#[test]
fn get_object_acl_nonexistent_bucket() {
    // GetObjectAcl only declares NoSuchKey in its Smithy `errors`.
    let svc = make_service();
    let req = make_request(Method::GET, "/nope/key", &[("acl", "")], b"");
    assert_aws_err(
        svc.get_object_acl("123456789012", &req, "nope", "key"),
        "NoSuchKey",
    );
}

#[test]
fn get_object_acl_nonexistent_key() {
    let svc = make_service();
    seed_bucket(&svc, "acl-b2");
    let req = make_request(Method::GET, "/acl-b2/missing", &[("acl", "")], b"");
    assert_aws_err(
        svc.get_object_acl("123456789012", &req, "acl-b2", "missing"),
        "NoSuchKey",
    );
}

#[test]
fn put_object_acl() {
    let svc = make_service();
    seed_bucket(&svc, "acl-put-b");
    seed_object(&svc, "acl-put-b", "file.txt", b"data");

    let acl_xml = b"<AccessControlPolicy><Owner><ID>owner</ID></Owner><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>owner</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>";
    let req = make_request(Method::PUT, "/acl-put-b/file.txt", &[("acl", "")], acl_xml);
    svc.put_object_acl("123456789012", &req, "acl-put-b", "file.txt")
        .unwrap();
}

// ── Happy-path handler tests (objects.rs coverage) ──

#[tokio::test]
async fn put_object_via_handler_and_get_back() {
    let svc = make_service();
    seed_bucket(&svc, "hp");

    // PUT through handler (not seed_object)
    let req = make_request(Method::PUT, "/hp/test.txt", &[], b"hello world");
    let resp = svc
        .put_object("123456789012", &req, "hp", "test.txt")
        .await
        .unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // GET back
    let req = make_request(Method::GET, "/hp/test.txt", &[], b"");
    let resp = svc
        .get_object("123456789012", &req, "hp", "test.txt")
        .unwrap();
    assert_eq!(resp.body.expect_bytes(), b"hello world");
}

#[tokio::test]
async fn put_object_with_content_type() {
    let svc = make_service();
    seed_bucket(&svc, "ct");

    let mut req = make_request(Method::PUT, "/ct/doc.json", &[], b"{\"key\":\"val\"}");
    req.headers
        .insert("content-type", "application/json".parse().unwrap());
    svc.put_object("123456789012", &req, "ct", "doc.json")
        .await
        .unwrap();

    let req = make_request(Method::GET, "/ct/doc.json", &[], b"");
    let resp = svc
        .get_object("123456789012", &req, "ct", "doc.json")
        .unwrap();
    assert_eq!(resp.content_type, "application/json");
}

#[tokio::test]
async fn put_object_with_metadata() {
    let svc = make_service();
    seed_bucket(&svc, "meta");

    let mut req = make_request(Method::PUT, "/meta/obj", &[], b"data");
    req.headers
        .insert("x-amz-meta-color", "blue".parse().unwrap());
    req.headers
        .insert("x-amz-meta-size", "large".parse().unwrap());
    svc.put_object("123456789012", &req, "meta", "obj")
        .await
        .unwrap();

    let req = make_request(Method::HEAD, "/meta/obj", &[], b"");
    let resp = svc
        .head_object("123456789012", &req, "meta", "obj")
        .unwrap();
    assert!(resp
        .headers
        .get("x-amz-meta-color")
        .is_some_and(|v| v == "blue"));
}

#[tokio::test]
async fn put_object_returns_etag() {
    let svc = make_service();
    seed_bucket(&svc, "etag");

    let req = make_request(Method::PUT, "/etag/f.txt", &[], b"content");
    let resp = svc
        .put_object("123456789012", &req, "etag", "f.txt")
        .await
        .unwrap();
    assert!(resp.headers.get("etag").is_some());
}

#[tokio::test]
async fn head_object_returns_headers() {
    let svc = make_service();
    seed_bucket(&svc, "head");

    let req = make_request(Method::PUT, "/head/f.txt", &[], b"12345");
    svc.put_object("123456789012", &req, "head", "f.txt")
        .await
        .unwrap();

    let req = make_request(Method::HEAD, "/head/f.txt", &[], b"");
    let resp = svc
        .head_object("123456789012", &req, "head", "f.txt")
        .unwrap();
    assert_eq!(
        resp.headers
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap(),
        "5"
    );
}

#[tokio::test]
async fn head_object_defaults_sse_and_gates_checksum_on_mode() {
    let svc = make_service();
    seed_bucket(&svc, "sse");

    // PUT with a CRC32 checksum requested (the Go SDK does this automatically).
    let mut put = make_request(Method::PUT, "/sse/f.txt", &[], b"12345");
    put.headers.insert(
        "x-amz-checksum-algorithm",
        http::HeaderValue::from_static("CRC32"),
    );
    svc.put_object("123456789012", &put, "sse", "f.txt")
        .await
        .unwrap();

    // HEAD without checksum mode: SSE defaults to AES256, no checksum header.
    let req = make_request(Method::HEAD, "/sse/f.txt", &[], b"");
    let resp = svc
        .head_object("123456789012", &req, "sse", "f.txt")
        .unwrap();
    assert_eq!(
        resp.headers.get("x-amz-server-side-encryption").unwrap(),
        "AES256"
    );
    assert!(
        resp.headers.get("x-amz-checksum-crc32").is_none(),
        "checksum must not be returned without x-amz-checksum-mode: ENABLED"
    );

    // HEAD with checksum mode enabled: the stored checksum is returned.
    let mut req = make_request(Method::HEAD, "/sse/f.txt", &[], b"");
    req.headers.insert(
        "x-amz-checksum-mode",
        http::HeaderValue::from_static("ENABLED"),
    );
    let resp = svc
        .head_object("123456789012", &req, "sse", "f.txt")
        .unwrap();
    assert!(resp.headers.get("x-amz-checksum-crc32").is_some());
}

#[tokio::test]
async fn delete_object_via_handler() {
    let svc = make_service();
    seed_bucket(&svc, "del");

    let req = make_request(Method::PUT, "/del/rm.txt", &[], b"bye");
    svc.put_object("123456789012", &req, "del", "rm.txt")
        .await
        .unwrap();

    let req = make_request(Method::DELETE, "/del/rm.txt", &[], b"");
    svc.delete_object("123456789012", &req, "del", "rm.txt")
        .unwrap();

    let req = make_request(Method::GET, "/del/rm.txt", &[], b"");
    assert_aws_err(
        svc.get_object("123456789012", &req, "del", "rm.txt"),
        "NoSuchKey",
    );
}

#[tokio::test]
async fn copy_object_via_handler() {
    let svc = make_service();
    seed_bucket(&svc, "cpsrc");
    seed_bucket(&svc, "cpdst");

    let req = make_request(Method::PUT, "/cpsrc/orig.txt", &[], b"original");
    svc.put_object("123456789012", &req, "cpsrc", "orig.txt")
        .await
        .unwrap();

    let mut req = make_request(Method::PUT, "/cpdst/copy.txt", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "cpsrc/orig.txt".parse().unwrap());
    svc.copy_object("123456789012", &req, "cpdst", "copy.txt")
        .unwrap();

    let req = make_request(Method::GET, "/cpdst/copy.txt", &[], b"");
    let resp = svc
        .get_object("123456789012", &req, "cpdst", "copy.txt")
        .unwrap();
    assert_eq!(resp.body.expect_bytes(), b"original");
}

#[tokio::test]
async fn copy_object_within_same_bucket() {
    let svc = make_service();
    seed_bucket(&svc, "same");

    let req = make_request(Method::PUT, "/same/a.txt", &[], b"aaa");
    svc.put_object("123456789012", &req, "same", "a.txt")
        .await
        .unwrap();

    let mut req = make_request(Method::PUT, "/same/b.txt", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "same/a.txt".parse().unwrap());
    svc.copy_object("123456789012", &req, "same", "b.txt")
        .unwrap();

    let req = make_request(Method::GET, "/same/b.txt", &[], b"");
    let resp = svc
        .get_object("123456789012", &req, "same", "b.txt")
        .unwrap();
    assert_eq!(resp.body.expect_bytes(), b"aaa");
}

#[tokio::test]
async fn list_objects_v2_via_handler() {
    let svc = make_service();
    seed_bucket(&svc, "lsv2");

    for i in 0..3 {
        let key = format!("file{i}.txt");
        let req = make_request(Method::PUT, &format!("/lsv2/{key}"), &[], b"data");
        svc.put_object("123456789012", &req, "lsv2", &key)
            .await
            .unwrap();
    }

    let req = make_request(Method::GET, "/lsv2", &[("list-type", "2")], b"");
    let resp = svc.list_objects_v2("123456789012", &req, "lsv2").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<KeyCount>3</KeyCount>"));
}

#[tokio::test]
async fn list_objects_v2_with_prefix() {
    let svc = make_service();
    seed_bucket(&svc, "pfx");

    for key in &["docs/a.txt", "docs/b.txt", "images/c.png"] {
        let req = make_request(Method::PUT, &format!("/pfx/{key}"), &[], b"x");
        svc.put_object("123456789012", &req, "pfx", key)
            .await
            .unwrap();
    }

    let req = make_request(
        Method::GET,
        "/pfx",
        &[("list-type", "2"), ("prefix", "docs/")],
        b"",
    );
    let resp = svc.list_objects_v2("123456789012", &req, "pfx").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<KeyCount>2</KeyCount>"));
}

#[tokio::test]
async fn list_objects_v2_with_delimiter() {
    let svc = make_service();
    seed_bucket(&svc, "dlm");

    for key in &["a/1.txt", "a/2.txt", "b/3.txt", "root.txt"] {
        let req = make_request(Method::PUT, &format!("/dlm/{key}"), &[], b"x");
        svc.put_object("123456789012", &req, "dlm", key)
            .await
            .unwrap();
    }

    let req = make_request(
        Method::GET,
        "/dlm",
        &[("list-type", "2"), ("delimiter", "/")],
        b"",
    );
    let resp = svc.list_objects_v2("123456789012", &req, "dlm").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    // Should have common prefixes a/ and b/
    assert!(body.contains("<CommonPrefixes>"));
    // root.txt should be in contents
    assert!(body.contains("root.txt"));
}

#[tokio::test]
async fn list_objects_v1_via_handler() {
    let svc = make_service();
    seed_bucket(&svc, "lsv1");

    let req = make_request(Method::PUT, "/lsv1/test.txt", &[], b"data");
    svc.put_object("123456789012", &req, "lsv1", "test.txt")
        .await
        .unwrap();

    let req = make_request(Method::GET, "/lsv1", &[], b"");
    let resp = svc.list_objects_v1("123456789012", &req, "lsv1").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Key>test.txt</Key>"));
}

#[tokio::test]
async fn delete_objects_batch() {
    let svc = make_service();
    seed_bucket(&svc, "bdel");

    for i in 0..3 {
        let key = format!("d{i}.txt");
        let req = make_request(Method::PUT, &format!("/bdel/{key}"), &[], b"x");
        svc.put_object("123456789012", &req, "bdel", &key)
            .await
            .unwrap();
    }

    let delete_xml =
        b"<Delete><Object><Key>d0.txt</Key></Object><Object><Key>d1.txt</Key></Object></Delete>";
    let req = make_request(Method::POST, "/bdel", &[("delete", "")], delete_xml);
    let resp = svc.delete_objects("123456789012", &req, "bdel").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Deleted>"));

    // d2.txt should still exist
    let req = make_request(Method::GET, "/bdel/d2.txt", &[], b"");
    svc.get_object("123456789012", &req, "bdel", "d2.txt")
        .unwrap();
}

#[tokio::test]
async fn put_object_overwrites_existing() {
    let svc = make_service();
    seed_bucket(&svc, "ow");

    let req = make_request(Method::PUT, "/ow/f.txt", &[], b"version1");
    svc.put_object("123456789012", &req, "ow", "f.txt")
        .await
        .unwrap();

    let req = make_request(Method::PUT, "/ow/f.txt", &[], b"version2");
    svc.put_object("123456789012", &req, "ow", "f.txt")
        .await
        .unwrap();

    let req = make_request(Method::GET, "/ow/f.txt", &[], b"");
    let resp = svc.get_object("123456789012", &req, "ow", "f.txt").unwrap();
    assert_eq!(resp.body.expect_bytes(), b"version2");
}

#[tokio::test]
async fn get_object_attributes_via_handler() {
    let svc = make_service();
    seed_bucket(&svc, "attr");

    let req = make_request(Method::PUT, "/attr/f.txt", &[], b"content");
    svc.put_object("123456789012", &req, "attr", "f.txt")
        .await
        .unwrap();

    let mut req = make_request(Method::GET, "/attr/f.txt", &[], b"");
    req.headers.insert(
        "x-amz-object-attributes",
        "ETag,ObjectSize".parse().unwrap(),
    );
    let resp = svc
        .get_object_attributes("123456789012", &req, "attr", "f.txt")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<ObjectSize>"));
}

// ── Multipart upload happy path ──

#[tokio::test]
async fn multipart_upload_lifecycle() {
    let svc = make_service();
    seed_bucket(&svc, "mp");

    // Create
    let req = make_request(Method::POST, "/mp/big.bin", &[("uploads", "")], b"");
    let resp = svc
        .create_multipart_upload("123456789012", &req, "mp", "big.bin")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let uid_start = body.find("<UploadId>").unwrap() + 10;
    let uid_end = body.find("</UploadId>").unwrap();
    let upload_id = &body[uid_start..uid_end];

    // Upload part 1 (>5MB to pass minimum size check)
    let big_data = vec![b'A'; 5 * 1024 * 1024 + 1];
    let req = make_request(Method::PUT, "/mp/big.bin", &[], &big_data);
    let resp = svc
        .upload_part("123456789012", &req, "mp", "big.bin", upload_id, 1)
        .await
        .unwrap();
    let etag1 = resp
        .headers
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Upload part 2 (last part can be any size)
    let req = make_request(Method::PUT, "/mp/big.bin", &[], b"part2-data");
    let resp = svc
        .upload_part("123456789012", &req, "mp", "big.bin", upload_id, 2)
        .await
        .unwrap();
    let etag2 = resp
        .headers
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // List parts
    let req = make_request(Method::GET, "/mp/big.bin", &[], b"");
    let resp = svc
        .list_parts("123456789012", &req, "mp", "big.bin", upload_id)
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Part>"));

    // Complete
    let complete_xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"
    );
    let req = make_request(Method::POST, "/mp/big.bin", &[], complete_xml.as_bytes());
    svc.complete_multipart_upload("123456789012", &req, "mp", "big.bin", upload_id)
        .unwrap();

    // Verify object exists
    let req = make_request(Method::GET, "/mp/big.bin", &[], b"");
    let resp = svc
        .get_object("123456789012", &req, "mp", "big.bin")
        .unwrap();
    let body = resp.body.expect_bytes();
    // First part is 5MB+1 of 'A', second is "part2-data"
    assert!(body.len() > 5 * 1024 * 1024);
}

#[test]
fn multipart_upload_abort() {
    let svc = make_service();
    seed_bucket(&svc, "mpa");

    let req = make_request(Method::POST, "/mpa/abort.bin", &[("uploads", "")], b"");
    let resp = svc
        .create_multipart_upload("123456789012", &req, "mpa", "abort.bin")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let uid_start = body.find("<UploadId>").unwrap() + 10;
    let uid_end = body.find("</UploadId>").unwrap();
    let upload_id = body[uid_start..uid_end].to_string();

    svc.abort_multipart_upload("123456789012", "mpa", "abort.bin", &upload_id)
        .unwrap();

    // Upload should be gone
    let req = make_request(Method::GET, "/mpa/abort.bin", &[], b"");
    assert_aws_err(
        svc.list_parts("123456789012", &req, "mpa", "abort.bin", &upload_id),
        "NoSuchUpload",
    );
}

#[test]
fn list_multipart_uploads() {
    let svc = make_service();
    seed_bucket(&svc, "mpl");

    let req = make_request(Method::POST, "/mpl/f1.bin", &[("uploads", "")], b"");
    svc.create_multipart_upload("123456789012", &req, "mpl", "f1.bin")
        .unwrap();

    let resp = svc
        .list_multipart_uploads("123456789012", "mpl", &std::collections::HashMap::new())
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Upload>"));
    assert!(body.contains("f1.bin"));
}

#[test]
fn list_multipart_uploads_common_prefixes_count_toward_max_uploads() {
    let svc = make_service();
    seed_bucket(&svc, "mpcp");
    for key in ["a.bin", "dir1/x", "dir1/y", "dir2/z", "zz.bin"] {
        let req = make_request(
            Method::POST,
            &format!("/mpcp/{key}"),
            &[("uploads", "")],
            b"",
        );
        svc.create_multipart_upload("123456789012", &req, "mpcp", key)
            .unwrap();
    }
    let list = |params: &[(&str, &str)]| -> String {
        let q: std::collections::HashMap<String, String> = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let resp = svc
            .list_multipart_uploads("123456789012", "mpcp", &q)
            .unwrap();
        std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string()
    };
    // Entries in order: a.bin, dir1/, dir2/, zz.bin. Page of 2 = a.bin + dir1/.
    let p1 = list(&[("delimiter", "/"), ("max-uploads", "2")]);
    assert!(p1.contains("<IsTruncated>true</IsTruncated>"), "{p1}");
    assert_eq!(p1.matches("<Upload>").count(), 1, "{p1}");
    assert!(p1.contains("<Key>a.bin</Key>"), "{p1}");
    assert_eq!(p1.matches("<CommonPrefixes>").count(), 1, "{p1}");
    assert!(p1.contains("<Prefix>dir1/</Prefix>"), "{p1}");
    assert!(p1.contains("<NextKeyMarker>dir1/</NextKeyMarker>"), "{p1}");
    assert!(!p1.contains("<NextUploadIdMarker>"), "{p1}");

    // Resuming from the CommonPrefix marker does not repeat dir1/.
    let p2 = list(&[
        ("delimiter", "/"),
        ("max-uploads", "2"),
        ("key-marker", "dir1/"),
    ]);
    assert!(!p2.contains("<Prefix>dir1/</Prefix>"), "{p2}");
    assert!(p2.contains("<Prefix>dir2/</Prefix>"), "{p2}");
    assert!(p2.contains("<Key>zz.bin</Key>"), "{p2}");
    assert!(p2.contains("<IsTruncated>false</IsTruncated>"), "{p2}");

    // Exactly max-uploads entries in total is not truncated.
    let all = list(&[("delimiter", "/"), ("max-uploads", "4")]);
    assert!(all.contains("<IsTruncated>false</IsTruncated>"), "{all}");
    let over = list(&[("delimiter", "/"), ("max-uploads", "3")]);
    assert!(over.contains("<IsTruncated>true</IsTruncated>"), "{over}");
    assert!(
        over.contains("<NextKeyMarker>dir2/</NextKeyMarker>"),
        "{over}"
    );
    assert!(!over.contains("zz.bin"), "{over}");
}

// ── Config handler happy paths ──

#[test]
fn put_and_get_bucket_versioning() {
    let svc = make_service();
    seed_bucket(&svc, "ver");

    let req = make_request(
        Method::PUT,
        "/ver",
        &[("versioning", "")],
        b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
    );
    svc.put_bucket_versioning("123456789012", &req, "ver")
        .unwrap();

    let resp = svc.get_bucket_versioning("123456789012", "ver").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("Enabled"));
}

#[test]
fn put_and_get_bucket_lifecycle() {
    let svc = make_service();
    seed_bucket(&svc, "lc");

    let xml = b"<LifecycleConfiguration><Rule><ID>expire</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>";
    let req = make_request(Method::PUT, "/lc", &[("lifecycle", "")], xml);
    svc.put_bucket_lifecycle("123456789012", &req, "lc")
        .unwrap();

    let resp = svc.get_bucket_lifecycle("123456789012", "lc").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Rule>"));
}

#[test]
fn put_and_get_bucket_notification() {
    let svc = make_service();
    seed_bucket(&svc, "notif");

    let xml = b"<NotificationConfiguration></NotificationConfiguration>";
    let req = make_request(Method::PUT, "/notif", &[("notification", "")], xml);
    svc.put_bucket_notification("123456789012", &req, "notif")
        .unwrap();

    let resp = svc
        .get_bucket_notification("123456789012", "notif")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("NotificationConfiguration"));
}

#[test]
fn put_and_get_and_delete_bucket_encryption() {
    let svc = make_service();
    seed_bucket(&svc, "enc");

    let xml = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";
    let req = make_request(Method::PUT, "/enc", &[("encryption", "")], xml);
    svc.put_bucket_encryption("123456789012", &req, "enc")
        .unwrap();

    let resp = svc.get_bucket_encryption("123456789012", "enc").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("AES256"));

    svc.delete_bucket_encryption("123456789012", "enc").unwrap();
}

#[test]
fn bucket_logging_put_and_get() {
    let svc = make_service();
    seed_bucket(&svc, "logging-b");

    let xml = b"<BucketLoggingStatus><LoggingEnabled><TargetBucket>logging-b</TargetBucket><TargetPrefix>logs/</TargetPrefix></LoggingEnabled></BucketLoggingStatus>";
    let req = make_request(Method::PUT, "/logging-b", &[("logging", "")], xml);
    svc.put_bucket_logging("123456789012", &req, "logging-b")
        .unwrap();

    let resp = svc.get_bucket_logging("123456789012", "logging-b").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("LoggingEnabled"));
}

// ── Versioned object operations ──

#[tokio::test]
async fn versioned_put_and_get() {
    let svc = make_service();
    seed_bucket(&svc, "vb");

    // Enable versioning
    let req = make_request(
        Method::PUT,
        "/vb",
        &[("versioning", "")],
        b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
    );
    svc.put_bucket_versioning("123456789012", &req, "vb")
        .unwrap();

    // Put v1
    let req = make_request(Method::PUT, "/vb/key", &[], b"version1");
    let resp = svc
        .put_object("123456789012", &req, "vb", "key")
        .await
        .unwrap();
    let v1 = resp
        .headers
        .get("x-amz-version-id")
        .map(|h| h.to_str().unwrap().to_string());
    assert!(v1.is_some());

    // Put v2
    let req = make_request(Method::PUT, "/vb/key", &[], b"version2");
    let resp = svc
        .put_object("123456789012", &req, "vb", "key")
        .await
        .unwrap();
    let _v2 = resp
        .headers
        .get("x-amz-version-id")
        .map(|h| h.to_str().unwrap().to_string());

    // Get latest
    let req = make_request(Method::GET, "/vb/key", &[], b"");
    let resp = svc.get_object("123456789012", &req, "vb", "key").unwrap();
    assert_eq!(resp.body.expect_bytes(), b"version2");

    // List versions
    let req = make_request(Method::GET, "/vb", &[("versions", "")], b"");
    let resp = svc
        .list_object_versions("123456789012", &req, "vb")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Version>"));
}

// ── Conditional GET ──

#[tokio::test]
async fn get_object_if_match_succeeds() {
    let svc = make_service();
    seed_bucket(&svc, "cond");

    let req = make_request(Method::PUT, "/cond/f.txt", &[], b"data");
    let resp = svc
        .put_object("123456789012", &req, "cond", "f.txt")
        .await
        .unwrap();
    let etag = resp
        .headers
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let mut req = make_request(Method::GET, "/cond/f.txt", &[], b"");
    req.headers.insert("if-match", etag.parse().unwrap());
    let resp = svc
        .get_object("123456789012", &req, "cond", "f.txt")
        .unwrap();
    assert_eq!(resp.body.expect_bytes(), b"data");
}

#[tokio::test]
async fn get_object_if_match_fails() {
    let svc = make_service();
    seed_bucket(&svc, "cond2");

    let req = make_request(Method::PUT, "/cond2/f.txt", &[], b"data");
    svc.put_object("123456789012", &req, "cond2", "f.txt")
        .await
        .unwrap();

    let mut req = make_request(Method::GET, "/cond2/f.txt", &[], b"");
    req.headers
        .insert("if-match", "\"wrong-etag\"".parse().unwrap());
    assert_aws_err(
        svc.get_object("123456789012", &req, "cond2", "f.txt"),
        "PreconditionFailed",
    );
}

#[tokio::test]
async fn get_object_if_none_match_returns_304() {
    let svc = make_service();
    seed_bucket(&svc, "cond3");

    let req = make_request(Method::PUT, "/cond3/f.txt", &[], b"data");
    let resp = svc
        .put_object("123456789012", &req, "cond3", "f.txt")
        .await
        .unwrap();
    let etag = resp
        .headers
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let mut req = make_request(Method::GET, "/cond3/f.txt", &[], b"");
    req.headers.insert("if-none-match", etag.parse().unwrap());
    let err = svc.get_object("123456789012", &req, "cond3", "f.txt");
    // Should return PreconditionFailed or 304 Not Modified
    assert!(err.is_err());
}

// ── Put with if-none-match (conditional put) ──

#[tokio::test]
async fn put_object_if_none_match_prevents_overwrite() {
    let svc = make_service();
    seed_bucket(&svc, "cnm");

    let req = make_request(Method::PUT, "/cnm/f.txt", &[], b"first");
    svc.put_object("123456789012", &req, "cnm", "f.txt")
        .await
        .unwrap();

    // Try to put again with if-none-match: *
    let mut req = make_request(Method::PUT, "/cnm/f.txt", &[], b"second");
    req.headers.insert("if-none-match", "*".parse().unwrap());
    assert_aws_err(
        svc.put_object("123456789012", &req, "cnm", "f.txt").await,
        "PreconditionFailed",
    );
}

#[tokio::test]
async fn put_object_if_none_match_on_nonexistent_key() {
    let svc = make_service();
    seed_bucket(&svc, "cnm2");

    let mut req = make_request(Method::PUT, "/cnm2/fresh.txt", &[], b"data");
    req.headers.insert("if-none-match", "*".parse().unwrap());
    svc.put_object("123456789012", &req, "cnm2", "fresh.txt")
        .await
        .unwrap();
}

#[tokio::test]
async fn put_object_if_none_match_succeeds_after_versioned_delete() {
    let svc = make_service();
    seed_bucket(&svc, "cnm3");

    let req = make_request(
        Method::PUT,
        "/cnm3",
        &[("versioning", "")],
        b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
    );
    svc.put_bucket_versioning("123456789012", &req, "cnm3")
        .unwrap();

    let req = make_request(Method::PUT, "/cnm3/lock.txt", &[], b"v1");
    svc.put_object("123456789012", &req, "cnm3", "lock.txt")
        .await
        .unwrap();

    let req = make_request(Method::DELETE, "/cnm3/lock.txt", &[], b"");
    let resp = svc
        .delete_object("123456789012", &req, "cnm3", "lock.txt")
        .unwrap();
    assert_eq!(resp.headers.get("x-amz-delete-marker").unwrap(), "true");

    let mut req = make_request(Method::PUT, "/cnm3/lock.txt", &[], b"v2");
    req.headers.insert("if-none-match", "*".parse().unwrap());
    svc.put_object("123456789012", &req, "cnm3", "lock.txt")
        .await
        .unwrap();
}

#[tokio::test]
async fn put_object_if_match_returns_no_such_key_for_delete_marker() {
    let svc = make_service();
    seed_bucket(&svc, "cnm4");

    let req = make_request(
        Method::PUT,
        "/cnm4",
        &[("versioning", "")],
        b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
    );
    svc.put_bucket_versioning("123456789012", &req, "cnm4")
        .unwrap();

    let req = make_request(Method::PUT, "/cnm4/obj.txt", &[], b"data");
    let resp = svc
        .put_object("123456789012", &req, "cnm4", "obj.txt")
        .await
        .unwrap();
    let etag = resp
        .headers
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let req = make_request(Method::DELETE, "/cnm4/obj.txt", &[], b"");
    svc.delete_object("123456789012", &req, "cnm4", "obj.txt")
        .unwrap();

    let mut req = make_request(Method::PUT, "/cnm4/obj.txt", &[], b"update");
    req.headers.insert("if-match", etag.parse().unwrap());
    assert_aws_err(
        svc.put_object("123456789012", &req, "cnm4", "obj.txt")
            .await,
        "NoSuchKey",
    );
}

/// Start a one-part multipart upload of `bucket/key` and return the request
/// that completes it.
async fn single_part_completion(svc: &S3Service, bucket: &str, key: &str) -> (String, AwsRequest) {
    let path = format!("/{bucket}/{key}");
    let init_req = make_request(Method::POST, &path, &[("uploads", "")], b"");
    let resp = svc
        .create_multipart_upload("123456789012", &init_req, bucket, key)
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let start = body.find("<UploadId>").unwrap() + "<UploadId>".len();
    let end = body.find("</UploadId>").unwrap();
    let upload_id = body[start..end].to_string();
    let req = make_request(Method::PUT, &path, &[("partNumber", "1")], b"part");
    let part = svc
        .upload_part("123456789012", &req, bucket, key, &upload_id, 1)
        .await
        .unwrap();
    let etag = part
        .headers
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let complete = make_request(
        Method::POST,
        &path,
        &[("uploadId", upload_id.as_str())],
        xml.as_bytes(),
    );
    (upload_id, complete)
}

fn enable_versioning(svc: &S3Service, bucket: &str) {
    let req = make_request(
        Method::PUT,
        &format!("/{bucket}"),
        &[("versioning", "")],
        b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
    );
    svc.put_bucket_versioning("123456789012", &req, bucket)
        .unwrap();
}

#[tokio::test]
async fn complete_multipart_if_none_match_succeeds_after_versioned_delete() {
    let svc = make_service();
    seed_bucket(&svc, "mpu-inm");
    enable_versioning(&svc, "mpu-inm");
    let req = make_request(Method::PUT, "/mpu-inm/k", &[], b"v1");
    svc.put_object("123456789012", &req, "mpu-inm", "k")
        .await
        .unwrap();

    let (upload_id, mut complete) = single_part_completion(&svc, "mpu-inm", "k").await;
    complete
        .headers
        .insert("if-none-match", "*".parse().unwrap());
    assert_aws_err(
        svc.complete_multipart_upload("123456789012", &complete, "mpu-inm", "k", &upload_id),
        "PreconditionFailed",
    );

    let req = make_request(Method::DELETE, "/mpu-inm/k", &[], b"");
    svc.delete_object("123456789012", &req, "mpu-inm", "k")
        .unwrap();
    svc.complete_multipart_upload("123456789012", &complete, "mpu-inm", "k", &upload_id)
        .unwrap();
}

#[tokio::test]
async fn complete_multipart_honors_if_match() {
    let svc = make_service();
    seed_bucket(&svc, "mpu-im");
    enable_versioning(&svc, "mpu-im");

    // Missing key -> NoSuchKey.
    let (upload_id, mut complete) = single_part_completion(&svc, "mpu-im", "k").await;
    complete.headers.insert(
        "if-match",
        "\"0123456789abcdef0123456789abcdef\"".parse().unwrap(),
    );
    assert_aws_err(
        svc.complete_multipart_upload("123456789012", &complete, "mpu-im", "k", &upload_id),
        "NoSuchKey",
    );

    let req = make_request(Method::PUT, "/mpu-im/k", &[], b"v1");
    let put = svc
        .put_object("123456789012", &req, "mpu-im", "k")
        .await
        .unwrap();
    let etag = put
        .headers
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // ETag mismatch -> 412, upload left intact.
    assert_aws_err(
        svc.complete_multipart_upload("123456789012", &complete, "mpu-im", "k", &upload_id),
        "PreconditionFailed",
    );

    // Matching ETag completes.
    complete.headers.insert("if-match", etag.parse().unwrap());
    svc.complete_multipart_upload("123456789012", &complete, "mpu-im", "k", &upload_id)
        .unwrap();

    // A delete marker as current version reads as a missing key.
    let req = make_request(Method::DELETE, "/mpu-im/k", &[], b"");
    svc.delete_object("123456789012", &req, "mpu-im", "k")
        .unwrap();
    let (upload_id, mut complete) = single_part_completion(&svc, "mpu-im", "k").await;
    complete.headers.insert("if-match", etag.parse().unwrap());
    assert_aws_err(
        svc.complete_multipart_upload("123456789012", &complete, "mpu-im", "k", &upload_id),
        "NoSuchKey",
    );
}

#[tokio::test]
async fn delete_object_if_match_returns_no_such_key_for_delete_marker() {
    let svc = make_service();
    seed_bucket(&svc, "del-im");
    enable_versioning(&svc, "del-im");
    let req = make_request(Method::PUT, "/del-im/k", &[], b"v1");
    let put = svc
        .put_object("123456789012", &req, "del-im", "k")
        .await
        .unwrap();
    let etag = put
        .headers
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let req = make_request(Method::DELETE, "/del-im/k", &[], b"");
    svc.delete_object("123456789012", &req, "del-im", "k")
        .unwrap();

    let mut req = make_request(Method::DELETE, "/del-im/k", &[], b"");
    req.headers.insert("if-match", etag.parse().unwrap());
    assert_aws_err(
        svc.delete_object("123456789012", &req, "del-im", "k"),
        "NoSuchKey",
    );
}

// ── Storage class ──

#[tokio::test]
async fn put_object_with_storage_class() {
    let svc = make_service();
    seed_bucket(&svc, "sc");

    let mut req = make_request(Method::PUT, "/sc/f.txt", &[], b"data");
    req.headers
        .insert("x-amz-storage-class", "GLACIER".parse().unwrap());
    svc.put_object("123456789012", &req, "sc", "f.txt")
        .await
        .unwrap();

    let req = make_request(Method::HEAD, "/sc/f.txt", &[], b"");
    let resp = svc
        .head_object("123456789012", &req, "sc", "f.txt")
        .unwrap();
    assert_eq!(
        resp.headers
            .get("x-amz-storage-class")
            .unwrap()
            .to_str()
            .unwrap(),
        "GLACIER"
    );
}

// ── Delete versioned object creates delete marker ──

#[tokio::test]
async fn delete_versioned_object_creates_marker() {
    let svc = make_service();
    seed_bucket(&svc, "dv");

    let req = make_request(
        Method::PUT,
        "/dv",
        &[("versioning", "")],
        b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
    );
    svc.put_bucket_versioning("123456789012", &req, "dv")
        .unwrap();

    let req = make_request(Method::PUT, "/dv/key", &[], b"data");
    svc.put_object("123456789012", &req, "dv", "key")
        .await
        .unwrap();

    let req = make_request(Method::DELETE, "/dv/key", &[], b"");
    let resp = svc
        .delete_object("123456789012", &req, "dv", "key")
        .unwrap();
    assert!(resp.headers.get("x-amz-delete-marker").is_some());

    // GET should fail (object is "deleted")
    let req = make_request(Method::GET, "/dv/key", &[], b"");
    assert_aws_err(
        svc.get_object("123456789012", &req, "dv", "key"),
        "NoSuchKey",
    );
}

// ── Copy with metadata replacement ──

#[tokio::test]
async fn copy_object_with_metadata_replace() {
    let svc = make_service();
    seed_bucket(&svc, "cpm");

    let mut req = make_request(Method::PUT, "/cpm/src", &[], b"data");
    req.headers
        .insert("x-amz-meta-original", "yes".parse().unwrap());
    svc.put_object("123456789012", &req, "cpm", "src")
        .await
        .unwrap();

    let mut req = make_request(Method::PUT, "/cpm/dst", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "cpm/src".parse().unwrap());
    req.headers
        .insert("x-amz-metadata-directive", "REPLACE".parse().unwrap());
    req.headers
        .insert("x-amz-meta-new-key", "new-val".parse().unwrap());
    svc.copy_object("123456789012", &req, "cpm", "dst").unwrap();

    let req = make_request(Method::HEAD, "/cpm/dst", &[], b"");
    let resp = svc.head_object("123456789012", &req, "cpm", "dst").unwrap();
    assert!(resp
        .headers
        .get("x-amz-meta-new-key")
        .is_some_and(|v| v == "new-val"));
    // Original metadata should NOT be present
    assert!(resp.headers.get("x-amz-meta-original").is_none());
}

// ── Large list with pagination (max-keys) ──

#[tokio::test]
async fn list_objects_v2_with_max_keys() {
    let svc = make_service();
    seed_bucket(&svc, "pg");

    for i in 0..5 {
        let key = format!("k{i}");
        let req = make_request(Method::PUT, &format!("/pg/{key}"), &[], b"x");
        svc.put_object("123456789012", &req, "pg", &key)
            .await
            .unwrap();
    }

    let req = make_request(
        Method::GET,
        "/pg",
        &[("list-type", "2"), ("max-keys", "2")],
        b"",
    );
    let resp = svc.list_objects_v2("123456789012", &req, "pg").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<IsTruncated>true</IsTruncated>"));
    assert!(body.contains("<MaxKeys>2</MaxKeys>"));
}

// ── buckets.rs coverage (list/create/delete/head/location) ──

#[test]
fn list_buckets_empty_account() {
    let svc = make_service();
    let req = make_request(Method::GET, "/", &[], b"");
    let resp = svc.list_buckets("123456789012", &req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<ListAllMyBucketsResult"));
    assert!(body.contains("<Owner><ID>123456789012</ID>"));
    assert!(body.contains("<Buckets></Buckets>"));
}

#[test]
fn list_buckets_sorted_by_name() {
    let svc = make_service();
    seed_bucket(&svc, "zeta");
    seed_bucket(&svc, "alpha");
    seed_bucket(&svc, "middle");

    let req = make_request(Method::GET, "/", &[], b"");
    let resp = svc.list_buckets("123456789012", &req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let a = body.find("alpha").unwrap();
    let m = body.find("middle").unwrap();
    let z = body.find("zeta").unwrap();
    assert!(a < m && m < z, "buckets must be sorted");
}

fn seed_bucket_in_region(svc: &S3Service, name: &str, region: &str) {
    let mut mas = svc.state.write();
    let state = mas.default_mut();
    state
        .buckets
        .insert(name.to_string(), S3Bucket::new(name, region, "owner"));
}

#[test]
fn list_buckets_includes_bucket_region() {
    let svc = make_service();
    seed_bucket(&svc, "alpha");

    let req = make_request(Method::GET, "/", &[], b"");
    let resp = svc.list_buckets("123456789012", &req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("<BucketRegion>us-east-1</BucketRegion>"),
        "response should include BucketRegion per-bucket: {body}"
    );
}

#[test]
fn list_buckets_filter_by_bucket_region() {
    let svc = make_service();
    seed_bucket_in_region(&svc, "east-bucket", "us-east-1");
    seed_bucket_in_region(&svc, "west-bucket", "us-west-2");

    let req = make_request(Method::GET, "/", &[("bucket-region", "us-west-2")], b"");
    let resp = svc.list_buckets("123456789012", &req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("west-bucket"));
    assert!(!body.contains("east-bucket"));
    assert!(body.contains("<BucketRegion>us-west-2</BucketRegion>"));
}

#[test]
fn list_buckets_filter_by_prefix() {
    let svc = make_service();
    seed_bucket(&svc, "foo-1");
    seed_bucket(&svc, "foo-2");
    seed_bucket(&svc, "bar");

    let req = make_request(Method::GET, "/", &[("prefix", "foo-")], b"");
    let resp = svc.list_buckets("123456789012", &req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("foo-1"));
    assert!(body.contains("foo-2"));
    assert!(!body.contains("<Name>bar</Name>"));
    assert!(body.contains("<Prefix>foo-</Prefix>"));
}

#[test]
fn list_buckets_max_buckets_paginates() {
    let svc = make_service();
    for n in &["a", "b", "c", "d", "e"] {
        seed_bucket(&svc, n);
    }

    let req = make_request(Method::GET, "/", &[("max-buckets", "2")], b"");
    let resp = svc.list_buckets("123456789012", &req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Name>a</Name>"));
    assert!(body.contains("<Name>b</Name>"));
    assert!(!body.contains("<Name>c</Name>"));
    assert!(body.contains("<ContinuationToken>"));
}

#[test]
fn list_buckets_continuation_token_resumes() {
    let svc = make_service();
    for n in &["a", "b", "c", "d", "e"] {
        seed_bucket(&svc, n);
    }

    let req = make_request(Method::GET, "/", &[("max-buckets", "2")], b"");
    let resp = svc.list_buckets("123456789012", &req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let start = body.find("<ContinuationToken>").unwrap() + "<ContinuationToken>".len();
    let end = body.find("</ContinuationToken>").unwrap();
    let token = body[start..end].to_string();

    let req2 = make_request(
        Method::GET,
        "/",
        &[("max-buckets", "2"), ("continuation-token", &token)],
        b"",
    );
    let resp2 = svc.list_buckets("123456789012", &req2).unwrap();
    let body2 = std::str::from_utf8(resp2.body.expect_bytes()).unwrap();
    assert!(body2.contains("<Name>c</Name>"));
    assert!(body2.contains("<Name>d</Name>"));
    assert!(!body2.contains("<Name>a</Name>"));
    assert!(!body2.contains("<Name>b</Name>"));
    // page 2 has more (e remains) so still emits a token
    assert!(body2.contains("<ContinuationToken>"));

    // page 3: should be e + no continuation
    let start = body2.find("<ContinuationToken>").unwrap() + "<ContinuationToken>".len();
    let end = body2.find("</ContinuationToken>").unwrap();
    let token2 = body2[start..end].to_string();
    let req3 = make_request(
        Method::GET,
        "/",
        &[("max-buckets", "2"), ("continuation-token", &token2)],
        b"",
    );
    let resp3 = svc.list_buckets("123456789012", &req3).unwrap();
    let body3 = std::str::from_utf8(resp3.body.expect_bytes()).unwrap();
    assert!(body3.contains("<Name>e</Name>"));
    assert!(!body3.contains("<ContinuationToken>"));
}

#[test]
fn list_buckets_invalid_max_buckets_errors() {
    let svc = make_service();
    let req = make_request(Method::GET, "/", &[("max-buckets", "0")], b"");
    assert_aws_err(svc.list_buckets("123456789012", &req), "InvalidArgument");

    let req2 = make_request(Method::GET, "/", &[("max-buckets", "20000")], b"");
    assert_aws_err(svc.list_buckets("123456789012", &req2), "InvalidArgument");

    let req3 = make_request(Method::GET, "/", &[("max-buckets", "abc")], b"");
    assert_aws_err(svc.list_buckets("123456789012", &req3), "InvalidArgument");
}

#[test]
fn list_buckets_invalid_continuation_token_errors() {
    let svc = make_service();
    let req = make_request(
        Method::GET,
        "/",
        &[("continuation-token", "!!!notb64!!!")],
        b"",
    );
    assert_aws_err(svc.list_buckets("123456789012", &req), "InvalidArgument");

    let req2 = make_request(Method::GET, "/", &[("continuation-token", "")], b"");
    assert_aws_err(svc.list_buckets("123456789012", &req2), "InvalidArgument");
}

#[test]
fn create_bucket_invalid_name_errors() {
    let svc = make_service();
    let req = make_request(Method::PUT, "/AB", &[], b"");
    assert_aws_err(
        svc.create_bucket("123456789012", &req, "AB"),
        "InvalidBucketName",
    );
}

#[test]
fn create_bucket_idempotent_same_region_us_east_1() {
    let svc = make_service();
    let req = make_request(Method::PUT, "/idem", &[], b"");
    svc.create_bucket("123456789012", &req, "idem").unwrap();
    let resp = svc.create_bucket("123456789012", &req, "idem").unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[test]
fn create_bucket_honors_grant_headers() {
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/granted", &[], b"");
    req.headers.insert(
        "x-amz-grant-read",
        "uri=http://acs.amazonaws.com/groups/global/AllUsers"
            .parse()
            .unwrap(),
    );
    svc.create_bucket("123456789012", &req, "granted").unwrap();

    let get = make_request(Method::GET, "/granted", &[("acl", "")], b"");
    let resp = svc.get_bucket_acl("123456789012", &get, "granted").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("AllUsers"), "grant header ignored: {body}");
    assert!(body.contains("<Permission>READ</Permission>"), "{body}");
}

#[test]
fn create_bucket_rejects_canned_acl_and_grant_headers_together() {
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/both-acl", &[], b"");
    req.headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    req.headers.insert(
        "x-amz-grant-read",
        "uri=http://acs.amazonaws.com/groups/global/AllUsers"
            .parse()
            .unwrap(),
    );
    assert_aws_err(
        svc.create_bucket("123456789012", &req, "both-acl"),
        "InvalidRequest",
    );
    assert_aws_err(svc.head_bucket("123456789012", "both-acl"), "NotFound");
}

#[test]
fn create_bucket_keeps_an_email_grantee_instead_of_dropping_or_refusing_it() {
    // S3 resolves `emailAddress=` against its own directory and answers 200.
    // There is nothing to resolve against here, so the grant keeps the address
    // as an AmazonCustomerByEmail grantee: dropping it would silently lose a
    // grant, and refusing would fail a call AWS accepts.
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/email-grant", &[], b"");
    req.headers.insert(
        "x-amz-grant-full-control",
        "emailAddress=someone@example.com".parse().unwrap(),
    );
    svc.create_bucket("123456789012", &req, "email-grant")
        .unwrap();

    let get = make_request(Method::GET, "/email-grant", &[("acl", "")], b"");
    let resp = svc
        .get_bucket_acl("123456789012", &get, "email-grant")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("AmazonCustomerByEmail")
            && body.contains("<EmailAddress>someone@example.com</EmailAddress>"),
        "email grantee not preserved: {body}"
    );
}

#[test]
fn create_bucket_rejects_an_unparseable_grantee_rather_than_writing_an_empty_acl() {
    // A clause naming no known grantee key parses to nothing. Storing that as
    // the bucket's ACL would leave it with no grants at all -- not even the
    // owner -- which the loader honors as an explicit empty ACL.
    let svc = make_service();
    for value in ["nonsense=whoever", "justtext"] {
        let mut req = make_request(Method::PUT, "/bad-grantee", &[], b"");
        req.headers
            .insert("x-amz-grant-full-control", value.parse().unwrap());
        assert_aws_err(
            svc.create_bucket("123456789012", &req, "bad-grantee"),
            "InvalidArgument",
        );
    }
    assert_aws_err(svc.head_bucket("123456789012", "bad-grantee"), "NotFound");
}

#[test]
fn create_bucket_rejects_an_acl_alongside_bucket_owner_enforced() {
    // Real S3 answers InvalidBucketAclWithObjectOwnership: BucketOwnerEnforced
    // disables ACLs, so a create cannot also ask for one. Allowing it would
    // leave a publicly readable bucket whose ACL PutBucketAcl refuses to edit.
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/owner-enforced", &[], b"");
    req.headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    req.headers.insert(
        "x-amz-object-ownership",
        "BucketOwnerEnforced".parse().unwrap(),
    );
    assert_aws_err(
        svc.create_bucket("123456789012", &req, "owner-enforced"),
        "InvalidBucketAclWithObjectOwnership",
    );

    // `private` grants nothing beyond the owner, so S3 accepts it alongside
    // BucketOwnerEnforced -- and so do we.
    let mut ok_req = make_request(Method::PUT, "/owner-enforced-ok", &[], b"");
    ok_req
        .headers
        .insert("x-amz-acl", "private".parse().unwrap());
    ok_req.headers.insert(
        "x-amz-object-ownership",
        "BucketOwnerEnforced".parse().unwrap(),
    );
    svc.create_bucket("123456789012", &ok_req, "owner-enforced-ok")
        .unwrap();

    // A grant header conflicts on presence alone, even one naming the owner:
    // the caller is asking for an ACL on a bucket that has none.
    let mut grant_req = make_request(Method::PUT, "/owner-enforced-grant", &[], b"");
    grant_req.headers.insert(
        "x-amz-grant-full-control",
        "id=123456789012".parse().unwrap(),
    );
    grant_req.headers.insert(
        "x-amz-object-ownership",
        "BucketOwnerEnforced".parse().unwrap(),
    );
    assert_aws_err(
        svc.create_bucket("123456789012", &grant_req, "owner-enforced-grant"),
        "InvalidBucketAclWithObjectOwnership",
    );
}

#[test]
fn create_bucket_rejects_a_canned_acl_that_is_not_a_bucket_canned_acl() {
    // `com.amazonaws.s3#BucketCannedACL` has exactly four members;
    // `bucket-owner-full-control` is an OBJECT canned ACL, and a typo is not an
    // ACL at all. Both used to fall through to owner-only grants silently, and
    // this branch would then persist that as the bucket's explicit ACL.
    let svc = make_service();
    for value in ["pubic-read", "not-an-acl"] {
        let mut req = make_request(Method::PUT, "/bad-canned", &[], b"");
        req.headers.insert("x-amz-acl", value.parse().unwrap());
        assert_aws_err(
            svc.create_bucket("123456789012", &req, "bad-canned"),
            "InvalidArgument",
        );
    }
    assert_aws_err(svc.head_bucket("123456789012", "bad-canned"), "NotFound");

    // The object-scoped canned ACLs are IGNORED on a bucket rather than
    // refused, so they are accepted and resolve to the owner's FULL_CONTROL.
    let mut owner_acl = make_request(Method::PUT, "/owner-canned", &[], b"");
    owner_acl
        .headers
        .insert("x-amz-acl", "bucket-owner-full-control".parse().unwrap());
    svc.create_bucket("123456789012", &owner_acl, "owner-canned")
        .unwrap();
}

#[test]
fn create_bucket_accepts_log_delivery_write_with_real_grants() {
    // log-delivery-write is bucket-scoped in S3's canned-ACL table even though
    // `com.amazonaws.s3#BucketCannedACL` omits it, and it has to actually grant
    // the log-delivery group write access -- that is the whole point of using
    // it on a logging target.
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/canned-logdelivery", &[], b"");
    req.headers
        .insert("x-amz-acl", "log-delivery-write".parse().unwrap());
    svc.create_bucket("123456789012", &req, "canned-logdelivery")
        .unwrap();

    let get = make_request(Method::GET, "/canned-logdelivery", &[("acl", "")], b"");
    let resp = svc
        .get_bucket_acl("123456789012", &get, "canned-logdelivery")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("s3/LogDelivery"), "{body}");
    assert!(body.contains("<Permission>WRITE</Permission>"), "{body}");
}

#[test]
fn create_bucket_accepts_aws_exec_read_but_treats_it_as_granting_past_the_owner() {
    // aws-exec-read is bucket-scoped and AWS answers 200 (CloudFormation's
    // AccessControl: AwsExecRead maps to it), so refusing it would break a
    // working call. Its READ grant to the EC2 service's canonical user is not
    // modeled, so the stored ACL is owner-only -- but the request still asks for
    // an ACL reaching past the owner, so it conflicts with BucketOwnerEnforced.
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/aws-exec", &[], b"");
    req.headers
        .insert("x-amz-acl", "aws-exec-read".parse().unwrap());
    svc.create_bucket("123456789012", &req, "aws-exec").unwrap();

    let mut enforced = make_request(Method::PUT, "/aws-exec-enforced", &[], b"");
    enforced
        .headers
        .insert("x-amz-acl", "aws-exec-read".parse().unwrap());
    enforced.headers.insert(
        "x-amz-object-ownership",
        "BucketOwnerEnforced".parse().unwrap(),
    );
    assert_aws_err(
        svc.create_bucket("123456789012", &enforced, "aws-exec-enforced"),
        "InvalidBucketAclWithObjectOwnership",
    );
}

#[test]
fn put_object_acl_rejects_a_body_that_is_not_an_access_control_policy() {
    // A non-XML body parses to zero grants; treating that as "remove every
    // grant" strips the object's owner FULL_CONTROL and writes the empty list
    // into its meta.
    let svc = make_service();
    seed_bucket(&svc, "obj-junk");
    seed_object(&svc, "obj-junk", "k.txt", b"body");

    let json = make_request(Method::PUT, "/obj-junk/k.txt", &[("acl", "")], b"{}");
    assert_aws_err(
        svc.put_object_acl("123456789012", &json, "obj-junk", "k.txt"),
        "MalformedXML",
    );

    let blank = make_request(Method::PUT, "/obj-junk/k.txt", &[("acl", "")], b"  ");
    assert_aws_err(
        svc.put_object_acl("123456789012", &blank, "obj-junk", "k.txt"),
        "MalformedACLError",
    );
}

#[test]
fn put_object_acl_rejects_a_canned_acl_with_grants_or_a_body() {
    // Letting the canned value win discarded the grants the caller asked for,
    // and the result is written into the object meta.
    let svc = make_service();
    seed_bucket(&svc, "obj-both");
    seed_object(&svc, "obj-both", "k.txt", b"body");

    let mut with_grant = make_request(Method::PUT, "/obj-both/k.txt", &[("acl", "")], b"");
    with_grant
        .headers
        .insert("x-amz-acl", "private".parse().unwrap());
    with_grant.headers.insert(
        "x-amz-grant-read",
        "uri=http://acs.amazonaws.com/groups/global/AllUsers"
            .parse()
            .unwrap(),
    );
    assert_aws_err(
        svc.put_object_acl("123456789012", &with_grant, "obj-both", "k.txt"),
        "InvalidRequest",
    );

    let mut with_body = make_request(
        Method::PUT,
        "/obj-both/k.txt",
        &[("acl", "")],
        b"<AccessControlPolicy><Owner><ID>123456789012</ID></Owner><AccessControlList/></AccessControlPolicy>",
    );
    with_body
        .headers
        .insert("x-amz-acl", "private".parse().unwrap());
    assert_aws_err(
        svc.put_object_acl("123456789012", &with_body, "obj-both", "k.txt"),
        "InvalidRequest",
    );
}

#[test]
fn create_multipart_upload_rejects_an_acl_when_ownership_disables_them() {
    // Every sibling ACL-setting path refuses this; multipart used to accept it
    // and carry the grants into the completed object, which is now persisted.
    let svc = make_service();
    seed_bucket(&svc, "mpu-owner");
    {
        let mut mas = svc.state.write();
        let state = mas.default_mut();
        let b = state.buckets.get_mut("mpu-owner").unwrap();
        b.ownership_controls = Some(
            "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership>\
             </Rule></OwnershipControls>"
                .to_string(),
        );
    }

    let mut canned = make_request(Method::POST, "/mpu-owner/k.txt", &[("uploads", "")], b"");
    canned
        .headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    assert_aws_err(
        svc.create_multipart_upload("123456789012", &canned, "mpu-owner", "k.txt"),
        "AccessControlListNotSupported",
    );

    let mut granted = make_request(Method::POST, "/mpu-owner/k.txt", &[("uploads", "")], b"");
    granted.headers.insert(
        "x-amz-grant-read",
        "uri=http://acs.amazonaws.com/groups/global/AllUsers"
            .parse()
            .unwrap(),
    );
    assert_aws_err(
        svc.create_multipart_upload("123456789012", &granted, "mpu-owner", "k.txt"),
        "AccessControlListNotSupported",
    );
}

#[test]
fn put_bucket_ownership_controls_rejects_an_unknown_value() {
    // An unrecognized value used to be stored verbatim and reloaded as an
    // ownership rule meaning nothing, silently returning the bucket to
    // ACLs-enabled.
    let svc = make_service();
    seed_bucket(&svc, "own-bad");
    let req = make_request(
        Method::PUT,
        "/own-bad",
        &[("ownershipControls", "")],
        b"<OwnershipControls><Rule><ObjectOwnership>Whatever</ObjectOwnership></Rule></OwnershipControls>",
    );
    assert_aws_err(
        svc.put_bucket_ownership_controls("123456789012", &req, "own-bad"),
        "InvalidArgument",
    );
}

#[test]
fn put_object_acl_updates_the_versioned_copy_too() {
    // A versioned bucket keeps a second copy of the current version in
    // `object_versions`. Leaving it stale let a later delete re-derive the
    // current object from the old grants, silently reverting a change that had
    // already been persisted. The ids have to be real: with None on both sides
    // the version guard matches anything and the test proves nothing.
    let svc = make_service();
    seed_bucket(&svc, "ver-acl");
    seed_object(&svc, "ver-acl", "k.txt", b"body");
    {
        let mut mas = svc.state.write();
        let state = mas.default_mut();
        let b = state.buckets.get_mut("ver-acl").unwrap();
        b.versioning = Some("Enabled".to_string());
        let mut current = b.objects.get("k.txt").unwrap().clone();
        current.version_id = Some("v2".to_string());
        let mut older = current.clone();
        older.version_id = Some("v1".to_string());
        b.objects.insert("k.txt".to_string(), current.clone());
        b.object_versions
            .insert("k.txt".to_string(), vec![older, current]);
    }

    let mut req = make_request(Method::PUT, "/ver-acl/k.txt", &[("acl", "")], b"");
    req.headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    svc.put_object_acl("123456789012", &req, "ver-acl", "k.txt")
        .unwrap();

    let mas = svc.state.read();
    let state = mas.get("123456789012").unwrap();
    let b = state.buckets.get("ver-acl").unwrap();
    let versions = b.object_versions.get("k.txt").unwrap();
    let public = |o: &crate::state::S3Object| {
        o.acl_grants.iter().any(|g| {
            g.grantee_uri
                .as_deref()
                .is_some_and(|u| u.contains("AllUsers"))
        })
    };
    let current = versions
        .iter()
        .find(|v| v.version_id.as_deref() == Some("v2"));
    assert!(
        public(current.expect("v2 present")),
        "the current version's copy kept the old grants"
    );
    let older = versions
        .iter()
        .find(|v| v.version_id.as_deref() == Some("v1"));
    assert!(
        !public(older.expect("v1 present")),
        "an older version must not be touched"
    );
}

#[test]
fn bucket_owner_full_control_is_accepted_when_ownership_disables_acls() {
    // AWS refuses ACLs on a BucketOwnerEnforced bucket with one exception:
    // `bucket-owner-full-control`, which grants nothing the owner does not
    // already have. All three object-write paths share the check, so all three
    // have to honor the exception.
    let svc = make_service();
    seed_bucket(&svc, "bofc");
    seed_object(&svc, "bofc", "src.txt", b"body");
    {
        let mut mas = svc.state.write();
        let state = mas.default_mut();
        let b = state.buckets.get_mut("bofc").unwrap();
        b.ownership_controls = Some(
            "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership>\
             </Rule></OwnershipControls>"
                .to_string(),
        );
    }

    let allowed = svc
        .resolve_write_acl_headers("123456789012", "bofc", &{
            let mut h = HeaderMap::new();
            h.insert("x-amz-acl", "bucket-owner-full-control".parse().unwrap());
            h
        })
        .expect("bucket-owner-full-control must be accepted");
    // Assert what it resolves to, not that the function echoed back the header
    // it was handed: the whole point is that the owner ends up with full
    // control and nobody else appears.
    // Resolved against the bucket's own ACL owner, which is what the write paths
    // pass and therefore what ends up stored.
    let grants = allowed
        .grants_for("owner")
        .expect("the canned value resolves to grants");
    assert_eq!(grants.len(), 1, "{grants:?}");
    assert_eq!(grants[0].permission, "FULL_CONTROL");
    assert_eq!(grants[0].grantee_type, "CanonicalUser");
    assert_eq!(grants[0].grantee_id.as_deref(), Some("owner"));

    // The exception is about what the ACL GRANTS, not how it is spelled: AWS
    // accepts "an equivalent form of this ACL" too, so an explicit full-control
    // grant to the BUCKET OWNER is accepted just like the canned value.
    //
    // `seed_bucket` owns this bucket as "owner" while the caller is
    // 123456789012, which is the case that distinguishes the two. They coincide
    // for a bucket the caller created; they diverge for one persisted by another
    // account and hydrated into the default account on restart.
    svc.resolve_write_acl_headers("123456789012", "bofc", &{
        let mut h = HeaderMap::new();
        h.insert("x-amz-grant-full-control", "id=owner".parse().unwrap());
        h
    })
    .expect("full control to the bucket owner is the equivalent form");

    // ...and a grant to anyone else is not, even the caller. Such a request
    // would leave an object on a BucketOwnerEnforced bucket whose ACL gives the
    // bucket owner nothing, which is the state that setting exists to prevent.
    let err = svc
        .resolve_write_acl_headers("123456789012", "bofc", &{
            let mut h = HeaderMap::new();
            h.insert(
                "x-amz-grant-full-control",
                "id=123456789012".parse().unwrap(),
            );
            h
        })
        .expect_err("full control to a non-owner is not the equivalent form");
    assert_eq!(err.code(), "AccessControlListNotSupported", "{err:?}");

    // Every other ACL is refused. The model's wording is narrow -- such a
    // bucket "only accept[s] PUT requests that don't specify an ACL or PUT
    // requests that specify bucket owner full control ACLs" -- so the three
    // values that merely RESOLVE to owner-only grants are refused too:
    // `private` is owner-only by definition, and `bucket-owner-read` and
    // `aws-exec-read` collapse onto an owner-only grant only because their real
    // grantees are not modeled. Judging the resolved shape rather than the ACL
    // the request names accepted all three.
    for (header, value) in [
        ("x-amz-acl", "public-read"),
        ("x-amz-acl", "private"),
        ("x-amz-acl", "bucket-owner-read"),
        ("x-amz-acl", "aws-exec-read"),
        (
            "x-amz-grant-read",
            "uri=http://acs.amazonaws.com/groups/global/AllUsers",
        ),
    ] {
        let refused = svc.resolve_write_acl_headers("123456789012", "bofc", &{
            let mut h = HeaderMap::new();
            h.insert(header, value.parse().unwrap());
            h
        });
        let err = refused.expect_err(&format!("{header}: {value} must be refused"));
        assert_eq!(err.code(), "AccessControlListNotSupported", "{err:?}");
    }

    // A write with no ACL header at all is what such a bucket is for.
    svc.resolve_write_acl_headers("123456789012", "bofc", &HeaderMap::new())
        .expect("a write specifying no ACL must be accepted");
}

#[tokio::test]
async fn put_object_honors_the_bucket_owner_enforced_rules_end_to_end() {
    // The three write paths share the resolver, but the helper-level test above
    // proves nothing about whether a handler actually calls it before writing.
    let svc = make_service();
    seed_bucket(&svc, "bofc-e2e");
    {
        let mut mas = svc.state.write();
        let state = mas.default_mut();
        let b = state.buckets.get_mut("bofc-e2e").unwrap();
        b.ownership_controls = Some(
            "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership>\
             </Rule></OwnershipControls>"
                .to_string(),
        );
    }

    let mut refused = make_request(Method::PUT, "/bofc-e2e/k", &[], b"body");
    refused
        .headers
        .insert("x-amz-acl", "private".parse().unwrap());
    let err = match svc
        .put_object("123456789012", &refused, "bofc-e2e", "k")
        .await
    {
        Ok(_) => panic!("`--acl private` is not one of the two ACLs such a bucket accepts"),
        Err(e) => e,
    };
    assert_eq!(err.code(), "AccessControlListNotSupported", "{err:?}");
    assert!(
        svc.get_object(
            "123456789012",
            &make_request(Method::GET, "/bofc-e2e/k", &[], b""),
            "bofc-e2e",
            "k"
        )
        .is_err(),
        "the refused write must not have stored the object"
    );

    let mut allowed = make_request(Method::PUT, "/bofc-e2e/ok", &[], b"body");
    allowed
        .headers
        .insert("x-amz-acl", "bucket-owner-full-control".parse().unwrap());
    svc.put_object("123456789012", &allowed, "bofc-e2e", "ok")
        .await
        .expect("bucket-owner-full-control must be accepted");
}

#[tokio::test]
async fn a_blank_acl_header_is_rejected_on_every_acl_setting_path() {
    // `""` is not a member of `ObjectCannedACL`, and every path that accepts a
    // canned ACL answers 400 for it. Treating it as "no ACL asked for" on the
    // object-write paths alone made one operation disagree with four; the
    // `x-amz-grant-*` family's blank rule is a different case, since an empty
    // value there names no grantee rather than naming an invalid ACL.
    let svc = make_service();
    seed_bucket(&svc, "blank-acl");

    let err = svc
        .resolve_write_acl_headers("123456789012", "blank-acl", &{
            let mut h = HeaderMap::new();
            h.insert("x-amz-acl", "".parse().unwrap());
            h
        })
        .expect_err("a blank canned ACL is not a canned ACL");
    assert_eq!(err.code(), "InvalidArgument", "{err:?}");

    let mut req = make_request(Method::PUT, "/blank-acl/k", &[], b"body");
    req.headers.insert("x-amz-acl", "".parse().unwrap());
    let err = match svc.put_object("123456789012", &req, "blank-acl", "k").await {
        Ok(_) => panic!("a blank canned ACL must not be accepted"),
        Err(e) => e,
    };
    assert_eq!(err.code(), "InvalidArgument", "{err:?}");
}

#[test]
fn object_writes_refuse_a_public_acl_when_block_public_acls_is_set() {
    // Only the Put*Acl paths enforced BlockPublicAcls, so `put-object
    // --acl public-read` stored an AllUsers grant on a bucket that blocks
    // exactly that, while `put-object-acl --acl public-read` was refused. AWS
    // refuses both, and all three write paths share this resolver.
    let svc = make_service();
    seed_bucket(&svc, "pab-write");
    {
        let mut mas = svc.state.write();
        let state = mas.default_mut();
        let b = state.buckets.get_mut("pab-write").unwrap();
        b.public_access_block = Some(
            "<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls>\
             </PublicAccessBlockConfiguration>"
                .to_string(),
        );
    }

    let refused = svc.resolve_write_acl_headers("123456789012", "pab-write", &{
        let mut h = HeaderMap::new();
        h.insert("x-amz-acl", "public-read".parse().unwrap());
        h
    });
    let err = refused.expect_err("a public ACL must be refused");
    assert_eq!(err.code(), "AccessDenied", "{err:?}");

    // A non-public ACL is unaffected.
    svc.resolve_write_acl_headers("123456789012", "pab-write", &{
        let mut h = HeaderMap::new();
        h.insert("x-amz-acl", "private".parse().unwrap());
        h
    })
    .expect("private is not a public ACL");
}

#[test]
fn create_multipart_upload_reports_a_bad_acl_value_before_ownership() {
    // Sharing one resolver normalized the order across the write paths: an
    // invalid canned value on a BucketOwnerEnforced bucket answers
    // InvalidArgument (the value is wrong whatever the bucket allows) rather
    // than AccessControlListNotSupported, which is what PutObject already did.
    let svc = make_service();
    seed_bucket(&svc, "mpu-order");
    {
        let mut mas = svc.state.write();
        let state = mas.default_mut();
        let b = state.buckets.get_mut("mpu-order").unwrap();
        b.ownership_controls = Some(
            "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership>\
             </Rule></OwnershipControls>"
                .to_string(),
        );
    }

    let mut req = make_request(Method::POST, "/mpu-order/k.txt", &[("uploads", "")], b"");
    req.headers
        .insert("x-amz-acl", "pubic-read".parse().unwrap());
    assert_aws_err(
        svc.create_multipart_upload("123456789012", &req, "mpu-order", "k.txt"),
        "InvalidArgument",
    );

    // A valid value that reaches past the owner still reports the ownership.
    let mut valid = make_request(Method::POST, "/mpu-order/k.txt", &[("uploads", "")], b"");
    valid
        .headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    assert_aws_err(
        svc.create_multipart_upload("123456789012", &valid, "mpu-order", "k.txt"),
        "AccessControlListNotSupported",
    );
}

#[test]
fn copy_object_honors_the_canned_acl_header() {
    // A copy used to ignore the header entirely: `copy-object --acl public-read`
    // answered 200 and produced a private object, so the caller believed the
    // copy was published.
    let svc = make_service();
    seed_bucket(&svc, "copy-src");
    seed_bucket(&svc, "copy-dst");
    seed_object(&svc, "copy-src", "k.txt", b"body");

    let mut req = make_request(Method::PUT, "/copy-dst/k.txt", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "copy-src/k.txt".parse().unwrap());
    req.headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    svc.copy_object("123456789012", &req, "copy-dst", "k.txt")
        .unwrap();

    let get = make_request(Method::GET, "/copy-dst/k.txt", &[("acl", "")], b"");
    let resp = svc
        .get_object_acl("123456789012", &get, "copy-dst", "k.txt")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("AllUsers"),
        "canned ACL ignored on copy: {body}"
    );
}

#[test]
fn copy_object_honors_grant_headers() {
    let svc = make_service();
    seed_bucket(&svc, "copy-src2");
    seed_bucket(&svc, "copy-dst2");
    seed_object(&svc, "copy-src2", "k.txt", b"body");

    let mut req = make_request(Method::PUT, "/copy-dst2/k.txt", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "copy-src2/k.txt".parse().unwrap());
    req.headers.insert(
        "x-amz-grant-read",
        "uri=http://acs.amazonaws.com/groups/global/AllUsers"
            .parse()
            .unwrap(),
    );
    svc.copy_object("123456789012", &req, "copy-dst2", "k.txt")
        .unwrap();

    let get = make_request(Method::GET, "/copy-dst2/k.txt", &[("acl", "")], b"");
    let resp = svc
        .get_object_acl("123456789012", &get, "copy-dst2", "k.txt")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("AllUsers"),
        "grant headers ignored on copy: {body}"
    );
    assert!(body.contains("<Permission>READ</Permission>"), "{body}");
}

#[test]
fn copy_object_without_acl_headers_stays_private() {
    // A copy is a new object, so an unspecified ACL is the default owner grant
    // rather than whatever the source carried.
    let svc = make_service();
    seed_bucket(&svc, "copy-src3");
    seed_bucket(&svc, "copy-dst3");
    seed_object(&svc, "copy-src3", "k.txt", b"body");
    {
        let mut mas = svc.state.write();
        let state = mas.default_mut();
        let src = state.buckets.get_mut("copy-src3").unwrap();
        let obj = src.objects.get_mut("k.txt").unwrap();
        obj.acl_grants = canned_acl_grants_for_object("public-read", "123456789012");
    }

    let mut req = make_request(Method::PUT, "/copy-dst3/k.txt", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "copy-src3/k.txt".parse().unwrap());
    svc.copy_object("123456789012", &req, "copy-dst3", "k.txt")
        .unwrap();

    let get = make_request(Method::GET, "/copy-dst3/k.txt", &[("acl", "")], b"");
    let resp = svc
        .get_object_acl("123456789012", &get, "copy-dst3", "k.txt")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        !body.contains("AllUsers"),
        "the copy inherited the source's public grant: {body}"
    );
}

#[test]
fn copy_object_rejects_conflicting_and_disallowed_acls() {
    let svc = make_service();
    seed_bucket(&svc, "copy-src4");
    seed_bucket(&svc, "copy-dst4");
    seed_object(&svc, "copy-src4", "k.txt", b"body");

    // A typo used to be accepted here while PutObject refused it.
    let mut bad = make_request(Method::PUT, "/copy-dst4/k.txt", &[], b"");
    bad.headers
        .insert("x-amz-copy-source", "copy-src4/k.txt".parse().unwrap());
    bad.headers
        .insert("x-amz-acl", "pubic-read".parse().unwrap());
    assert_aws_err(
        svc.copy_object("123456789012", &bad, "copy-dst4", "k.txt"),
        "InvalidArgument",
    );

    // Canned plus grants names the ACL two ways.
    let mut both = make_request(Method::PUT, "/copy-dst4/k.txt", &[], b"");
    both.headers
        .insert("x-amz-copy-source", "copy-src4/k.txt".parse().unwrap());
    both.headers.insert("x-amz-acl", "private".parse().unwrap());
    both.headers.insert(
        "x-amz-grant-read",
        "uri=http://acs.amazonaws.com/groups/global/AllUsers"
            .parse()
            .unwrap(),
    );
    assert_aws_err(
        svc.copy_object("123456789012", &both, "copy-dst4", "k.txt"),
        "InvalidRequest",
    );

    // And a destination whose ownership disables ACLs refuses them.
    {
        let mut mas = svc.state.write();
        let state = mas.default_mut();
        let b = state.buckets.get_mut("copy-dst4").unwrap();
        b.ownership_controls = Some(
            "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership>\
             </Rule></OwnershipControls>"
                .to_string(),
        );
    }
    let mut enforced = make_request(Method::PUT, "/copy-dst4/k.txt", &[], b"");
    enforced
        .headers
        .insert("x-amz-copy-source", "copy-src4/k.txt".parse().unwrap());
    enforced
        .headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    assert_aws_err(
        svc.copy_object("123456789012", &enforced, "copy-dst4", "k.txt"),
        "AccessControlListNotSupported",
    );
}

#[test]
fn put_object_acl_rejects_an_unresolvable_grantee() {
    // The object paths share parse_grant_headers, which drops what it cannot
    // resolve. Without the shared guard they stored an object ACL with not even
    // the owner's FULL_CONTROL.
    let svc = make_service();
    seed_bucket(&svc, "obj-acl");
    seed_object(&svc, "obj-acl", "k.txt", b"body");

    let mut req = make_request(Method::PUT, "/obj-acl/k.txt", &[("acl", "")], b"");
    req.headers.insert(
        "x-amz-grant-full-control",
        "nonsense=alice".parse().unwrap(),
    );
    assert_aws_err(
        svc.put_object_acl("123456789012", &req, "obj-acl", "k.txt"),
        "InvalidArgument",
    );
}

#[test]
fn create_bucket_rejects_a_grant_header_with_an_empty_grantee() {
    // `id=` with nothing after it names no grantee. Storing a grant with an
    // empty canonical id would put an entry in the bucket's ACL that can never
    // match anyone.
    let svc = make_service();
    for value in ["id=", "uri=", "id=  "] {
        let mut req = make_request(Method::PUT, "/empty-grantee", &[], b"");
        req.headers
            .insert("x-amz-grant-full-control", value.parse().unwrap());
        assert_aws_err(
            svc.create_bucket("123456789012", &req, "empty-grantee"),
            "InvalidArgument",
        );
    }
    assert_aws_err(svc.head_bucket("123456789012", "empty-grantee"), "NotFound");
}

#[test]
fn put_bucket_acl_rejects_a_body_that_is_not_an_access_control_policy() {
    // parse_acl_xml returns no grants for a body with no <Grant> and no
    // <AccessControlPolicy -- whitespace, JSON, a misspelled root. Treating
    // that as "drop every grant" wiped the bucket's ACL, persisted.
    let svc = make_service();
    let create = make_request(Method::PUT, "/pba-junk", &[], b"");
    svc.create_bucket("123456789012", &create, "pba-junk")
        .unwrap();

    let blank = make_request(Method::PUT, "/pba-junk", &[("acl", "")], b"   ");
    assert_aws_err(
        svc.put_bucket_acl("123456789012", &blank, "pba-junk"),
        "MalformedACLError",
    );
    let json = make_request(Method::PUT, "/pba-junk", &[("acl", "")], b"{}");
    assert_aws_err(
        svc.put_bucket_acl("123456789012", &json, "pba-junk"),
        "MalformedXML",
    );

    // An explicit empty AccessControlList still means "remove every grant".
    let empty_list = make_request(
        Method::PUT,
        "/pba-junk",
        &[("acl", "")],
        b"<AccessControlPolicy><Owner><ID>123456789012</ID></Owner><AccessControlList/></AccessControlPolicy>",
    );
    svc.put_bucket_acl("123456789012", &empty_list, "pba-junk")
        .unwrap();
}

#[test]
fn put_bucket_acl_rejects_a_canned_acl_alongside_a_policy_body() {
    // Letting the canned value win would answer 200 while dropping everything
    // the body granted, and this branch persists the result.
    let svc = make_service();
    let create = make_request(Method::PUT, "/pba-canned-body", &[], b"");
    svc.create_bucket("123456789012", &create, "pba-canned-body")
        .unwrap();

    let mut req = make_request(
        Method::PUT,
        "/pba-canned-body",
        &[("acl", "")],
        b"<AccessControlPolicy><Owner><ID>123456789012</ID></Owner><AccessControlList><Grant><Grantee xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>READ</Permission></Grant></AccessControlList></AccessControlPolicy>",
    );
    req.headers.insert("x-amz-acl", "private".parse().unwrap());
    assert_aws_err(
        svc.put_bucket_acl("123456789012", &req, "pba-canned-body"),
        "InvalidRequest",
    );
}

#[test]
fn bucket_acl_email_grantee_round_trips_through_the_policy_body() {
    // GetBucketAcl now emits AmazonCustomerByEmail, so its output can be fed
    // straight back into PutBucketAcl. Reading that grantee as a CanonicalUser
    // would store an empty-id grant -- an entry matching nobody, which the
    // header path refuses.
    let svc = make_service();
    let mut create = make_request(Method::PUT, "/email-rt", &[], b"");
    create.headers.insert(
        "x-amz-grant-read",
        "emailAddress=someone@example.com".parse().unwrap(),
    );
    svc.create_bucket("123456789012", &create, "email-rt")
        .unwrap();

    let get = make_request(Method::GET, "/email-rt", &[("acl", "")], b"");
    let first = svc
        .get_bucket_acl("123456789012", &get, "email-rt")
        .unwrap();
    let xml = std::str::from_utf8(first.body.expect_bytes())
        .unwrap()
        .to_string();

    let put = make_request(Method::PUT, "/email-rt", &[("acl", "")], xml.as_bytes());
    svc.put_bucket_acl("123456789012", &put, "email-rt")
        .unwrap();

    let second = svc
        .get_bucket_acl("123456789012", &get, "email-rt")
        .unwrap();
    let body = std::str::from_utf8(second.body.expect_bytes()).unwrap();
    assert!(
        body.contains("<EmailAddress>someone@example.com</EmailAddress>"),
        "email grantee lost on the body round trip: {body}"
    );
    // Assert against the stored ACL, not the rendering: the grant has to BE an
    // email grantee, not merely render without an empty <ID>.
    let mas = svc.state.read();
    let state = mas.get("123456789012").unwrap();
    let stored = &state.buckets.get("email-rt").unwrap().acl_grants;
    assert!(
        stored.iter().any(|g| {
            g.grantee_type == "AmazonCustomerByEmail"
                && g.grantee_display_name.as_deref() == Some("someone@example.com")
        }),
        "the round trip did not store an email grantee: {stored:?}"
    );
    assert!(
        stored.iter().all(|g| g.grantee_id.as_deref() != Some("")),
        "stored a grant naming nobody: {stored:?}"
    );
}

#[test]
fn put_bucket_acl_rejects_a_grantee_naming_nobody() {
    let svc = make_service();
    let create = make_request(Method::PUT, "/pba-empty-id", &[], b"");
    svc.create_bucket("123456789012", &create, "pba-empty-id")
        .unwrap();

    let req = make_request(
        Method::PUT,
        "/pba-empty-id",
        &[("acl", "")],
        b"<AccessControlPolicy><Owner><ID>123456789012</ID></Owner><AccessControlList><Grant><Grantee xsi:type=\"CanonicalUser\"><ID></ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>",
    );
    assert_aws_err(
        svc.put_bucket_acl("123456789012", &req, "pba-empty-id"),
        "MalformedACLError",
    );
}

#[test]
fn put_bucket_acl_rejects_grant_headers_alongside_a_policy_body() {
    let svc = make_service();
    let create = make_request(Method::PUT, "/pba-both", &[], b"");
    svc.create_bucket("123456789012", &create, "pba-both")
        .unwrap();

    let mut req = make_request(
        Method::PUT,
        "/pba-both",
        &[("acl", "")],
        b"<AccessControlPolicy><Owner><ID>123456789012</ID></Owner><AccessControlList><Grant><Grantee><ID>123456789012</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>",
    );
    req.headers.insert(
        "x-amz-grant-read",
        "uri=http://acs.amazonaws.com/groups/global/AllUsers"
            .parse()
            .unwrap(),
    );
    assert_aws_err(
        svc.put_bucket_acl("123456789012", &req, "pba-both"),
        "InvalidRequest",
    );
}

#[test]
fn create_bucket_rejects_an_unknown_object_ownership_value() {
    // An unrecognized value used to be stored verbatim, and this branch
    // persists it -- so the bucket would come back after a restart with an
    // OwnershipControls rule that means nothing.
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/bad-ownership", &[], b"");
    req.headers
        .insert("x-amz-object-ownership", "Whatever".parse().unwrap());
    assert_aws_err(
        svc.create_bucket("123456789012", &req, "bad-ownership"),
        "InvalidArgument",
    );

    // Casing is significant: `bucket_owner_enforced` matches the stored XML
    // case-sensitively, so a differently cased value must not be accepted here
    // and then read back as "ACLs enabled" by the next call.
    let mut cased = make_request(Method::PUT, "/cased-ownership", &[], b"");
    cased.headers.insert(
        "x-amz-object-ownership",
        "bucketownerenforced".parse().unwrap(),
    );
    assert_aws_err(
        svc.create_bucket("123456789012", &cased, "cased-ownership"),
        "InvalidArgument",
    );
}

#[test]
fn put_bucket_acl_rejects_an_unknown_canned_value_instead_of_wiping_grants() {
    // The catch-all in `canned_acl_grants` turned an unrecognized canned ACL
    // into owner-only, silently stripping every public grant -- and that wipe
    // is now persisted.
    let svc = make_service();
    let create = make_request(Method::PUT, "/pba-canned", &[], b"");
    svc.create_bucket("123456789012", &create, "pba-canned")
        .unwrap();

    let mut req = make_request(Method::PUT, "/pba-canned", &[("acl", "")], b"");
    req.headers
        .insert("x-amz-acl", "pubic-read".parse().unwrap());
    assert_aws_err(
        svc.put_bucket_acl("123456789012", &req, "pba-canned"),
        "InvalidArgument",
    );
}

#[test]
fn put_bucket_acl_honors_grant_headers() {
    let svc = make_service();
    let create = make_request(Method::PUT, "/pba-grant", &[], b"");
    svc.create_bucket("123456789012", &create, "pba-grant")
        .unwrap();

    let mut req = make_request(Method::PUT, "/pba-grant", &[("acl", "")], b"");
    req.headers.insert(
        "x-amz-grant-read",
        "uri=http://acs.amazonaws.com/groups/global/AllUsers"
            .parse()
            .unwrap(),
    );
    svc.put_bucket_acl("123456789012", &req, "pba-grant")
        .unwrap();

    let get = make_request(Method::GET, "/pba-grant", &[("acl", "")], b"");
    let resp = svc
        .get_bucket_acl("123456789012", &get, "pba-grant")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("AllUsers"), "grant headers ignored: {body}");
}

#[test]
fn put_bucket_acl_rejects_a_request_that_names_no_acl() {
    // No canned header, no grant header, no body: malformed, not "drop every
    // grant". Without this the bucket's ACL was wiped to nothing, persisted.
    let svc = make_service();
    let create = make_request(Method::PUT, "/pba-empty", &[], b"");
    svc.create_bucket("123456789012", &create, "pba-empty")
        .unwrap();

    let req = make_request(Method::PUT, "/pba-empty", &[("acl", "")], b"");
    assert_aws_err(
        svc.put_bucket_acl("123456789012", &req, "pba-empty"),
        "MalformedACLError",
    );

    // The grants the bucket had are untouched.
    let get = make_request(Method::GET, "/pba-empty", &[("acl", "")], b"");
    let resp = svc
        .get_bucket_acl("123456789012", &get, "pba-empty")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("FULL_CONTROL"), "{body}");
}

#[test]
fn create_bucket_rejects_an_empty_grant_header() {
    // An empty or blank header value resolves to no grants at all. Storing that
    // would leave the bucket with not even an owner grant, permanently.
    // A blank value is what a client sends for an unset config field, and S3
    // treats the header as absent rather than failing the call.
    let svc = make_service();
    for (name, value) in [("blank-empty", ""), ("blank-space", " ")] {
        let path = format!("/{name}");
        let mut req = make_request(Method::PUT, &path, &[], b"");
        req.headers
            .insert("x-amz-grant-read", value.parse().unwrap());
        svc.create_bucket("123456789012", &req, name)
            .unwrap_or_else(|e| panic!("blank grant header should be ignored: {e:?}"));
    }

    // A value that carries something but names no grantee is still refused: a
    // bare separator, or a grantee form this build cannot resolve.
    for value in [",", "nonsense=x"] {
        let mut bad = make_request(Method::PUT, "/blank-grant", &[], b"");
        bad.headers
            .insert("x-amz-grant-read", value.parse().unwrap());
        assert_aws_err(
            svc.create_bucket("123456789012", &bad, "blank-grant"),
            "InvalidArgument",
        );
    }
    assert_aws_err(svc.head_bucket("123456789012", "blank-grant"), "NotFound");
}

#[test]
fn create_bucket_rejects_partially_unresolvable_grant_headers() {
    // A mixed request is the dangerous case: the resolvable clause would be
    // stored as the bucket's authoritative ACL while the unresolvable one is
    // dropped, so GetBucketAcl would report a permission set nobody asked for.
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/mixed-grant", &[], b"");
    req.headers.insert(
        "x-amz-grant-full-control",
        "nonsense=alice".parse().unwrap(),
    );
    req.headers.insert(
        "x-amz-grant-read",
        "uri=http://acs.amazonaws.com/groups/global/AllUsers"
            .parse()
            .unwrap(),
    );
    assert_aws_err(
        svc.create_bucket("123456789012", &req, "mixed-grant"),
        "InvalidArgument",
    );
    assert_aws_err(svc.head_bucket("123456789012", "mixed-grant"), "NotFound");
}

#[test]
fn create_bucket_ignores_an_unrecognized_grant_header() {
    // Only the five real `x-amz-grant-*` headers count. A lookalike must not
    // turn a valid create into an error, nor collide with a canned ACL.
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/odd-grant", &[], b"");
    req.headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    req.headers
        .insert("x-amz-grant-nonsense", "id=whoever".parse().unwrap());
    svc.create_bucket("123456789012", &req, "odd-grant")
        .unwrap();

    let get = make_request(Method::GET, "/odd-grant", &[("acl", "")], b"");
    let resp = svc
        .get_bucket_acl("123456789012", &get, "odd-grant")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("AllUsers"), "canned ACL not applied: {body}");
}

#[test]
fn create_bucket_stores_tags_from_create_bucket_configuration() {
    let svc = make_service();
    let req = make_request(
        Method::PUT,
        "/tagged",
        &[],
        b"<CreateBucketConfiguration><Tags><Tag><Key>team</Key><Value>a</Value></Tag>\
          <Tag><Key>env</Key><Value>prod</Value></Tag></Tags></CreateBucketConfiguration>",
    );
    svc.create_bucket("123456789012", &req, "tagged").unwrap();

    let get = make_request(Method::GET, "/tagged", &[("tagging", "")], b"");
    let resp = svc
        .get_bucket_tagging("123456789012", &get, "tagged")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("<Tag><Key>env</Key><Value>prod</Value></Tag>"),
        "tag set missing env: {body}"
    );
    assert!(
        body.contains("<Tag><Key>team</Key><Value>a</Value></Tag>"),
        "tag set missing team: {body}"
    );
}

#[test]
fn create_bucket_without_tags_has_no_tag_set() {
    let svc = make_service();
    let req = make_request(
        Method::PUT,
        "/untagged",
        &[],
        b"<CreateBucketConfiguration></CreateBucketConfiguration>",
    );
    svc.create_bucket("123456789012", &req, "untagged").unwrap();

    let get = make_request(Method::GET, "/untagged", &[("tagging", "")], b"");
    assert_aws_err(
        svc.get_bucket_tagging("123456789012", &get, "untagged"),
        "NoSuchTagSet",
    );
}

#[test]
fn create_bucket_rejects_duplicate_tag_keys() {
    let svc = make_service();
    let req = make_request(
        Method::PUT,
        "/dup-tags",
        &[],
        b"<CreateBucketConfiguration><Tags><Tag><Key>team</Key><Value>a</Value></Tag>\
          <Tag><Key>team</Key><Value>b</Value></Tag></Tags></CreateBucketConfiguration>",
    );
    assert_aws_err(
        svc.create_bucket("123456789012", &req, "dup-tags"),
        "InvalidTag",
    );
    // The rejected request must not have left a bucket behind.
    assert_aws_err(svc.head_bucket("123456789012", "dup-tags"), "NotFound");
}

#[test]
fn create_bucket_tags_decode_xml_entities() {
    let svc = make_service();
    let req = make_request(
        Method::PUT,
        "/amp-tags",
        &[],
        b"<CreateBucketConfiguration><Tags><Tag><Key>a&amp;b</Key><Value>c&amp;d</Value></Tag></Tags></CreateBucketConfiguration>",
    );
    svc.create_bucket("123456789012", &req, "amp-tags").unwrap();

    let get = make_request(Method::GET, "/amp-tags", &[("tagging", "")], b"");
    let resp = svc
        .get_bucket_tagging("123456789012", &get, "amp-tags")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("<Tag><Key>a&amp;b</Key><Value>c&amp;d</Value></Tag>"),
        "entity round-trip wrong: {body}"
    );
}

#[test]
fn put_bucket_replication_requires_pass_role_on_the_replication_role() {
    use fakecloud_core::service::AwsService as _;

    let svc = make_service();
    let body = br#"<ReplicationConfiguration><Role>arn:aws:iam::123456789012:role/repl</Role><Rule><Status>Enabled</Status><Destination><Bucket>arn:aws:s3:::dst</Bucket></Destination></Rule></ReplicationConfiguration>"#;
    let req = make_request(Method::PUT, "/src", &[("replication", "")], body);
    let actions = svc.iam_actions_for(&req);
    assert_eq!(actions.len(), 2, "{actions:?}");
    assert_eq!(actions[0].action, "PutBucketReplication");
    assert!(actions[1].is_pass_role());
    assert_eq!(actions[1].resource, "arn:aws:iam::123456789012:role/repl");
    let keys = svc.iam_condition_keys_for(&req, &actions[1]);
    assert_eq!(
        keys["iam:passedtoservice"],
        vec!["s3.amazonaws.com".to_string()]
    );
}

#[test]
fn create_bucket_requires_a_permission_per_setting_it_configures() {
    use fakecloud_core::service::AwsService as _;

    let svc = make_service();
    let names = |req: &AwsRequest| -> Vec<&'static str> {
        svc.iam_actions_for(req).iter().map(|a| a.action).collect()
    };

    // A plain create needs only s3:CreateBucket.
    let plain = make_request(Method::PUT, "/perm-plain", &[], b"");
    assert_eq!(names(&plain), vec!["CreateBucket"]);

    // An ACL that reaches past the owner needs PutBucketAcl -- the settings
    // persist now, so a create that configures them is a configuration call as
    // much as a create.
    let mut canned = make_request(Method::PUT, "/perm-acl", &[], b"");
    canned
        .headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    assert_eq!(names(&canned), vec!["CreateBucket", "PutBucketAcl"]);

    // ...but `private` does not. The model'"'"'s CreateBucket permissions are
    // explicit: "if you set the ACL to private, or if you don'"'"'t specify any
    // ACLs, only the s3:CreateBucket permission is required". A least-privilege
    // caller that always sends `--acl private` must not be refused.
    let mut private = make_request(Method::PUT, "/perm-private", &[], b"");
    private
        .headers
        .insert("x-amz-acl", "private".parse().unwrap());
    assert_eq!(names(&private), vec!["CreateBucket"]);

    let mut granted = make_request(Method::PUT, "/perm-grant", &[], b"");
    granted.headers.insert(
        "x-amz-grant-read",
        "uri=http://acs.amazonaws.com/groups/global/AllUsers"
            .parse()
            .unwrap(),
    );
    assert_eq!(names(&granted), vec!["CreateBucket", "PutBucketAcl"]);

    let mut owned = make_request(Method::PUT, "/perm-own", &[], b"");
    owned.headers.insert(
        "x-amz-object-ownership",
        "BucketOwnerEnforced".parse().unwrap(),
    );
    assert_eq!(
        names(&owned),
        vec!["CreateBucket", "PutBucketOwnershipControls"]
    );

    // Object lock turns versioning on, so both permissions are required.
    let mut locked = make_request(Method::PUT, "/perm-lock", &[], b"");
    locked
        .headers
        .insert("x-amz-bucket-object-lock-enabled", "true".parse().unwrap());
    assert_eq!(
        names(&locked),
        vec![
            "CreateBucket",
            "PutBucketObjectLockConfiguration",
            "PutBucketVersioning"
        ]
    );

    // `aws-exec-read` resolves to owner-only grants (its READ to the EC2
    // service's canonical user is not modeled), so judging by grants alone let
    // it slip past PutBucketAcl -- while the BucketOwnerEnforced conflict check
    // has always treated it as an ACL reaching outside the owner. CloudFormation
    // sends it for `AccessControl: AwsExecRead`, so the gap was reachable.
    let mut exec_read = make_request(Method::PUT, "/perm-exec", &[], b"");
    exec_read
        .headers
        .insert("x-amz-acl", "aws-exec-read".parse().unwrap());
    assert_eq!(names(&exec_read), vec!["CreateBucket", "PutBucketAcl"]);

    // A blank value asks for nothing, so it configures nothing either -- the
    // condition keys skip blanks, and demanding a permission whose key is absent
    // turns a `StringEquals`-gated Allow into a 403 on a request the handler
    // answers with a 400.
    let mut blank = make_request(Method::PUT, "/perm-blank", &[], b"");
    blank.headers.insert("x-amz-acl", "".parse().unwrap());
    blank
        .headers
        .insert("x-amz-object-ownership", "".parse().unwrap());
    assert_eq!(names(&blank), vec!["CreateBucket"]);

    // A false object-lock header configures nothing.
    let mut unlocked = make_request(Method::PUT, "/perm-unlocked", &[], b"");
    unlocked
        .headers
        .insert("x-amz-bucket-object-lock-enabled", "false".parse().unwrap());
    assert_eq!(names(&unlocked), vec!["CreateBucket"]);

    // Everything at once, tags included, in one authorization set.
    let mut everything = make_request(
        Method::PUT,
        "/perm-all",
        &[],
        b"<CreateBucketConfiguration><Tags><Tag><Key>team</Key><Value>a</Value></Tag></Tags></CreateBucketConfiguration>",
    );
    everything
        .headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    everything
        .headers
        .insert("x-amz-object-ownership", "ObjectWriter".parse().unwrap());
    assert_eq!(
        names(&everything),
        vec![
            "CreateBucket",
            "TagResource",
            "PutBucketAcl",
            "PutBucketOwnershipControls"
        ]
    );
}

#[test]
fn acl_condition_keys_are_populated_from_the_request_headers() {
    // A guardrail like `Deny s3:CreateBucket when s3:x-amz-acl != private` is
    // useless while the key is never emitted, which is what made the
    // over-permission above invisible to policy.
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-acl", "public-read".parse().unwrap());
    let keys = s3_condition_keys("CreateBucket", &HashMap::new(), &headers);
    assert_eq!(
        keys.get("s3:x-amz-acl"),
        Some(&vec!["public-read".to_string()])
    );

    let mut grants = HeaderMap::new();
    grants.insert("x-amz-grant-full-control", "id=abc123".parse().unwrap());
    let keys = s3_condition_keys("PutObject", &HashMap::new(), &grants);
    assert_eq!(
        keys.get("s3:x-amz-grant-full-control"),
        Some(&vec!["id=abc123".to_string()])
    );

    // Object ownership and object lock are gated by `iam_actions_for` too, so
    // their keys are emitted as well -- `Deny CreateBucket unless
    // s3:x-amz-object-ownership == BucketOwnerEnforced` is a guardrail people
    // actually write.
    let mut settings = HeaderMap::new();
    settings.insert(
        "x-amz-object-ownership",
        "BucketOwnerEnforced".parse().unwrap(),
    );
    settings.insert("x-amz-bucket-object-lock-enabled", "true".parse().unwrap());
    let keys = s3_condition_keys("CreateBucket", &HashMap::new(), &settings);
    assert_eq!(
        keys.get("s3:x-amz-object-ownership"),
        Some(&vec!["BucketOwnerEnforced".to_string()])
    );
    // AWS defines no condition key for the object-lock header, so emitting one
    // would give policy authors a guardrail that works here and silently does
    // nothing on AWS.
    assert_eq!(keys.get("s3:x-amz-bucket-object-lock-enabled"), None);

    // Absent headers emit nothing, so a policy condition on them safe-fails to
    // "does not apply" rather than matching an empty value.
    assert!(s3_condition_keys("CreateBucket", &HashMap::new(), &HeaderMap::new()).is_empty());

    // A present-but-empty value is skipped for the same reason, so a `Null`
    // check does not read it as set.
    let mut blank = HeaderMap::new();
    blank.insert("x-amz-object-ownership", "".parse().unwrap());
    blank.insert("x-amz-bucket-object-lock-enabled", "".parse().unwrap());
    blank.insert("x-amz-acl", "".parse().unwrap());
    blank.insert("x-amz-grant-read", "".parse().unwrap());
    assert!(
        s3_condition_keys("CreateBucket", &HashMap::new(), &blank).is_empty(),
        "blank values must be skipped on EVERY header, not most of them: {:?}",
        s3_condition_keys("CreateBucket", &HashMap::new(), &blank)
    );
}

#[test]
fn create_bucket_with_tags_also_requires_tag_resource() {
    use fakecloud_core::service::AwsService as _;

    let svc = make_service();
    let tagged = make_request(
        Method::PUT,
        "/tagged-perm",
        &[],
        b"<CreateBucketConfiguration><Tags><Tag><Key>team</Key><Value>a</Value></Tag></Tags></CreateBucketConfiguration>",
    );
    let actions = svc.iam_actions_for(&tagged);
    let names: Vec<&str> = actions.iter().map(|a| a.action).collect();
    assert_eq!(names, vec!["CreateBucket", "TagResource"]);
    // Both are authorized against the same bucket ARN.
    assert_eq!(actions[0].resource, actions[1].resource);
    assert_eq!(actions[1].service, "s3");

    // An untagged create still needs only s3:CreateBucket.
    let plain = make_request(Method::PUT, "/plain-perm", &[], b"");
    let names: Vec<&str> = svc
        .iam_actions_for(&plain)
        .iter()
        .map(|a| a.action)
        .collect();
    assert_eq!(names, vec!["CreateBucket"]);

    // ...as does a CreateBucketConfiguration that carries no tag set.
    let loc_only = make_request(
        Method::PUT,
        "/loc-perm",
        &[],
        b"<CreateBucketConfiguration><LocationConstraint>eu-west-1</LocationConstraint></CreateBucketConfiguration>",
    );
    let names: Vec<&str> = svc
        .iam_actions_for(&loc_only)
        .iter()
        .map(|a| a.action)
        .collect();
    assert_eq!(names, vec!["CreateBucket"]);
}

#[test]
fn create_bucket_request_tags_feed_condition_keys() {
    let req = make_request(
        Method::PUT,
        "/req-tags",
        &[],
        b"<CreateBucketConfiguration><Tags><Tag><Key>team</Key><Value>a</Value></Tag></Tags></CreateBucketConfiguration>",
    );
    // Every action the create implies, not just the create and its TagResource:
    // dispatch rebuilds the context per action, and AWS evaluates one context
    // for the whole request. An `aws:RequestTag/*` guardrail that permits the
    // create must not then deny the ACL or object-lock action it implies.
    for action in [
        "CreateBucket",
        "TagResource",
        "PutBucketAcl",
        "PutBucketOwnershipControls",
        "PutBucketObjectLockConfiguration",
        "PutBucketVersioning",
    ] {
        let tags = s3_request_tags(&req, action).expect("tags extracted");
        assert_eq!(tags.get("team").map(String::as_str), Some("a"), "{action}");
    }

    // On their own operations those actions carry no create-time tag set: the
    // body is not a `CreateBucketConfiguration`, so the map is empty rather
    // than picking up a foreign body's tags.
    let acl_body = make_request(
        Method::PUT,
        "/req-acl?acl",
        &[("acl", "")],
        b"<AccessControlPolicy><Owner><ID>o</ID></Owner></AccessControlPolicy>",
    );
    assert!(s3_request_tags(&acl_body, "PutBucketAcl")
        .expect("tags extracted")
        .is_empty());

    // A body with no tag set yields an empty map, not a miss.
    let plain = make_request(Method::PUT, "/req-plain", &[], b"");
    assert!(s3_request_tags(&plain, "CreateBucket")
        .expect("tags extracted")
        .is_empty());
}

#[test]
fn create_bucket_configuration_tags_ignores_foreign_bodies() {
    // A PutBucketTagging body must not be mistaken for a create-time tag set:
    // the helper is gated on the CreateBucketConfiguration root element.
    let tagging =
        r#"<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>"#;
    assert!(create_bucket_configuration_tags(tagging).is_empty());
    assert!(create_bucket_configuration_tags("").is_empty());

    // A namespaced or attributed <Tags> element is still parsed — the scan
    // matches the <Tag> children, not the container.
    let ns = r#"<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Tags xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Tag><Key>team</Key><Value>a</Value></Tag></Tags></CreateBucketConfiguration>"#;
    assert_eq!(
        create_bucket_configuration_tags(ns),
        vec![("team".to_string(), "a".to_string())]
    );
}

#[test]
fn create_bucket_idempotent_recreate_does_not_reapply_create_time_tags() {
    // The us-east-1 idempotent re-create is a no-op on the existing bucket. It
    // re-applies no create-time setting, tags included — PutBucketTagging is
    // what changes an existing bucket's tags.
    let svc = make_service();
    let plain = make_request(Method::PUT, "/idem-tags", &[], b"");
    svc.create_bucket("123456789012", &plain, "idem-tags")
        .unwrap();

    let tagged = make_request(
        Method::PUT,
        "/idem-tags",
        &[],
        b"<CreateBucketConfiguration><Tags><Tag><Key>team</Key><Value>a</Value></Tag></Tags></CreateBucketConfiguration>",
    );
    let resp = svc
        .create_bucket("123456789012", &tagged, "idem-tags")
        .unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let get = make_request(Method::GET, "/idem-tags", &[("tagging", "")], b"");
    assert_aws_err(
        svc.get_bucket_tagging("123456789012", &get, "idem-tags"),
        "NoSuchTagSet",
    );
}

#[test]
fn create_bucket_already_owned_other_region() {
    let svc = make_service();
    let mut req = make_request(
        Method::PUT,
        "/bk1",
        &[],
        b"<CreateBucketConfiguration><LocationConstraint>eu-west-1</LocationConstraint></CreateBucketConfiguration>",
    );
    req.region = "eu-west-1".to_string();
    svc.create_bucket("123456789012", &req, "bk1").unwrap();
    assert_aws_err(
        svc.create_bucket("123456789012", &req, "bk1"),
        "BucketAlreadyOwnedByYou",
    );
}

#[test]
fn create_bucket_us_east_1_with_explicit_constraint_invalid() {
    let svc = make_service();
    let req = make_request(
        Method::PUT,
        "/bk2",
        &[],
        b"<CreateBucketConfiguration><LocationConstraint>us-east-1</LocationConstraint></CreateBucketConfiguration>",
    );
    assert_aws_err(
        svc.create_bucket("123456789012", &req, "bk2"),
        "InvalidLocationConstraint",
    );
}

#[test]
fn create_bucket_constraint_mismatch_region_errors() {
    let svc = make_service();
    let mut req = make_request(
        Method::PUT,
        "/bk3",
        &[],
        b"<CreateBucketConfiguration><LocationConstraint>us-west-2</LocationConstraint></CreateBucketConfiguration>",
    );
    req.region = "eu-west-1".to_string();
    assert_aws_err(
        svc.create_bucket("123456789012", &req, "bk3"),
        "IllegalLocationConstraintException",
    );
}

#[test]
fn create_bucket_missing_constraint_in_non_default_region_errors() {
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/bk4", &[], b"");
    req.region = "eu-west-1".to_string();
    assert_aws_err(
        svc.create_bucket("123456789012", &req, "bk4"),
        "IllegalLocationConstraintException",
    );
}

#[test]
fn create_bucket_invalid_region_constraint() {
    let svc = make_service();
    let req = make_request(
        Method::PUT,
        "/bk5",
        &[],
        b"<CreateBucketConfiguration><LocationConstraint>not-a-region</LocationConstraint></CreateBucketConfiguration>",
    );
    assert_aws_err(
        svc.create_bucket("123456789012", &req, "bk5"),
        "InvalidLocationConstraint",
    );
}

#[test]
fn create_bucket_with_us_east_1_constraint_when_region_not_matching() {
    let svc = make_service();
    let mut req = make_request(
        Method::PUT,
        "/bk6",
        &[],
        b"<CreateBucketConfiguration><LocationConstraint>us-east-1</LocationConstraint></CreateBucketConfiguration>",
    );
    req.region = "eu-west-1".to_string();
    assert_aws_err(
        svc.create_bucket("123456789012", &req, "bk6"),
        "IllegalLocationConstraintException",
    );
}

#[test]
fn create_bucket_with_object_lock_enables_versioning() {
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/olb", &[], b"");
    req.headers
        .insert("x-amz-bucket-object-lock-enabled", "true".parse().unwrap());
    svc.create_bucket("123456789012", &req, "olb").unwrap();
    let accts = svc.state.read();
    let state = accts.get("123456789012").unwrap();
    let b = state.buckets.get("olb").unwrap();
    assert_eq!(b.versioning.as_deref(), Some("Enabled"));
    assert!(b
        .object_lock_config
        .as_deref()
        .unwrap_or("")
        .contains("ObjectLockEnabled"));
}

#[test]
fn create_bucket_with_object_ownership_header() {
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/own", &[], b"");
    req.headers.insert(
        "x-amz-object-ownership",
        "BucketOwnerEnforced".parse().unwrap(),
    );
    svc.create_bucket("123456789012", &req, "own").unwrap();
    let accts = svc.state.read();
    let state = accts.get("123456789012").unwrap();
    let b = state.buckets.get("own").unwrap();
    assert!(b
        .ownership_controls
        .as_deref()
        .unwrap_or("")
        .contains("BucketOwnerEnforced"));
}

#[test]
fn create_bucket_with_canned_acl_public_read() {
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/prb", &[], b"");
    req.headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    svc.create_bucket("123456789012", &req, "prb").unwrap();
    let accts = svc.state.read();
    let state = accts.get("123456789012").unwrap();
    let b = state.buckets.get("prb").unwrap();
    assert!(!b.acl_grants.is_empty());
}

#[test]
fn delete_bucket_nonexistent_errors() {
    let svc = make_service();
    let req = make_request(Method::DELETE, "/nope", &[], b"");
    assert_aws_err(
        svc.delete_bucket("123456789012", &req, "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn delete_bucket_not_empty_errors() {
    let svc = make_service();
    seed_bucket(&svc, "full");
    seed_object(&svc, "full", "k", b"x");
    let req = make_request(Method::DELETE, "/full", &[], b"");
    assert_aws_err(
        svc.delete_bucket("123456789012", &req, "full"),
        "BucketNotEmpty",
    );
}

#[test]
fn delete_bucket_with_versions_not_empty_errors() {
    let svc = make_service();
    seed_bucket(&svc, "ver");
    {
        let mut mas = svc.state.write();
        let state = mas.default_mut();
        let b = state.buckets.get_mut("ver").unwrap();
        b.object_versions.insert(
            "k".to_string(),
            vec![S3Object {
                key: "k".to_string(),
                body: fakecloud_persistence::BodyRef::Memory(Bytes::from_static(b"v")),
                content_type: "text/plain".to_string(),
                etag: "\"abc\"".to_string(),
                size: 1,
                last_modified: chrono::Utc::now(),
                ..Default::default()
            }],
        );
    }
    let req = make_request(Method::DELETE, "/ver", &[], b"");
    assert_aws_err(
        svc.delete_bucket("123456789012", &req, "ver"),
        "BucketNotEmpty",
    );
}

#[test]
fn delete_bucket_empty_succeeds() {
    let svc = make_service();
    seed_bucket(&svc, "empty");
    let req = make_request(Method::DELETE, "/empty", &[], b"");
    let resp = svc.delete_bucket("123456789012", &req, "empty").unwrap();
    assert_eq!(resp.status, StatusCode::NO_CONTENT);
}

#[test]
fn head_bucket_missing_errors() {
    // HeadBucket's Smithy `errors:` list is just [NotFound]; the wire-
    // level code lives in `x-amz-error-code` because HEAD has no body.
    // Use the model-declared code instead of `NoSuchBucket`.
    let svc = make_service();
    assert_aws_err(svc.head_bucket("123456789012", "nope"), "NotFound");
}

#[tokio::test]
async fn bucket_subresource_without_bucket_errors() {
    // Regression: requests like `GET /?tagging` (bucket name omitted)
    // used to fall through to `list_buckets` and return 200, masking the
    // missing required bucket. Real S3 rejects these with a validation
    // error; mirror that so conformance probes targeting required-field
    // omission see the expected 4xx.
    use fakecloud_core::service::AwsService;
    let svc = make_service();
    let cases: &[(Method, &str)] = &[
        (Method::GET, "tagging"),
        (Method::GET, "acl"),
        (Method::GET, "lifecycle"),
        (Method::GET, "encryption"),
        (Method::PUT, "tagging"),
        (Method::PUT, "encryption"),
        (Method::DELETE, "tagging"),
        (Method::DELETE, "lifecycle"),
    ];
    for (method, q) in cases {
        let req = make_request(method.clone(), "/", &[(q, "")], b"");
        let resp = svc.handle(req).await;
        let err = match resp {
            Ok(_) => panic!("{method} /?{q} should error, got Ok"),
            Err(e) => e,
        };
        assert_eq!(
            err.code(),
            "InvalidBucketName",
            "method={method} q={q} got {:?}",
            err.code()
        );
    }
}

#[test]
fn head_bucket_exists_returns_ok() {
    let svc = make_service();
    seed_bucket(&svc, "hb");
    let resp = svc.head_bucket("123456789012", "hb").unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[test]
fn head_bucket_returns_x_amz_bucket_region_header() {
    // Regression for issue #816 / #817 class: AWS Toolkit reads
    // x-amz-bucket-region from HeadBucket to do region routing.
    let svc = make_service();
    seed_bucket(&svc, "hbr");
    let resp = svc.head_bucket("123456789012", "hbr").unwrap();
    assert_eq!(
        resp.headers
            .get("x-amz-bucket-region")
            .and_then(|v| v.to_str().ok()),
        Some("us-east-1"),
    );
    assert_eq!(
        resp.headers
            .get("x-amz-bucket-location-type")
            .and_then(|v| v.to_str().ok()),
        Some("Region"),
    );
}

#[test]
fn get_bucket_location_us_east_1_returns_empty() {
    let svc = make_service();
    seed_bucket(&svc, "loc");
    let resp = svc.get_bucket_location("123456789012", "loc").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<LocationConstraint"));
    assert!(body.contains("></LocationConstraint>"));
}

#[test]
fn get_bucket_location_other_region_returns_region() {
    let svc = make_service();
    {
        let mut mas = svc.state.write();
        let state = mas.default_mut();
        state
            .buckets
            .insert("eu".to_string(), S3Bucket::new("eu", "eu-west-1", "owner"));
    }
    let resp = svc.get_bucket_location("123456789012", "eu").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains(">eu-west-1<"));
}

// ── objects.rs additional coverage ──

#[tokio::test]
async fn get_object_range_request() {
    let svc = make_service();
    seed_bucket(&svc, "range");
    let req = make_request(Method::PUT, "/range/k", &[], b"0123456789ABCDEF");
    svc.put_object("123456789012", &req, "range", "k")
        .await
        .unwrap();

    let mut req = make_request(Method::GET, "/range/k", &[], b"");
    req.headers.insert("range", "bytes=2-5".parse().unwrap());
    let resp = svc.get_object("123456789012", &req, "range", "k").unwrap();
    assert_eq!(resp.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.body.expect_bytes(), b"2345");
}

#[tokio::test]
async fn get_object_range_suffix() {
    let svc = make_service();
    seed_bucket(&svc, "rsx");
    let req = make_request(Method::PUT, "/rsx/k", &[], b"0123456789");
    svc.put_object("123456789012", &req, "rsx", "k")
        .await
        .unwrap();

    let mut req = make_request(Method::GET, "/rsx/k", &[], b"");
    req.headers.insert("range", "bytes=-3".parse().unwrap());
    let resp = svc.get_object("123456789012", &req, "rsx", "k").unwrap();
    assert_eq!(resp.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.body.expect_bytes(), b"789");
}

#[tokio::test]
async fn get_object_range_open_ended() {
    let svc = make_service();
    seed_bucket(&svc, "roe");
    let req = make_request(Method::PUT, "/roe/k", &[], b"0123456789");
    svc.put_object("123456789012", &req, "roe", "k")
        .await
        .unwrap();

    let mut req = make_request(Method::GET, "/roe/k", &[], b"");
    req.headers.insert("range", "bytes=7-".parse().unwrap());
    let resp = svc.get_object("123456789012", &req, "roe", "k").unwrap();
    assert_eq!(resp.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.body.expect_bytes(), b"789");
}

// A large object read (the case the spawn_blocking / block_in_place offload
// targets) must still return the exact bytes for both a full GET and a ranged
// GET. Behavior is unchanged; this guards the run_blocking_io wrapping.
#[tokio::test]
async fn get_object_large_full_and_range_returns_exact_bytes() {
    let svc = make_service();
    seed_bucket(&svc, "big");
    // ~1 MiB of a repeating, position-dependent pattern so a wrong offset or a
    // truncated read is detectable.
    let data: Vec<u8> = (0..1_048_576u32).map(|i| (i % 251) as u8).collect();
    let req = make_request(Method::PUT, "/big/obj", &[], &data);
    svc.put_object("123456789012", &req, "big", "obj")
        .await
        .unwrap();

    // Full GET returns every byte.
    let req = make_request(Method::GET, "/big/obj", &[], b"");
    let resp = svc.get_object("123456789012", &req, "big", "obj").unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.body.expect_bytes(), Bytes::from(data.clone()));

    // Ranged GET slices the correct window.
    let mut req = make_request(Method::GET, "/big/obj", &[], b"");
    req.headers
        .insert("range", "bytes=1000-1999".parse().unwrap());
    let resp = svc.get_object("123456789012", &req, "big", "obj").unwrap();
    assert_eq!(resp.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.body.expect_bytes(),
        Bytes::copy_from_slice(&data[1000..=1999])
    );
}

#[tokio::test]
async fn get_object_range_invalid_format() {
    let svc = make_service();
    seed_bucket(&svc, "rinv");
    let req = make_request(Method::PUT, "/rinv/k", &[], b"hello");
    svc.put_object("123456789012", &req, "rinv", "k")
        .await
        .unwrap();

    let mut req = make_request(Method::GET, "/rinv/k", &[], b"");
    req.headers.insert("range", "bogus=2-5".parse().unwrap());
    // Non-standard prefix -> full content expected
    let resp = svc.get_object("123456789012", &req, "rinv", "k").unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.body.expect_bytes(), b"hello");
}

#[tokio::test]
async fn get_object_if_match_mismatch_errors() {
    let svc = make_service();
    seed_bucket(&svc, "ifm");
    let req = make_request(Method::PUT, "/ifm/k", &[], b"abc");
    svc.put_object("123456789012", &req, "ifm", "k")
        .await
        .unwrap();

    let mut req = make_request(Method::GET, "/ifm/k", &[], b"");
    req.headers
        .insert("if-match", "\"nomatch\"".parse().unwrap());
    let err = svc.get_object("123456789012", &req, "ifm", "k");
    assert_aws_err(err, "PreconditionFailed");
}

#[tokio::test]
async fn get_object_if_none_match_star_not_modified() {
    let svc = make_service();
    seed_bucket(&svc, "inm");
    let req = make_request(Method::PUT, "/inm/k", &[], b"abc");
    svc.put_object("123456789012", &req, "inm", "k")
        .await
        .unwrap();

    let mut req = make_request(Method::GET, "/inm/k", &[], b"");
    req.headers.insert("if-none-match", "*".parse().unwrap());
    let err = svc.get_object("123456789012", &req, "inm", "k");
    assert!(err.is_err());
}

#[tokio::test]
async fn head_object_range_request() {
    let svc = make_service();
    seed_bucket(&svc, "hrng");
    let req = make_request(Method::PUT, "/hrng/k", &[], b"0123456789");
    svc.put_object("123456789012", &req, "hrng", "k")
        .await
        .unwrap();

    let mut req = make_request(Method::HEAD, "/hrng/k", &[], b"");
    req.headers.insert("range", "bytes=2-5".parse().unwrap());
    let resp = svc.head_object("123456789012", &req, "hrng", "k").unwrap();
    assert_eq!(resp.status, StatusCode::PARTIAL_CONTENT);
}

#[tokio::test]
async fn put_object_with_metadata_headers() {
    let svc = make_service();
    seed_bucket(&svc, "meta");
    let mut req = make_request(Method::PUT, "/meta/k", &[], b"x");
    req.headers
        .insert("x-amz-meta-user", "alice".parse().unwrap());
    req.headers
        .insert("x-amz-meta-env", "prod".parse().unwrap());
    svc.put_object("123456789012", &req, "meta", "k")
        .await
        .unwrap();

    let req = make_request(Method::HEAD, "/meta/k", &[], b"");
    let resp = svc.head_object("123456789012", &req, "meta", "k").unwrap();
    assert_eq!(resp.headers.get("x-amz-meta-user").unwrap(), "alice");
    assert_eq!(resp.headers.get("x-amz-meta-env").unwrap(), "prod");
}

#[tokio::test]
async fn put_object_with_storage_class_header() {
    let svc = make_service();
    seed_bucket(&svc, "stor");
    let mut req = make_request(Method::PUT, "/stor/k", &[], b"x");
    req.headers
        .insert("x-amz-storage-class", "STANDARD_IA".parse().unwrap());
    svc.put_object("123456789012", &req, "stor", "k")
        .await
        .unwrap();

    let req = make_request(Method::HEAD, "/stor/k", &[], b"");
    let resp = svc.head_object("123456789012", &req, "stor", "k").unwrap();
    assert_eq!(
        resp.headers.get("x-amz-storage-class").unwrap(),
        "STANDARD_IA"
    );
}

#[tokio::test]
async fn put_object_with_website_redirect() {
    let svc = make_service();
    seed_bucket(&svc, "wr");
    let mut req = make_request(Method::PUT, "/wr/k", &[], b"x");
    req.headers.insert(
        "x-amz-website-redirect-location",
        "/elsewhere".parse().unwrap(),
    );
    svc.put_object("123456789012", &req, "wr", "k")
        .await
        .unwrap();
    let req = make_request(Method::GET, "/wr/k", &[], b"");
    let resp = svc.get_object("123456789012", &req, "wr", "k").unwrap();
    assert_eq!(
        resp.headers.get("x-amz-website-redirect-location").unwrap(),
        "/elsewhere"
    );
}

#[test]
fn delete_object_nonexistent_is_ok() {
    let svc = make_service();
    seed_bucket(&svc, "dne");
    let req = make_request(Method::DELETE, "/dne/missing", &[], b"");
    let resp = svc
        .delete_object("123456789012", &req, "dne", "missing")
        .unwrap();
    assert_eq!(resp.status, StatusCode::NO_CONTENT);
}

#[test]
fn delete_object_bucket_not_found() {
    let svc = make_service();
    let req = make_request(Method::DELETE, "/nope/k", &[], b"");
    assert_aws_err(
        svc.delete_object("123456789012", &req, "nope", "k"),
        "NoSuchBucket",
    );
}

#[tokio::test]
async fn list_objects_v2_with_prefix_and_delimiter() {
    let svc = make_service();
    seed_bucket(&svc, "pfxd");
    for k in &["a/1", "a/2", "b/1"] {
        let req = make_request(Method::PUT, &format!("/pfxd/{k}"), &[], b"x");
        svc.put_object("123456789012", &req, "pfxd", k)
            .await
            .unwrap();
    }
    let req = make_request(
        Method::GET,
        "/pfxd",
        &[("list-type", "2"), ("prefix", "a/"), ("delimiter", "/")],
        b"",
    );
    let resp = svc.list_objects_v2("123456789012", &req, "pfxd").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Contents>"));
}

#[tokio::test]
async fn list_objects_v1_basic() {
    let svc = make_service();
    seed_bucket(&svc, "v1");
    for k in &["a", "b"] {
        let req = make_request(Method::PUT, &format!("/v1/{k}"), &[], b"x");
        svc.put_object("123456789012", &req, "v1", k).await.unwrap();
    }
    let req = make_request(Method::GET, "/v1", &[], b"");
    let resp = svc.list_objects_v1("123456789012", &req, "v1").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<ListBucketResult"));
    assert!(body.contains("<Key>a</Key>"));
    assert!(body.contains("<Key>b</Key>"));
}

/// ListObjects v1: a truncated listing WITHOUT a delimiter must NOT emit
/// NextMarker (the client resumes from the last key itself). AWS only
/// returns NextMarker when a delimiter is supplied.
#[tokio::test]
async fn list_objects_v1_truncated_no_delimiter_omits_next_marker() {
    let svc = make_service();
    seed_bucket(&svc, "nm1");
    for k in &["k0", "k1", "k2"] {
        let req = make_request(Method::PUT, &format!("/nm1/{k}"), &[], b"x");
        svc.put_object("123456789012", &req, "nm1", k)
            .await
            .unwrap();
    }
    let req = make_request(Method::GET, "/nm1", &[("max-keys", "1")], b"");
    let resp = svc.list_objects_v1("123456789012", &req, "nm1").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<IsTruncated>true</IsTruncated>"));
    assert!(
        !body.contains("<NextMarker>"),
        "NextMarker must be omitted without a delimiter: {body}"
    );
}

/// ListObjects v1: a truncated listing WITH a delimiter includes NextMarker
/// so the client can resume past the whole common-prefix group.
#[tokio::test]
async fn list_objects_v1_truncated_with_delimiter_includes_next_marker() {
    let svc = make_service();
    seed_bucket(&svc, "nm2");
    for k in &["p1/a", "p2/b", "p3/c"] {
        let req = make_request(Method::PUT, &format!("/nm2/{k}"), &[], b"x");
        svc.put_object("123456789012", &req, "nm2", k)
            .await
            .unwrap();
    }
    let req = make_request(
        Method::GET,
        "/nm2",
        &[("max-keys", "1"), ("delimiter", "/")],
        b"",
    );
    let resp = svc.list_objects_v1("123456789012", &req, "nm2").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<IsTruncated>true</IsTruncated>"));
    assert!(
        body.contains("<NextMarker>p1/</NextMarker>"),
        "NextMarker should carry the common-prefix cursor: {body}"
    );
}

#[test]
fn get_object_key_not_found() {
    let svc = make_service();
    seed_bucket(&svc, "gkn");
    let req = make_request(Method::GET, "/gkn/missing", &[], b"");
    assert_aws_err(
        svc.get_object("123456789012", &req, "gkn", "missing"),
        "NoSuchKey",
    );
}

#[test]
fn get_object_bucket_not_found() {
    // Strict conformance: GetObject's Smithy `errors` excludes NoSuchBucket,
    // so missing-bucket collapses to NoSuchKey.
    let svc = make_service();
    let req = make_request(Method::GET, "/ghost/k", &[], b"");
    assert_aws_err(
        svc.get_object("123456789012", &req, "ghost", "k"),
        "NoSuchKey",
    );
}

// ── restore_object ──

#[tokio::test]
async fn restore_object_non_archival_errors() {
    let svc = make_service();
    seed_bucket(&svc, "roc");
    let req = make_request(Method::PUT, "/roc/k", &[], b"x");
    svc.put_object("123456789012", &req, "roc", "k")
        .await
        .unwrap();

    let req = make_request(Method::POST, "/roc/k", &[("restore", "")], b"");
    assert_aws_err(
        svc.restore_object("123456789012", &req, "roc", "k"),
        "InvalidObjectState",
    );
}

#[tokio::test]
async fn restore_object_glacier_accepted() {
    let svc = make_service();
    seed_bucket(&svc, "rog");
    let mut req = make_request(Method::PUT, "/rog/k", &[], b"x");
    req.headers
        .insert("x-amz-storage-class", "GLACIER".parse().unwrap());
    svc.put_object("123456789012", &req, "rog", "k")
        .await
        .unwrap();

    let req = make_request(Method::POST, "/rog/k", &[("restore", "")], b"");
    let resp = svc
        .restore_object("123456789012", &req, "rog", "k")
        .unwrap();
    assert_eq!(resp.status, StatusCode::ACCEPTED);
}

#[test]
fn restore_object_nonexistent_key() {
    let svc = make_service();
    seed_bucket(&svc, "rnk");
    let req = make_request(Method::POST, "/rnk/ghost", &[("restore", "")], b"");
    assert_aws_err(
        svc.restore_object("123456789012", &req, "rnk", "ghost"),
        "NoSuchKey",
    );
}

// ── list_object_versions ──

#[tokio::test]
async fn list_object_versions_basic() {
    let svc = make_service();
    seed_bucket(&svc, "lov");
    {
        let mut mas = svc.state.write();
        let state = mas.default_mut();
        let b = state.buckets.get_mut("lov").unwrap();
        b.versioning = Some("Enabled".to_string());
    }
    let req = make_request(Method::PUT, "/lov/k", &[], b"v1");
    svc.put_object("123456789012", &req, "lov", "k")
        .await
        .unwrap();
    let req = make_request(Method::PUT, "/lov/k", &[], b"v2");
    svc.put_object("123456789012", &req, "lov", "k")
        .await
        .unwrap();

    let req = make_request(Method::GET, "/lov", &[("versions", "")], b"");
    let resp = svc
        .list_object_versions("123456789012", &req, "lov")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<ListVersionsResult"));
}

#[test]
fn delete_objects_nonexistent_bucket() {
    let svc = make_service();
    let xml = b"<Delete><Object><Key>k</Key></Object></Delete>";
    let req = make_request(Method::POST, "/ghost", &[("delete", "")], xml);
    assert_aws_err(
        svc.delete_objects("123456789012", &req, "ghost"),
        "NoSuchBucket",
    );
}

#[tokio::test]
async fn put_object_custom_content_type() {
    let svc = make_service();
    seed_bucket(&svc, "ct");
    let mut req = make_request(Method::PUT, "/ct/k", &[], b"hi");
    req.headers
        .insert("content-type", "text/plain".parse().unwrap());
    svc.put_object("123456789012", &req, "ct", "k")
        .await
        .unwrap();
    let req = make_request(Method::GET, "/ct/k", &[], b"");
    let resp = svc.get_object("123456789012", &req, "ct", "k").unwrap();
    assert_eq!(resp.content_type, "text/plain");
}

#[test]
fn head_object_bucket_not_found() {
    let svc = make_service();
    let req = make_request(Method::HEAD, "/ghost/k", &[], b"");
    let result = svc.head_object("123456789012", &req, "ghost", "k");
    assert!(result.is_err());
}

#[tokio::test]
async fn get_object_attributes_basic() {
    let svc = make_service();
    seed_bucket(&svc, "goa");
    let req = make_request(Method::PUT, "/goa/k", &[], b"hi");
    svc.put_object("123456789012", &req, "goa", "k")
        .await
        .unwrap();

    let mut req = make_request(Method::GET, "/goa/k", &[("attributes", "")], b"");
    req.headers.insert(
        "x-amz-object-attributes",
        "ETag,ObjectSize".parse().unwrap(),
    );
    let resp = svc
        .get_object_attributes("123456789012", &req, "goa", "k")
        .unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn get_object_attributes_etag_is_unquoted() {
    // Regression: GetObjectAttributes returns the ETag WITHOUT the
    // surrounding quotes that GetObject/HeadObject wrap it in.
    let svc = make_service();
    seed_bucket(&svc, "goaetag");
    let req = make_request(Method::PUT, "/goaetag/k", &[], b"hi");
    svc.put_object("123456789012", &req, "goaetag", "k")
        .await
        .unwrap();

    let mut req = make_request(Method::GET, "/goaetag/k", &[("attributes", "")], b"");
    req.headers
        .insert("x-amz-object-attributes", "ETag".parse().unwrap());
    let resp = svc
        .get_object_attributes("123456789012", &req, "goaetag", "k")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let md5 = compute_md5(b"hi");
    assert!(
        body.contains(&format!("<ETag>{md5}</ETag>")),
        "ETag must be unquoted: {body}"
    );
    assert!(!body.contains("&quot;"), "ETag must not be quoted: {body}");
}

#[test]
fn get_object_attributes_multipart_etag_is_unquoted() {
    // Regression: a multipart ETag (`md5-N`) is also returned unquoted.
    let svc = make_service();
    seed_bucket(&svc, "goampu");
    {
        let mut mas = svc.state.write();
        let state = mas.default_mut();
        let b = state.buckets.get_mut("goampu").expect("bucket seeded");
        let obj = S3Object {
            key: "k".to_string(),
            body: fakecloud_persistence::BodyRef::Memory(Bytes::from_static(b"x")),
            content_type: "application/octet-stream".to_string(),
            etag: "d41d8cd98f00b204e9800998ecf8427e-3".to_string(),
            size: 1,
            parts_count: Some(3),
            last_modified: chrono::Utc::now(),
            ..Default::default()
        };
        b.objects.insert("k".to_string(), obj);
    }
    let mut req = make_request(Method::GET, "/goampu/k", &[("attributes", "")], b"");
    req.headers
        .insert("x-amz-object-attributes", "ETag".parse().unwrap());
    let resp = svc
        .get_object_attributes("123456789012", &req, "goampu", "k")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("<ETag>d41d8cd98f00b204e9800998ecf8427e-3</ETag>"),
        "multipart ETag must be unquoted: {body}"
    );
    assert!(!body.contains("&quot;"), "ETag must not be quoted: {body}");
}

#[test]
fn get_object_attributes_bucket_not_found() {
    let svc = make_service();
    let req = make_request(Method::GET, "/ghost/k", &[("attributes", "")], b"");
    let result = svc.get_object_attributes("123456789012", &req, "ghost", "k");
    assert!(result.is_err());
}

#[test]
fn get_object_attributes_key_not_found() {
    let svc = make_service();
    seed_bucket(&svc, "gak");
    let req = make_request(Method::GET, "/gak/ghost", &[("attributes", "")], b"");
    let result = svc.get_object_attributes("123456789012", &req, "gak", "ghost");
    assert!(result.is_err());
}

// ── ACL ──

#[test]
fn get_object_acl_bucket_not_found() {
    let svc = make_service();
    let req = make_request(Method::GET, "/ghost/k", &[("acl", "")], b"");
    assert!(svc
        .get_object_acl("123456789012", &req, "ghost", "k")
        .is_err());
}

#[test]
fn put_object_acl_bucket_not_found() {
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/ghost/k", &[("acl", "")], b"");
    req.headers.insert("x-amz-acl", "private".parse().unwrap());
    assert!(svc
        .put_object_acl("123456789012", &req, "ghost", "k")
        .is_err());
}

#[tokio::test]
async fn get_object_acl_returns_acl_xml() {
    let svc = make_service();
    seed_bucket(&svc, "acl");
    let req = make_request(Method::PUT, "/acl/k", &[], b"x");
    svc.put_object("123456789012", &req, "acl", "k")
        .await
        .unwrap();
    let req = make_request(Method::GET, "/acl/k", &[("acl", "")], b"");
    let resp = svc
        .get_object_acl("123456789012", &req, "acl", "k")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("AccessControlPolicy"));
}

// ── object lock ──

#[test]
fn put_object_retention_bucket_not_found() {
    let svc = make_service();
    let xml = b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>";
    let req = make_request(Method::PUT, "/ghost/k", &[("retention", "")], xml);
    assert!(svc
        .put_object_retention("123456789012", &req, "ghost", "k")
        .is_err());
}

#[test]
fn get_object_legal_hold_bucket_not_found() {
    let svc = make_service();
    let req = make_request(Method::GET, "/ghost/k", &[("legal-hold", "")], b"");
    assert!(svc
        .get_object_legal_hold("123456789012", &req, "ghost", "k")
        .is_err());
}

#[test]
fn get_object_retention_bucket_not_found() {
    let svc = make_service();
    let req = make_request(Method::GET, "/ghost/k", &[("retention", "")], b"");
    assert!(svc
        .get_object_retention("123456789012", &req, "ghost", "k")
        .is_err());
}

// ── Multipart variations ──

#[test]
fn list_multipart_uploads_nonexistent_bucket() {
    let svc = make_service();
    assert!(svc
        .list_multipart_uploads("123456789012", "ghost", &std::collections::HashMap::new())
        .is_err());
}

#[test]
fn list_multipart_uploads_empty() {
    let svc = make_service();
    seed_bucket(&svc, "empmp");
    let resp = svc
        .list_multipart_uploads("123456789012", "empmp", &std::collections::HashMap::new())
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<ListMultipartUploadsResult"));
}

#[test]
fn put_public_access_block_bucket_not_found() {
    let svc = make_service();
    let xml = b"<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls></PublicAccessBlockConfiguration>";
    let req = make_request(Method::PUT, "/ghost", &[("publicAccessBlock", "")], xml);
    assert!(svc
        .put_public_access_block("123456789012", &req, "ghost")
        .is_err());
}

#[test]
fn public_access_block_lifecycle() {
    let svc = make_service();
    seed_bucket(&svc, "pab");
    let xml = b"<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls></PublicAccessBlockConfiguration>";
    let req = make_request(Method::PUT, "/pab", &[("publicAccessBlock", "")], xml);
    svc.put_public_access_block("123456789012", &req, "pab")
        .unwrap();

    let resp = svc.get_public_access_block("123456789012", "pab").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("BlockPublicAcls"));

    svc.delete_public_access_block("123456789012", "pab")
        .unwrap();
}

#[test]
fn put_bucket_replication_bucket_not_found() {
    let svc = make_service();
    let xml = b"<ReplicationConfiguration><Role>arn</Role></ReplicationConfiguration>";
    let req = make_request(Method::PUT, "/ghost", &[("replication", "")], xml);
    assert!(svc
        .put_bucket_replication("123456789012", &req, "ghost")
        .is_err());
}

#[test]
fn put_ownership_controls_bucket_not_found() {
    let svc = make_service();
    let xml = b"<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>";
    let req = make_request(Method::PUT, "/ghost", &[("ownershipControls", "")], xml);
    assert!(svc
        .put_bucket_ownership_controls("123456789012", &req, "ghost")
        .is_err());
}

#[test]
fn put_bucket_accelerate_bucket_not_found() {
    let svc = make_service();
    let xml = b"<AccelerateConfiguration><Status>Enabled</Status></AccelerateConfiguration>";
    let req = make_request(Method::PUT, "/ghost", &[("accelerate", "")], xml);
    assert!(svc
        .put_bucket_accelerate("123456789012", &req, "ghost")
        .is_err());
}

#[test]
fn get_bucket_accelerate_lifecycle() {
    let svc = make_service();
    seed_bucket(&svc, "acc");
    let xml = b"<AccelerateConfiguration><Status>Enabled</Status></AccelerateConfiguration>";
    let req = make_request(Method::PUT, "/acc", &[("accelerate", "")], xml);
    svc.put_bucket_accelerate("123456789012", &req, "acc")
        .unwrap();
    let resp = svc.get_bucket_accelerate("123456789012", "acc").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("AccelerateConfiguration"));
}

#[test]
fn put_bucket_website_bucket_not_found() {
    let svc = make_service();
    let xml = b"<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument></WebsiteConfiguration>";
    let req = make_request(Method::PUT, "/ghost", &[("website", "")], xml);
    assert!(svc
        .put_bucket_website("123456789012", &req, "ghost")
        .is_err());
}

#[test]
fn put_object_tagging_bucket_not_found() {
    let svc = make_service();
    let xml = b"<Tagging><TagSet></TagSet></Tagging>";
    let req = make_request(Method::PUT, "/ghost/k", &[("tagging", "")], xml);
    assert!(svc
        .put_object_tagging("123456789012", &req, "ghost", "k")
        .is_err());
}

#[test]
fn put_object_tagging_key_not_found() {
    let svc = make_service();
    seed_bucket(&svc, "pot");
    let xml = b"<Tagging><TagSet></TagSet></Tagging>";
    let req = make_request(Method::PUT, "/pot/ghost", &[("tagging", "")], xml);
    assert!(svc
        .put_object_tagging("123456789012", &req, "pot", "ghost")
        .is_err());
}

#[tokio::test]
async fn put_object_tagging_lifecycle() {
    let svc = make_service();
    seed_bucket(&svc, "pota");
    let req = make_request(Method::PUT, "/pota/k", &[], b"x");
    svc.put_object("123456789012", &req, "pota", "k")
        .await
        .unwrap();

    let xml = b"<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
    let req = make_request(Method::PUT, "/pota/k", &[("tagging", "")], xml);
    svc.put_object_tagging("123456789012", &req, "pota", "k")
        .unwrap();

    let req = make_request(Method::GET, "/pota/k", &[("tagging", "")], b"");
    let resp = svc
        .get_object_tagging("123456789012", &req, "pota", "k")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Key>env</Key>"));

    svc.delete_object_tagging("123456789012", "pota", "k")
        .unwrap();
}

#[test]
fn delete_object_tagging_bucket_not_found() {
    let svc = make_service();
    assert!(svc
        .delete_object_tagging("123456789012", "ghost", "k")
        .is_err());
}

#[test]
fn put_bucket_tagging_bucket_not_found() {
    let svc = make_service();
    let xml = b"<Tagging><TagSet></TagSet></Tagging>";
    let req = make_request(Method::PUT, "/ghost", &[("tagging", "")], xml);
    assert!(svc
        .put_bucket_tagging("123456789012", &req, "ghost")
        .is_err());
}

#[test]
fn bucket_tagging_lifecycle() {
    let svc = make_service();
    seed_bucket(&svc, "bt");
    let xml = b"<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
    let req = make_request(Method::PUT, "/bt", &[("tagging", "")], xml);
    svc.put_bucket_tagging("123456789012", &req, "bt").unwrap();

    let req = make_request(Method::GET, "/bt", &[("tagging", "")], b"");
    let resp = svc.get_bucket_tagging("123456789012", &req, "bt").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Key>env</Key>"));

    let req = make_request(Method::DELETE, "/bt", &[("tagging", "")], b"");
    svc.delete_bucket_tagging("123456789012", &req, "bt")
        .unwrap();
}

#[tokio::test]
async fn delete_objects_batch_respects_compliance_retention() {
    // Single DeleteObject already gates COMPLIANCE retention; the
    // batch DeleteObjects unversioned arm previously skipped the
    // gate, letting a caller sidestep compliance via the batch
    // endpoint. This test pins the gate in place.
    let svc = make_service();
    seed_bucket(&svc, "comp-batch");
    seed_object(&svc, "comp-batch", "locked.txt", b"x");
    seed_object(&svc, "comp-batch", "free.txt", b"y");

    // Apply COMPLIANCE retention to one of the two objects.
    let body = b"<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2099-01-01T00:00:00Z</RetainUntilDate></Retention>";
    let req = make_request(
        Method::PUT,
        "/comp-batch/locked.txt",
        &[("retention", "")],
        body,
    );
    svc.put_object_retention("123456789012", &req, "comp-batch", "locked.txt")
        .unwrap();

    // Batch delete both — compliance object must come back as Error.
    let xml = r#"<Delete><Object><Key>locked.txt</Key></Object><Object><Key>free.txt</Key></Object></Delete>"#;
    let req = make_request(
        Method::POST,
        "/comp-batch",
        &[("delete", "")],
        xml.as_bytes(),
    );
    let resp = svc
        .delete_objects("123456789012", &req, "comp-batch")
        .unwrap();
    let body_str = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body_str.contains("<Key>locked.txt</Key>") && body_str.contains("InvalidRequest")
            || body_str.contains("<Key>locked.txt</Key>") && body_str.contains("AccessDenied"),
        "expected locked.txt to surface as Error in DeleteResult, got: {body_str}"
    );
    assert!(
        body_str.contains("<Deleted><Key>free.txt</Key>"),
        "expected free.txt to be deleted, got: {body_str}"
    );
}

#[tokio::test]
async fn bucket_owner_enforced_rejects_put_bucket_acl_and_object_acl() {
    let svc = make_service();
    seed_bucket(&svc, "owner-enforced");

    let body = br#"<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>"#;
    let req = make_request(
        Method::PUT,
        "/owner-enforced",
        &[("ownershipControls", "")],
        body,
    );
    svc.put_bucket_ownership_controls("123456789012", &req, "owner-enforced")
        .unwrap();

    let mut req = make_request(Method::PUT, "/owner-enforced", &[("acl", "")], b"");
    req.headers.insert("x-amz-acl", "private".parse().unwrap());
    let err = match svc.put_bucket_acl("123456789012", &req, "owner-enforced") {
        Err(e) => e,
        Ok(_) => panic!("PutBucketAcl should be rejected under BucketOwnerEnforced"),
    };
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);

    // Seed an object first (PutObject without ACL header is fine).
    let req = make_request(Method::PUT, "/owner-enforced/k", &[], b"x");
    svc.put_object("123456789012", &req, "owner-enforced", "k")
        .await
        .unwrap();

    let mut req = make_request(Method::PUT, "/owner-enforced/k", &[("acl", "")], b"");
    req.headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    let err = match svc.put_object_acl("123456789012", &req, "owner-enforced", "k") {
        Err(e) => e,
        Ok(_) => panic!("PutObjectAcl should be rejected under BucketOwnerEnforced"),
    };
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn bucket_owner_enforced_rejects_put_object_with_acl_header() {
    let svc = make_service();
    seed_bucket(&svc, "noaclonput");

    let body = br#"<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>"#;
    let req = make_request(
        Method::PUT,
        "/noaclonput",
        &[("ownershipControls", "")],
        body,
    );
    svc.put_bucket_ownership_controls("123456789012", &req, "noaclonput")
        .unwrap();

    // Plain PutObject still works.
    let req = make_request(Method::PUT, "/noaclonput/plain", &[], b"x");
    svc.put_object("123456789012", &req, "noaclonput", "plain")
        .await
        .unwrap();

    // PutObject with x-amz-acl rejects.
    let mut req = make_request(Method::PUT, "/noaclonput/withacl", &[], b"x");
    req.headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    let err = match svc
        .put_object("123456789012", &req, "noaclonput", "withacl")
        .await
    {
        Err(e) => e,
        Ok(_) => panic!("PutObject with x-amz-acl should be rejected under BucketOwnerEnforced"),
    };
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
}

/// A default of aws:kms without a KMSMasterKeyID is accepted, as on AWS,
/// stored and returned without a key, and objects written under it are
/// encrypted under the account's AWS-managed `aws/s3` key.
#[tokio::test]
async fn put_bucket_encryption_aws_kms_without_a_key_uses_aws_s3() {
    let svc = kms_service();
    seed_bucket(&svc, "kmsenc");

    let body = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";
    let req = make_request(Method::PUT, "/kmsenc", &[("encryption", "")], body);
    svc.put_bucket_encryption("123456789012", &req, "kmsenc")
        .expect("aws:kms with no KMSMasterKeyID is valid");
    let got = svc.get_bucket_encryption("123456789012", "kmsenc").unwrap();
    let xml = std::str::from_utf8(got.body.expect_bytes()).unwrap();
    assert!(
        xml.contains("<SSEAlgorithm>aws:kms</SSEAlgorithm>"),
        "{xml}"
    );
    assert!(!xml.contains("KMSMasterKeyID"), "{xml}");

    let req = make_request(Method::PUT, "/kmsenc/k", &[], b"payload");
    let resp = svc
        .put_object("123456789012", &req, "kmsenc", "k")
        .await
        .unwrap();
    assert_eq!(resp.headers["x-amz-server-side-encryption"], "aws:kms");
    let obj = bucket_of(&svc, "kmsenc").objects["k"].clone();
    let stored = svc.state.read().default_ref().read_body(&obj.body).unwrap();
    assert_eq!(&stored[..], b"enc:aws/s3:payload");
    let req = make_request(Method::GET, "/kmsenc/k", &[], b"");
    let got = svc.get_object("123456789012", &req, "kmsenc", "k").unwrap();
    assert_eq!(got.body.expect_bytes(), b"payload");
}

#[test]
fn put_bucket_encryption_rejects_control_char_in_kms_key_id() {
    // Regression (bug-audit 2026-06-13 2.1): a control byte (newline) in the
    // bucket-default KMSMasterKeyID used to be persisted verbatim, applied as
    // an object default on PutObject, then panic the GET/HEAD response path
    // when round-tripped into x-amz-server-side-encryption-aws-kms-key-id.
    // It must now be rejected up front with 400, not a panic.
    let svc = make_service();
    seed_bucket(&svc, "kmsfuzz");

    let body = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>bad\nkey</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";
    let req = make_request(Method::PUT, "/kmsfuzz", &[("encryption", "")], body);
    let err = match svc.put_bucket_encryption("123456789012", &req, "kmsfuzz") {
        Err(e) => e,
        Ok(_) => panic!("control char in KMSMasterKeyID should be rejected"),
    };
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);

    // A NUL byte is likewise rejected.
    let body = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>k\x00id</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";
    let req = make_request(Method::PUT, "/kmsfuzz", &[("encryption", "")], body);
    assert!(
        svc.put_bucket_encryption("123456789012", &req, "kmsfuzz")
            .is_err(),
        "NUL byte in KMSMasterKeyID should be rejected"
    );

    // A legitimate KMS key ARN still round-trips and persists.
    let arn = "arn:aws:kms:us-east-1:123456789012:key/abcd-1234";
    let body = format!(
        "<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>{arn}</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"
    );
    let req = make_request(
        Method::PUT,
        "/kmsfuzz",
        &[("encryption", "")],
        body.as_bytes(),
    );
    svc.put_bucket_encryption("123456789012", &req, "kmsfuzz")
        .expect("a valid KMS key ARN must be accepted");
}

#[test]
fn put_bucket_encryption_rejects_invalid_sse_algorithm() {
    // Regression (bug-audit 2026-06-13 2.2): SSEAlgorithm is a closed enum.
    // A garbage value (including one carrying a control char) used to be
    // stored verbatim and later panic the GET response header. AWS returns
    // MalformedXML for an unknown algorithm.
    let svc = make_service();
    seed_bucket(&svc, "algofuzz");

    let body = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>x\ny</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";
    let req = make_request(Method::PUT, "/algofuzz", &[("encryption", "")], body);
    let err = match svc.put_bucket_encryption("123456789012", &req, "algofuzz") {
        Err(e) => e,
        Ok(_) => panic!("invalid SSEAlgorithm should be rejected"),
    };
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);

    // AES256 still accepted.
    let body = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";
    let req = make_request(Method::PUT, "/algofuzz", &[("encryption", "")], body);
    svc.put_bucket_encryption("123456789012", &req, "algofuzz")
        .expect("AES256 is a valid SSEAlgorithm");
}

#[tokio::test]
async fn bucket_default_kms_key_round_trips_on_get_without_panic() {
    // End-to-end of the fixed path: a valid bucket-default KMS config is
    // applied to an object PUT with no SSE headers and surfaces cleanly on
    // GET — no dropped connection, and the key id is returned verbatim.
    let svc = make_service();
    seed_bucket(&svc, "kmsdef");

    let arn = "arn:aws:kms:us-east-1:123456789012:key/abcd-1234";
    let enc = format!(
        "<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>{arn}</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"
    );
    let enc_req = make_request(
        Method::PUT,
        "/kmsdef",
        &[("encryption", "")],
        enc.as_bytes(),
    );
    svc.put_bucket_encryption("123456789012", &enc_req, "kmsdef")
        .unwrap();

    let put_req = make_request(Method::PUT, "/kmsdef/obj", &[], b"data");
    svc.put_object("123456789012", &put_req, "kmsdef", "obj")
        .await
        .unwrap();

    let get_req = make_request(Method::GET, "/kmsdef/obj", &[], b"");
    let resp = svc
        .get_object("123456789012", &get_req, "kmsdef", "obj")
        .unwrap();
    assert_eq!(
        resp.headers
            .get("x-amz-server-side-encryption-aws-kms-key-id")
            .and_then(|v| v.to_str().ok()),
        Some(arn),
        "valid bucket-default KMS key id must round-trip on GET"
    );
    assert_eq!(
        resp.headers
            .get("x-amz-server-side-encryption")
            .and_then(|v| v.to_str().ok()),
        Some("aws:kms")
    );
}

#[test]
fn get_bucket_policy_status_uses_real_json_parse() {
    // Substring scan would falsely flag a Description string as
    // public. Real JSON parse only flags actual Principal=*.
    let svc = make_service();
    seed_bucket(&svc, "polstat");

    let policy = br#"{"Version":"2012-10-17","Id":"some \"Principal\":\"*\" string","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:role/r"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::polstat/*"}]}"#;
    let req = make_request(Method::PUT, "/polstat", &[("policy", "")], policy);
    svc.put_bucket_policy("123456789012", &req, "polstat")
        .unwrap();
    let resp = svc
        .get_bucket_policy_status("123456789012", "polstat")
        .unwrap();
    assert!(std::str::from_utf8(resp.body.expect_bytes())
        .unwrap()
        .contains("<IsPublic>false</IsPublic>"));

    let policy = br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::polstat/*"}]}"#;
    let req = make_request(Method::PUT, "/polstat", &[("policy", "")], policy);
    svc.put_bucket_policy("123456789012", &req, "polstat")
        .unwrap();
    let resp = svc
        .get_bucket_policy_status("123456789012", "polstat")
        .unwrap();
    assert!(std::str::from_utf8(resp.body.expect_bytes())
        .unwrap()
        .contains("<IsPublic>true</IsPublic>"));
}

#[test]
fn crc32c_and_crc64nvme_hello_world_reference_values() {
    use crate::checksum::{checksum, ChecksumAlgorithm};
    // Reference CRC32C("hello world") = 0xC99465AA, big-endian -> base64.
    let expected = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        0xC99465AAu32.to_be_bytes(),
    );
    assert_eq!(
        checksum(ChecksumAlgorithm::Crc32c, b"hello world"),
        expected
    );
    // 8-byte digest, base64 = 12 chars
    assert_eq!(
        checksum(ChecksumAlgorithm::Crc64Nvme, b"hello world").len(),
        12
    );
}

#[tokio::test]
async fn access_point_control_plane_crud() {
    let state: SharedS3State = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("000000000000", "us-east-1", ""),
    ));
    let delivery = Arc::new(fakecloud_core::delivery::DeliveryBus::new());
    let service = crate::S3Service::new(state, delivery);

    // Pre-create bucket.
    let req = AwsRequest {
        service: "s3".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        request_id: "req-1".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec!["ap-bucket".to_string()],
        raw_path: "/ap-bucket".to_string(),
        raw_query: String::new(),
        method: http::Method::PUT,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    service.handle(req).await.unwrap();

    // Create access point.
    let req = AwsRequest {
        service: "s3".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        request_id: "req-2".to_string(),
        headers: {
            let mut h = http::HeaderMap::new();
            h.insert("host", "000000000000.s3-control.us-east-1.localhost.localstack.cloud:4566".parse().unwrap());
            h
        },
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from_static(b"<CreateAccessPointConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>ap-bucket</Bucket></CreateAccessPointConfiguration>"),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec!["v20180820".to_string(), "accesspoint".to_string(), "my-ap".to_string()],
        raw_path: "/v20180820/accesspoint/my-ap".to_string(),
        raw_query: String::new(),
        method: http::Method::PUT,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let resp = service.handle(req).await.unwrap();
    assert_eq!(resp.status, http::StatusCode::OK);
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("AccessPointArn"));
    assert!(body.contains("my-ap-000000000000"));

    // Get access point.
    let req = AwsRequest {
        service: "s3".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        request_id: "req-3".to_string(),
        headers: {
            let mut h = http::HeaderMap::new();
            h.insert(
                "host",
                "000000000000.s3-control.us-east-1.localhost.localstack.cloud:4566"
                    .parse()
                    .unwrap(),
            );
            h
        },
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![
            "v20180820".to_string(),
            "accesspoint".to_string(),
            "my-ap".to_string(),
        ],
        raw_path: "/v20180820/accesspoint/my-ap".to_string(),
        raw_query: String::new(),
        method: http::Method::GET,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let resp = service.handle(req).await.unwrap();
    assert_eq!(resp.status, http::StatusCode::OK);
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("my-ap"));
    assert!(body.contains("ap-bucket"));

    // List access points.
    let req = AwsRequest {
        service: "s3".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        request_id: "req-4".to_string(),
        headers: {
            let mut h = http::HeaderMap::new();
            h.insert(
                "host",
                "000000000000.s3-control.us-east-1.localhost.localstack.cloud:4566"
                    .parse()
                    .unwrap(),
            );
            h
        },
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec!["v20180820".to_string(), "accesspoint".to_string()],
        raw_path: "/v20180820/accesspoint".to_string(),
        raw_query: String::new(),
        method: http::Method::GET,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let resp = service.handle(req).await.unwrap();
    assert_eq!(resp.status, http::StatusCode::OK);
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("my-ap"));

    // Delete access point.
    let req = AwsRequest {
        service: "s3".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        request_id: "req-5".to_string(),
        headers: {
            let mut h = http::HeaderMap::new();
            h.insert(
                "host",
                "000000000000.s3-control.us-east-1.localhost.localstack.cloud:4566"
                    .parse()
                    .unwrap(),
            );
            h
        },
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![
            "v20180820".to_string(),
            "accesspoint".to_string(),
            "my-ap".to_string(),
        ],
        raw_path: "/v20180820/accesspoint/my-ap".to_string(),
        raw_query: String::new(),
        method: http::Method::DELETE,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let resp = service.handle(req).await.unwrap();
    assert_eq!(resp.status, http::StatusCode::NO_CONTENT);
}

#[test]
fn access_point_data_plane_routes_to_bucket() {
    let state: SharedS3State = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("000000000000", "us-east-1", ""),
    ));
    let delivery = Arc::new(fakecloud_core::delivery::DeliveryBus::new());
    let service = crate::S3Service::new(state.clone(), delivery);

    // Create bucket and access point directly in state.
    {
        let mut st = state.write();
        let s3_st = st.get_or_create("000000000000");
        s3_st.buckets.insert(
            "ap-data-bucket".to_string(),
            crate::state::S3Bucket::new("ap-data-bucket", "us-east-1", "000000000000"),
        );
        s3_st.access_points.insert(
            "data-ap".to_string(),
            crate::state::S3AccessPoint {
                name: "data-ap".to_string(),
                bucket: "ap-data-bucket".to_string(),
                account_id: "000000000000".to_string(),
                network_origin: "Internet".to_string(),
                vpc_configuration: None,
                creation_date: chrono::Utc::now(),
                public_access_block: None,
                bucket_account_id: Some("000000000000".to_string()),
                tags: Default::default(),
            },
        );
    }

    let mut req = AwsRequest {
        service: "s3".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        request_id: "req-1".to_string(),
        headers: {
            let mut h = http::HeaderMap::new();
            h.insert(
                "host",
                "data-ap-000000000000.s3-accesspoint.us-east-1.localhost.localstack.cloud:4566"
                    .parse()
                    .unwrap(),
            );
            h
        },
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec!["data-ap-000000000000".to_string(), "key.txt".to_string()],
        raw_path: "/data-ap-000000000000/key.txt".to_string(),
        raw_query: String::new(),
        method: http::Method::GET,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };

    crate::service::access_points::resolve_access_point(&service, &mut req).unwrap();

    assert_eq!(req.path_segments[0], "ap-data-bucket");
    assert_eq!(req.raw_path, "/ap-data-bucket/key.txt");
}

#[test]
fn iam_action_for_maps_access_point_control_ops() {
    let state: SharedS3State = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("000000000000", "us-east-1", ""),
    ));
    let delivery = Arc::new(fakecloud_core::delivery::DeliveryBus::new());
    let service = crate::S3Service::new(state, delivery);

    let s3_control_req = |method: http::Method, segs: &[&str]| {
        let mut h = http::HeaderMap::new();
        h.insert(
            "host",
            "000000000000.s3-control.us-east-1.localhost.localstack.cloud:4566"
                .parse()
                .unwrap(),
        );
        AwsRequest {
            service: "s3".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: "000000000000".to_string(),
            request_id: "req".to_string(),
            headers: h,
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: segs.iter().map(|s| s.to_string()).collect(),
            raw_path: format!("/{}", segs.join("/")),
            raw_query: String::new(),
            method,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    };

    // Create: PUT /v20180820/accesspoint/my-ap -> s3:CreateAccessPoint, not PutObject.
    let a = service
        .iam_action_for(&s3_control_req(
            http::Method::PUT,
            &["v20180820", "accesspoint", "my-ap"],
        ))
        .expect("access-point PUT must map to an IAM action");
    assert_eq!(a.action, "CreateAccessPoint");
    assert_eq!(
        a.resource,
        "arn:aws:s3:us-east-1:000000000000:accesspoint/my-ap"
    );

    let g = service
        .iam_action_for(&s3_control_req(
            http::Method::GET,
            &["v20180820", "accesspoint", "my-ap"],
        ))
        .unwrap();
    assert_eq!(g.action, "GetAccessPoint");

    let d = service
        .iam_action_for(&s3_control_req(
            http::Method::DELETE,
            &["v20180820", "accesspoint", "my-ap"],
        ))
        .unwrap();
    assert_eq!(d.action, "DeleteAccessPoint");

    let l = service
        .iam_action_for(&s3_control_req(
            http::Method::GET,
            &["v20180820", "accesspoint"],
        ))
        .unwrap();
    assert_eq!(l.action, "ListAccessPoints");
    assert_eq!(l.resource, "*");

    // Sanity: a normal object GET still maps to GetObject, unaffected.
    let mut obj = s3_control_req(http::Method::GET, &["my-bucket", "key.txt"]);
    obj.headers = http::HeaderMap::new(); // plain S3 host, not s3-control
    let o = service.iam_action_for(&obj).unwrap();
    assert_eq!(o.action, "GetObject");
}

#[tokio::test]
async fn mpu_complete_on_versioned_bucket_pushes_new_version() {
    // On a versioned bucket, CompleteMultipartUpload must add a NEW version
    // (not mutate/corrupt the prior version's body in place) -- bug-audit
    // 2026-06-20, 4.1.
    let svc = make_service();
    seed_bucket(&svc, "ver");
    {
        let mut mas = svc.state.write();
        let b = mas.default_mut().buckets.get_mut("ver").unwrap();
        b.versioning = Some("Enabled".to_string());
    }

    // v1 via a normal PutObject.
    let put_req = make_request(Method::PUT, "/ver/k", &[], b"original-v1");
    svc.put_object("123456789012", &put_req, "ver", "k")
        .await
        .unwrap();

    // v2 via a single-part multipart upload.
    let init_req = make_request(Method::POST, "/ver/k", &[("uploads", "")], b"");
    let resp = svc
        .create_multipart_upload("123456789012", &init_req, "ver", "k")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let start = body.find("<UploadId>").unwrap() + "<UploadId>".len();
    let end = body.find("</UploadId>").unwrap();
    let upload_id = body[start..end].to_string();

    let part = b"multipart-v2-body".to_vec();
    let part_req = make_request(Method::PUT, "/ver/k", &[("partNumber", "1")], &part);
    let r = svc
        .upload_part("123456789012", &part_req, "ver", "k", &upload_id, 1)
        .await
        .unwrap();
    let etag = r.headers.get("etag").unwrap().to_str().unwrap().to_string();

    let complete_xml = format!(
        r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"#,
    );
    let complete_req = make_request(
        Method::POST,
        "/ver/k",
        &[("uploadId", &upload_id)],
        complete_xml.as_bytes(),
    );
    svc.complete_multipart_upload("123456789012", &complete_req, "ver", "k", &upload_id)
        .unwrap();

    let mas = svc.state.read();
    let b = mas.default_ref().buckets.get("ver").unwrap();
    let versions = b
        .object_versions
        .get("k")
        .expect("k must be tracked in object_versions");
    assert_eq!(
        versions.len(),
        2,
        "PutObject + completed MPU must yield 2 versions, got {}",
        versions.len()
    );
}

// ────────────────────────────────────────────────────────────────
// Persistence regression tests (bug-hunt 2026-07-13).
//
// RenameObject, UpdateObjectEncryption, and the metadata
// inventory/journal-table updates used to mutate RAM only and never
// call the store, so they silently reverted on restart in disk mode.
// A recording store proves the handler now drives the store write
// path (the harness can't restart the server in-sandbox).
// ────────────────────────────────────────────────────────────────

struct RecordingStore {
    inner: fakecloud_persistence::MemoryS3Store,
    calls: parking_lot::Mutex<Vec<String>>,
    /// `bucket/key` whose object writes fail, standing in for a disk error.
    fail_put: parking_lot::Mutex<Option<String>>,
}

impl RecordingStore {
    fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            inner: fakecloud_persistence::MemoryS3Store::new(),
            calls: parking_lot::Mutex::new(Vec::new()),
            fail_put: parking_lot::Mutex::new(None),
        })
    }
    fn record(&self, call: String) {
        self.calls.lock().push(call);
    }
    fn calls(&self) -> Vec<String> {
        self.calls.lock().clone()
    }
}

impl fakecloud_persistence::S3Store for RecordingStore {
    fn load(&self) -> fakecloud_persistence::StoreResult<fakecloud_persistence::S3StateSnapshot> {
        self.inner.load()
    }
    fn put_bucket_meta(
        &self,
        bucket: &str,
        meta: &fakecloud_persistence::BucketMeta,
    ) -> fakecloud_persistence::StoreResult<()> {
        self.record(format!("put_bucket_meta:{bucket}"));
        self.inner.put_bucket_meta(bucket, meta)
    }
    fn put_bucket_subresource(
        &self,
        bucket: &str,
        kind: fakecloud_persistence::BucketSubresource,
        payload: &str,
    ) -> fakecloud_persistence::StoreResult<()> {
        self.record(format!("put_bucket_subresource:{bucket}:{kind:?}"));
        self.inner.put_bucket_subresource(bucket, kind, payload)
    }
    fn delete_bucket_subresource(
        &self,
        bucket: &str,
        kind: fakecloud_persistence::BucketSubresource,
    ) -> fakecloud_persistence::StoreResult<()> {
        self.record(format!("delete_bucket_subresource:{bucket}:{kind:?}"));
        self.inner.delete_bucket_subresource(bucket, kind)
    }
    fn delete_bucket(&self, bucket: &str) -> fakecloud_persistence::StoreResult<()> {
        self.record(format!("delete_bucket:{bucket}"));
        self.inner.delete_bucket(bucket)
    }
    fn put_object(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        body: fakecloud_persistence::BodySource,
        meta: &fakecloud_persistence::ObjectMeta,
    ) -> fakecloud_persistence::StoreResult<fakecloud_persistence::BodyRef> {
        self.record(format!("put_object:{bucket}/{key}"));
        if self.fail_put.lock().as_deref() == Some(format!("{bucket}/{key}").as_str()) {
            return Err(fakecloud_persistence::StoreError::Other(
                "disk full".to_string(),
            ));
        }
        self.inner.put_object(bucket, key, version, body, meta)
    }
    fn put_object_meta(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        meta: &fakecloud_persistence::ObjectMeta,
    ) -> fakecloud_persistence::StoreResult<()> {
        self.record(format!("put_object_meta:{bucket}/{key}"));
        self.inner.put_object_meta(bucket, key, version, meta)
    }
    fn put_annotation(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        record: &fakecloud_persistence::AnnotationRecord,
        payload: &[u8],
    ) -> fakecloud_persistence::StoreResult<()> {
        self.record(format!("put_annotation:{bucket}/{key}/{}", record.name));
        self.inner
            .put_annotation(bucket, key, version, record, payload)
    }
    fn delete_annotation(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        name: &str,
    ) -> fakecloud_persistence::StoreResult<()> {
        self.record(format!("delete_annotation:{bucket}/{key}/{name}"));
        self.inner.delete_annotation(bucket, key, version, name)
    }
    fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
    ) -> fakecloud_persistence::StoreResult<()> {
        self.record(format!("delete_object:{bucket}/{key}"));
        self.inner.delete_object(bucket, key, version)
    }
    fn open_object_body(
        &self,
        body: &fakecloud_persistence::BodyRef,
    ) -> fakecloud_persistence::StoreResult<Bytes> {
        self.inner.open_object_body(body)
    }
    fn mpu_create(
        &self,
        bucket: &str,
        upload_id: &str,
        init: &fakecloud_persistence::MpuInit,
    ) -> fakecloud_persistence::StoreResult<()> {
        self.inner.mpu_create(bucket, upload_id, init)
    }
    fn mpu_put_part(
        &self,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
        body: fakecloud_persistence::BodySource,
        etag: &str,
        checksum: Option<fakecloud_persistence::PartChecksum<'_>>,
    ) -> fakecloud_persistence::StoreResult<fakecloud_persistence::BodyRef> {
        self.inner
            .mpu_put_part(bucket, upload_id, part_number, body, etag, checksum)
    }
    fn mpu_abort(&self, bucket: &str, upload_id: &str) -> fakecloud_persistence::StoreResult<()> {
        self.inner.mpu_abort(bucket, upload_id)
    }
    fn mpu_complete(
        &self,
        bucket: &str,
        upload_id: &str,
        final_key: &str,
        version: Option<&str>,
        meta: &fakecloud_persistence::ObjectMeta,
    ) -> fakecloud_persistence::StoreResult<fakecloud_persistence::BodyRef> {
        self.inner
            .mpu_complete(bucket, upload_id, final_key, version, meta)
    }
}

fn make_recording_service() -> (S3Service, std::sync::Arc<RecordingStore>) {
    let state: SharedS3State = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let store = RecordingStore::new();
    let svc = S3Service::with_store(state, Arc::new(DeliveryBus::new()), store.clone());
    (svc, store)
}

#[test]
fn annotation_writes_persist_only_that_annotation() {
    let (svc, store) = make_recording_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "k", b"payload");
    for name in ["one", "two"] {
        let req = make_request(
            Method::PUT,
            "/b/k",
            &[("annotation", ""), ("annotationName", name)],
            b"note",
        );
        svc.put_object_annotation("123456789012", &req, "b", "k")
            .unwrap();
    }
    let req = make_request(
        Method::DELETE,
        "/b/k",
        &[("annotation", ""), ("annotationName", "one")],
        b"",
    );
    svc.delete_object_annotation("123456789012", &req, "b", "k")
        .unwrap();
    let calls: Vec<String> = store
        .calls()
        .into_iter()
        .filter(|c| !c.starts_with("put_bucket_meta"))
        .collect();
    // One record per write, never the object's metadata (which would carry
    // every other annotation's payload with it).
    assert_eq!(
        calls,
        vec![
            "put_annotation:b/k/one".to_string(),
            "put_annotation:b/k/two".to_string(),
            "delete_annotation:b/k/one".to_string(),
        ]
    );
}

#[test]
fn rename_object_carries_annotations_to_the_new_key() {
    let (svc, store) = make_recording_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "old", b"payload");
    let req = make_request(
        Method::PUT,
        "/b/old",
        &[("annotation", ""), ("annotationName", "note")],
        b"x",
    );
    svc.put_object_annotation("123456789012", &req, "b", "old")
        .unwrap();
    let mut rename = make_request(Method::PUT, "/b/new", &[("renameObject", "")], b"");
    rename
        .headers
        .insert("x-amz-rename-source", "/old".parse().unwrap());
    svc.rename_object("123456789012", &rename, "b", "new")
        .unwrap();
    assert!(
        store
            .calls()
            .iter()
            .any(|c| c == "put_annotation:b/new/note"),
        "{:?}",
        store.calls()
    );
}

#[test]
fn rename_object_persists_move_through_store() {
    let (svc, store) = make_recording_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "old", b"payload");

    let mut req = make_request(Method::PUT, "/b/new", &[("renameObject", "")], b"");
    req.headers
        .insert("x-amz-rename-source", "/old".parse().unwrap());
    svc.rename_object("123456789012", &req, "b", "new").unwrap();

    // In-memory move happened.
    {
        let mas = svc.state.read();
        let b = mas.default_ref().buckets.get("b").unwrap();
        assert!(b.objects.contains_key("new"));
        assert!(!b.objects.contains_key("old"));
    }
    // …and it was persisted: the new key written, the old key deleted.
    let calls = store.calls();
    assert!(
        calls.iter().any(|c| c == "put_object:b/new"),
        "expected put_object for the new key, got {calls:?}"
    );
    assert!(
        calls.iter().any(|c| c == "delete_object:b/old"),
        "expected delete_object for the old key, got {calls:?}"
    );
}

#[test]
fn update_object_encryption_persists_reencrypted_body() {
    let (svc, store) = make_recording_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "k", b"payload");

    let mut req = make_request(Method::PUT, "/b/k", &[("encryption", "")], b"");
    req.headers
        .insert("x-amz-server-side-encryption", "AES256".parse().unwrap());
    svc.update_object_encryption("123456789012", &req, "b", "k")
        .unwrap();

    // Algorithm changed -> the body path must be re-persisted, not RAM-only.
    let calls = store.calls();
    assert!(
        calls.iter().any(|c| c == "put_object:b/k"),
        "expected put_object to persist the re-encrypted body, got {calls:?}"
    );
    // Metadata reflects the new algorithm in memory too.
    let mas = svc.state.read();
    let obj = mas
        .default_ref()
        .buckets
        .get("b")
        .unwrap()
        .objects
        .get("k")
        .unwrap();
    assert_eq!(obj.sse_algorithm.as_deref(), Some("AES256"));
}

#[test]
fn inventory_optional_field_intelligent_tiering_reference_date_round_trips() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let body = br#"<InventoryConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Id>inv1</Id><IsEnabled>true</IsEnabled><Destination><S3BucketDestination><Bucket>arn:aws:s3:::dest</Bucket><Format>CSV</Format></S3BucketDestination></Destination><Schedule><Frequency>Daily</Frequency></Schedule><IncludedObjectVersions>Current</IncludedObjectVersions><OptionalFields><Field>IntelligentTieringAccessTier</Field><Field>IntelligentTieringReferenceDate</Field></OptionalFields></InventoryConfiguration>"#;
    let req = make_request(
        Method::PUT,
        "/b",
        &[("inventory", ""), ("id", "inv1")],
        body,
    );
    svc.put_bucket_inventory("123456789012", &req, "b").unwrap();

    let req = make_request(Method::GET, "/b", &[("inventory", ""), ("id", "inv1")], b"");
    let resp = svc.get_bucket_inventory("123456789012", &req, "b").unwrap();
    let xml = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(
        xml.contains("<Field>IntelligentTieringReferenceDate</Field>"),
        "{xml}"
    );
}

#[test]
fn update_metadata_inventory_table_persists_config() {
    let (svc, store) = make_recording_service();
    seed_bucket(&svc, "b");

    let body = b"<InventoryTableConfiguration><ConfigurationState>ENABLED</ConfigurationState></InventoryTableConfiguration>";
    let req = make_request(Method::PUT, "/b", &[("metadataInventoryTable", "")], body);
    svc.update_bucket_metadata_inventory_table("123456789012", &req, "b")
        .unwrap();

    let calls = store.calls();
    assert!(
        calls
            .iter()
            .any(|c| c == "put_bucket_subresource:b:MetadataConfiguration"),
        "inventory-table update must persist the composite metadata config, got {calls:?}"
    );
    // Journal-table sibling persists the same way.
    let req2 = make_request(Method::PUT, "/b", &[("metadataJournalTable", "")], body);
    svc.update_bucket_metadata_journal_table("123456789012", &req2, "b")
        .unwrap();
    let calls = store.calls();
    assert!(
        calls
            .iter()
            .filter(|c| *c == "put_bucket_subresource:b:MetadataConfiguration")
            .count()
            >= 2,
        "journal-table update must persist too, got {calls:?}"
    );
}

#[test]
fn put_object_retention_mode_without_date_is_malformed_xml() {
    // A retention carrying a Mode but no valid RetainUntilDate is rejected by
    // AWS with 400 MalformedXML, not silently stored (bug-hunt 2026-07-13).
    let svc = make_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "k", b"payload");

    let body = b"<Retention><Mode>GOVERNANCE</Mode></Retention>";
    let req = make_request(Method::PUT, "/b/k", &[("retention", "")], body);
    let err = match svc.put_object_retention("123456789012", &req, "b", "k") {
        Ok(_) => panic!("mode without retain-until must be rejected"),
        Err(e) => e,
    };
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    assert_eq!(err.code(), "MalformedXML");

    // A well-formed retention (mode + date) still succeeds.
    let ok_body =
        b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2099-01-01T00:00:00Z</RetainUntilDate></Retention>";
    let ok_req = make_request(Method::PUT, "/b/k", &[("retention", "")], ok_body);
    svc.put_object_retention("123456789012", &ok_req, "b", "k")
        .expect("valid retention must succeed");
}

fn retention_body(mode: &str, until: &str) -> Vec<u8> {
    format!("<Retention><Mode>{mode}</Mode><RetainUntilDate>{until}</RetainUntilDate></Retention>")
        .into_bytes()
}

fn put_retention(
    svc: &S3Service,
    bucket: &str,
    key: &str,
    body: &[u8],
    bypass: bool,
) -> Result<AwsResponse, AwsServiceError> {
    let mut req = make_request(
        Method::PUT,
        &format!("/{bucket}/{key}"),
        &[("retention", "")],
        body,
    );
    if bypass {
        req.headers
            .insert("x-amz-bypass-governance-retention", "true".parse().unwrap());
    }
    svc.put_object_retention("123456789012", &req, bucket, key)
}

fn stored_retention(svc: &S3Service, bucket: &str, key: &str) -> (Option<String>, Option<String>) {
    let accts = svc.state.read();
    let obj = &accts.default_ref().buckets[bucket].objects[key];
    (
        obj.lock_mode.clone(),
        obj.lock_retain_until.map(|u| u.to_rfc3339()),
    )
}

#[test]
fn put_object_retention_governance_cannot_be_weakened_without_bypass() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "k", b"payload");
    put_retention(
        &svc,
        "b",
        "k",
        &retention_body("GOVERNANCE", "2099-01-01T00:00:00Z"),
        false,
    )
    .unwrap();

    // Shortening, switching mode, and clearing are all refused while the
    // retention is active, and leave it untouched.
    for body in [
        retention_body("GOVERNANCE", "2098-01-01T00:00:00Z"),
        retention_body("COMPLIANCE", "2099-06-01T00:00:00Z"),
        b"<Retention/>".to_vec(),
    ] {
        let err = assert_aws_err(put_retention(&svc, "b", "k", &body, false), "AccessDenied");
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
    }
    assert_eq!(
        stored_retention(&svc, "b", "k"),
        (
            Some("GOVERNANCE".to_string()),
            Some("2099-01-01T00:00:00+00:00".to_string())
        )
    );

    // Extending needs no bypass.
    put_retention(
        &svc,
        "b",
        "k",
        &retention_body("GOVERNANCE", "2100-01-01T00:00:00Z"),
        false,
    )
    .expect("extending a GOVERNANCE retention must succeed");

    // With the bypass header the retention can be shortened.
    put_retention(
        &svc,
        "b",
        "k",
        &retention_body("GOVERNANCE", "2098-01-01T00:00:00Z"),
        true,
    )
    .expect("bypass must allow shortening a GOVERNANCE retention");
    assert_eq!(
        stored_retention(&svc, "b", "k").1.as_deref(),
        Some("2098-01-01T00:00:00+00:00")
    );
}

#[test]
fn put_object_retention_compliance_cannot_be_weakened_even_with_bypass() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "k", b"payload");
    put_retention(
        &svc,
        "b",
        "k",
        &retention_body("COMPLIANCE", "2099-01-01T00:00:00Z"),
        false,
    )
    .unwrap();

    for body in [
        retention_body("COMPLIANCE", "2098-01-01T00:00:00Z"),
        retention_body("GOVERNANCE", "2099-06-01T00:00:00Z"),
        b"<Retention/>".to_vec(),
    ] {
        for bypass in [false, true] {
            let err = assert_aws_err(put_retention(&svc, "b", "k", &body, bypass), "AccessDenied");
            assert_eq!(err.status(), StatusCode::FORBIDDEN);
        }
    }
    assert_eq!(
        stored_retention(&svc, "b", "k"),
        (
            Some("COMPLIANCE".to_string()),
            Some("2099-01-01T00:00:00+00:00".to_string())
        )
    );

    put_retention(
        &svc,
        "b",
        "k",
        &retention_body("COMPLIANCE", "2100-01-01T00:00:00Z"),
        false,
    )
    .expect("extending a COMPLIANCE retention must succeed");
}

#[test]
fn put_object_retention_expired_retention_can_be_replaced() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "k", b"payload");
    {
        let mut accts = svc.state.write();
        let obj = accts
            .default_mut()
            .buckets
            .get_mut("b")
            .unwrap()
            .objects
            .get_mut("k")
            .unwrap();
        obj.lock_mode = Some("COMPLIANCE".to_string());
        obj.lock_retain_until = Some(chrono::Utc::now() - chrono::Duration::days(1));
    }
    put_retention(
        &svc,
        "b",
        "k",
        &retention_body("GOVERNANCE", "2099-01-01T00:00:00Z"),
        false,
    )
    .expect("an expired retention no longer protects the object");
}

#[tokio::test]
async fn put_object_retention_by_version_id_respects_the_lock() {
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/vlock", &[], b"");
    req.headers
        .insert("x-amz-bucket-object-lock-enabled", "true".parse().unwrap());
    svc.create_bucket("123456789012", &req, "vlock").unwrap();

    let mut put = make_request(Method::PUT, "/vlock/k", &[], b"hello");
    put.headers
        .insert("content-md5", content_md5(b"hello").parse().unwrap());
    put.headers
        .insert("x-amz-object-lock-mode", "GOVERNANCE".parse().unwrap());
    put.headers.insert(
        "x-amz-object-lock-retain-until-date",
        "2099-01-01T00:00:00Z".parse().unwrap(),
    );
    let resp = svc
        .put_object("123456789012", &put, "vlock", "k")
        .await
        .unwrap();
    let vid = resp.headers["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_string();

    let body = retention_body("GOVERNANCE", "2098-01-01T00:00:00Z");
    let req = make_request(
        Method::PUT,
        "/vlock/k",
        &[("retention", ""), ("versionId", &vid)],
        &body,
    );
    assert_aws_err(
        svc.put_object_retention("123456789012", &req, "vlock", "k"),
        "AccessDenied",
    );
}

#[test]
fn object_lock_headers_require_retention_and_legal_hold_permissions() {
    use fakecloud_core::service::AwsService as _;

    let svc = make_service();
    let actions = |req: &AwsRequest| -> Vec<(&'static str, String)> {
        svc.iam_actions_for(req)
            .into_iter()
            .map(|a| (a.action, a.resource))
            .collect()
    };
    let with = |mut req: AwsRequest, headers: &[(&'static str, &str)]| {
        for (name, value) in headers {
            req.headers.insert(*name, value.parse().unwrap());
        }
        req
    };
    let retention = [
        ("x-amz-object-lock-mode", "GOVERNANCE"),
        (
            "x-amz-object-lock-retain-until-date",
            "2099-01-01T00:00:00Z",
        ),
    ];
    let legal_hold = [("x-amz-object-lock-legal-hold", "ON")];
    let all = [retention[0], retention[1], legal_hold[0]];
    let obj = |key: &str| format!("arn:aws:s3:::b/{key}");

    // PutObject.
    let put = || make_request(Method::PUT, "/b/k", &[], b"x");
    assert_eq!(actions(&put()), vec![("PutObject", obj("k"))]);
    assert_eq!(
        actions(&with(put(), &retention)),
        vec![("PutObject", obj("k")), ("PutObjectRetention", obj("k"))]
    );
    // Either retention header alone asks for the permission.
    assert_eq!(
        actions(&with(put(), &retention[..1])),
        vec![("PutObject", obj("k")), ("PutObjectRetention", obj("k"))]
    );
    assert_eq!(
        actions(&with(put(), &retention[1..])),
        vec![("PutObject", obj("k")), ("PutObjectRetention", obj("k"))]
    );
    assert_eq!(
        actions(&with(put(), &legal_hold)),
        vec![("PutObject", obj("k")), ("PutObjectLegalHold", obj("k"))]
    );

    // CopyObject: evaluated on the destination object; the source read is
    // authorized separately.
    let copy = with(
        make_request(Method::PUT, "/b/dst", &[], b""),
        &[("x-amz-copy-source", "src/k")],
    );
    let base = actions(&copy);
    let copy_locked = actions(&with(copy, &all));
    assert_eq!(copy_locked[..base.len()], base[..]);
    assert_eq!(
        copy_locked[base.len()..],
        [
            ("PutObjectRetention", obj("dst")),
            ("PutObjectLegalHold", obj("dst"))
        ]
    );

    // CreateMultipartUpload.
    let create = || make_request(Method::POST, "/b/mpu", &[("uploads", "")], b"");
    let base = actions(&create());
    assert_eq!(base.len(), 1);
    assert_eq!(
        actions(&with(create(), &all))[1..],
        [
            ("PutObjectRetention", obj("mpu")),
            ("PutObjectLegalHold", obj("mpu"))
        ]
    );

    // Requests that do not create an object version ignore the headers.
    let get = with(make_request(Method::GET, "/b/k", &[], b""), &all);
    assert_eq!(actions(&get), vec![("GetObject", obj("k"))]);
}

#[test]
fn governance_bypass_requires_bypass_governance_retention_permission() {
    use fakecloud_core::service::AwsService as _;

    let svc = make_service();
    let actions = |req: &AwsRequest| -> Vec<(&'static str, String)> {
        svc.iam_actions_for(req)
            .into_iter()
            .map(|a| (a.action, a.resource))
            .collect()
    };
    let with_bypass = |mut req: AwsRequest, value: &str| {
        req.headers
            .insert("x-amz-bypass-governance-retention", value.parse().unwrap());
        req
    };

    let body = retention_body("GOVERNANCE", "2098-01-01T00:00:00Z");
    let retention = make_request(Method::PUT, "/b/k", &[("retention", "")], &body);
    assert_eq!(
        actions(&retention),
        vec![("PutObjectRetention", "arn:aws:s3:::b/k".to_string())]
    );
    assert_eq!(
        actions(&with_bypass(retention, "true")),
        vec![
            ("PutObjectRetention", "arn:aws:s3:::b/k".to_string()),
            ("BypassGovernanceRetention", "arn:aws:s3:::b/k".to_string()),
        ]
    );

    let delete = make_request(Method::DELETE, "/b/k", &[("versionId", "v1")], b"");
    assert_eq!(actions(&delete).len(), 1);
    // Only an explicit `true` asks for the bypass.
    assert_eq!(
        actions(&with_bypass(
            make_request(Method::DELETE, "/b/k", &[], b""),
            "false"
        ))
        .len(),
        1
    );
    assert_eq!(
        actions(&with_bypass(delete, "TRUE")),
        vec![
            ("DeleteObject", "arn:aws:s3:::b/k".to_string()),
            ("BypassGovernanceRetention", "arn:aws:s3:::b/k".to_string()),
        ]
    );

    // Other operations ignore the header. DeleteObjects is authorized per key
    // by AWS, so a request-level check would wrongly fail the whole batch.
    let put = with_bypass(make_request(Method::PUT, "/b/k", &[], b"x"), "true");
    assert_eq!(
        actions(&put),
        vec![("PutObject", "arn:aws:s3:::b/k".to_string())]
    );
    let batch = make_request(
        Method::POST,
        "/b",
        &[("delete", "")],
        b"<Delete><Object><Key>a/1</Key></Object></Delete>",
    );
    assert_eq!(
        actions(&with_bypass(batch, "true")),
        vec![("DeleteObjects", "arn:aws:s3:::b".to_string())]
    );
}

#[test]
fn list_objects_v2_rejects_non_integer_max_keys() {
    // A non-integer / out-of-range max-keys is a 400 InvalidArgument, not a
    // silent coercion to 1000 (bug-hunt 2026-07-13).
    let svc = make_service();
    seed_bucket(&svc, "b");

    for bad in ["abc", "-1", "99999999999999999999"] {
        let req = make_request(
            Method::GET,
            "/b",
            &[("list-type", "2"), ("max-keys", bad)],
            b"",
        );
        let err = match svc.list_objects_v2("123456789012", &req, "b") {
            Ok(_) => panic!("max-keys={bad} must be rejected"),
            Err(e) => e,
        };
        assert_eq!(err.status(), StatusCode::BAD_REQUEST, "max-keys={bad}");
        assert_eq!(err.code(), "InvalidArgument", "max-keys={bad}");
    }

    // A valid value still works and caps at 1000.
    let ok = make_request(
        Method::GET,
        "/b",
        &[("list-type", "2"), ("max-keys", "5000")],
        b"",
    );
    let resp = svc.list_objects_v2("123456789012", &ok, "b").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<MaxKeys>1000</MaxKeys>"), "body: {body}");
}

#[test]
fn list_object_versions_keeps_low_sorting_common_prefix_on_first_page() {
    // When plain keys fill max-keys before a lower-sorting delimiter-rolled
    // prefix, the prefix must still appear on the page it belongs to instead
    // of being dropped as the marker advances past it (bug-hunt 2026-07-13).
    let svc = make_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "a/x", b"1");
    seed_object(&svc, "b", "bb", b"2");
    seed_object(&svc, "b", "cc", b"3");

    let req = make_request(
        Method::GET,
        "/b",
        &[("versions", ""), ("delimiter", "/"), ("max-keys", "2")],
        b"",
    );
    let resp = svc.list_object_versions("123456789012", &req, "b").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();

    // "a/" sorts before "bb"/"cc" so it must be emitted on page 1, not lost.
    assert!(
        body.contains("<CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes>"),
        "page 1 must keep the a/ CommonPrefix, body: {body}"
    );
    assert!(
        body.contains("<IsTruncated>true</IsTruncated>"),
        "body: {body}"
    );
    assert!(body.contains("<Key>bb</Key>"), "body: {body}");
    // The page is capped at max-keys: "cc" spills to the next page.
    assert!(!body.contains("<Key>cc</Key>"), "body: {body}");
}

#[tokio::test]
async fn put_object_accepts_every_modeled_storage_class() {
    // The `StorageClass` allowlist drifted behind `aws-models/s3.json`: the
    // FSx and AWS Backup classes are modeled values AWS accepts, and a stale
    // list turned them into `InvalidStorageClass` on a valid request.
    let svc = make_service();
    seed_bucket(&svc, "classes");
    for class in [
        "STANDARD",
        "REDUCED_REDUNDANCY",
        "STANDARD_IA",
        "ONEZONE_IA",
        "INTELLIGENT_TIERING",
        "GLACIER",
        "DEEP_ARCHIVE",
        "GLACIER_IR",
        "OUTPOSTS",
        "SNOW",
        "EXPRESS_ONEZONE",
        "FSX_OPENZFS",
        "FSX_ONTAP",
        "AWS_BACKUP_WARM",
        "AWS_BACKUP_LOW_COST_WARM",
    ] {
        let key = format!("k-{class}");
        let path = format!("/classes/{key}");
        let mut req = make_request(Method::PUT, &path, &[], b"x");
        req.headers
            .insert("x-amz-storage-class", class.parse().unwrap());
        svc.put_object("123456789012", &req, "classes", &key)
            .await
            .unwrap_or_else(|e| panic!("storage class {class} rejected: {e:?}"));

        let req = make_request(Method::HEAD, &path, &[], b"");
        let resp = svc
            .head_object("123456789012", &req, "classes", &key)
            .unwrap();
        // STANDARD is the default and AWS omits the header for it.
        if class != "STANDARD" {
            assert_eq!(resp.headers.get("x-amz-storage-class").unwrap(), class);
        }
    }

    // A value outside the enum is still rejected.
    let mut req = make_request(Method::PUT, "/classes/bad", &[], b"x");
    req.headers
        .insert("x-amz-storage-class", "NOT_A_CLASS".parse().unwrap());
    assert!(svc
        .put_object("123456789012", &req, "classes", "bad")
        .await
        .is_err());
}

#[test]
fn put_bucket_encryption_accepts_every_modeled_sse_algorithm() {
    // `ServerSideEncryption` models five values; the validator only knew the
    // three customer-facing ones, so `aws:fsx` / `aws:backup` came back as
    // MalformedXML on a request AWS accepts.
    let svc = make_service();
    seed_bucket(&svc, "sse");
    for algo in ["AES256", "aws:fsx", "aws:backup"] {
        let body = format!(
            "<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>{algo}</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"
        );
        let req = make_request(Method::PUT, "/sse", &[("encryption", "")], body.as_bytes());
        svc.put_bucket_encryption("123456789012", &req, "sse")
            .unwrap_or_else(|e| panic!("SSEAlgorithm {algo} rejected: {e:?}"));
    }

    // Anything outside the enum is still MalformedXML.
    let body = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:nope</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";
    let req = make_request(Method::PUT, "/sse", &[("encryption", "")], body);
    assert_aws_err(
        svc.put_bucket_encryption("123456789012", &req, "sse"),
        "MalformedXML",
    );
}

// ---------------------------------------------------------------------
// Object annotations
// ---------------------------------------------------------------------

fn put_annotation(
    svc: &S3Service,
    bucket: &str,
    key: &str,
    name: &str,
    payload: &[u8],
) -> Result<AwsResponse, AwsServiceError> {
    let req = make_request(
        Method::PUT,
        &format!("/{bucket}/{key}"),
        &[
            ("annotation", ""),
            (super::annotations::ANNOTATION_NAME_QUERY, name),
        ],
        payload,
    );
    svc.put_object_annotation("123456789012", &req, bucket, key)
}

#[test]
fn object_annotations_round_trip_independently_of_the_object() {
    let svc = make_service();
    seed_bucket(&svc, "annot");
    seed_object(&svc, "annot", "doc.txt", b"the object body");

    put_annotation(&svc, "annot", "doc.txt", "review", b"looks good").unwrap();
    put_annotation(&svc, "annot", "doc.txt", "summary", b"a summary").unwrap();

    // Get returns the payload verbatim, with its own ETag and length.
    let req = make_request(
        Method::GET,
        "/annot/doc.txt",
        &[
            ("annotation", ""),
            (super::annotations::ANNOTATION_NAME_QUERY, "review"),
        ],
        b"",
    );
    let resp = svc
        .get_object_annotation("123456789012", &req, "annot", "doc.txt")
        .unwrap();
    assert_eq!(resp.body.expect_bytes(), b"looks good");
    assert_eq!(resp.headers.get("Content-Length").unwrap(), "10");
    assert!(resp.headers.get("ETag").is_some());

    // The object itself is untouched.
    let req = make_request(Method::GET, "/annot/doc.txt", &[], b"");
    let resp = svc
        .get_object("123456789012", &req, "annot", "doc.txt")
        .unwrap();
    assert_eq!(resp.body.expect_bytes(), b"the object body");

    // Overwriting replaces the payload rather than adding a second entry.
    put_annotation(&svc, "annot", "doc.txt", "review", b"revised").unwrap();
    let req = make_request(
        Method::GET,
        "/annot/doc.txt",
        &[
            ("annotation", ""),
            (super::annotations::ANNOTATION_NAME_QUERY, "review"),
        ],
        b"",
    );
    let resp = svc
        .get_object_annotation("123456789012", &req, "annot", "doc.txt")
        .unwrap();
    assert_eq!(resp.body.expect_bytes(), b"revised");

    // Delete is idempotent, and a deleted annotation then 404s.
    for _ in 0..2 {
        let req = make_request(
            Method::DELETE,
            "/annot/doc.txt",
            &[
                ("annotation", ""),
                (super::annotations::ANNOTATION_NAME_QUERY, "review"),
            ],
            b"",
        );
        let resp = svc
            .delete_object_annotation("123456789012", &req, "annot", "doc.txt")
            .unwrap();
        assert_eq!(resp.status, StatusCode::NO_CONTENT);
    }
    let req = make_request(
        Method::GET,
        "/annot/doc.txt",
        &[
            ("annotation", ""),
            (super::annotations::ANNOTATION_NAME_QUERY, "review"),
        ],
        b"",
    );
    assert_aws_err(
        svc.get_object_annotation("123456789012", &req, "annot", "doc.txt"),
        "NoSuchAnnotation",
    );
}

#[test]
fn list_object_annotations_filters_by_prefix_and_pages() {
    let svc = make_service();
    seed_bucket(&svc, "annot");
    seed_object(&svc, "annot", "k", b"body");
    for name in ["a-one", "a-two", "a-three", "b-one"] {
        put_annotation(&svc, "annot", "k", name, name.as_bytes()).unwrap();
    }

    let list = |query: &[(&str, &str)]| -> String {
        let mut q = vec![("annotation", "")];
        q.extend_from_slice(query);
        let req = make_request(Method::GET, "/annot/k", &q, b"");
        let resp = svc
            .list_object_annotations("123456789012", &req, "annot", "k")
            .unwrap();
        String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap()
    };

    let all = list(&[]);
    assert!(
        all.contains("<AnnotationCount>4</AnnotationCount>"),
        "{all}"
    );
    assert!(all.contains("<AnnotationName>b-one</AnnotationName>"));
    // Size is the payload length, not the object's.
    assert!(all.contains("<Size>5</Size>"), "{all}");

    let prefixed = list(&[(super::annotations::ANNOTATION_PREFIX_QUERY, "a-")]);
    assert!(prefixed.contains("<AnnotationCount>3</AnnotationCount>"));
    assert!(!prefixed.contains("b-one"));

    // A short page reports a continuation token; following it returns the rest
    // without repeating an entry.
    let page1 = list(&[(super::annotations::MAX_ANNOTATION_RESULTS_QUERY, "2")]);
    assert!(
        page1.contains("<AnnotationCount>2</AnnotationCount>"),
        "{page1}"
    );
    let token = page1
        .split("<NextContinuationToken>")
        .nth(1)
        .and_then(|s| s.split("</NextContinuationToken>").next())
        .expect("continuation token")
        .to_string();
    let page2 = list(&[
        (super::annotations::MAX_ANNOTATION_RESULTS_QUERY, "2"),
        (super::annotations::CONTINUATION_TOKEN_QUERY, &token),
    ]);
    assert!(
        page2.contains("<AnnotationCount>2</AnnotationCount>"),
        "{page2}"
    );
    // Names sort, so page 1 is a-one/a-three and page 2 is a-two/b-one.
    assert!(
        !page2.contains("<AnnotationName>a-one</AnnotationName>"),
        "{page2}"
    );
    assert!(
        page2.contains("<AnnotationName>b-one</AnnotationName>"),
        "{page2}"
    );
}

#[test]
fn object_annotations_validate_names_and_missing_resources() {
    let svc = make_service();
    seed_bucket(&svc, "annot");
    seed_object(&svc, "annot", "k", b"body");

    // AnnotationName is required, length-bounded, and control-char free.
    let req = make_request(Method::PUT, "/annot/k", &[("annotation", "")], b"x");
    assert_aws_err(
        svc.put_object_annotation("123456789012", &req, "annot", "k"),
        "InvalidAnnotationName",
    );
    // The cap is 512 bytes, not characters.
    put_annotation(&svc, "annot", "k", &"n".repeat(512), b"x").unwrap();
    assert_aws_err(
        put_annotation(&svc, "annot", "k", &"n".repeat(513), b"x"),
        "AnnotationNameTooLong",
    );
    assert_aws_err(
        put_annotation(&svc, "annot", "k", &"é".repeat(257), b"x"),
        "AnnotationNameTooLong",
    );
    assert_aws_err(
        put_annotation(&svc, "annot", "k", "bad\nname", b"x"),
        "InvalidAnnotationName",
    );
    assert_aws_err(
        put_annotation(&svc, "annot", "k", "", b"x"),
        "InvalidAnnotationName",
    );
    // Exactly the documented maximum is accepted.
    put_annotation(&svc, "annot", "k", &"n".repeat(255), b"x").unwrap();
    let at_limit = make_request(
        Method::DELETE,
        "/annot/k",
        &[
            ("annotation", ""),
            (super::annotations::ANNOTATION_NAME_QUERY, &"n".repeat(255)),
        ],
        b"",
    );
    svc.delete_object_annotation("123456789012", &at_limit, "annot", "k")
        .unwrap();

    // Missing bucket / key report the object-level codes.
    assert_aws_err(
        put_annotation(&svc, "ghost", "k", "n", b"x"),
        "NoSuchBucket",
    );
    assert_aws_err(
        put_annotation(&svc, "annot", "missing", "n", b"x"),
        "NoSuchKey",
    );

    // The 1,000-annotation ceiling is enforced, and overwriting stays allowed.
    {
        let mut mas = svc.state.write();
        let obj = mas
            .default_mut()
            .buckets
            .get_mut("annot")
            .unwrap()
            .objects
            .get_mut("k")
            .unwrap();
        obj.annotations.clear();
    }
    for i in 0..1000 {
        put_annotation(&svc, "annot", "k", &format!("a{i:04}"), b"x").unwrap();
    }
    assert_aws_err(
        put_annotation(&svc, "annot", "k", "one-too-many", b"x"),
        "AnnotationLimitExceeded",
    );
    put_annotation(&svc, "annot", "k", "a0000", b"replaced").unwrap();
}

#[test]
fn update_bucket_metadata_annotation_table_configuration_round_trips() {
    let svc = make_service();
    seed_bucket(&svc, "annot");
    let body = b"<AnnotationTableConfigurationUpdates><Annotations><Annotation>review</Annotation></Annotations></AnnotationTableConfigurationUpdates>";
    let req = make_request(
        Method::PUT,
        "/annot",
        &[("metadataAnnotationTable", "")],
        body,
    );
    let resp = svc
        .update_bucket_metadata_annotation_table_configuration("123456789012", &req, "annot")
        .unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    {
        let state = svc.state.read();
        let stored = state
            .get("123456789012")
            .unwrap()
            .buckets
            .get("annot")
            .unwrap()
            .annotation_table_config
            .as_deref()
            .expect("configuration stored");
        assert!(stored.contains("review"));
    }

    // An empty payload is MalformedXML, and an unknown bucket is NoSuchBucket.
    let req = make_request(
        Method::PUT,
        "/annot",
        &[("metadataAnnotationTable", "")],
        b"",
    );
    assert_aws_err(
        svc.update_bucket_metadata_annotation_table_configuration("123456789012", &req, "annot"),
        "MalformedXML",
    );
    let req = make_request(
        Method::PUT,
        "/ghost",
        &[("metadataAnnotationTable", "")],
        body,
    );
    assert_aws_err(
        svc.update_bucket_metadata_annotation_table_configuration("123456789012", &req, "ghost"),
        "NoSuchBucket",
    );
}

fn create_bucket_in(svc: &S3Service, bucket: &str, endpoint_region: &str, constraint: &str) {
    let body = format!(
        "<CreateBucketConfiguration><LocationConstraint>{constraint}</LocationConstraint></CreateBucketConfiguration>"
    );
    let mut req = make_request(Method::PUT, &format!("/{bucket}"), &[], body.as_bytes());
    req.region = endpoint_region.to_string();
    svc.create_bucket("123456789012", &req, bucket)
        .unwrap_or_else(|e| panic!("constraint {constraint} rejected: {e:?}"));
}

fn bucket_location(svc: &S3Service, bucket: &str) -> String {
    let resp = svc.get_bucket_location("123456789012", bucket).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes())
        .unwrap()
        .to_string();
    let start = body.find('>').unwrap();
    let rest = &body[start + 1..];
    let open = rest.find('>').unwrap();
    let close = rest.find("</LocationConstraint>").unwrap();
    rest[open + 1..close].to_string()
}

#[test]
fn create_bucket_accepts_recently_launched_regions() {
    // ap-southeast-5/6/7, ap-east-2 and mx-central-1 are modeled
    // `BucketLocationConstraint` values AWS accepts; a stale allowlist
    // answered InvalidLocationConstraint for them.
    let svc = make_service();
    for region in [
        "ap-southeast-5",
        "ap-southeast-6",
        "ap-southeast-7",
        "ap-east-2",
        "mx-central-1",
    ] {
        let bucket = format!("bk-{region}");
        create_bucket_in(&svc, &bucket, region, region);
        assert_eq!(bucket_location(&svc, &bucket), region);
    }
}

#[test]
fn create_bucket_legacy_eu_constraint_is_an_eu_west_1_bucket() {
    let svc = make_service();
    // Sent to the eu-west-1 endpoint and to the global (us-east-1) one.
    create_bucket_in(&svc, "eu-legacy-a", "eu-west-1", "EU");
    create_bucket_in(&svc, "eu-legacy-b", "us-east-1", "EU");
    for bucket in ["eu-legacy-a", "eu-legacy-b"] {
        // GetBucketLocation keeps reporting the legacy constraint ...
        assert_eq!(bucket_location(&svc, bucket), "EU");
        // ... while the bucket itself lives in eu-west-1.
        let resp = svc.head_bucket("123456789012", bucket).unwrap();
        assert_eq!(
            resp.headers.get("x-amz-bucket-region").unwrap(),
            "eu-west-1"
        );
    }
    // A regional endpoint other than eu-west-1 refuses it, like any mismatch.
    let mut req = make_request(
        Method::PUT,
        "/eu-legacy-c",
        &[],
        b"<CreateBucketConfiguration><LocationConstraint>EU</LocationConstraint></CreateBucketConfiguration>",
    );
    req.region = "eu-central-1".to_string();
    assert_aws_err(
        svc.create_bucket("123456789012", &req, "eu-legacy-c"),
        "IllegalLocationConstraintException",
    );
    // A plain eu-west-1 bucket still reports its region name.
    create_bucket_in(&svc, "eu-modern", "eu-west-1", "eu-west-1");
    assert_eq!(bucket_location(&svc, "eu-modern"), "eu-west-1");
}

#[test]
fn bucket_location_constraints_cover_the_model() {
    // Drift guard: every `BucketLocationConstraint` value in the vendored
    // Smithy model must be accepted by CreateBucket.
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../aws-models/s3.json");
    let model: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let members = model["shapes"]["com.amazonaws.s3#BucketLocationConstraint"]["members"]
        .as_object()
        .unwrap();
    assert!(!members.is_empty());
    for member in members.values() {
        let value = member["traits"]["smithy.api#enumValue"].as_str().unwrap();
        assert!(
            is_valid_region(value),
            "BucketLocationConstraint {value} is modeled but rejected"
        );
    }
}

fn lock_config(retention: &str) -> Vec<u8> {
    format!(
        "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
         <Rule><DefaultRetention><Mode>GOVERNANCE</Mode>{retention}</DefaultRetention></Rule>\
         </ObjectLockConfiguration>"
    )
    .into_bytes()
}

#[tokio::test]
async fn object_lock_default_retention_is_range_checked_and_never_overflows() {
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/olr", &[], b"");
    req.headers
        .insert("x-amz-bucket-object-lock-enabled", "true".parse().unwrap());
    svc.create_bucket("123456789012", &req, "olr").unwrap();

    for (retention, code) in [
        ("<Days>9223372036854775807</Days>", "InvalidArgument"),
        ("<Days>36501</Days>", "InvalidArgument"),
        ("<Days>0</Days>", "InvalidArgument"),
        ("<Years>101</Years>", "InvalidArgument"),
        ("<Years>-1</Years>", "InvalidArgument"),
        ("<Days>1</Days><Years>1</Years>", "MalformedXML"),
    ] {
        let put = make_request(
            Method::PUT,
            "/olr",
            &[("object-lock", "")],
            &lock_config(retention),
        );
        assert_aws_err(
            svc.put_object_lock_config("123456789012", &put, "olr"),
            code,
        );
    }
    let put = make_request(
        Method::PUT,
        "/olr",
        &[("object-lock", "")],
        &lock_config("<Years>100</Years>"),
    );
    svc.put_object_lock_config("123456789012", &put, "olr")
        .unwrap();

    // A configuration persisted before the range check existed must not
    // panic PutObject's retain-until arithmetic.
    {
        let mut accts = svc.state.write();
        let b = accts
            .get_mut("123456789012")
            .unwrap()
            .buckets
            .get_mut("olr")
            .unwrap();
        b.object_lock_config =
            Some(String::from_utf8(lock_config("<Years>9223372036854775807</Years>")).unwrap());
    }
    let put_req = make_request(Method::PUT, "/olr/k", &[], b"hello");
    svc.put_object("123456789012", &put_req, "olr", "k")
        .await
        .unwrap();
}

#[tokio::test]
async fn object_lock_default_retention_years_are_calendar_years() {
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/oly", &[], b"");
    req.headers
        .insert("x-amz-bucket-object-lock-enabled", "true".parse().unwrap());
    svc.create_bucket("123456789012", &req, "oly").unwrap();
    let put = make_request(
        Method::PUT,
        "/oly",
        &[("object-lock", "")],
        &lock_config("<Years>4</Years>"),
    );
    svc.put_object_lock_config("123456789012", &put, "oly")
        .unwrap();

    // Any four-year span contains a leap day, so 4 * 365 days falls a day
    // short of four calendar years.
    let four_years = chrono::Months::new(48);
    let before = chrono::Utc::now().checked_add_months(four_years).unwrap();
    let put_req = make_request(Method::PUT, "/oly/k", &[], b"hello");
    svc.put_object("123456789012", &put_req, "oly", "k")
        .await
        .unwrap();
    let after = chrono::Utc::now().checked_add_months(four_years).unwrap();

    let until = svc.state.read().default_ref().buckets["oly"].objects["k"]
        .lock_retain_until
        .unwrap();
    assert!(
        before <= until && until <= after,
        "retain-until {until} should be four calendar years out ({before}..={after})"
    );
}

#[tokio::test]
async fn object_lock_default_retention_tolerates_whitespace_around_period() {
    // The stored configuration is validated with surrounding whitespace
    // trimmed, so applying it must trim too or the default silently drops.
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/olw", &[], b"");
    req.headers
        .insert("x-amz-bucket-object-lock-enabled", "true".parse().unwrap());
    svc.create_bucket("123456789012", &req, "olw").unwrap();
    for (key, period) in [("d", "<Days> 3 </Days>"), ("y", "<Years> 1 </Years>")] {
        let put = make_request(
            Method::PUT,
            "/olw",
            &[("object-lock", "")],
            &lock_config(period),
        );
        svc.put_object_lock_config("123456789012", &put, "olw")
            .unwrap();
        let put_req = make_request(Method::PUT, &format!("/olw/{key}"), &[], b"hello");
        svc.put_object("123456789012", &put_req, "olw", key)
            .await
            .unwrap();
        let state = svc.state.read();
        let obj = &state.default_ref().buckets["olw"].objects[key];
        assert_eq!(obj.lock_mode.as_deref(), Some("GOVERNANCE"), "{period}");
        assert!(obj.lock_retain_until.is_some(), "{period}");
    }
}

/// A bucket created with Object Lock, optionally with a default retention.
fn seed_lock_bucket(svc: &S3Service, name: &str, default_retention: Option<&str>) {
    let mut req = make_request(Method::PUT, &format!("/{name}"), &[], b"");
    req.headers
        .insert("x-amz-bucket-object-lock-enabled", "true".parse().unwrap());
    svc.create_bucket("123456789012", &req, name).unwrap();
    if let Some(retention) = default_retention {
        let put = make_request(
            Method::PUT,
            &format!("/{name}"),
            &[("object-lock", "")],
            &lock_config(retention),
        );
        svc.put_object_lock_config("123456789012", &put, name)
            .unwrap();
    }
}

fn explicit_lock_headers(req: &mut AwsRequest, mode: &str, until: &str, legal_hold: &str) {
    req.headers
        .insert("x-amz-object-lock-mode", mode.parse().unwrap());
    req.headers.insert(
        "x-amz-object-lock-retain-until-date",
        until.parse().unwrap(),
    );
    req.headers
        .insert("x-amz-object-lock-legal-hold", legal_hold.parse().unwrap());
}

fn current_object(svc: &S3Service, bucket: &str, key: &str) -> S3Object {
    svc.state.read().default_ref().buckets[bucket].objects[key].clone()
}

fn copy_request(dest_bucket: &str, dest_key: &str, source: &str) -> AwsRequest {
    let mut req = make_request(Method::PUT, &format!("/{dest_bucket}/{dest_key}"), &[], b"");
    req.headers
        .insert("x-amz-copy-source", source.parse().unwrap());
    req
}

#[tokio::test]
async fn copy_object_applies_the_destination_default_retention() {
    let svc = make_service();
    seed_bucket(&svc, "plain-src");
    seed_object(&svc, "plain-src", "k", b"payload");
    seed_lock_bucket(&svc, "lock-default", Some("<Days>3</Days>"));

    let before = chrono::Utc::now() + chrono::Duration::days(3);
    svc.copy_object(
        "123456789012",
        &copy_request("lock-default", "copy", "plain-src/k"),
        "lock-default",
        "copy",
    )
    .unwrap();
    let after = chrono::Utc::now() + chrono::Duration::days(3);

    let obj = current_object(&svc, "lock-default", "copy");
    assert_eq!(obj.lock_mode.as_deref(), Some("GOVERNANCE"));
    let until = obj.lock_retain_until.expect("default retention applied");
    assert!(before <= until && until <= after, "{until} not ~3 days out");
    assert!(obj.lock_legal_hold.is_none());
}

#[tokio::test]
async fn copy_object_applies_explicit_lock_headers() {
    let svc = make_service();
    seed_bucket(&svc, "plain-src");
    seed_object(&svc, "plain-src", "k", b"payload");
    seed_lock_bucket(&svc, "lock-default", Some("<Days>3</Days>"));

    let mut req = copy_request("lock-default", "copy", "plain-src/k");
    explicit_lock_headers(&mut req, "COMPLIANCE", "2099-01-01T00:00:00Z", "ON");
    svc.copy_object("123456789012", &req, "lock-default", "copy")
        .unwrap();

    // The explicit headers win over the bucket default.
    let obj = current_object(&svc, "lock-default", "copy");
    assert_eq!(obj.lock_mode.as_deref(), Some("COMPLIANCE"));
    assert_eq!(
        obj.lock_retain_until,
        Some("2099-01-01T00:00:00Z".parse().unwrap())
    );
    assert_eq!(obj.lock_legal_hold.as_deref(), Some("ON"));
}

#[tokio::test]
async fn copy_object_does_not_inherit_the_source_lock() {
    let svc = make_service();
    seed_lock_bucket(&svc, "lock-src", None);
    let mut put = make_request(Method::PUT, "/lock-src/k", &[], b"payload");
    put.headers
        .insert("content-md5", content_md5(b"payload").parse().unwrap());
    explicit_lock_headers(&mut put, "COMPLIANCE", "2099-01-01T00:00:00Z", "ON");
    svc.put_object("123456789012", &put, "lock-src", "k")
        .await
        .unwrap();
    assert_eq!(
        current_object(&svc, "lock-src", "k").lock_mode.as_deref(),
        Some("COMPLIANCE")
    );

    // Into a lock bucket with no default retention, and into a plain bucket:
    // neither copy carries the source's retention or legal hold.
    seed_lock_bucket(&svc, "lock-nodefault", None);
    seed_bucket(&svc, "plain-dst");
    for dest in ["lock-nodefault", "plain-dst"] {
        svc.copy_object(
            "123456789012",
            &copy_request(dest, "copy", "lock-src/k"),
            dest,
            "copy",
        )
        .unwrap();
        let obj = current_object(&svc, dest, "copy");
        assert!(obj.lock_mode.is_none(), "{dest}");
        assert!(obj.lock_retain_until.is_none(), "{dest}");
        assert!(obj.lock_legal_hold.is_none(), "{dest}");
    }
}

#[tokio::test]
async fn object_lock_headers_on_a_bucket_without_object_lock_are_refused_like_put_object() {
    let svc = make_service();
    seed_bucket(&svc, "plain");
    seed_object(&svc, "plain", "src", b"payload");

    let mut put = make_request(Method::PUT, "/plain/k", &[], b"payload");
    explicit_lock_headers(&mut put, "GOVERNANCE", "2099-01-01T00:00:00Z", "OFF");
    let put_err = match svc.put_object("123456789012", &put, "plain", "k").await {
        Err(e) => e,
        Ok(_) => panic!("PutObject accepted lock headers on a plain bucket"),
    };

    let mut copy = copy_request("plain", "copy", "plain/src");
    explicit_lock_headers(&mut copy, "GOVERNANCE", "2099-01-01T00:00:00Z", "OFF");
    let copy_err = assert_aws_err(
        svc.copy_object("123456789012", &copy, "plain", "copy"),
        "InvalidRequest",
    );

    let mut create = make_request(Method::POST, "/plain/mpu", &[("uploads", "")], b"");
    explicit_lock_headers(&mut create, "GOVERNANCE", "2099-01-01T00:00:00Z", "OFF");
    let create_err = assert_aws_err(
        svc.create_multipart_upload("123456789012", &create, "plain", "mpu"),
        "InvalidRequest",
    );

    let wire = |e: &AwsServiceError| match e {
        AwsServiceError::AwsError {
            status,
            code,
            message,
            ..
        } => (*status, code.clone(), message.clone()),
        other => panic!("expected AwsError, got {other:?}"),
    };
    assert_eq!(
        wire(&put_err),
        (
            StatusCode::BAD_REQUEST,
            "InvalidRequest".to_string(),
            "Bucket is missing ObjectLockConfiguration".to_string()
        )
    );
    assert_eq!(wire(&copy_err), wire(&put_err));
    assert_eq!(wire(&create_err), wire(&put_err));
    // Nothing was written by the refused requests.
    let state = svc.state.read();
    let b = &state.default_ref().buckets["plain"];
    assert!(!b.objects.contains_key("copy"));
    assert!(b.multipart_uploads.is_empty());
}

#[tokio::test]
async fn object_lock_headers_are_validated_on_every_creating_operation() {
    let svc = make_service();
    seed_lock_bucket(&svc, "lockv", None);
    seed_object(&svc, "lockv", "src", b"payload");
    let past = (chrono::Utc::now() - chrono::Duration::minutes(5))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();
    let future = "2099-01-01T00:00:00Z";
    let both =
        "x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied";
    let cases: Vec<(Vec<(&str, &str)>, &str)> = vec![
        (vec![("x-amz-object-lock-mode", "GOVERNANCE")], both),
        (vec![("x-amz-object-lock-retain-until-date", future)], both),
        (
            vec![
                ("x-amz-object-lock-mode", "GOVERNANCE"),
                ("x-amz-object-lock-retain-until-date", "tomorrow"),
            ],
            "The retain until date must be provided in ISO 8601 format",
        ),
        (
            vec![
                ("x-amz-object-lock-mode", "governance"),
                ("x-amz-object-lock-retain-until-date", future),
            ],
            "Unknown wormMode directive.",
        ),
        (
            vec![
                ("x-amz-object-lock-mode", "COMPLIANCE"),
                ("x-amz-object-lock-retain-until-date", &past),
            ],
            "The retain until date must be in the future!",
        ),
        (
            vec![("x-amz-object-lock-legal-hold", "YES")],
            "Legal Hold must be either of 'ON' or 'OFF'",
        ),
    ];
    let check = |result: Result<AwsResponse, AwsServiceError>, message: &str, op: &str| {
        match assert_aws_err(result, "InvalidArgument") {
            AwsServiceError::AwsError {
                status, message: m, ..
            } => {
                assert_eq!(status, StatusCode::BAD_REQUEST, "{op}");
                assert_eq!(m, message, "{op}");
            }
            other => panic!("{other:?}"),
        }
    };
    for (headers, message) in &cases {
        let apply = |mut req: AwsRequest| {
            for (name, value) in headers {
                req.headers.insert(*name, value.parse().unwrap());
            }
            req
        };
        let mut put = apply(make_request(Method::PUT, "/lockv/k", &[], b"payload"));
        put.headers
            .insert("content-md5", content_md5(b"payload").parse().unwrap());
        check(
            svc.put_object("123456789012", &put, "lockv", "k").await,
            message,
            "PutObject",
        );
        let copy = apply(copy_request("lockv", "copy", "lockv/src"));
        check(
            svc.copy_object("123456789012", &copy, "lockv", "copy"),
            message,
            "CopyObject",
        );
        let create = apply(make_request(
            Method::POST,
            "/lockv/mpu",
            &[("uploads", "")],
            b"",
        ));
        check(
            svc.create_multipart_upload("123456789012", &create, "lockv", "mpu"),
            message,
            "CreateMultipartUpload",
        );
    }
    let state = svc.state.read();
    let b = &state.default_ref().buckets["lockv"];
    assert!(!b.objects.contains_key("k") && !b.objects.contains_key("copy"));
    assert!(b.multipart_uploads.is_empty());
}

#[tokio::test]
async fn put_object_with_object_lock_requires_a_payload_integrity_header() {
    let svc = make_service();
    seed_lock_bucket(&svc, "lockmd5", None);
    let locked_put = |key: &str| {
        let mut req = make_request(Method::PUT, &format!("/lockmd5/{key}"), &[], b"payload");
        req.headers
            .insert("x-amz-object-lock-legal-hold", "ON".parse().unwrap());
        req
    };

    let err = assert_aws_err(
        svc.put_object("123456789012", &locked_put("none"), "lockmd5", "none")
            .await,
        "InvalidRequest",
    );
    match err {
        AwsServiceError::AwsError {
            status, message, ..
        } => {
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(
                message,
                "Content-MD5 OR x-amz-checksum- HTTP header is required for Put Object \
                 requests with Object Lock parameters"
            );
        }
        other => panic!("{other:?}"),
    }

    let crc32 = crate::checksum::checksum(crate::checksum::ChecksumAlgorithm::Crc32, b"payload");
    let integrity: [(&str, &str, &str); 4] = [
        ("md5", "content-md5", &content_md5(b"payload")),
        ("crc", "x-amz-checksum-crc32", &crc32),
        ("trailer", "x-amz-trailer", "x-amz-checksum-crc32"),
        ("sdk", "x-amz-sdk-checksum-algorithm", "CRC32"),
    ];
    for (key, name, value) in integrity {
        let mut req = if key == "trailer" {
            // A trailing checksum arrives after an aws-chunked body.
            let framed = format!("7\r\npayload\r\n0\r\nx-amz-checksum-crc32:{crc32}\r\n\r\n");
            let mut req = make_request(
                Method::PUT,
                &format!("/lockmd5/{key}"),
                &[],
                framed.as_bytes(),
            );
            req.headers
                .insert("x-amz-object-lock-legal-hold", "ON".parse().unwrap());
            req.headers
                .insert("content-encoding", "aws-chunked".parse().unwrap());
            req
        } else {
            locked_put(key)
        };
        req.headers.insert(name, value.parse().unwrap());
        svc.put_object("123456789012", &req, "lockmd5", key)
            .await
            .unwrap_or_else(|e| panic!("{name}: {e:?}"));
        assert_eq!(
            current_object(&svc, "lockmd5", key)
                .lock_legal_hold
                .as_deref(),
            Some("ON"),
            "{name}"
        );
    }

    // A PutObject without lock headers needs no integrity header; neither do
    // CopyObject and CreateMultipartUpload with them.
    svc.put_object(
        "123456789012",
        &make_request(Method::PUT, "/lockmd5/plain", &[], b"payload"),
        "lockmd5",
        "plain",
    )
    .await
    .unwrap();
    let mut copy = copy_request("lockmd5", "copy", "lockmd5/plain");
    copy.headers
        .insert("x-amz-object-lock-legal-hold", "ON".parse().unwrap());
    svc.copy_object("123456789012", &copy, "lockmd5", "copy")
        .unwrap();
    let mut create = make_request(Method::POST, "/lockmd5/mpu", &[("uploads", "")], b"");
    create
        .headers
        .insert("x-amz-object-lock-legal-hold", "ON".parse().unwrap());
    svc.create_multipart_upload("123456789012", &create, "lockmd5", "mpu")
        .unwrap();
}

#[tokio::test]
async fn copy_object_reads_the_destination_default_retention_when_it_copies() {
    // The default is read from the destination bucket when the copy commits:
    // with no default configured, a copy into the bucket stays unlocked.
    let svc = make_service();
    seed_bucket(&svc, "plain-src");
    seed_object(&svc, "plain-src", "k", b"payload");
    seed_lock_bucket(&svc, "lock-later", None);
    svc.copy_object(
        "123456789012",
        &copy_request("lock-later", "before", "plain-src/k"),
        "lock-later",
        "before",
    )
    .unwrap();
    assert!(current_object(&svc, "lock-later", "before")
        .lock_mode
        .is_none());
    let put = make_request(
        Method::PUT,
        "/lock-later",
        &[("object-lock", "")],
        &lock_config("<Years>1</Years>"),
    );
    svc.put_object_lock_config("123456789012", &put, "lock-later")
        .unwrap();
    let before = chrono::Utc::now()
        .checked_add_months(chrono::Months::new(12))
        .unwrap();
    svc.copy_object(
        "123456789012",
        &copy_request("lock-later", "after", "plain-src/k"),
        "lock-later",
        "after",
    )
    .unwrap();
    let after = chrono::Utc::now()
        .checked_add_months(chrono::Months::new(12))
        .unwrap();
    let until = current_object(&svc, "lock-later", "after")
        .lock_retain_until
        .unwrap();
    assert!(before <= until && until <= after);
}

/// Create (with `lock` headers applied), upload one part and complete a
/// multipart upload of `bucket/key`.
async fn multipart_upload_with(
    svc: &S3Service,
    bucket: &str,
    key: &str,
    lock: Option<(&str, &str, &str)>,
) {
    let mut create = make_request(
        Method::POST,
        &format!("/{bucket}/{key}"),
        &[("uploads", "")],
        b"",
    );
    if let Some((mode, until, legal_hold)) = lock {
        explicit_lock_headers(&mut create, mode, until, legal_hold);
    }
    let resp = svc
        .create_multipart_upload("123456789012", &create, bucket, key)
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let start = body.find("<UploadId>").unwrap() + "<UploadId>".len();
    let end = body.find("</UploadId>").unwrap();
    let upload_id = body[start..end].to_string();

    let part = make_request(
        Method::PUT,
        &format!("/{bucket}/{key}"),
        &[("partNumber", "1")],
        b"hello",
    );
    let resp = svc
        .upload_part("123456789012", &part, bucket, key, &upload_id, 1)
        .await
        .unwrap();
    let etag = resp.headers["etag"].to_str().unwrap().to_string();
    let complete_xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let complete = make_request(
        Method::POST,
        &format!("/{bucket}/{key}"),
        &[("uploadId", &upload_id)],
        complete_xml.as_bytes(),
    );
    svc.complete_multipart_upload("123456789012", &complete, bucket, key, &upload_id)
        .unwrap();
}

#[tokio::test]
async fn multipart_upload_applies_explicit_lock_headers() {
    let svc = make_service();
    seed_lock_bucket(&svc, "lock-default", Some("<Days>3</Days>"));
    multipart_upload_with(
        &svc,
        "lock-default",
        "k",
        Some(("COMPLIANCE", "2099-01-01T00:00:00Z", "ON")),
    )
    .await;

    let obj = current_object(&svc, "lock-default", "k");
    assert_eq!(obj.lock_mode.as_deref(), Some("COMPLIANCE"));
    assert_eq!(
        obj.lock_retain_until,
        Some("2099-01-01T00:00:00Z".parse().unwrap())
    );
    assert_eq!(obj.lock_legal_hold.as_deref(), Some("ON"));
    // The version history carries the same lock as the current object.
    let state = svc.state.read();
    let versions = &state.default_ref().buckets["lock-default"].object_versions["k"];
    assert_eq!(
        versions.last().unwrap().lock_mode.as_deref(),
        Some("COMPLIANCE")
    );
}

#[tokio::test]
async fn multipart_upload_applies_the_bucket_default_retention_at_completion() {
    let svc = make_service();
    seed_lock_bucket(&svc, "lock-default", Some("<Days>3</Days>"));
    let before = chrono::Utc::now() + chrono::Duration::days(3);
    multipart_upload_with(&svc, "lock-default", "k", None).await;
    let after = chrono::Utc::now() + chrono::Duration::days(3);

    let obj = current_object(&svc, "lock-default", "k");
    assert_eq!(obj.lock_mode.as_deref(), Some("GOVERNANCE"));
    let until = obj.lock_retain_until.expect("default retention applied");
    assert!(before <= until && until <= after, "{until} not ~3 days out");
    assert!(obj.lock_legal_hold.is_none());

    // A lock bucket with no default retention leaves the object unlocked.
    seed_lock_bucket(&svc, "lock-nodefault", None);
    multipart_upload_with(&svc, "lock-nodefault", "k", None).await;
    let obj = current_object(&svc, "lock-nodefault", "k");
    assert!(obj.lock_mode.is_none() && obj.lock_retain_until.is_none());
}

// ── SelectObjectContent compression ──────────────────────────────

fn select_request_xml(compression: &str) -> String {
    format!(
        "<SelectObjectContentRequest><Expression>SELECT * FROM S3Object</Expression>\
         <ExpressionType>SQL</ExpressionType><InputSerialization>\
         <CompressionType>{compression}</CompressionType><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV>\
         </InputSerialization><OutputSerialization><CSV/></OutputSerialization>\
         </SelectObjectContentRequest>"
    )
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

#[test]
fn select_object_content_decompresses_gzip_and_bzip2_input() {
    use std::io::Write;
    let svc = make_service();
    seed_bucket(&svc, "b");
    let csv = b"id,name\n1,alice\n2,bob\n";
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(csv).unwrap();
    seed_object(&svc, "b", "data.csv.gz", &gz.finish().unwrap());
    let mut bz = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::default());
    bz.write_all(csv).unwrap();
    seed_object(&svc, "b", "data.csv.bz2", &bz.finish().unwrap());

    for (key, codec) in [("data.csv.gz", "GZIP"), ("data.csv.bz2", "BZIP2")] {
        let xml = select_request_xml(codec);
        let req = make_request(Method::POST, &format!("/b/{key}"), &[], xml.as_bytes());
        let resp = svc
            .select_object_content("123456789012", &req, "b", key)
            .unwrap();
        let body = resp.body.expect_bytes();
        assert!(contains(body, b"1,alice"), "{codec}: decoded rows missing");
        assert!(contains(body, b"2,bob"), "{codec}: decoded rows missing");
    }
}

#[test]
fn select_object_content_rejects_unsupported_or_mismatched_compression() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "plain.csv", b"id\n1\n");
    let xml = select_request_xml("LZO");
    let req = make_request(Method::POST, "/b/plain.csv", &[], xml.as_bytes());
    assert_aws_err(
        svc.select_object_content("123456789012", &req, "b", "plain.csv"),
        "InvalidCompressionFormat",
    );
    // Declaring GZIP on an uncompressed object fails instead of returning
    // garbage rows.
    let xml = select_request_xml("GZIP");
    let req = make_request(Method::POST, "/b/plain.csv", &[], xml.as_bytes());
    assert_aws_err(
        svc.select_object_content("123456789012", &req, "b", "plain.csv"),
        "InvalidCompressionFormat",
    );
}

// ── CopyObject / UploadPartCopy through a disk store ────────────

fn make_disk_service(dir: &std::path::Path) -> S3Service {
    let state: SharedS3State = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let store = Arc::new(fakecloud_persistence::s3::DiskS3Store::new(
        dir.to_path_buf(),
        Arc::new(fakecloud_persistence::cache::BodyCache::new(0)),
    ));
    S3Service::with_store(state, Arc::new(DeliveryBus::new()), store)
}

fn disk_body_of(svc: &S3Service, bucket: &str, key: &str) -> Vec<u8> {
    let mas = svc.state.read();
    let obj = &mas.default_ref().buckets[bucket].objects[key];
    match &obj.body {
        fakecloud_persistence::BodyRef::Disk { path, .. } => std::fs::read(path).unwrap(),
        other => panic!("expected a disk-backed body, got {other:?}"),
    }
}

fn spool_is_empty(dir: &std::path::Path) -> bool {
    match std::fs::read_dir(dir.join(".spool")) {
        Ok(entries) => entries.count() == 0,
        Err(_) => true,
    }
}

#[tokio::test]
async fn copy_object_writes_destination_through_disk_store() {
    let dir = tempfile::tempdir().unwrap();
    let svc = make_disk_service(dir.path());
    seed_bucket(&svc, "src");
    seed_bucket(&svc, "dst");
    let req = make_request(Method::PUT, "/src/a", &[], b"copy me please");
    svc.put_object("123456789012", &req, "src", "a")
        .await
        .unwrap();

    let mut req = make_request(Method::PUT, "/dst/b", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "src/a".parse().unwrap());
    svc.copy_object("123456789012", &req, "dst", "b").unwrap();

    assert_eq!(disk_body_of(&svc, "dst", "b"), b"copy me please");
    assert!(spool_is_empty(dir.path()), "staged copy body left behind");

    // A copy into a missing bucket fails without leaving a staged file.
    let mut req = make_request(Method::PUT, "/nope/b", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "src/a".parse().unwrap());
    assert_aws_err(
        svc.copy_object("123456789012", &req, "nope", "b"),
        "NoSuchBucket",
    );
    assert!(spool_is_empty(dir.path()));
}

#[tokio::test]
async fn upload_part_copy_reads_only_the_range_and_persists_the_part() {
    let dir = tempfile::tempdir().unwrap();
    let svc = make_disk_service(dir.path());
    seed_bucket(&svc, "b");
    let req = make_request(Method::PUT, "/b/src", &[], b"0123456789");
    svc.put_object("123456789012", &req, "b", "src")
        .await
        .unwrap();

    let req = make_request(Method::POST, "/b/dst", &[("uploads", "")], b"");
    let resp = svc
        .create_multipart_upload("123456789012", &req, "b", "dst")
        .unwrap();
    let xml = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    let upload_id = xml
        .split("<UploadId>")
        .nth(1)
        .and_then(|s| s.split("</UploadId>").next())
        .unwrap()
        .to_string();

    let mut req = make_request(Method::PUT, "/b/dst", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "b/src".parse().unwrap());
    req.headers
        .insert("x-amz-copy-source-range", "bytes=2-5".parse().unwrap());
    svc.upload_part_copy("123456789012", &req, "b", "dst", &upload_id, 1)
        .unwrap();

    {
        let mas = svc.state.read();
        let part = &mas.default_ref().buckets["b"].multipart_uploads[&upload_id].parts[&1];
        assert_eq!(part.size, 4);
        match &part.body {
            fakecloud_persistence::BodyRef::Disk { path, .. } => {
                assert_eq!(std::fs::read(path).unwrap(), b"2345")
            }
            other => panic!("part must be disk-backed in persistent mode, got {other:?}"),
        }
    }
    assert!(spool_is_empty(dir.path()));

    // Out-of-range copy ranges are still rejected against the source size.
    let mut req = make_request(Method::PUT, "/b/dst", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "b/src".parse().unwrap());
    req.headers
        .insert("x-amz-copy-source-range", "bytes=5-10".parse().unwrap());
    assert_aws_err(
        svc.upload_part_copy("123456789012", &req, "b", "dst", &upload_id, 2),
        "InvalidArgument",
    );
    // An unknown upload fails before any source IO.
    let mut req = make_request(Method::PUT, "/b/dst", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "b/src".parse().unwrap());
    assert_aws_err(
        svc.upload_part_copy("123456789012", &req, "b", "dst", "no-such-upload", 1),
        "NoSuchUpload",
    );
}

// ── Cross-account bucket addressing ─────────────────────────────

const OTHER_ACCOUNT: &str = "222222222222";

/// A service whose `theirs` bucket (holding `doc.txt`) belongs to
/// [`OTHER_ACCOUNT`], not to the caller account `make_request` uses.
fn service_with_foreign_bucket() -> S3Service {
    let svc = make_service();
    {
        let mut mas = svc.state.write();
        let state = mas.get_or_create(OTHER_ACCOUNT);
        let mut b = S3Bucket::new("theirs", "us-east-1", OTHER_ACCOUNT);
        b.objects.insert(
            "doc.txt".to_string(),
            S3Object {
                key: "doc.txt".to_string(),
                body: fakecloud_persistence::BodyRef::Memory(Bytes::from_static(b"owned by b")),
                content_type: "text/plain".to_string(),
                etag: compute_md5(b"owned by b"),
                size: 10,
                last_modified: chrono::Utc::now(),
                ..Default::default()
            },
        );
        state.buckets.insert("theirs".to_string(), b);
    }
    svc
}

#[tokio::test]
async fn object_requests_on_another_accounts_bucket_are_served_from_its_owner() {
    let svc = service_with_foreign_bucket();
    let resp = svc
        .handle(make_request(Method::GET, "/theirs/doc.txt", &[], b""))
        .await
        .expect("GetObject on the owner's bucket");
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.body.expect_bytes(), b"owned by b");

    svc.handle(make_request(
        Method::PUT,
        "/theirs/new.txt",
        &[],
        b"written",
    ))
    .await
    .expect("PutObject into the owner's bucket");
    {
        let mas = svc.state.read();
        assert!(mas
            .get(OTHER_ACCOUNT)
            .unwrap()
            .buckets
            .get("theirs")
            .unwrap()
            .objects
            .contains_key("new.txt"));
        assert!(
            !mas.default_ref().buckets.contains_key("theirs"),
            "the write must not create a bucket in the caller's account"
        );
    }

    assert_aws_err(
        svc.handle(make_request(Method::GET, "/theirs/absent.txt", &[], b""))
            .await,
        "NoSuchKey",
    );
}

#[tokio::test]
async fn create_bucket_named_like_another_accounts_bucket_is_already_exists() {
    let svc = service_with_foreign_bucket();
    assert_aws_err(
        svc.handle(make_request(Method::PUT, "/theirs", &[], b""))
            .await,
        "BucketAlreadyExists",
    );
    let resp = svc
        .handle(make_request(Method::GET, "/", &[], b""))
        .await
        .unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(
        !body.contains("<Name>theirs</Name>"),
        "ListBuckets is per account: {body}"
    );
}

#[tokio::test]
async fn copy_object_reads_a_source_bucket_owned_by_another_account() {
    let svc = service_with_foreign_bucket();
    seed_bucket(&svc, "mine");
    let mut req = make_request(Method::PUT, "/mine/copy.txt", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "theirs/doc.txt".parse().unwrap());
    svc.handle(req)
        .await
        .expect("CopyObject from the foreign source");
    let mas = svc.state.read();
    let copied = mas.default_ref().buckets["mine"].objects["copy.txt"].clone();
    assert_eq!(
        mas.default_ref().read_body(&copied.body).unwrap(),
        Bytes::from_static(b"owned by b")
    );
}

#[test]
fn copy_requests_also_need_read_on_the_source_object() {
    let svc = service_with_foreign_bucket();
    let mut req = make_request(Method::PUT, "/mine/copy.txt", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "theirs/doc.txt".parse().unwrap());
    let actions = svc.iam_actions_for(&req);
    assert_eq!(actions.len(), 2);
    assert_eq!(actions[0].action, "PutObject");
    assert_eq!(actions[1].action, "GetObject");
    assert_eq!(actions[1].resource, "arn:aws:s3:::theirs/doc.txt");

    req.headers.insert(
        "x-amz-copy-source",
        "/theirs/doc.txt?versionId=v1".parse().unwrap(),
    );
    let actions = svc.iam_actions_for(&req);
    assert_eq!(actions[1].action, "GetObjectVersion");
    assert_eq!(actions[1].resource, "arn:aws:s3:::theirs/doc.txt");

    let plain = make_request(Method::PUT, "/mine/plain.txt", &[], b"x");
    assert_eq!(svc.iam_actions_for(&plain).len(), 1);
}

// ── Cross-account SSE-KMS and copy-source parsing ───────────────

/// [`service_with_foreign_bucket`] with a KMS hook over fresh KMS state.
fn kms_service_with_foreign_bucket(
    mode: fakecloud_core::auth::IamMode,
) -> (S3Service, fakecloud_kms::SharedKmsState) {
    let kms: fakecloud_kms::SharedKmsState = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let hook = fakecloud_kms::hook::KmsServiceHook::new(kms.clone(), Default::default());
    let svc = service_with_foreign_bucket()
        .with_kms(kms.clone())
        .with_kms_hook(Arc::new(hook))
        .with_iam_mode(mode);
    (svc, kms)
}

fn sse_kms_put(path: &str, key_id: Option<&str>) -> AwsRequest {
    let mut req = make_request(Method::PUT, path, &[], b"secret body");
    req.headers
        .insert("x-amz-server-side-encryption", "aws:kms".parse().unwrap());
    if let Some(key_id) = key_id {
        req.headers.insert(
            "x-amz-server-side-encryption-aws-kms-key-id",
            key_id.parse().unwrap(),
        );
    }
    req
}

#[tokio::test]
async fn cross_account_put_with_the_callers_key_arn_encrypts_under_that_key() {
    let (svc, kms) = kms_service_with_foreign_bucket(fakecloud_core::auth::IamMode::Strict);
    // The caller's own key, named by ARN on a write into the owner's bucket.
    let caller_key = fakecloud_kms::hook::KmsServiceHook::new(kms.clone(), Default::default())
        .resolve_key_arn("123456789012", "us-east-1", "aws/s3", "s3.amazonaws.com")
        .unwrap();
    svc.handle(sse_kms_put("/theirs/enc.txt", Some(&caller_key)))
        .await
        .expect("the caller's key ARN is usable on another account's bucket");
    {
        let mas = svc.state.read();
        let obj = &mas.get(OTHER_ACCOUNT).unwrap().buckets["theirs"].objects["enc.txt"];
        assert_eq!(obj.sse_kms_key_id.as_deref(), Some(caller_key.as_str()));
    }
    let resp = svc
        .handle(make_request(Method::GET, "/theirs/enc.txt", &[], b""))
        .await
        .expect("the caller decrypts under its own key");
    assert_eq!(resp.body.expect_bytes(), b"secret body");
}

#[tokio::test]
async fn cross_account_put_with_a_bare_alias_uses_the_bucket_owners_key() {
    // Strict: the owner's aws/s3 key trusts only the owner's account, so the
    // other account's write under it is AccessDenied -- not a 500.
    let (svc, _) = kms_service_with_foreign_bucket(fakecloud_core::auth::IamMode::Strict);
    let err = assert_aws_err(
        svc.handle(sse_kms_put("/theirs/enc.txt", Some("alias/aws/s3")))
            .await,
        "AccessDenied",
    );
    assert_eq!(err.status(), StatusCode::FORBIDDEN);

    // IAM off: the alias resolves -- and encrypts -- in the owner's account.
    let (svc, kms) = kms_service_with_foreign_bucket(fakecloud_core::auth::IamMode::Off);
    svc.handle(sse_kms_put("/theirs/enc.txt", Some("alias/aws/s3")))
        .await
        .expect("the owner's aws/s3 key encrypts the write");
    let stored = {
        let mas = svc.state.read();
        mas.get(OTHER_ACCOUNT).unwrap().buckets["theirs"].objects["enc.txt"]
            .sse_kms_key_id
            .clone()
            .unwrap()
    };
    assert!(
        stored.contains(&format!(":{OTHER_ACCOUNT}:key/")),
        "{stored} is not a key in the bucket owner's account"
    );
    assert!(kms.read().get(OTHER_ACCOUNT).is_some());
    let resp = svc
        .handle(make_request(Method::GET, "/theirs/enc.txt", &[], b""))
        .await
        .unwrap();
    assert_eq!(resp.body.expect_bytes(), b"secret body");
}

#[tokio::test]
async fn sse_kms_put_with_an_unknown_key_is_kms_not_found_not_a_500() {
    let (svc, _) = kms_service_with_foreign_bucket(fakecloud_core::auth::IamMode::Off);
    let err = assert_aws_err(
        svc.handle(sse_kms_put(
            "/theirs/enc.txt",
            Some("1234abcd-12ab-34cd-56ef-1234567890ab"),
        ))
        .await,
        "KMS.NotFoundException",
    );
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn copy_source_parsing_is_shared_by_the_copy_and_its_authorization() {
    // An encoded `?` stays in the key; the raw `?` starts the query.
    let parsed = parse_copy_source("/src/a%3Fb?versionId=v1").unwrap();
    assert_eq!(
        parsed,
        CopySource {
            bucket: "src".into(),
            key: "a?b".into(),
            version_id: Some("v1".into()),
        }
    );
    let raw = parse_copy_source("src/a?b").unwrap();
    assert_eq!((raw.key.as_str(), raw.version_id), ("a", None));

    let svc = make_service();
    for header in [
        "src/a%3Fb?versionId=v1",
        "src/a?b",
        "src/a?x=1&versionId=v2",
    ] {
        let mut req = make_request(Method::PUT, "/dst/k", &[], b"");
        req.headers
            .insert("x-amz-copy-source", header.parse().unwrap());
        let source = parse_copy_source(header).unwrap();
        let authorized = svc.iam_actions_for(&req).pop().unwrap();
        assert_eq!(
            authorized.resource,
            format!("arn:aws:s3:::{}/{}", source.bucket, source.key),
            "{header}"
        );
        assert_eq!(
            authorized.action,
            if source.version_id.is_some() {
                "GetObjectVersion"
            } else {
                "GetObject"
            },
            "{header}"
        );
    }
}

#[tokio::test]
async fn upload_part_copy_reads_the_object_its_authorization_names() {
    // A key with an encoded `?`: UploadPartCopy copies exactly the object the
    // copy-source authorization checked, not a prefix of it.
    let svc = make_service();
    seed_bucket(&svc, "src");
    seed_bucket(&svc, "dst");
    seed_object(&svc, "src", "a?b", b"question");
    seed_object(&svc, "src", "a", b"prefix");
    let created = svc
        .handle(make_request(
            Method::POST,
            "/dst/k",
            &[("uploads", "")],
            b"",
        ))
        .await
        .expect("CreateMultipartUpload");
    let xml = String::from_utf8(created.body.expect_bytes().to_vec()).unwrap();
    let upload_id = extract_xml_value(&xml, "UploadId").unwrap();
    let mut req = make_request(
        Method::PUT,
        "/dst/k",
        &[("partNumber", "1"), ("uploadId", upload_id.as_str())],
        b"",
    );
    req.headers
        .insert("x-amz-copy-source", "src/a%3Fb".parse().unwrap());
    svc.handle(req).await.expect("UploadPartCopy");
    let mas = svc.state.read();
    let part = &mas.default_ref().buckets["dst"].multipart_uploads[&upload_id].parts[&1];
    assert_eq!(part.size, b"question".len() as u64);
}

// ---- Service Quotas: general purpose buckets ----

/// The bucket quota as Service Quotas resolves a global quota: one value
/// whichever region asks. Records the regions it was asked in.
struct GlobalBucketQuota(f64, std::sync::Mutex<Vec<String>>);

impl GlobalBucketQuota {
    fn new(limit: f64) -> Self {
        Self(limit, Default::default())
    }
}

impl fakecloud_core::quota::QuotaProvider for GlobalBucketQuota {
    fn applied_value(&self, _: &str, region: &str, service: &str, code: &str) -> Option<f64> {
        self.1.lock().unwrap().push(region.to_string());
        (service == "s3" && code == "L-DC2B2D3D").then_some(self.0)
    }

    fn enforced_limit(
        &self,
        account: &str,
        region: &str,
        service: &str,
        code: &str,
    ) -> Option<f64> {
        self.applied_value(account, region, service, code)
    }
}

fn create_in(svc: &S3Service, bucket: &str, region: &str) -> Result<AwsResponse, AwsServiceError> {
    let mut req = make_request(Method::PUT, &format!("/{bucket}"), &[], b"");
    req.region = region.to_string();
    svc.create_bucket("123456789012", &req, bucket)
}

#[test]
fn bucket_quota_is_not_enforced_without_a_provider() {
    let svc = make_service();
    for i in 0..3 {
        create_in(&svc, &format!("free-{i}"), "us-east-1").unwrap();
    }
}

#[test]
fn bucket_quota_counts_buckets_in_every_region_against_one_account_value() {
    let quota = Arc::new(GlobalBucketQuota::new(2.0));
    let svc = make_service().with_quota_provider(Some(quota.clone()));
    create_in(&svc, "quota-a", "us-east-1").unwrap();
    // A bucket created from another Region counts toward the same quota.
    let mut req = make_request(
        Method::PUT,
        "/quota-b",
        &[],
        b"<CreateBucketConfiguration><LocationConstraint>eu-west-1</LocationConstraint></CreateBucketConfiguration>",
    );
    req.region = "eu-west-1".to_string();
    svc.create_bucket("123456789012", &req, "quota-b").unwrap();

    let err = create_in(&svc, "quota-c", "us-east-1").err().unwrap();
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    assert_eq!(err.code(), "TooManyBuckets");
    assert_eq!(
        err.message(),
        "You have attempted to create more buckets than allowed"
    );
    // Re-creating an owned bucket in us-east-1 is still the idempotent no-op.
    create_in(&svc, "quota-a", "us-east-1").unwrap();
    // The limit is asked for in the request's own region (the quota is
    // global, so no remapping is needed).
    assert!(quota.1.lock().unwrap().iter().any(|r| r == "eu-west-1"));

    use fakecloud_core::quota::QuotaUsageSource;
    let usage = crate::quota::S3QuotaUsage::new(svc.state.clone());
    assert_eq!(
        usage.usage("123456789012", "us-east-1", "s3", "L-DC2B2D3D"),
        Some(2.0)
    );
    assert_eq!(usage.usage("123456789012", "us-east-1", "s3", "L-0"), None);
}

// ── Flexible checksums ───────────────────────────────────────────

mod flexible_checksums {
    use super::*;
    use crate::checksum::{checksum, digest, ChecksumAlgorithm, ChecksumType};

    const ACCT: &str = "123456789012";

    fn header(resp: &AwsResponse, name: &str) -> Option<String> {
        resp.headers
            .get(name)
            .map(|v| v.to_str().unwrap().to_string())
    }

    fn body_text(resp: &AwsResponse) -> String {
        String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap()
    }

    fn with_headers(mut req: AwsRequest, headers: &[(&str, &str)]) -> AwsRequest {
        for (k, v) in headers {
            req.headers.insert(
                http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        req
    }

    fn object(svc: &S3Service, bucket: &str, key: &str) -> S3Object {
        svc.state.read().default_ref().buckets[bucket].objects[key].clone()
    }

    async fn put(
        svc: &S3Service,
        bucket: &str,
        key: &str,
        body: &[u8],
        headers: &[(&str, &str)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let req = with_headers(
            make_request(Method::PUT, &format!("/{bucket}/{key}"), &[], body),
            headers,
        );
        svc.put_object(ACCT, &req, bucket, key).await
    }

    fn head_with_checksum(
        svc: &S3Service,
        bucket: &str,
        key: &str,
        query: &[(&str, &str)],
    ) -> AwsResponse {
        let req = with_headers(
            make_request(Method::HEAD, &format!("/{bucket}/{key}"), query, b""),
            &[("x-amz-checksum-mode", "ENABLED")],
        );
        svc.head_object(ACCT, &req, bucket, key).unwrap()
    }

    #[tokio::test]
    async fn put_object_verifies_and_stores_every_algorithm() {
        let svc = make_service();
        seed_bucket(&svc, "ck");
        let body = b"hello world";
        for algo in ChecksumAlgorithm::ALL {
            let key = algo.lowercase_name();
            let value = checksum(algo, body);
            let resp = put(&svc, "ck", key, body, &[(algo.header_name(), &value)])
                .await
                .unwrap_or_else(|e| panic!("{algo}: {e:?}"));
            assert_eq!(
                header(&resp, algo.header_name()),
                Some(value.clone()),
                "{algo}"
            );
            assert_eq!(
                header(&resp, "x-amz-checksum-type").as_deref(),
                Some("FULL_OBJECT")
            );

            let obj = object(&svc, "ck", key);
            assert_eq!(obj.checksum_algorithm, Some(algo));
            assert_eq!(obj.checksum_value.as_deref(), Some(value.as_str()));

            let head = head_with_checksum(&svc, "ck", key, &[]);
            assert_eq!(
                header(&head, algo.header_name()),
                Some(value.clone()),
                "{algo}"
            );
            assert_eq!(
                header(&head, "x-amz-checksum-type").as_deref(),
                Some("FULL_OBJECT")
            );

            // The wrong digest for the same algorithm is BadDigest, and
            // nothing is stored.
            let wrong = checksum(algo, b"tampered");
            let err = put(&svc, "ck", "wrong", body, &[(algo.header_name(), &wrong)]).await;
            assert_aws_err(err, "BadDigest");
            assert!(!svc.state.read().default_ref().buckets["ck"]
                .objects
                .contains_key("wrong"));
        }
    }

    #[tokio::test]
    async fn put_object_without_a_checksum_gets_the_crc64nvme_default() {
        let svc = make_service();
        seed_bucket(&svc, "ck");
        let resp = put(&svc, "ck", "plain", b"data", &[]).await.unwrap();
        let expected = checksum(ChecksumAlgorithm::Crc64Nvme, b"data");
        assert_eq!(
            header(&resp, "x-amz-checksum-crc64nvme"),
            Some(expected.clone())
        );
        let obj = object(&svc, "ck", "plain");
        assert_eq!(obj.checksum_algorithm, Some(ChecksumAlgorithm::Crc64Nvme));
        assert_eq!(obj.checksum_type, Some(ChecksumType::FullObject));

        // A plain HEAD does not echo it; checksum mode does.
        let req = make_request(Method::HEAD, "/ck/plain", &[], b"");
        let plain = svc.head_object(ACCT, &req, "ck", "plain").unwrap();
        assert!(plain.headers.get("x-amz-checksum-crc64nvme").is_none());
        let head = head_with_checksum(&svc, "ck", "plain", &[]);
        assert_eq!(header(&head, "x-amz-checksum-crc64nvme"), Some(expected));

        // The SDK-declared algorithm alone selects what is computed.
        put(
            &svc,
            "ck",
            "sdk",
            b"data",
            &[("x-amz-sdk-checksum-algorithm", "xxhash128")],
        )
        .await
        .unwrap();
        let obj = object(&svc, "ck", "sdk");
        assert_eq!(obj.checksum_algorithm, Some(ChecksumAlgorithm::XxHash128));
        assert_eq!(
            obj.checksum_value,
            Some(checksum(ChecksumAlgorithm::XxHash128, b"data"))
        );
    }

    #[tokio::test]
    async fn put_object_rejects_malformed_integrity_headers() {
        let svc = make_service();
        seed_bucket(&svc, "ck");
        assert_aws_err(
            put(
                &svc,
                "ck",
                "k",
                b"x",
                &[("x-amz-sdk-checksum-algorithm", "CRC16")],
            )
            .await,
            "InvalidRequest",
        );
        assert_aws_err(
            put(
                &svc,
                "ck",
                "k",
                b"x",
                &[("x-amz-checksum-sha256", "not-base64")],
            )
            .await,
            "InvalidRequest",
        );
        let crc = checksum(ChecksumAlgorithm::Crc32, b"x");
        let crcc = checksum(ChecksumAlgorithm::Crc32c, b"x");
        assert_aws_err(
            put(
                &svc,
                "ck",
                "k",
                b"x",
                &[
                    ("x-amz-checksum-crc32", &crc),
                    ("x-amz-checksum-crc32c", &crcc),
                ],
            )
            .await,
            "InvalidRequest",
        );
        assert_aws_err(
            put(&svc, "ck", "k", b"x", &[("content-md5", "short")]).await,
            "InvalidDigest",
        );
        let wrong_md5 = content_md5(b"y");
        assert_aws_err(
            put(&svc, "ck", "k", b"x", &[("content-md5", &wrong_md5)]).await,
            "BadDigest",
        );
        // Declared trailer that never arrives.
        assert_aws_err(
            put(
                &svc,
                "ck",
                "k",
                b"x",
                &[("x-amz-trailer", "x-amz-checksum-crc32")],
            )
            .await,
            "MalformedTrailerError",
        );
    }

    #[tokio::test]
    async fn missing_bucket_and_failed_preconditions_win_over_bad_digest() {
        let svc = make_service();
        let wrong = checksum(ChecksumAlgorithm::Sha256, b"other");
        assert_aws_err(
            put(
                &svc,
                "ghost",
                "k",
                b"x",
                &[("x-amz-checksum-sha256", &wrong)],
            )
            .await,
            "NoSuchBucket",
        );
        assert_aws_err(
            put(
                &svc,
                "ghost",
                "k",
                b"x",
                &[("x-amz-checksum-sha256", "garbage")],
            )
            .await,
            "NoSuchBucket",
        );
        seed_bucket(&svc, "ck");
        put(&svc, "ck", "k", b"x", &[]).await.unwrap();
        assert_aws_err(
            put(
                &svc,
                "ck",
                "k",
                b"x",
                &[("if-none-match", "*"), ("x-amz-checksum-sha256", &wrong)],
            )
            .await,
            "PreconditionFailed",
        );
    }

    #[tokio::test]
    async fn put_object_verifies_an_aws_chunked_trailing_checksum() {
        let svc = make_service();
        seed_bucket(&svc, "ck");
        let payload = b"chunked payload with a trailing checksum";
        let good = checksum(ChecksumAlgorithm::XxHash64, payload);
        let frame = |value: &str| {
            let mut out = format!("{:x};chunk-signature={}\r\n", 10, "0".repeat(64)).into_bytes();
            out.extend_from_slice(&payload[..10]);
            out.extend_from_slice(format!("\r\n{:x}\r\n", payload.len() - 10).as_bytes());
            out.extend_from_slice(&payload[10..]);
            out.extend_from_slice(
                format!("\r\n0\r\nx-amz-checksum-xxhash64:{value}\r\nx-amz-trailer-signature:abc\r\n\r\n")
                    .as_bytes(),
            );
            out
        };
        let headers = [
            ("content-encoding", "aws-chunked"),
            ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
            ("x-amz-trailer", "x-amz-checksum-xxhash64"),
            ("x-amz-sdk-checksum-algorithm", "XXHASH64"),
        ];
        let resp = put(&svc, "ck", "t", &frame(&good), &headers).await.unwrap();
        assert_eq!(header(&resp, "x-amz-checksum-xxhash64"), Some(good.clone()));
        let obj = object(&svc, "ck", "t");
        assert_eq!(obj.size, payload.len() as u64);
        assert_eq!(obj.checksum_value, Some(good));

        let wrong = checksum(ChecksumAlgorithm::XxHash64, b"nope");
        assert_aws_err(
            put(&svc, "ck", "t2", &frame(&wrong), &headers).await,
            "BadDigest",
        );
    }

    async fn create_upload(
        svc: &S3Service,
        key: &str,
        headers: &[(&str, &str)],
    ) -> Result<(String, AwsResponse), AwsServiceError> {
        let req = with_headers(
            make_request(Method::POST, &format!("/mp/{key}"), &[("uploads", "")], b""),
            headers,
        );
        let resp = svc.create_multipart_upload(ACCT, &req, "mp", key)?;
        let text = body_text(&resp);
        let start = text.find("<UploadId>").unwrap() + "<UploadId>".len();
        let end = text.find("</UploadId>").unwrap();
        Ok((text[start..end].to_string(), resp))
    }

    async fn upload_part(
        svc: &S3Service,
        key: &str,
        upload_id: &str,
        n: u32,
        data: &[u8],
        headers: &[(&str, &str)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let req = with_headers(
            make_request(
                Method::PUT,
                &format!("/mp/{key}"),
                &[("partNumber", &n.to_string()), ("uploadId", upload_id)],
                data,
            ),
            headers,
        );
        svc.upload_part(ACCT, &req, "mp", key, upload_id, n as i64)
            .await
    }

    /// (part number, ETag, optional part checksum) as listed in the request.
    type CompletePart = (u32, String, Option<(ChecksumAlgorithm, String)>);

    fn complete(
        svc: &S3Service,
        key: &str,
        upload_id: &str,
        parts: &[CompletePart],
        headers: &[(&str, &str)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut xml = String::from("<CompleteMultipartUpload>");
        for (n, etag, ck) in parts {
            xml.push_str(&format!(
                "<Part><PartNumber>{n}</PartNumber><ETag>{etag}</ETag>"
            ));
            if let Some((a, v)) = ck {
                xml.push_str(&crate::checksum::xml_element(*a, v));
            }
            xml.push_str("</Part>");
        }
        xml.push_str("</CompleteMultipartUpload>");
        let req = with_headers(
            make_request(
                Method::POST,
                &format!("/mp/{key}"),
                &[("uploadId", upload_id)],
                xml.as_bytes(),
            ),
            headers,
        );
        svc.complete_multipart_upload(ACCT, &req, "mp", key, upload_id)
    }

    #[tokio::test]
    async fn create_multipart_upload_validates_checksum_type() {
        let svc = make_service();
        seed_bucket(&svc, "mp");
        let (_, resp) = create_upload(&svc, "a", &[("x-amz-checksum-algorithm", "CRC64NVME")])
            .await
            .unwrap();
        assert_eq!(
            header(&resp, "x-amz-checksum-algorithm").as_deref(),
            Some("CRC64NVME")
        );
        assert_eq!(
            header(&resp, "x-amz-checksum-type").as_deref(),
            Some("FULL_OBJECT")
        );
        let (_, resp) = create_upload(&svc, "b", &[("x-amz-checksum-algorithm", "sha512")])
            .await
            .unwrap();
        assert_eq!(
            header(&resp, "x-amz-checksum-algorithm").as_deref(),
            Some("SHA512")
        );
        assert_eq!(
            header(&resp, "x-amz-checksum-type").as_deref(),
            Some("COMPOSITE")
        );

        for headers in [
            vec![
                ("x-amz-checksum-algorithm", "SHA256"),
                ("x-amz-checksum-type", "FULL_OBJECT"),
            ],
            vec![
                ("x-amz-checksum-algorithm", "CRC64NVME"),
                ("x-amz-checksum-type", "COMPOSITE"),
            ],
            vec![("x-amz-checksum-type", "COMPOSITE")],
            vec![("x-amz-checksum-algorithm", "CRC99")],
        ] {
            assert_aws_err(
                create_upload(&svc, "c", &headers).await.map(|(_, r)| r),
                "InvalidRequest",
            );
        }
    }

    #[tokio::test]
    async fn composite_multipart_checksums_for_new_algorithms() {
        let svc = make_service();
        seed_bucket(&svc, "mp");
        let algo = ChecksumAlgorithm::XxHash3;
        let (upload_id, _) = create_upload(&svc, "k", &[("x-amz-checksum-algorithm", "XXHASH3")])
            .await
            .unwrap();
        let p1 = vec![7u8; 5 * 1024 * 1024];
        let p2 = b"tail".to_vec();

        // A part in another algorithm is a type mismatch.
        let other = checksum(ChecksumAlgorithm::Crc32, &p1);
        assert_aws_err(
            upload_part(
                &svc,
                "k",
                &upload_id,
                1,
                &p1,
                &[("x-amz-checksum-crc32", &other)],
            )
            .await,
            "InvalidRequest",
        );
        // A part without a checksum header still gets the upload's algorithm.
        let r1 = upload_part(&svc, "k", &upload_id, 1, &p1, &[])
            .await
            .unwrap();
        assert_eq!(
            header(&r1, "x-amz-checksum-xxhash3"),
            Some(checksum(algo, &p1))
        );
        let p2_ck = checksum(algo, &p2);
        let r2 = upload_part(
            &svc,
            "k",
            &upload_id,
            2,
            &p2,
            &[("x-amz-checksum-xxhash3", &p2_ck)],
        )
        .await
        .unwrap();
        let e1 = header(&r1, "etag").unwrap();
        let e2 = header(&r2, "etag").unwrap();

        // ListParts carries the part checksums and the upload's settings.
        let lp = make_request(Method::GET, "/mp/k", &[("uploadId", &upload_id)], b"");
        let listed = body_text(&svc.list_parts(ACCT, &lp, "mp", "k", &upload_id).unwrap());
        assert!(
            listed.contains(&format!("<ChecksumXXHASH3>{p2_ck}</ChecksumXXHASH3>")),
            "{listed}"
        );
        assert!(
            listed.contains("<ChecksumAlgorithm>XXHASH3</ChecksumAlgorithm>"),
            "{listed}"
        );
        assert!(
            listed.contains("<ChecksumType>COMPOSITE</ChecksumType>"),
            "{listed}"
        );
        let uploads = body_text(
            &svc.list_multipart_uploads(ACCT, "mp", &HashMap::new())
                .unwrap(),
        );
        assert!(
            uploads.contains("<ChecksumAlgorithm>XXHASH3</ChecksumAlgorithm><ChecksumType>COMPOSITE</ChecksumType>"),
            "{uploads}"
        );

        // COMPOSITE needs every part's checksum, and they must match.
        assert_aws_err(
            complete(
                &svc,
                "k",
                &upload_id,
                &[
                    (1, e1.clone(), None),
                    (2, e2.clone(), Some((algo, p2_ck.clone()))),
                ],
                &[],
            ),
            "InvalidRequest",
        );
        assert_aws_err(
            complete(
                &svc,
                "k",
                &upload_id,
                &[
                    (1, e1.clone(), Some((algo, p2_ck.clone()))),
                    (2, e2.clone(), Some((algo, p2_ck.clone()))),
                ],
                &[],
            ),
            "InvalidPart",
        );
        let expected =
            crate::checksum::composite(algo, &[digest(algo, &p1), digest(algo, &p2)]).unwrap();
        let wrong_whole = format!("{}-2", checksum(algo, b"x"));
        let parts = [
            (1, e1.clone(), Some((algo, checksum(algo, &p1)))),
            (2, e2.clone(), Some((algo, p2_ck.clone()))),
        ];
        assert_aws_err(
            complete(
                &svc,
                "k",
                &upload_id,
                &parts,
                &[("x-amz-checksum-xxhash3", &wrong_whole)],
            ),
            "BadDigest",
        );
        let resp = complete(
            &svc,
            "k",
            &upload_id,
            &parts,
            &[("x-amz-checksum-xxhash3", &expected)],
        )
        .unwrap();
        let text = body_text(&resp);
        assert!(
            text.contains(&format!("<ChecksumXXHASH3>{expected}</ChecksumXXHASH3>")),
            "{text}"
        );
        assert!(
            text.contains("<ChecksumType>COMPOSITE</ChecksumType>"),
            "{text}"
        );

        let obj = object(&svc, "mp", "k");
        assert_eq!(obj.checksum_value.as_deref(), Some(expected.as_str()));
        assert_eq!(obj.checksum_type, Some(ChecksumType::Composite));

        // Whole-object and per-part checksums on read.
        let head = head_with_checksum(&svc, "mp", "k", &[]);
        assert_eq!(header(&head, "x-amz-checksum-xxhash3"), Some(expected));
        assert_eq!(
            header(&head, "x-amz-checksum-type").as_deref(),
            Some("COMPOSITE")
        );
        let head2 = head_with_checksum(&svc, "mp", "k", &[("partNumber", "2")]);
        assert_eq!(
            header(&head2, "x-amz-checksum-xxhash3"),
            Some(p2_ck.clone())
        );
        let get = with_headers(
            make_request(Method::GET, "/mp/k", &[], b""),
            &[
                ("x-amz-checksum-mode", "ENABLED"),
                (
                    "range",
                    &format!("bytes={}-{}", p1.len(), p1.len() + p2.len() - 1),
                ),
            ],
        );
        let ranged = svc.get_object(ACCT, &get, "mp", "k").unwrap();
        assert_eq!(
            header(&ranged, "x-amz-checksum-xxhash3"),
            Some(p2_ck.clone())
        );

        let attrs = with_headers(
            make_request(Method::GET, "/mp/k", &[("attributes", "")], b""),
            &[("x-amz-object-attributes", "Checksum,ObjectParts")],
        );
        let text = body_text(&svc.get_object_attributes(ACCT, &attrs, "mp", "k").unwrap());
        assert!(text.contains("<PartsCount>2</PartsCount>"), "{text}");
        assert!(
            text.contains(&format!("<Part><PartNumber>2</PartNumber><Size>4</Size><ChecksumXXHASH3>{p2_ck}</ChecksumXXHASH3></Part>")),
            "{text}"
        );
        // The SDKs send one header line per attribute.
        let mut split = make_request(Method::GET, "/mp/k", &[("attributes", "")], b"");
        split
            .headers
            .append("x-amz-object-attributes", "Checksum".parse().unwrap());
        split
            .headers
            .append("x-amz-object-attributes", "ObjectParts".parse().unwrap());
        let text = body_text(&svc.get_object_attributes(ACCT, &split, "mp", "k").unwrap());
        assert!(text.contains("<Checksum><ChecksumXXHASH3>"), "{text}");
        assert!(text.contains("<ObjectParts>"), "{text}");
        let paged = with_headers(
            make_request(Method::GET, "/mp/k", &[("attributes", "")], b""),
            &[
                ("x-amz-object-attributes", "ObjectParts"),
                ("x-amz-max-parts", "1"),
            ],
        );
        let text = body_text(&svc.get_object_attributes(ACCT, &paged, "mp", "k").unwrap());
        assert!(text.contains("<IsTruncated>true</IsTruncated>"), "{text}");
        assert!(
            text.contains("<NextPartNumberMarker>1</NextPartNumberMarker>"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn full_object_multipart_checksum_covers_the_whole_object() {
        let svc = make_service();
        seed_bucket(&svc, "mp");
        let algo = ChecksumAlgorithm::Crc32c;
        let (upload_id, _) = create_upload(
            &svc,
            "k",
            &[
                ("x-amz-checksum-algorithm", "CRC32C"),
                ("x-amz-checksum-type", "FULL_OBJECT"),
            ],
        )
        .await
        .unwrap();
        let p1 = vec![1u8; 5 * 1024 * 1024];
        let p2 = b"end".to_vec();
        let e1 = header(
            &upload_part(&svc, "k", &upload_id, 1, &p1, &[])
                .await
                .unwrap(),
            "etag",
        )
        .unwrap();
        let e2 = header(
            &upload_part(&svc, "k", &upload_id, 2, &p2, &[])
                .await
                .unwrap(),
            "etag",
        )
        .unwrap();
        let mut whole = p1.clone();
        whole.extend_from_slice(&p2);
        let expected = checksum(algo, &whole);
        let parts = [(1, e1, None), (2, e2, None)];
        assert_aws_err(
            complete(
                &svc,
                "k",
                &upload_id,
                &parts,
                &[("x-amz-checksum-type", "COMPOSITE")],
            ),
            "InvalidRequest",
        );
        assert_aws_err(
            complete(
                &svc,
                "k",
                &upload_id,
                &parts,
                &[("x-amz-mp-object-size", "3")],
            ),
            "InvalidRequest",
        );
        let size = whole.len().to_string();
        let resp = complete(
            &svc,
            "k",
            &upload_id,
            &parts,
            &[
                ("x-amz-checksum-crc32c", &expected),
                ("x-amz-checksum-type", "FULL_OBJECT"),
                ("x-amz-mp-object-size", &size),
            ],
        )
        .unwrap();
        assert!(body_text(&resp).contains("<ChecksumType>FULL_OBJECT</ChecksumType>"));
        let obj = object(&svc, "mp", "k");
        assert_eq!(obj.checksum_value, Some(expected));
        assert_eq!(obj.checksum_type, Some(ChecksumType::FullObject));
    }

    #[tokio::test]
    async fn multipart_without_an_algorithm_gets_full_object_crc64nvme() {
        let svc = make_service();
        seed_bucket(&svc, "mp");
        let (upload_id, _) = create_upload(&svc, "k", &[]).await.unwrap();
        // The SDK checksums each part (CRC32 by default); it is verified.
        let p1 = b"only part".to_vec();
        let crc = checksum(ChecksumAlgorithm::Crc32, &p1);
        let r1 = upload_part(
            &svc,
            "k",
            &upload_id,
            1,
            &p1,
            &[("x-amz-checksum-crc32", &crc)],
        )
        .await
        .unwrap();
        assert_eq!(header(&r1, "x-amz-checksum-crc32"), Some(crc.clone()));
        let e1 = header(&r1, "etag").unwrap();
        // A newer algorithm in Complete needs it named at Create.
        let sha512 = checksum(ChecksumAlgorithm::Sha512, &p1);
        assert_aws_err(
            complete(
                &svc,
                "k",
                &upload_id,
                &[(1, e1.clone(), None)],
                &[("x-amz-checksum-sha512", &sha512)],
            ),
            "InvalidRequest",
        );
        // An original algorithm is accepted without being validated.
        let bogus_sha256 = checksum(ChecksumAlgorithm::Sha256, b"unrelated");
        complete(
            &svc,
            "k",
            &upload_id,
            &[(1, e1, Some((ChecksumAlgorithm::Crc32, crc)))],
            &[("x-amz-checksum-sha256", &bogus_sha256)],
        )
        .unwrap();
        let obj = object(&svc, "mp", "k");
        assert_eq!(obj.checksum_algorithm, Some(ChecksumAlgorithm::Crc64Nvme));
        assert_eq!(obj.checksum_type, Some(ChecksumType::FullObject));
        assert_eq!(
            obj.checksum_value,
            Some(checksum(ChecksumAlgorithm::Crc64Nvme, &p1))
        );
        assert!(obj.part_checksums.is_none());
    }

    #[tokio::test]
    async fn copy_object_rehashes_a_composite_source_as_full_object() {
        let svc = make_service();
        seed_bucket(&svc, "mp");
        let algo = ChecksumAlgorithm::Sha1;
        let (upload_id, _) = create_upload(&svc, "src", &[("x-amz-checksum-algorithm", "SHA1")])
            .await
            .unwrap();
        let data = b"single part".to_vec();
        let r = upload_part(&svc, "src", &upload_id, 1, &data, &[])
            .await
            .unwrap();
        let etag = header(&r, "etag").unwrap();
        complete(
            &svc,
            "src",
            &upload_id,
            &[(1, etag, Some((algo, checksum(algo, &data))))],
            &[],
        )
        .unwrap();
        assert_eq!(
            object(&svc, "mp", "src").checksum_type,
            Some(ChecksumType::Composite)
        );

        let resp = svc
            .copy_object(ACCT, &copy_request("mp", "dst", "mp/src"), "mp", "dst")
            .unwrap();
        let text = body_text(&resp);
        let full = checksum(algo, &data);
        assert!(
            text.contains(&format!("<ChecksumSHA1>{full}</ChecksumSHA1>")),
            "{text}"
        );
        assert!(
            text.contains("<ChecksumType>FULL_OBJECT</ChecksumType>"),
            "{text}"
        );
        let dst = object(&svc, "mp", "dst");
        assert_eq!(dst.checksum_value, Some(full));
        assert_eq!(dst.checksum_type, Some(ChecksumType::FullObject));

        // Requesting another algorithm recomputes in it.
        let req = with_headers(
            copy_request("mp", "dst2", "mp/src"),
            &[("x-amz-checksum-algorithm", "MD5")],
        );
        svc.copy_object(ACCT, &req, "mp", "dst2").unwrap();
        assert_eq!(
            object(&svc, "mp", "dst2").checksum_value,
            Some(checksum(ChecksumAlgorithm::Md5, &data))
        );
    }

    /// Create an upload, upload `parts` (each with the upload's checksum
    /// computed server-side), returning (upload id, ETags).
    async fn upload_parts(
        svc: &S3Service,
        key: &str,
        create_headers: &[(&str, &str)],
        parts: &[&[u8]],
    ) -> (String, Vec<String>) {
        let (upload_id, _) = create_upload(svc, key, create_headers).await.unwrap();
        let mut etags = Vec::new();
        for (i, data) in parts.iter().enumerate() {
            let r = upload_part(svc, key, &upload_id, i as u32 + 1, data, &[])
                .await
                .unwrap();
            etags.push(header(&r, "etag").unwrap());
        }
        (upload_id, etags)
    }

    #[tokio::test]
    async fn complete_rejects_part_checksums_it_cannot_back() {
        let svc = make_service();
        seed_bucket(&svc, "mp");
        let data = b"only".to_vec();
        // Another algorithm than the upload's is a type mismatch.
        let (id, etags) = upload_parts(
            &svc,
            "a",
            &[("x-amz-checksum-algorithm", "SHA256")],
            &[&data],
        )
        .await;
        let crc = checksum(ChecksumAlgorithm::Crc32, &data);
        assert_aws_err(
            complete(
                &svc,
                "a",
                &id,
                &[(
                    1,
                    etags[0].clone(),
                    Some((ChecksumAlgorithm::Crc32, crc.clone())),
                )],
                &[],
            ),
            "InvalidRequest",
        );
        // A part stored without a checksum cannot match a listed one.
        let (id, etags) = upload_parts(&svc, "b", &[], &[&data]).await;
        assert_aws_err(
            complete(
                &svc,
                "b",
                &id,
                &[(1, etags[0].clone(), Some((ChecksumAlgorithm::Crc32, crc)))],
                &[],
            ),
            "InvalidPart",
        );
        // A wrong ETag is InvalidPart even when a checksum error is pending.
        let (id, _) = upload_parts(
            &svc,
            "c",
            &[("x-amz-checksum-algorithm", "SHA256")],
            &[&data],
        )
        .await;
        assert_aws_err(
            complete(&svc, "c", &id, &[(1, "\"nope\"".to_string(), None)], &[]),
            "InvalidPart",
        );
    }

    #[tokio::test]
    async fn complete_enforces_the_whole_object_value_shape() {
        let svc = make_service();
        seed_bucket(&svc, "mp");
        let data = b"payload".to_vec();
        let crc = checksum(ChecksumAlgorithm::Crc32, &data);

        // FULL_OBJECT: a bare digest; `-N` is malformed.
        let (id, etags) = upload_parts(
            &svc,
            "full",
            &[
                ("x-amz-checksum-algorithm", "CRC32"),
                ("x-amz-checksum-type", "FULL_OBJECT"),
            ],
            &[&data],
        )
        .await;
        let parts = [(1, etags[0].clone(), None)];
        assert_aws_err(
            complete(
                &svc,
                "full",
                &id,
                &parts,
                &[("x-amz-checksum-crc32", &format!("{crc}-1"))],
            ),
            "InvalidRequest",
        );
        complete(&svc, "full", &id, &parts, &[("x-amz-checksum-crc32", &crc)]).unwrap();

        // COMPOSITE: needs `-<part count>`, and the right count.
        let (id, etags) = upload_parts(
            &svc,
            "comp",
            &[("x-amz-checksum-algorithm", "CRC32")],
            &[&data],
        )
        .await;
        let parts = [(
            1,
            etags[0].clone(),
            Some((ChecksumAlgorithm::Crc32, crc.clone())),
        )];
        let composite = crate::checksum::composite(
            ChecksumAlgorithm::Crc32,
            &[digest(ChecksumAlgorithm::Crc32, &data)],
        )
        .unwrap();
        let bare = composite.trim_end_matches("-1").to_string();
        for bad in [bare, format!("{}-2", composite.trim_end_matches("-1"))] {
            assert_aws_err(
                complete(&svc, "comp", &id, &parts, &[("x-amz-checksum-crc32", &bad)]),
                "InvalidRequest",
            );
        }
        complete(
            &svc,
            "comp",
            &id,
            &parts,
            &[("x-amz-checksum-crc32", &composite)],
        )
        .unwrap();
    }

    #[tokio::test]
    async fn full_object_crc_is_combined_from_part_checksums() {
        let svc = make_service();
        seed_bucket(&svc, "mp");
        let p1: Vec<u8> = (0..5 * 1024 * 1024u32).map(|i| (i % 253) as u8).collect();
        let p2: Vec<u8> = (0..5 * 1024 * 1024u32).map(|i| (i % 241) as u8).collect();
        let p3 = b"short tail".to_vec();
        let mut whole = p1.clone();
        whole.extend_from_slice(&p2);
        whole.extend_from_slice(&p3);
        for algo in [
            ChecksumAlgorithm::Crc32,
            ChecksumAlgorithm::Crc32c,
            ChecksumAlgorithm::Crc64Nvme,
        ] {
            let key = algo.lowercase_name();
            let (id, etags) = upload_parts(
                &svc,
                key,
                &[
                    ("x-amz-checksum-algorithm", algo.as_str()),
                    ("x-amz-checksum-type", "FULL_OBJECT"),
                ],
                &[&p1, &p2, &p3],
            )
            .await;
            let parts: Vec<_> = etags
                .iter()
                .enumerate()
                .map(|(i, e)| (i as u32 + 1, e.clone(), None))
                .collect();
            complete(&svc, key, &id, &parts, &[]).unwrap();
            // The combined value equals rehashing the assembled object.
            assert_eq!(
                object(&svc, "mp", key).checksum_value,
                Some(checksum(algo, &whole)),
                "{algo}"
            );
        }
        // No algorithm at Create, but every part carried CRC64NVME: combined too.
        let (id, _) = create_upload(&svc, "default", &[]).await.unwrap();
        let mut parts = Vec::new();
        for (i, data) in [&p1, &p2, &p3].into_iter().enumerate() {
            let value = checksum(ChecksumAlgorithm::Crc64Nvme, data);
            let r = upload_part(
                &svc,
                "default",
                &id,
                i as u32 + 1,
                data,
                &[("x-amz-checksum-crc64nvme", &value)],
            )
            .await
            .unwrap();
            parts.push((i as u32 + 1, header(&r, "etag").unwrap(), None));
        }
        complete(&svc, "default", &id, &parts, &[]).unwrap();
        assert_eq!(
            object(&svc, "mp", "default").checksum_value,
            Some(checksum(ChecksumAlgorithm::Crc64Nvme, &whole))
        );
    }

    #[tokio::test]
    async fn empty_integrity_headers_are_treated_as_absent() {
        let svc = make_service();
        seed_bucket(&svc, "ck");
        put(
            &svc,
            "ck",
            "k",
            b"x",
            &[
                ("content-md5", ""),
                ("x-amz-checksum-sha256", ""),
                ("x-amz-sdk-checksum-algorithm", ""),
            ],
        )
        .await
        .unwrap();
        assert_eq!(
            object(&svc, "ck", "k").checksum_algorithm,
            Some(ChecksumAlgorithm::Crc64Nvme)
        );
    }

    #[tokio::test]
    async fn buffered_aws_chunked_body_is_decoded_and_its_trailer_verified() {
        use fakecloud_core::service::AwsService;
        let svc = make_service();
        seed_bucket(&svc, "hb");
        let xml = r#"<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>"#;
        let frame = |value: &str| {
            format!(
                "{:x}\r\n{xml}\r\n0\r\nx-amz-checksum-crc32:{value}\r\n\r\n",
                xml.len()
            )
        };
        let headers = [
            ("content-encoding", "aws-chunked"),
            ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
            ("x-amz-trailer", "x-amz-checksum-crc32"),
        ];
        let wrong = checksum(ChecksumAlgorithm::Crc32, b"other");
        let req = with_headers(
            make_request(
                Method::PUT,
                "/hb",
                &[("tagging", "")],
                frame(&wrong).as_bytes(),
            ),
            &headers,
        );
        assert_aws_err(svc.handle(req).await, "BadDigest");
        let good = checksum(ChecksumAlgorithm::Crc32, xml.as_bytes());
        let req = with_headers(
            make_request(
                Method::PUT,
                "/hb",
                &[("tagging", "")],
                frame(&good).as_bytes(),
            ),
            &headers,
        );
        svc.handle(req).await.unwrap();
        // The handler saw the decoded XML, not the framing.
        let tags = svc.state.read().default_ref().buckets["hb"].tags.clone();
        assert_eq!(tags.get("a").map(String::as_str), Some("b"));
    }

    #[test]
    fn copy_into_a_missing_bucket_is_no_such_bucket_before_the_algorithm_check() {
        let svc = make_service();
        seed_bucket(&svc, "src");
        seed_object(&svc, "src", "k", b"x");
        let req = with_headers(
            copy_request("ghost", "k", "src/k"),
            &[("x-amz-checksum-algorithm", "CRC16")],
        );
        assert_aws_err(svc.copy_object(ACCT, &req, "ghost", "k"), "NoSuchBucket");
        let req = with_headers(
            copy_request("src", "k2", "src/k"),
            &[("x-amz-checksum-algorithm", "CRC16")],
        );
        assert_aws_err(svc.copy_object(ACCT, &req, "src", "k2"), "InvalidRequest");
    }

    fn put_annotation_req(name: &str, body: &[u8], headers: &[(&str, &str)]) -> AwsRequest {
        with_headers(
            make_request(
                Method::PUT,
                "/an/k",
                &[("annotation", ""), ("annotationName", name)],
                body,
            ),
            headers,
        )
    }

    fn get_annotation(svc: &S3Service, name: &str, checksum_mode: bool) -> AwsResponse {
        let mut req = make_request(
            Method::GET,
            "/an/k",
            &[("annotation", ""), ("annotationName", name)],
            b"",
        );
        if checksum_mode {
            req = with_headers(req, &[("x-amz-checksum-mode", "ENABLED")]);
        }
        svc.get_object_annotation(ACCT, &req, "an", "k").unwrap()
    }

    #[test]
    fn annotation_checksums_round_trip_for_every_algorithm() {
        let svc = make_service();
        seed_bucket(&svc, "an");
        seed_object(&svc, "an", "k", b"object");
        let payload = b"reviewed: ok";
        for algo in ChecksumAlgorithm::ALL {
            let name = algo.lowercase_name();
            let value = checksum(algo, payload);
            let req = put_annotation_req(name, payload, &[(algo.header_name(), &value)]);
            let resp = svc.put_object_annotation(ACCT, &req, "an", "k").unwrap();
            assert_eq!(
                header(&resp, algo.header_name()),
                Some(value.clone()),
                "{algo}"
            );
            assert_eq!(
                header(&resp, "x-amz-checksum-type").as_deref(),
                Some("FULL_OBJECT")
            );

            let got = get_annotation(&svc, name, true);
            assert_eq!(got.body.expect_bytes(), &payload[..]);
            assert_eq!(
                header(&got, algo.header_name()),
                Some(value.clone()),
                "{algo}"
            );
            assert_eq!(
                header(&got, "x-amz-checksum-type").as_deref(),
                Some("FULL_OBJECT")
            );
            let plain = get_annotation(&svc, name, false);
            assert!(plain.headers.get(algo.header_name()).is_none());

            let wrong = checksum(algo, b"other");
            let req = put_annotation_req("bad", payload, &[(algo.header_name(), &wrong)]);
            assert_aws_err(
                svc.put_object_annotation(ACCT, &req, "an", "k"),
                "BadDigest",
            );
        }
        // No algorithm named: the CRC64NVME default.
        let req = put_annotation_req("default", payload, &[]);
        let resp = svc.put_object_annotation(ACCT, &req, "an", "k").unwrap();
        assert_eq!(
            header(&resp, "x-amz-checksum-crc64nvme"),
            Some(checksum(ChecksumAlgorithm::Crc64Nvme, payload))
        );
        let obj = object(&svc, "an", "k");
        assert!(!obj.annotations.contains_key("bad"));
        assert_eq!(
            obj.annotations["default"].checksum_algorithm,
            Some(ChecksumAlgorithm::Crc64Nvme)
        );
    }

    #[test]
    fn annotation_aws_chunked_payload_is_decoded_and_trailer_verified() {
        let svc = make_service();
        seed_bucket(&svc, "an");
        seed_object(&svc, "an", "k", b"object");
        let payload = b"streamed annotation";
        let good = checksum(ChecksumAlgorithm::Sha256, payload);
        let framed = |value: &str| {
            format!(
                "{:x}\r\n{}\r\n0\r\nx-amz-checksum-sha256:{value}\r\n\r\n",
                payload.len(),
                std::str::from_utf8(payload).unwrap()
            )
        };
        let headers = [
            ("content-encoding", "aws-chunked"),
            ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
            ("x-amz-decoded-content-length", "19"),
            ("x-amz-trailer", "x-amz-checksum-sha256"),
            ("x-amz-sdk-checksum-algorithm", "SHA256"),
        ];
        let req = put_annotation_req("s", framed(&good).as_bytes(), &headers);
        let resp = svc.put_object_annotation(ACCT, &req, "an", "k").unwrap();
        assert_eq!(header(&resp, "x-amz-checksum-sha256"), Some(good.clone()));
        let got = get_annotation(&svc, "s", true);
        // The framing is not part of the stored payload.
        assert_eq!(got.body.expect_bytes(), &payload[..]);
        assert_eq!(header(&got, "content-length").as_deref(), Some("19"));
        assert_eq!(header(&got, "x-amz-checksum-sha256"), Some(good));

        let wrong = checksum(ChecksumAlgorithm::Sha256, b"x");
        let req = put_annotation_req("s2", framed(&wrong).as_bytes(), &headers);
        assert_aws_err(
            svc.put_object_annotation(ACCT, &req, "an", "k"),
            "BadDigest",
        );
    }

    #[test]
    fn annotation_payload_rules_and_conditional_write() {
        let svc = make_service();
        seed_bucket(&svc, "an");
        seed_object(&svc, "an", "k", b"object");
        assert_aws_err(
            svc.put_object_annotation(ACCT, &put_annotation_req("e", b"", &[]), "an", "k"),
            "InvalidRequest",
        );
        let big = vec![b'a'; 1024 * 1024 + 1];
        assert_aws_err(
            svc.put_object_annotation(ACCT, &put_annotation_req("b", &big, &[]), "an", "k"),
            "InvalidRequest",
        );
        assert_aws_err(
            svc.put_object_annotation(
                ACCT,
                &put_annotation_req("u", &[0xff, 0xfe], &[]),
                "an",
                "k",
            ),
            "UnsupportedMediaType",
        );
        let etag = object(&svc, "an", "k").etag;
        assert_aws_err(
            svc.put_object_annotation(
                ACCT,
                &put_annotation_req("c", b"x", &[("x-amz-object-if-match", "\"nope\"")]),
                "an",
                "k",
            ),
            "PreconditionFailed",
        );
        svc.put_object_annotation(
            ACCT,
            &put_annotation_req("c", b"x", &[("x-amz-object-if-match", &etag)]),
            "an",
            "k",
        )
        .unwrap();
    }

    #[test]
    fn annotation_list_wraps_entries_and_names_the_object_version() {
        let svc = make_service();
        seed_bucket(&svc, "an");
        seed_object(&svc, "an", "k", b"object");
        {
            let mut mas = svc.state.write();
            let obj = mas
                .default_mut()
                .buckets
                .get_mut("an")
                .unwrap()
                .objects
                .get_mut("k")
                .unwrap();
            obj.version_id = Some("v1".to_string());
        }
        let req = put_annotation_req("n", b"x", &[("x-amz-sdk-checksum-algorithm", "MD5")]);
        let resp = svc.put_object_annotation(ACCT, &req, "an", "k").unwrap();
        assert_eq!(
            header(&resp, "x-amz-object-version-id").as_deref(),
            Some("v1")
        );
        let list = make_request(Method::GET, "/an/k", &[("annotation", "")], b"");
        let resp = svc.list_object_annotations(ACCT, &list, "an", "k").unwrap();
        let text = body_text(&resp);
        assert!(
            text.contains("<Annotations><AnnotationEntry><AnnotationName>n</AnnotationName>"),
            "{text}"
        );
        assert!(
            text.contains("<ChecksumAlgorithm>MD5</ChecksumAlgorithm>"),
            "{text}"
        );
        assert_eq!(
            header(&resp, "x-amz-object-version-id").as_deref(),
            Some("v1")
        );
    }

    #[tokio::test]
    async fn buffered_request_bodies_are_verified_through_the_handler() {
        use fakecloud_core::service::AwsService;
        let svc = make_service();
        seed_bucket(&svc, "hb");
        let xml = r#"<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>"#;
        let tag_req = |headers: &[(&str, &str)]| {
            with_headers(
                make_request(Method::PUT, "/hb", &[("tagging", "")], xml.as_bytes()),
                headers,
            )
        };
        let wrong_md5 = content_md5(b"not the body");
        assert_aws_err(
            svc.handle(tag_req(&[("content-md5", &wrong_md5)])).await,
            "BadDigest",
        );
        let wrong_crc = checksum(ChecksumAlgorithm::Crc32, b"not the body");
        assert_aws_err(
            svc.handle(tag_req(&[("x-amz-checksum-crc32", &wrong_crc)]))
                .await,
            "BadDigest",
        );
        let good_md5 = content_md5(xml.as_bytes());
        let good_crc = checksum(ChecksumAlgorithm::Crc32, xml.as_bytes());
        svc.handle(tag_req(&[
            ("content-md5", &good_md5),
            ("x-amz-checksum-crc32", &good_crc),
        ]))
        .await
        .unwrap();
        // A bucket that does not exist still answers NoSuchBucket.
        let missing = with_headers(
            make_request(Method::PUT, "/nobucket", &[("tagging", "")], xml.as_bytes()),
            &[("content-md5", &wrong_md5)],
        );
        assert_aws_err(svc.handle(missing).await, "NoSuchBucket");
    }
}

/// Upload `body` to `bucket/key` as a one-part multipart upload and complete
/// it, returning the completion response.
async fn complete_single_part_mpu(
    svc: &S3Service,
    bucket: &str,
    key: &str,
    body: &[u8],
) -> fakecloud_core::service::AwsResponse {
    let path = format!("/{bucket}/{key}");
    let init_req = make_request(Method::POST, &path, &[("uploads", "")], b"");
    let resp = svc
        .create_multipart_upload("123456789012", &init_req, bucket, key)
        .unwrap();
    let xml = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let start = xml.find("<UploadId>").unwrap() + "<UploadId>".len();
    let end = xml.find("</UploadId>").unwrap();
    let upload_id = xml[start..end].to_string();

    let part_req = make_request(Method::PUT, &path, &[("partNumber", "1")], body);
    let r = svc
        .upload_part("123456789012", &part_req, bucket, key, &upload_id, 1)
        .await
        .unwrap();
    let etag = r.headers.get("etag").unwrap().to_str().unwrap().to_string();
    let complete_xml = format!(
        r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"#,
    );
    let complete_req = make_request(
        Method::POST,
        &path,
        &[("uploadId", &upload_id)],
        complete_xml.as_bytes(),
    );
    svc.complete_multipart_upload("123456789012", &complete_req, bucket, key, &upload_id)
        .unwrap()
}

/// A completed upload is recorded like any other write: on an enabled bucket
/// it is a new version (the object written before versioning kept as the
/// null version) whose id the response reports, and on a suspended bucket it
/// takes over the null version, keeping the enabled-era version.
#[tokio::test]
async fn mpu_complete_follows_enabled_then_suspended_versioning() {
    let svc = make_service();
    seed_bucket(&svc, "ver");
    seed_object(&svc, "ver", "k", b"pre-versioning");
    let set_versioning = |status: &str| {
        let mut mas = svc.state.write();
        mas.default_mut().buckets.get_mut("ver").unwrap().versioning = Some(status.to_string());
    };
    let read = |body: &fakecloud_persistence::BodyRef| {
        svc.state.read().default_ref().read_body(body).unwrap()
    };

    set_versioning("Enabled");
    let resp = complete_single_part_mpu(&svc, "ver", "k", b"mpu-enabled").await;
    let vid = resp
        .headers
        .get("x-amz-version-id")
        .expect("version id header")
        .to_str()
        .unwrap()
        .to_string();
    assert_ne!(vid, "null");
    {
        let mas = svc.state.read();
        let b = mas.default_ref().buckets.get("ver").unwrap();
        let versions = &b.object_versions["k"];
        assert_eq!(versions.len(), 2, "{versions:?}");
        assert_eq!(versions[0].version_id.as_deref(), Some("null"));
        assert_eq!(versions[1].version_id.as_deref(), Some(vid.as_str()));
        assert_eq!(b.objects["k"].version_id.as_deref(), Some(vid.as_str()));
        assert!(!b.multipart_uploads.values().any(|u| u.key == "k"));
    }
    let (null_body, new_body) = {
        let mas = svc.state.read();
        let versions = &mas.default_ref().buckets["ver"].object_versions["k"];
        (versions[0].body.clone(), versions[1].body.clone())
    };
    assert_eq!(read(&null_body), Bytes::from_static(b"pre-versioning"));
    assert_eq!(read(&new_body), Bytes::from_static(b"mpu-enabled"));

    set_versioning("Suspended");
    let resp = complete_single_part_mpu(&svc, "ver", "k", b"mpu-suspended").await;
    assert!(resp.headers.get("x-amz-version-id").is_none());
    let (current, versions) = {
        let mas = svc.state.read();
        let b = mas.default_ref().buckets.get("ver").unwrap();
        (b.objects["k"].clone(), b.object_versions["k"].clone())
    };
    assert_eq!(current.version_id.as_deref(), Some("null"));
    assert_eq!(read(&current.body), Bytes::from_static(b"mpu-suspended"));
    // The suspended completion replaced the old null version and kept the
    // enabled-era one.
    assert_eq!(versions.len(), 2, "{versions:?}");
    assert_eq!(versions[0].version_id.as_deref(), Some(vid.as_str()));
    assert_eq!(versions[1].version_id.as_deref(), Some("null"));
    assert_eq!(
        read(&versions[1].body),
        Bytes::from_static(b"mpu-suspended")
    );
}

/// Every kind of new version -- an object another service delivers, a copy,
/// a completed multipart upload and a replica -- takes the destination
/// bucket's default retention through the one commit path, measured from the
/// commit.
#[tokio::test]
async fn every_new_version_kind_gets_the_destination_default_retention() {
    let svc = make_service();
    seed_bucket(&svc, "src");
    seed_bucket(&svc, "locked");
    {
        let mut mas = svc.state.write();
        let buckets = &mut mas.default_mut().buckets;
        let src = buckets.get_mut("src").unwrap();
        src.versioning = Some("Enabled".to_string());
        src.replication_config = Some(
            "<ReplicationConfiguration><Rule><Status>Enabled</Status>\
             <Filter><Prefix>rep</Prefix></Filter>\
             <Destination><Bucket>arn:aws:s3:::locked</Bucket></Destination>\
             </Rule></ReplicationConfiguration>"
                .to_string(),
        );
        let locked = buckets.get_mut("locked").unwrap();
        locked.versioning = Some("Enabled".to_string());
        locked.object_lock_config = Some(
            "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
             <Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>2</Days>\
             </DefaultRetention></Rule></ObjectLockConfiguration>"
                .to_string(),
        );
    }
    let before = Utc::now();

    // Replica: a lock-less source version replicated into the lock bucket.
    let req = make_request(Method::PUT, "/src/rep", &[], b"replicated");
    svc.put_object("123456789012", &req, "src", "rep")
        .await
        .unwrap();
    // Copy.
    let mut req = make_request(Method::PUT, "/locked/copy", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "src/rep".parse().unwrap());
    svc.copy_object("123456789012", &req, "locked", "copy")
        .unwrap();
    // Completed multipart upload.
    complete_single_part_mpu(&svc, "locked", "mpu", b"assembled").await;
    // Object delivered by another service.
    crate::delivery::put_object_via_store(
        &svc.state,
        Some(&*svc.store),
        "123456789012",
        "locked",
        "delivered",
        b"delivered".to_vec(),
        None,
    )
    .unwrap();
    let after = Utc::now();

    let mas = svc.state.read();
    let locked = mas.default_ref().buckets.get("locked").unwrap();
    for key in ["rep", "copy", "mpu", "delivered"] {
        let obj = locked
            .objects
            .get(key)
            .unwrap_or_else(|| panic!("{key} missing"));
        assert_eq!(obj.lock_mode.as_deref(), Some("GOVERNANCE"), "{key}");
        let until = obj.lock_retain_until.expect(key);
        assert!(until >= before + chrono::Duration::days(2), "{key}");
        assert!(until <= after + chrono::Duration::days(2), "{key}");
        // The history entry carries the same lock.
        let version = locked.object_versions[key].last().unwrap();
        assert_eq!(version.lock_retain_until, Some(until), "{key}");
    }
    // The source bucket has no lock configuration, so its object has none.
    assert_eq!(
        mas.default_ref().buckets["src"].objects["rep"].lock_mode,
        None
    );
}

/// Re-replicating an older source version (v1) after a newer one (v2) has
/// already landed rewrites v1 in place: the history keeps its order, v1 keeps
/// its original LastModified, and v2 stays the current object.
#[test]
fn re_replicating_an_older_version_keeps_the_newer_one_current() {
    use crate::state::S3Bucket;

    let mut accounts =
        fakecloud_core::multi_account::MultiAccountState::<crate::state::S3State>::new(
            "123456789012",
            "us-east-1",
            "",
        );
    let state = accounts.get_or_create("123456789012");
    state.buckets.insert(
        "source".to_string(),
        replication_source(
            "<ReplicationConfiguration>\
             <Rule><Status>Enabled</Status>\
             <Filter><Prefix></Prefix></Filter>\
             <Destination><Bucket>arn:aws:s3:::destination</Bucket></Destination>\
             </Rule></ReplicationConfiguration>",
            "owner",
        ),
    );
    let mut dest = S3Bucket::new("destination", "us-east-1", "owner");
    dest.versioning = Some("Enabled".to_string());
    state.buckets.insert("destination".to_string(), dest);
    let set_source =
        |accounts: &mut fakecloud_core::multi_account::MultiAccountState<crate::state::S3State>,
         vid: &str,
         body: &'static [u8]| {
            let obj = accounts
                .get_mut("123456789012")
                .unwrap()
                .buckets
                .get_mut("source")
                .unwrap()
                .objects
                .get_mut("test-key")
                .unwrap();
            obj.version_id = Some(vid.to_string());
            obj.body = crate::state::memory_body(Bytes::from_static(body));
        };

    // v1 then v2 replicate in order.
    replicate(&mut accounts, "123456789012");
    let v1_written = accounts.get("123456789012").unwrap().buckets["destination"].objects
        ["test-key"]
        .last_modified;
    set_source(&mut accounts, "v2", b"second");
    replicate(&mut accounts, "123456789012");

    // v1 is replicated again, later.
    set_source(&mut accounts, "v1", b"hello-again");
    replicate(&mut accounts, "123456789012");

    let state = accounts.get("123456789012").unwrap();
    let dest = &state.buckets["destination"];
    let ids: Vec<_> = dest.object_versions["test-key"]
        .iter()
        .map(|o| o.version_id.clone().unwrap())
        .collect();
    assert_eq!(ids, vec!["v1".to_string(), "v2".to_string()]);
    let current = &dest.objects["test-key"];
    assert_eq!(current.version_id.as_deref(), Some("v2"));
    assert_eq!(
        state.read_body(&current.body).unwrap(),
        Bytes::from_static(b"second")
    );
    let v1 = &dest.object_versions["test-key"][0];
    assert_eq!(v1.last_modified, v1_written);
    assert_eq!(
        state.read_body(&v1.body).unwrap(),
        Bytes::from_static(b"hello-again")
    );
}

// ---- Replication across every write path ----

/// `src` (versioning-enabled, replicating with `config`) and versioned
/// destination buckets `dests`, seeded on `svc`.
fn seed_replication(svc: &S3Service, config: &str, dests: &[&str]) {
    let mut mas = svc.state.write();
    let state = mas.default_mut();
    let mut src = S3Bucket::new("src", "us-east-1", "owner");
    src.versioning = Some("Enabled".to_string());
    src.replication_config = Some(normalize_replication_xml(config));
    state.buckets.insert("src".to_string(), src);
    for dest in dests {
        state
            .buckets
            .insert(dest.to_string(), versioned_bucket(dest, "owner"));
    }
}

fn bucket_of(svc: &S3Service, name: &str) -> S3Bucket {
    svc.state.read().default_ref().buckets[name].clone()
}

const REPLICATE_ALL_TO_DST: &str = "<ReplicationConfiguration><Role>arn:aws:iam::123456789012:role/r</Role>\
     <Rule><ID>all</ID><Priority>1</Priority><Status>Enabled</Status><Filter><Prefix></Prefix></Filter>\
     <DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>\
     <Destination><Bucket>arn:aws:s3:::dst</Bucket></Destination></Rule>\
     </ReplicationConfiguration>";

/// CompleteMultipartUpload replicates the completed object exactly as
/// PutObject does: the replica keeps the version id, the multipart ETag, the
/// part layout and checksum, the source reports COMPLETED.
#[tokio::test]
async fn complete_multipart_upload_replicates_the_object() {
    let svc = make_service();
    seed_replication(&svc, REPLICATE_ALL_TO_DST, &["dst"]);

    let mut init = make_request(Method::POST, "/src/big.bin", &[("uploads", "")], b"");
    init.headers
        .insert("x-amz-meta-origin", http::HeaderValue::from_static("mpu"));
    let resp = svc
        .create_multipart_upload("123456789012", &init, "src", "big.bin")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let start = body.find("<UploadId>").unwrap() + "<UploadId>".len();
    let upload_id = body[start..body.find("</UploadId>").unwrap()].to_string();
    let part1 = vec![b'p'; 5 * 1024 * 1024];
    let part2 = b"tail".to_vec();
    let mut etags = Vec::new();
    for (n, data) in [(1i64, &part1), (2i64, &part2)] {
        let req = make_request(
            Method::PUT,
            "/src/big.bin",
            &[("partNumber", &n.to_string())],
            data,
        );
        let r = svc
            .upload_part("123456789012", &req, "src", "big.bin", &upload_id, n)
            .await
            .unwrap();
        etags.push(r.headers["etag"].to_str().unwrap().to_string());
    }
    let xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        etags[0], etags[1]
    );
    let req = make_request(
        Method::POST,
        "/src/big.bin",
        &[("uploadId", &upload_id)],
        xml.as_bytes(),
    );
    let resp = svc
        .complete_multipart_upload("123456789012", &req, "src", "big.bin", &upload_id)
        .unwrap();
    let version = resp.headers["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_string();

    let src = bucket_of(&svc, "src");
    let source = &src.objects["big.bin"];
    assert_eq!(source.replication_status.as_deref(), Some("COMPLETED"));
    let dst = bucket_of(&svc, "dst");
    let replica = &dst.objects["big.bin"];
    assert_eq!(replica.replication_status.as_deref(), Some("REPLICA"));
    assert_eq!(replica.version_id.as_deref(), Some(version.as_str()));
    assert_eq!(replica.etag, source.etag);
    assert!(replica.etag.ends_with("-2"), "{}", replica.etag);
    assert_eq!(replica.parts_count, Some(2));
    assert_eq!(replica.part_sizes, source.part_sizes);
    assert_eq!(replica.checksum_value, source.checksum_value);
    assert_eq!(
        replica.metadata.get("origin").map(String::as_str),
        Some("mpu")
    );
    assert_eq!(dst.object_versions["big.bin"].len(), 1);
    let state = svc.state.read();
    let bytes = state.default_ref().read_body(&replica.body).unwrap();
    assert_eq!(bytes.len(), part1.len() + part2.len());
    assert!(bytes.ends_with(b"tail"));
}

/// Two enabled rules selecting the same object for the same destination
/// produce one replica (from the higher-priority rule), not two versions.
#[tokio::test]
async fn overlapping_rules_produce_a_single_replica() {
    let svc = make_service();
    seed_replication(
        &svc,
        "<ReplicationConfiguration><Role>r</Role>\
         <Rule><ID>low</ID><Priority>1</Priority><Status>Enabled</Status>\
         <Filter><Prefix>star</Prefix></Filter>\
         <DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication>\
         <Destination><Bucket>arn:aws:s3:::dst</Bucket></Destination></Rule>\
         <Rule><ID>high</ID><Priority>2</Priority><Status>Enabled</Status>\
         <Filter><Prefix>starship</Prefix></Filter>\
         <DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication>\
         <Destination><Bucket>arn:aws:s3:::dst</Bucket>\
         <StorageClass>STANDARD_IA</StorageClass></Destination></Rule>\
         </ReplicationConfiguration>",
        &["dst"],
    );
    let req = make_request(Method::PUT, "/src/starship-x", &[], b"payload");
    svc.put_object("123456789012", &req, "src", "starship-x")
        .await
        .unwrap();

    let dst = bucket_of(&svc, "dst");
    assert_eq!(dst.object_versions["starship-x"].len(), 1);
    // The higher-priority rule's destination settings were the ones applied.
    assert_eq!(dst.objects["starship-x"].storage_class, "STANDARD_IA");
    // Without a destination storage class the replica keeps the source's.
    let req = make_request(Method::PUT, "/src/star-y", &[], b"payload");
    svc.put_object("123456789012", &req, "src", "star-y")
        .await
        .unwrap();
    assert_eq!(
        bucket_of(&svc, "dst").objects["star-y"].storage_class,
        "STANDARD"
    );
}

/// PutBucketReplication refuses two rules with the same priority and a
/// tag-based rule that replicates delete markers.
#[test]
fn put_bucket_replication_validates_rules() {
    let svc = make_service();
    seed_replication(&svc, REPLICATE_ALL_TO_DST, &["dst"]);
    let dup = b"<ReplicationConfiguration><Role>r</Role>\
        <Rule><Priority>1</Priority><Status>Enabled</Status><Filter><Prefix>a</Prefix></Filter>\
        <DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication>\
        <Destination><Bucket>arn:aws:s3:::dst</Bucket></Destination></Rule>\
        <Rule><Priority>1</Priority><Status>Enabled</Status><Filter><Prefix>b</Prefix></Filter>\
        <DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication>\
        <Destination><Bucket>arn:aws:s3:::dst</Bucket></Destination></Rule>\
        </ReplicationConfiguration>";
    let req = make_request(Method::PUT, "/src", &[("replication", "")], dup);
    assert_aws_err(
        svc.put_bucket_replication("123456789012", &req, "src"),
        "InvalidRequest",
    );
    let tag_dmr = b"<ReplicationConfiguration><Role>r</Role>\
        <Rule><Priority>1</Priority><Status>Enabled</Status>\
        <Filter><Tag><Key>k</Key><Value>v</Value></Tag></Filter>\
        <DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>\
        <Destination><Bucket>arn:aws:s3:::dst</Bucket></Destination></Rule>\
        </ReplicationConfiguration>";
    let req = make_request(Method::PUT, "/src", &[("replication", "")], tag_dmr);
    assert_aws_err(
        svc.put_bucket_replication("123456789012", &req, "src"),
        "InvalidRequest",
    );
    // The stored configuration is untouched by the refused requests.
    assert!(bucket_of(&svc, "src")
        .replication_config
        .unwrap()
        .contains("<ID>all</ID>"));
}

/// Versioning cannot be suspended on a replication source, so no write to it
/// can produce a null version to replicate.
#[test]
fn versioning_cannot_be_suspended_on_a_replication_source() {
    let svc = make_service();
    seed_replication(&svc, REPLICATE_ALL_TO_DST, &["dst"]);
    let body = b"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>";
    let req = make_request(Method::PUT, "/src", &[("versioning", "")], body);
    assert_aws_err(
        svc.put_bucket_versioning("123456789012", &req, "src"),
        "InvalidBucketState",
    );
    assert_eq!(
        bucket_of(&svc, "src").versioning.as_deref(),
        Some("Enabled")
    );
    // A destination can still be suspended (its replications then fail).
    let req = make_request(Method::PUT, "/dst", &[("versioning", "")], body);
    svc.put_bucket_versioning("123456789012", &req, "dst")
        .unwrap();
}

/// An object written before versioning was enabled (the null version) is
/// never replicated: the new version written on top of it is, and the
/// preserved null version stays only in the source.
#[tokio::test]
async fn null_versions_are_not_replicated() {
    let svc = make_service();
    seed_replication(&svc, REPLICATE_ALL_TO_DST, &["dst"]);
    // A pre-versioning object at the key.
    seed_object(&svc, "src", "k", b"old");
    let req = make_request(Method::PUT, "/src/k", &[], b"new");
    svc.put_object("123456789012", &req, "src", "k")
        .await
        .unwrap();

    let src = bucket_of(&svc, "src");
    let versions = &src.object_versions["k"];
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].version_id.as_deref(), Some("null"));
    assert_eq!(versions[0].replication_status, None);
    let dst = bucket_of(&svc, "dst");
    let replicas = &dst.object_versions["k"];
    assert_eq!(replicas.len(), 1, "only the new version: {replicas:?}");
    assert_eq!(replicas[0].version_id, versions[1].version_id);
}

/// DeleteObject on a replication source replicates the delete marker (same
/// version id) when the rule enables delete marker replication; a versioned
/// DELETE is never replicated.
#[tokio::test]
async fn delete_marker_is_replicated_when_enabled() {
    for enabled in [true, false] {
        let svc = make_service();
        let config = if enabled {
            REPLICATE_ALL_TO_DST.to_string()
        } else {
            REPLICATE_ALL_TO_DST.replace(
                "<DeleteMarkerReplication><Status>Enabled</Status>",
                "<DeleteMarkerReplication><Status>Disabled</Status>",
            )
        };
        seed_replication(&svc, &config, &["dst"]);
        let req = make_request(Method::PUT, "/src/k", &[], b"data");
        let put = svc
            .put_object("123456789012", &req, "src", "k")
            .await
            .unwrap();
        let object_version = put.headers["x-amz-version-id"]
            .to_str()
            .unwrap()
            .to_string();

        let req = make_request(Method::DELETE, "/src/k", &[], b"");
        let resp = svc.delete_object("123456789012", &req, "src", "k").unwrap();
        let marker = resp.headers["x-amz-version-id"]
            .to_str()
            .unwrap()
            .to_string();

        let dst = bucket_of(&svc, "dst");
        let versions = &dst.object_versions["k"];
        if enabled {
            assert_eq!(versions.len(), 2, "{versions:?}");
            assert!(versions[1].is_delete_marker);
            assert_eq!(versions[1].version_id.as_deref(), Some(marker.as_str()));
            assert!(dst.objects["k"].is_delete_marker);
        } else {
            assert_eq!(versions.len(), 1, "{versions:?}");
            assert!(!dst.objects["k"].is_delete_marker);
        }

        // Deleting a specific version in the source leaves the replica alone.
        let req = make_request(
            Method::DELETE,
            "/src/k",
            &[("versionId", object_version.as_str())],
            b"",
        );
        svc.delete_object("123456789012", &req, "src", "k").unwrap();
        assert!(bucket_of(&svc, "dst").object_versions["k"]
            .iter()
            .any(|v| v.version_id.as_deref() == Some(object_version.as_str())));
    }
}

/// DeleteObjects replicates the delete markers it creates, like DeleteObject.
#[test]
fn delete_objects_replicates_delete_markers() {
    let svc = make_service();
    seed_replication(&svc, REPLICATE_ALL_TO_DST, &["dst"]);
    let body = b"<Delete><Object><Key>a</Key></Object><Object><Key>b</Key></Object></Delete>";
    let req = make_request(Method::POST, "/src", &[("delete", "")], body);
    svc.delete_objects("123456789012", &req, "src").unwrap();
    let dst = bucket_of(&svc, "dst");
    for key in ["a", "b"] {
        assert!(dst.objects[key].is_delete_marker, "{key}");
        assert_eq!(
            dst.objects[key].version_id,
            bucket_of(&svc, "src").objects[key].version_id
        );
    }
}

/// A KMS hook whose envelope is readable: `enc:<key>:<plaintext>`.
struct ReadableKms;

impl fakecloud_core::delivery::KmsHook for ReadableKms {
    fn encrypt(
        &self,
        _account_id: &str,
        _region: &str,
        key_id: &str,
        plaintext: &[u8],
        _service_principal: &str,
        _encryption_context: HashMap<String, String>,
    ) -> Result<String, String> {
        Ok(format!(
            "enc:{key_id}:{}",
            String::from_utf8_lossy(plaintext)
        ))
    }

    fn decrypt(
        &self,
        _account_id: &str,
        ciphertext_b64: &str,
        _service_principal: &str,
        _encryption_context: HashMap<String, String>,
    ) -> Result<Vec<u8>, String> {
        let rest = ciphertext_b64
            .strip_prefix("enc:")
            .ok_or("not an envelope")?;
        let (_, plaintext) = rest.split_once(':').ok_or("not an envelope")?;
        Ok(plaintext.as_bytes().to_vec())
    }
}

/// An SSE-KMS object is replicated only by a rule that opts in to
/// SSE-KMS objects, and the replica is encrypted under the destination's
/// ReplicaKmsKeyID.
#[test]
fn sse_kms_objects_need_an_opted_in_rule_and_get_the_replica_key() {
    for opted_in in [false, true] {
        let ssc = if opted_in {
            "<SourceSelectionCriteria><SseKmsEncryptedObjects><Status>Enabled</Status>\
             </SseKmsEncryptedObjects></SourceSelectionCriteria>"
        } else {
            ""
        };
        let config = format!(
            "<ReplicationConfiguration><Rule><Priority>1</Priority><Status>Enabled</Status>\
             <Filter><Prefix></Prefix></Filter>{ssc}\
             <Destination><Bucket>arn:aws:s3:::destination</Bucket>\
             <EncryptionConfiguration><ReplicaKmsKeyID>replica-key</ReplicaKmsKeyID>\
             </EncryptionConfiguration></Destination></Rule></ReplicationConfiguration>"
        );
        let mut accounts =
            fakecloud_core::multi_account::MultiAccountState::<crate::state::S3State>::new(
                "123456789012",
                "us-east-1",
                "",
            );
        let state = accounts.get_or_create("123456789012");
        let mut src = replication_source(&config, "owner");
        let obj = src.objects.get_mut("test-key").unwrap();
        obj.sse_algorithm = Some("aws:kms".to_string());
        obj.sse_kms_key_id = Some("source-key".to_string());
        obj.body = crate::state::memory_body(Bytes::from_static(b"enc:source-key:hello"));
        state.buckets.insert("source".to_string(), src);
        state.buckets.insert(
            "destination".to_string(),
            versioned_bucket("destination", "owner"),
        );

        let store = fakecloud_persistence::MemoryS3Store::new();
        let effects = crate::side_effects::S3SideEffects::new(
            Arc::new(DeliveryBus::new()),
            Some(Arc::new(ReadableKms)),
            fakecloud_core::auth::IamMode::Off,
        );
        crate::service::replication::replicate_through_store(
            &mut accounts,
            "123456789012",
            Some(&store),
            "source",
            "test-key",
            Some(&effects),
        );

        let state = accounts.get("123456789012").unwrap();
        let replica = state.buckets["destination"].objects.get("test-key");
        if !opted_in {
            assert!(replica.is_none());
            assert_eq!(
                state.buckets["source"].objects["test-key"].replication_status,
                None
            );
            continue;
        }
        let replica = replica.expect("opted-in rule replicates");
        assert_eq!(replica.sse_kms_key_id.as_deref(), Some("replica-key"));
        assert_eq!(
            state.read_body(&replica.body).unwrap(),
            Bytes::from_static(b"enc:replica-key:hello")
        );
    }
}

// ---- Multipart uploads and encryption ----

fn kms_service() -> S3Service {
    let state: SharedS3State = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    S3Service::new(state, Arc::new(DeliveryBus::new())).with_kms_hook(Arc::new(ReadableKms))
}

/// Create, upload one part and complete `bucket/key`; returns the three
/// responses' headers.
async fn single_part_upload(
    svc: &S3Service,
    bucket: &str,
    key: &str,
    create_headers: &[(&'static str, &'static str)],
    body: &[u8],
) -> [HeaderMap; 3] {
    let path = format!("/{bucket}/{key}");
    let mut init = make_request(Method::POST, &path, &[("uploads", "")], b"");
    for (k, v) in create_headers {
        init.headers.insert(*k, http::HeaderValue::from_static(v));
    }
    let created = svc
        .create_multipart_upload("123456789012", &init, bucket, key)
        .unwrap();
    let text = std::str::from_utf8(created.body.expect_bytes()).unwrap();
    let start = text.find("<UploadId>").unwrap() + "<UploadId>".len();
    let upload_id = text[start..text.find("</UploadId>").unwrap()].to_string();
    let req = make_request(Method::PUT, &path, &[("partNumber", "1")], body);
    let part = svc
        .upload_part("123456789012", &req, bucket, key, &upload_id, 1)
        .await
        .unwrap();
    let xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part>\
         </CompleteMultipartUpload>",
        part.headers["etag"].to_str().unwrap()
    );
    let req = make_request(
        Method::POST,
        &path,
        &[("uploadId", &upload_id)],
        xml.as_bytes(),
    );
    let done = svc
        .complete_multipart_upload("123456789012", &req, bucket, key, &upload_id)
        .unwrap();
    [created.headers, part.headers, done.headers]
}

/// A multipart upload with no SSE headers takes the bucket's default SSE-KMS
/// encryption: every response reports it, and the completed object is stored
/// as one envelope over the plaintext, which GetObject decrypts.
#[tokio::test]
async fn multipart_upload_takes_default_sse_kms_and_stores_an_envelope() {
    let svc = kms_service();
    seed_bucket(&svc, "enc");
    svc.state
        .write()
        .default_mut()
        .buckets
        .get_mut("enc")
        .unwrap()
        .encryption_config = Some(
        "<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault>\
         <SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>k1</KMSMasterKeyID>\
         </ApplyServerSideEncryptionByDefault><BucketKeyEnabled>true</BucketKeyEnabled>\
         </Rule></ServerSideEncryptionConfiguration>"
            .to_string(),
    );
    let responses = single_part_upload(&svc, "enc", "obj", &[], b"multipart-plaintext").await;
    for headers in &responses {
        assert_eq!(headers["x-amz-server-side-encryption"], "aws:kms");
        assert_eq!(headers["x-amz-server-side-encryption-aws-kms-key-id"], "k1");
        assert_eq!(
            headers["x-amz-server-side-encryption-bucket-key-enabled"],
            "true"
        );
    }

    let obj = bucket_of(&svc, "enc").objects["obj"].clone();
    assert_eq!(obj.sse_algorithm.as_deref(), Some("aws:kms"));
    assert_eq!(obj.bucket_key_enabled, Some(true));
    assert_eq!(obj.size, 19);
    let stored = svc.state.read().default_ref().read_body(&obj.body).unwrap();
    assert_eq!(&stored[..], b"enc:k1:multipart-plaintext");

    let req = make_request(Method::GET, "/enc/obj", &[], b"");
    let got = svc.get_object("123456789012", &req, "enc", "obj").unwrap();
    assert_eq!(got.body.expect_bytes(), b"multipart-plaintext");
}

/// Without encryption headers or a bucket default, every multipart response
/// reports SSE-S3 and the body is stored as uploaded.
#[tokio::test]
async fn multipart_upload_without_encryption_reports_sse_s3() {
    let svc = kms_service();
    seed_bucket(&svc, "plain");
    let responses = single_part_upload(&svc, "plain", "obj", &[], b"data").await;
    for headers in &responses {
        assert_eq!(headers["x-amz-server-side-encryption"], "AES256");
        assert!(!headers.contains_key("x-amz-server-side-encryption-aws-kms-key-id"));
    }
    let obj = bucket_of(&svc, "plain").objects["obj"].clone();
    let stored = svc.state.read().default_ref().read_body(&obj.body).unwrap();
    assert_eq!(&stored[..], b"data");
}

/// UploadPartCopy from an SSE-KMS source copies its plaintext, and an
/// SSE-KMS upload stores the result encrypted under its own key.
#[tokio::test]
async fn upload_part_copy_reads_the_plaintext_of_an_sse_kms_source() {
    let svc = kms_service();
    seed_bucket(&svc, "b");
    let responses = single_part_upload(
        &svc,
        "b",
        "src",
        &[
            ("x-amz-server-side-encryption", "aws:kms"),
            ("x-amz-server-side-encryption-aws-kms-key-id", "src-key"),
        ],
        b"0123456789",
    )
    .await;
    assert_eq!(responses[2]["x-amz-server-side-encryption"], "aws:kms");

    let mut init = make_request(Method::POST, "/b/dst", &[("uploads", "")], b"");
    init.headers.insert(
        "x-amz-server-side-encryption",
        http::HeaderValue::from_static("aws:kms"),
    );
    init.headers.insert(
        "x-amz-server-side-encryption-aws-kms-key-id",
        http::HeaderValue::from_static("dst-key"),
    );
    let created = svc
        .create_multipart_upload("123456789012", &init, "b", "dst")
        .unwrap();
    let text = std::str::from_utf8(created.body.expect_bytes()).unwrap();
    let start = text.find("<UploadId>").unwrap() + "<UploadId>".len();
    let upload_id = text[start..text.find("</UploadId>").unwrap()].to_string();
    let mut req = make_request(
        Method::PUT,
        "/b/dst",
        &[("partNumber", "1"), ("uploadId", &upload_id)],
        b"",
    );
    req.headers
        .insert("x-amz-copy-source", http::HeaderValue::from_static("b/src"));
    req.headers.insert(
        "x-amz-copy-source-range",
        http::HeaderValue::from_static("bytes=2-5"),
    );
    let copied = svc
        .upload_part_copy("123456789012", &req, "b", "dst", &upload_id, 1)
        .unwrap();
    assert_eq!(copied.headers["x-amz-server-side-encryption"], "aws:kms");
    let text = std::str::from_utf8(copied.body.expect_bytes()).unwrap();
    let start = text.find("<ETag>").unwrap() + "<ETag>".len();
    let etag = text[start..text.find("</ETag>").unwrap()].replace("&quot;", "\"");
    let xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part>\
         </CompleteMultipartUpload>"
    );
    let req = make_request(
        Method::POST,
        "/b/dst",
        &[("uploadId", &upload_id)],
        xml.as_bytes(),
    );
    svc.complete_multipart_upload("123456789012", &req, "b", "dst", &upload_id)
        .unwrap();

    let obj = bucket_of(&svc, "b").objects["dst"].clone();
    let stored = svc.state.read().default_ref().read_body(&obj.body).unwrap();
    assert_eq!(&stored[..], b"enc:dst-key:2345");
}

/// A DeleteObjects batch stopped by a persistence failure still replicates
/// the delete markers it committed before the failure; the failed key leaves
/// no marker anywhere.
#[test]
fn delete_objects_replicates_markers_committed_before_a_failure() {
    let (svc, store) = make_recording_service();
    seed_replication(&svc, REPLICATE_ALL_TO_DST, &["dst"]);
    *store.fail_put.lock() = Some("src/b".to_string());
    let body = b"<Delete><Object><Key>a</Key></Object><Object><Key>b</Key></Object></Delete>";
    let req = make_request(Method::POST, "/src", &[("delete", "")], body);
    assert!(svc.delete_objects("123456789012", &req, "src").is_err());
    let dst = bucket_of(&svc, "dst");
    assert!(dst.objects["a"].is_delete_marker);
    assert!(!dst.objects.contains_key("b"));
}

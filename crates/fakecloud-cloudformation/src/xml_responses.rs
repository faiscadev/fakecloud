use crate::state::{Stack, StackResource};

use fakecloud_aws::xml::xml_escape;

pub fn create_stack_response(stack_id: &str, request_id: &str) -> String {
    format!(
        r#"<CreateStackResponse xmlns="http://cloudformation.amazonaws.com/doc/2010-05-15/">
  <CreateStackResult>
    <StackId>{stack_id}</StackId>
  </CreateStackResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</CreateStackResponse>"#,
        stack_id = xml_escape(stack_id),
        request_id = xml_escape(request_id),
    )
}

pub fn update_stack_response(stack_id: &str, request_id: &str) -> String {
    format!(
        r#"<UpdateStackResponse xmlns="http://cloudformation.amazonaws.com/doc/2010-05-15/">
  <UpdateStackResult>
    <StackId>{stack_id}</StackId>
  </UpdateStackResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</UpdateStackResponse>"#,
        stack_id = xml_escape(stack_id),
        request_id = xml_escape(request_id),
    )
}

pub fn delete_stack_response(request_id: &str) -> String {
    format!(
        r#"<DeleteStackResponse xmlns="http://cloudformation.amazonaws.com/doc/2010-05-15/">
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</DeleteStackResponse>"#,
        request_id = xml_escape(request_id),
    )
}

pub fn describe_stacks_response(stacks: &[Stack], request_id: &str) -> String {
    let members: String = stacks
        .iter()
        .map(stack_member_xml)
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"<DescribeStacksResponse xmlns="http://cloudformation.amazonaws.com/doc/2010-05-15/">
  <DescribeStacksResult>
    <Stacks>
{members}
    </Stacks>
  </DescribeStacksResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</DescribeStacksResponse>"#,
        request_id = xml_escape(request_id),
    )
}

fn stack_member_xml(stack: &Stack) -> String {
    let tags_xml = if stack.tags.is_empty() {
        String::new()
    } else {
        let tags: String = stack
            .tags
            .iter()
            .map(|(k, v)| {
                format!(
                    "          <member>\n            <Key>{}</Key>\n            <Value>{}</Value>\n          </member>",
                    xml_escape(k),
                    xml_escape(v),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        format!("\n        <Tags>\n{tags}\n        </Tags>")
    };

    let params_xml = if stack.parameters.is_empty() {
        String::new()
    } else {
        let params: String = stack
            .parameters
            .iter()
            .map(|(k, v)| {
                format!(
                    "          <member>\n            <ParameterKey>{}</ParameterKey>\n            <ParameterValue>{}</ParameterValue>\n          </member>",
                    xml_escape(k),
                    xml_escape(v),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        format!("\n        <Parameters>\n{params}\n        </Parameters>")
    };

    let description_xml = stack
        .description
        .as_ref()
        .map(|d| format!("\n        <Description>{}</Description>", xml_escape(d)))
        .unwrap_or_default();

    let notification_arns_xml = if stack.notification_arns.is_empty() {
        String::new()
    } else {
        let members: String = stack
            .notification_arns
            .iter()
            .map(|arn| format!("          <member>{}</member>", xml_escape(arn)))
            .collect::<Vec<_>>()
            .join("\n");
        format!("\n        <NotificationARNs>\n{members}\n        </NotificationARNs>")
    };

    let outputs_xml = if stack.outputs.is_empty() {
        String::new()
    } else {
        let members: String = stack
            .outputs
            .iter()
            .map(|o| {
                let desc = o
                    .description
                    .as_ref()
                    .map(|d| format!("\n            <Description>{}</Description>", xml_escape(d)))
                    .unwrap_or_default();
                let export = o
                    .export_name
                    .as_ref()
                    .map(|n| format!("\n            <ExportName>{}</ExportName>", xml_escape(n)))
                    .unwrap_or_default();
                format!(
                    "          <member>\n            <OutputKey>{}</OutputKey>\n            <OutputValue>{}</OutputValue>{}{}\n          </member>",
                    xml_escape(&o.key),
                    xml_escape(&o.value),
                    desc,
                    export,
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        format!("\n        <Outputs>\n{members}\n        </Outputs>")
    };

    format!(
        r#"      <member>
        <StackName>{name}</StackName>
        <StackId>{id}</StackId>
        <StackStatus>{status}</StackStatus>{status_reason_xml}
        <CreationTime>{created}</CreationTime>
        <EnableTerminationProtection>{termination_protection}</EnableTerminationProtection>{description_xml}{tags_xml}{params_xml}{notification_arns_xml}{outputs_xml}
      </member>"#,
        name = xml_escape(&stack.name),
        id = xml_escape(&stack.stack_id),
        status = xml_escape(&stack.status),
        status_reason_xml = status_reason_xml(stack.status_reason.as_deref()),
        created = stack.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
        termination_protection = stack.enable_termination_protection,
    )
}

/// `StackStatusReason` element, emitted only when the stack carries a reason.
/// AWS omits the member entirely on a healthy stack rather than sending an
/// empty one.
fn status_reason_xml(reason: Option<&str>) -> String {
    reason.map_or_else(String::new, |r| {
        format!(
            "\n        <StackStatusReason>{}</StackStatusReason>",
            xml_escape(r)
        )
    })
}

pub fn list_stacks_response(stacks: &[Stack], request_id: &str) -> String {
    let summaries: String = stacks
        .iter()
        .map(|s| {
            format!(
                r#"      <member>
        <StackName>{name}</StackName>
        <StackId>{id}</StackId>
        <StackStatus>{status}</StackStatus>{status_reason_xml}
        <CreationTime>{created}</CreationTime>
      </member>"#,
                name = xml_escape(&s.name),
                id = xml_escape(&s.stack_id),
                status = xml_escape(&s.status),
                status_reason_xml = status_reason_xml(s.status_reason.as_deref()),
                created = s.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"<ListStacksResponse xmlns="http://cloudformation.amazonaws.com/doc/2010-05-15/">
  <ListStacksResult>
    <StackSummaries>
{summaries}
    </StackSummaries>
  </ListStacksResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</ListStacksResponse>"#,
        request_id = xml_escape(request_id),
    )
}

pub fn list_stack_resources_response(resources: &[StackResource], request_id: &str) -> String {
    let summaries: String = resources
        .iter()
        .map(stack_resource_summary_xml)
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"<ListStackResourcesResponse xmlns="http://cloudformation.amazonaws.com/doc/2010-05-15/">
  <ListStackResourcesResult>
    <StackResourceSummaries>
{summaries}
    </StackResourceSummaries>
  </ListStackResourcesResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</ListStackResourcesResponse>"#,
        request_id = xml_escape(request_id),
    )
}

fn stack_resource_summary_xml(resource: &StackResource) -> String {
    format!(
        r#"      <member>
        <LogicalResourceId>{logical_id}</LogicalResourceId>
        <PhysicalResourceId>{physical_id}</PhysicalResourceId>
        <ResourceType>{resource_type}</ResourceType>
        <ResourceStatus>{status}</ResourceStatus>
      </member>"#,
        logical_id = xml_escape(&resource.logical_id),
        physical_id = xml_escape(&resource.physical_id),
        resource_type = xml_escape(&resource.resource_type),
        status = xml_escape(&resource.status),
    )
}

pub fn describe_stack_resources_response(
    resources: &[StackResource],
    stack_name: &str,
    request_id: &str,
) -> String {
    let members: String = resources
        .iter()
        .map(|r| {
            format!(
                r#"      <member>
        <StackName>{stack_name}</StackName>
        <LogicalResourceId>{logical_id}</LogicalResourceId>
        <PhysicalResourceId>{physical_id}</PhysicalResourceId>
        <ResourceType>{resource_type}</ResourceType>
        <ResourceStatus>{status}</ResourceStatus>
      </member>"#,
                stack_name = xml_escape(stack_name),
                logical_id = xml_escape(&r.logical_id),
                physical_id = xml_escape(&r.physical_id),
                resource_type = xml_escape(&r.resource_type),
                status = xml_escape(&r.status),
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"<DescribeStackResourcesResponse xmlns="http://cloudformation.amazonaws.com/doc/2010-05-15/">
  <DescribeStackResourcesResult>
    <StackResources>
{members}
    </StackResources>
  </DescribeStackResourcesResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</DescribeStackResourcesResponse>"#,
        request_id = xml_escape(request_id),
    )
}

pub fn get_template_response(template_body: &str, request_id: &str) -> String {
    format!(
        r#"<GetTemplateResponse xmlns="http://cloudformation.amazonaws.com/doc/2010-05-15/">
  <GetTemplateResult>
    <TemplateBody>{template_body}</TemplateBody>
  </GetTemplateResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</GetTemplateResponse>"#,
        template_body = xml_escape(template_body),
        request_id = xml_escape(request_id),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use fakecloud_aws::arn::Arn;
    use std::collections::BTreeMap;

    fn make_stack(name: &str) -> Stack {
        Stack {
            name: name.to_string(),
            stack_id: Arn::new(
                "cloudformation",
                "us-east-1",
                "123456789012",
                &format!("stack/{name}/abc123"),
            )
            .to_string(),
            template: "{}".to_string(),
            status: "CREATE_COMPLETE".to_string(),
            status_reason: None,
            resources: vec![],
            parameters: BTreeMap::new(),
            tags: BTreeMap::new(),
            created_at: Utc::now(),
            updated_at: None,
            description: None,
            notification_arns: vec![],
            outputs: vec![],
            enable_termination_protection: false,
        }
    }

    fn make_resource(logical_id: &str, resource_type: &str) -> StackResource {
        StackResource {
            logical_id: logical_id.to_string(),
            physical_id: format!("phys-{logical_id}"),
            resource_type: resource_type.to_string(),
            status: "CREATE_COMPLETE".to_string(),
            service_token: None,
            attributes: std::collections::BTreeMap::new(),
            deletion_policy: None,
            update_replace_policy: None,
        }
    }

    #[test]
    fn create_stack_response_contains_stack_id() {
        let xml = create_stack_response("stack-123", "req-1");
        assert!(xml.contains("<StackId>stack-123</StackId>"));
        assert!(xml.contains("<RequestId>req-1</RequestId>"));
        assert!(xml.contains("CreateStackResponse"));
    }

    #[test]
    fn update_stack_response_contains_stack_id() {
        let xml = update_stack_response("stack-456", "req-2");
        assert!(xml.contains("<StackId>stack-456</StackId>"));
        assert!(xml.contains("UpdateStackResponse"));
    }

    #[test]
    fn delete_stack_response_format() {
        let xml = delete_stack_response("req-3");
        assert!(xml.contains("<RequestId>req-3</RequestId>"));
        assert!(xml.contains("DeleteStackResponse"));
    }

    #[test]
    fn describe_stacks_response_lists_stacks() {
        let s1 = make_stack("my-stack");
        let xml = describe_stacks_response(&[s1], "req-4");
        assert!(xml.contains("<StackName>my-stack</StackName>"));
        assert!(xml.contains("CREATE_COMPLETE"));
        assert!(xml.contains("DescribeStacksResponse"));
    }

    #[test]
    fn status_reason_is_emitted_only_when_present() {
        // A healthy stack omits the member entirely, matching AWS.
        let healthy = make_stack("healthy");
        let xml = describe_stacks_response(std::slice::from_ref(&healthy), "req-a");
        assert!(!xml.contains("StackStatusReason"), "{xml}");
        assert!(!list_stacks_response(&[healthy], "req-b").contains("StackStatusReason"));

        // A failed stack carries the reason on both DescribeStacks and
        // ListStacks, XML-escaped.
        let mut failed = make_stack("failed");
        failed.status = "CREATE_FAILED".to_string();
        failed.status_reason = Some("Resource <NoType> must have a Type property".to_string());
        let xml = describe_stacks_response(std::slice::from_ref(&failed), "req-c");
        assert!(
            xml.contains(
                "<StackStatusReason>Resource &lt;NoType&gt; must have a Type property</StackStatusReason>"
            ),
            "{xml}"
        );
        assert!(list_stacks_response(&[failed], "req-d").contains("<StackStatusReason>"));
    }

    #[test]
    fn describe_stacks_with_tags_and_params() {
        let mut stack = make_stack("tagged-stack");
        stack.tags.insert("env".to_string(), "prod".to_string());
        stack
            .parameters
            .insert("Param1".to_string(), "Value1".to_string());
        stack.description = Some("My stack desc".to_string());

        let xml = describe_stacks_response(&[stack], "req-5");
        assert!(xml.contains("<Key>env</Key>"));
        assert!(xml.contains("<Value>prod</Value>"));
        assert!(xml.contains("<ParameterKey>Param1</ParameterKey>"));
        assert!(xml.contains("<Description>My stack desc</Description>"));
    }

    #[test]
    fn list_stacks_response_lists_summaries() {
        let stacks = vec![make_stack("s1"), make_stack("s2")];
        let xml = list_stacks_response(&stacks, "req-6");
        assert!(xml.contains("<StackName>s1</StackName>"));
        assert!(xml.contains("<StackName>s2</StackName>"));
        assert!(xml.contains("ListStacksResponse"));
    }

    #[test]
    fn list_stack_resources_response_format() {
        let resources = vec![
            make_resource("MyBucket", "AWS::S3::Bucket"),
            make_resource("MyTable", "AWS::DynamoDB::Table"),
        ];
        let xml = list_stack_resources_response(&resources, "req-7");
        assert!(xml.contains("<LogicalResourceId>MyBucket</LogicalResourceId>"));
        assert!(xml.contains("<ResourceType>AWS::S3::Bucket</ResourceType>"));
        assert!(xml.contains("<LogicalResourceId>MyTable</LogicalResourceId>"));
        assert!(xml.contains("ListStackResourcesResponse"));
    }

    #[test]
    fn describe_stack_resources_response_includes_stack_name() {
        let resources = vec![make_resource("Fn", "AWS::Lambda::Function")];
        let xml = describe_stack_resources_response(&resources, "my-stack", "req-8");
        assert!(xml.contains("<StackName>my-stack</StackName>"));
        assert!(xml.contains("<LogicalResourceId>Fn</LogicalResourceId>"));
        assert!(xml.contains("DescribeStackResourcesResponse"));
    }

    #[test]
    fn get_template_response_contains_body() {
        let xml = get_template_response(r#"{"AWSTemplateFormatVersion":"2010-09-09"}"#, "req-9");
        assert!(xml.contains("AWSTemplateFormatVersion"));
        assert!(xml.contains("GetTemplateResponse"));
    }

    #[test]
    fn xml_escaping_works() {
        let xml = create_stack_response("stack<>\"&'123", "req");
        assert!(xml.contains("&lt;"));
        assert!(xml.contains("&gt;"));
        assert!(xml.contains("&amp;"));
    }

    #[test]
    fn describe_stacks_with_notification_arns() {
        let mut stack = make_stack("notif-stack");
        stack
            .notification_arns
            .push("arn:aws:sns:us-east-1:123456789012:my-topic".to_string());
        let xml = describe_stacks_response(&[stack], "req-10");
        assert!(xml.contains("<NotificationARNs>"));
        assert!(xml.contains("my-topic"));
    }

    #[test]
    fn empty_stacks_produces_valid_xml() {
        let xml = describe_stacks_response(&[], "req-11");
        assert!(xml.contains("<Stacks>"));
        assert!(xml.contains("</Stacks>"));
    }
}

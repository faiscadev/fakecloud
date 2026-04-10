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

    format!(
        r#"      <member>
        <StackName>{name}</StackName>
        <StackId>{id}</StackId>
        <StackStatus>{status}</StackStatus>
        <CreationTime>{created}</CreationTime>{description_xml}{tags_xml}{params_xml}{notification_arns_xml}
      </member>"#,
        name = xml_escape(&stack.name),
        id = xml_escape(&stack.stack_id),
        status = xml_escape(&stack.status),
        created = stack.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
    )
}

pub fn list_stacks_response(stacks: &[Stack], request_id: &str) -> String {
    let summaries: String = stacks
        .iter()
        .map(|s| {
            format!(
                r#"      <member>
        <StackName>{name}</StackName>
        <StackId>{id}</StackId>
        <StackStatus>{status}</StackStatus>
        <CreationTime>{created}</CreationTime>
      </member>"#,
                name = xml_escape(&s.name),
                id = xml_escape(&s.stack_id),
                status = xml_escape(&s.status),
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

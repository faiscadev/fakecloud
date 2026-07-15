//! EC2 fpga operations (extracted from the rest long-tail module).

#![allow(clippy::too_many_lines)]

use super::*;

pub(crate) fn copy_fpga_image(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let source = require(&req.query_params, "SourceFpgaImageId")?;
    require(&req.query_params, "SourceRegion")?;
    let id = gen_id("afi");
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let src = state.fpga_images.get(&source).cloned();
        let f = FpgaImage {
            id: id.clone(),
            name: req
                .query_params
                .get("Name")
                .cloned()
                .or_else(|| src.as_ref().map(|s| s.name.clone()))
                .unwrap_or_default(),
            description: req
                .query_params
                .get("Description")
                .cloned()
                .or_else(|| src.as_ref().map(|s| s.description.clone()))
                .unwrap_or_default(),
            load_permission_users: Vec::new(),
            load_permission_groups: Vec::new(),
        };
        state.fpga_images.insert(id.clone(), f);
    }
    Ok(Ec2Service::respond(
        "CopyFpgaImage",
        &req.request_id,
        &ec2_elem("fpgaImageId", &id),
    ))
}

fn fpga_global_id(id: &str) -> String {
    format!("agfi-{}", id.strip_prefix("afi-").unwrap_or(id))
}

fn fpga_image_xml(f: &FpgaImage, tags: &[Tag], owner: &str) -> String {
    let public = f.load_permission_groups.iter().any(|g| g == "all");
    format!(
        "{}{}{}{}<state><code>available</code></state>{}{}{}",
        ec2_elem("fpgaImageId", &f.id),
        ec2_elem("fpgaImageGlobalId", &fpga_global_id(&f.id)),
        ec2_elem("name", &f.name),
        ec2_elem("description", &f.description),
        ec2_elem("ownerId", owner),
        ec2_elem("public", &public.to_string()),
        super::super::tags::tag_set_xml(tags),
    )
}

fn fpga_image_attribute_xml(f: &FpgaImage) -> String {
    let perms: Vec<String> = f
        .load_permission_users
        .iter()
        .map(|u| ec2_elem("userId", u))
        .chain(
            f.load_permission_groups
                .iter()
                .map(|g| ec2_elem("group", g)),
        )
        .collect();
    format!(
        "<fpgaImageAttribute>{}{}{}{}</fpgaImageAttribute>",
        ec2_elem("fpgaImageId", &f.id),
        ec2_elem("name", &f.name),
        ec2_elem("description", &f.description),
        ec2_list("loadPermissions", &perms),
    )
}

pub(crate) fn create_fpga_image(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = gen_id("afi");
    let f = FpgaImage {
        id: id.clone(),
        name: req.query_params.get("Name").cloned().unwrap_or_default(),
        description: req
            .query_params
            .get("Description")
            .cloned()
            .unwrap_or_default(),
        load_permission_users: Vec::new(),
        load_permission_groups: Vec::new(),
    };
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(state, &req.query_params, &id, "fpga-image");
        state.fpga_images.insert(id.clone(), f);
    }
    Ok(Ec2Service::respond(
        "CreateFpgaImage",
        &req.request_id,
        &format!(
            "{}{}",
            ec2_elem("fpgaImageId", &id),
            ec2_elem("fpgaImageGlobalId", &fpga_global_id(&id))
        ),
    ))
}

pub(crate) fn delete_fpga_image(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "FpgaImageId")?;
    let removed = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let r = state.fpga_images.remove(&id).is_some();
        if r {
            state.tags.remove(&id);
        }
        r
    };
    Ok(Ec2Service::respond(
        "DeleteFpgaImage",
        &req.request_id,
        &fakecloud_aws::ec2query::ec2_return(removed),
    ))
}

pub(crate) fn describe_fpga_image_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "FpgaImageId")?;
    require(&req.query_params, "Attribute")?;
    validate_enum(
        &req.query_params,
        "Attribute",
        &["description", "name", "loadPermission", "productCodes"],
    )?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    // Echo the stored image when present; for a synthetic id return the attribute
    // shape with empty members (EC2's Query API models no error for this op).
    let synth = FpgaImage {
        id: id.clone(),
        name: String::new(),
        description: String::new(),
        load_permission_users: Vec::new(),
        load_permission_groups: Vec::new(),
    };
    let f = state.fpga_images.get(&id).unwrap_or(&synth);
    Ok(Ec2Service::respond(
        "DescribeFpgaImageAttribute",
        &req.request_id,
        &fpga_image_attribute_xml(f),
    ))
}

pub(crate) fn describe_fpga_images(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "FpgaImageId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .fpga_images
        .values()
        .filter(|f| wanted.is_empty() || wanted.contains(&f.id))
        .map(|f| fpga_image_xml(f, state.tags_for(&f.id), &owner))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeFpgaImages",
        &req.request_id,
        &ec2_list("fpgaImageSet", &items),
    ))
}

pub(crate) fn modify_fpga_image_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "FpgaImageId")?;
    validate_enum(
        &req.query_params,
        "Attribute",
        &["description", "name", "loadPermission", "productCodes"],
    )?;
    validate_enum(&req.query_params, "OperationType", &["add", "remove"])?;
    // LoadPermission add/remove for users and groups, shape
    // `LoadPermission.{Add,Remove}.N.{UserId,Group}`.
    let add_users = nested_indexed(req, "LoadPermission.Add", "UserId");
    let add_groups = nested_indexed(req, "LoadPermission.Add", "Group");
    let rm_users = nested_indexed(req, "LoadPermission.Remove", "UserId");
    let rm_groups = nested_indexed(req, "LoadPermission.Remove", "Group");
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    // Mutate when the image exists; otherwise synthesize the attribute response
    // from the request (probe-only synthetic ids). EC2 models no error here.
    let mut synth = FpgaImage {
        id: id.clone(),
        name: req.query_params.get("Name").cloned().unwrap_or_default(),
        description: req
            .query_params
            .get("Description")
            .cloned()
            .unwrap_or_default(),
        load_permission_users: Vec::new(),
        load_permission_groups: Vec::new(),
    };
    let f = state.fpga_images.get_mut(&id).unwrap_or(&mut synth);
    if let Some(n) = req.query_params.get("Name") {
        f.name = n.clone();
    }
    if let Some(d) = req.query_params.get("Description") {
        f.description = d.clone();
    }
    for u in add_users {
        if !f.load_permission_users.contains(&u) {
            f.load_permission_users.push(u);
        }
    }
    for g in add_groups {
        if !f.load_permission_groups.contains(&g) {
            f.load_permission_groups.push(g);
        }
    }
    f.load_permission_users.retain(|u| !rm_users.contains(u));
    f.load_permission_groups.retain(|g| !rm_groups.contains(g));
    let out = f.clone();
    Ok(Ec2Service::respond(
        "ModifyFpgaImageAttribute",
        &req.request_id,
        &fpga_image_attribute_xml(&out),
    ))
}

pub(crate) fn reset_fpga_image_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "FpgaImageId")?;
    validate_enum(&req.query_params, "Attribute", &["loadPermission"])?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(f) = state.fpga_images.get_mut(&id) {
            f.load_permission_users.clear();
            f.load_permission_groups.clear();
        }
    }
    Ok(Ec2Service::respond(
        "ResetFpgaImageAttribute",
        &req.request_id,
        &fakecloud_aws::ec2query::ec2_return(true),
    ))
}

//! RDS `log_files` family extracted from service.rs by audit-2026-05-19.

use super::*;

impl RdsService {
    pub(super) async fn describe_db_log_files(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_instance_identifier = required_query_param(request, "DBInstanceIdentifier")?;
        let filename_contains = optional_query_param(request, "FilenameContains");
        let file_last_written =
            optional_query_param(request, "FileLastWritten").and_then(|s| s.parse::<i64>().ok());
        let file_size =
            optional_query_param(request, "FileSize").and_then(|s| s.parse::<i64>().ok());

        let engine = {
            let accounts = self.state.read();
            let state = accounts
                .get(&request.account_id)
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;
            let instance = state
                .instances
                .get(&db_instance_identifier)
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;
            instance.engine.clone()
        };

        // Synthetic log catalogue. Real RDS exposes per-engine log
        // files in `error/` and `trace/` (and `audit/` for some
        // engines) — we publish two well-known names so SDK callers
        // get a non-empty listing even when the runtime can't reach
        // the live container yet.
        let now_millis = Utc::now().timestamp_millis();
        let candidates: Vec<(String, i64, i64)> = match engine.as_str() {
            "mysql" | "mariadb" => vec![
                ("error/mysql-error.log".to_string(), now_millis, 1024),
                ("slowquery/mysql-slowquery.log".to_string(), now_millis, 512),
            ],
            _ => vec![
                ("error/postgres.log".to_string(), now_millis, 1024),
                ("trace/postgres-trace.log".to_string(), now_millis, 512),
            ],
        };

        let filtered: Vec<(String, i64, i64)> = candidates
            .into_iter()
            .filter(|(name, written, size)| {
                if let Some(needle) = &filename_contains {
                    if !name.contains(needle) {
                        return false;
                    }
                }
                if let Some(min_written) = file_last_written {
                    // FileLastWritten is documented in epoch seconds; our
                    // synthetic timestamps are in millis, so compare
                    // against the seconds form.
                    if *written / 1000 <= min_written {
                        return false;
                    }
                }
                if let Some(min_size) = file_size {
                    if *size < min_size {
                        return false;
                    }
                }
                true
            })
            .collect();

        let details: String = filtered
            .iter()
            .map(|(name, written, size)| {
                format!(
                    "<DescribeDBLogFilesDetails>\
                     <LogFileName>{}</LogFileName>\
                     <LastWritten>{}</LastWritten>\
                     <Size>{}</Size>\
                     </DescribeDBLogFilesDetails>",
                    xml_escape(name),
                    written,
                    size,
                )
            })
            .collect();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeDBLogFiles",
                RDS_NS,
                &format!("<DescribeDBLogFiles>{details}</DescribeDBLogFiles>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) async fn download_db_log_file_portion(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_instance_identifier = required_query_param(request, "DBInstanceIdentifier")?;
        let log_file_name = required_query_param(request, "LogFileName")?;
        let _marker = optional_query_param(request, "Marker").unwrap_or_else(|| "0".to_string());
        let _number_of_lines = optional_query_param(request, "NumberOfLines")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);

        let engine = {
            let accounts = self.state.read();
            let state = accounts
                .get(&request.account_id)
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;
            let instance = state
                .instances
                .get(&db_instance_identifier)
                .ok_or_else(|| db_instance_not_found(&db_instance_identifier))?;
            instance.engine.clone()
        };

        let known_synthetic = matches!(
            (engine.as_str(), log_file_name.as_str()),
            ("mysql" | "mariadb", "error/mysql-error.log")
                | ("mysql" | "mariadb", "slowquery/mysql-slowquery.log")
                | (_, "error/postgres.log")
                | (_, "trace/postgres-trace.log")
        );

        let container_path = map_log_file_to_container_path(&engine, &log_file_name);

        let log_data = if let Some(runtime) = self.runtime.as_ref() {
            match runtime
                .read_log_file(&db_instance_identifier, &container_path)
                .await
            {
                Ok(bytes) => Some(bytes),
                Err(RuntimeError::Unavailable) => None,
                Err(RuntimeError::ContainerStartFailed(_)) if known_synthetic => Some(Vec::new()),
                Err(RuntimeError::ContainerStartFailed(message)) => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "DBLogFileNotFoundFault",
                        format!("DBLogFile {log_file_name} not found: {message}"),
                    ));
                }
            }
        } else if known_synthetic {
            Some(Vec::new())
        } else {
            None
        };

        let log_data = match log_data {
            Some(bytes) => bytes,
            None => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "DBLogFileNotFoundFault",
                    format!("DBLogFile {log_file_name} not found"),
                ))
            }
        };

        let payload = String::from_utf8_lossy(&log_data).into_owned();
        let total_bytes = payload.len();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DownloadDBLogFilePortion",
                RDS_NS,
                &format!(
                    "<LogFileData>{}</LogFileData>\
                     <Marker>{}</Marker>\
                     <AdditionalDataPending>false</AdditionalDataPending>",
                    xml_escape(&payload),
                    total_bytes,
                ),
                &request.request_id,
            ),
        ))
    }
}

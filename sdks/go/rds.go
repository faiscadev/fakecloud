package fakecloud

import "context"

// RDSClient provides access to RDS introspection endpoints.
type RDSClient struct {
	fc *FakeCloud
}

// GetInstances lists fakecloud-managed RDS DB instances and runtime metadata.
func (c *RDSClient) GetInstances(ctx context.Context) (*RDSInstancesResponse, error) {
	var out RDSInstancesResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/rds/instances", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// LambdaInvoke is the bridge the PostgreSQL/MySQL `aws_lambda`
// extension calls into to invoke a Lambda function from inside SQL.
func (c *RDSClient) LambdaInvoke(ctx context.Context, req *RdsLambdaInvokeRequest) (*RdsLambdaInvokeResponse, error) {
	var out RdsLambdaInvokeResponse
	if err := c.fc.doPost(ctx, "/_fakecloud/rds/lambda-invoke", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// S3Import is the bridge the PostgreSQL `aws_s3` extension calls into
// to fetch an object from a fakecloud S3 bucket. The object body is
// returned base64-encoded so JSON transport stays text-only.
func (c *RDSClient) S3Import(ctx context.Context, req *RdsS3ImportRequest) (*RdsS3ImportResponse, error) {
	var out RdsS3ImportResponse
	if err := c.fc.doPost(ctx, "/_fakecloud/rds/s3-import", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// S3Export is the bridge equivalent of an S3 PutObject driven from
// inside the DB container.
func (c *RDSClient) S3Export(ctx context.Context, req *RdsS3ExportRequest) (*RdsS3ExportResponse, error) {
	var out RdsS3ExportResponse
	if err := c.fc.doPost(ctx, "/_fakecloud/rds/s3-export", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

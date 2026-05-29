package fakecloud

import "context"

// GlueClient provides access to Glue introspection endpoints.
// Lets tests assert what `CreateJob` recorded and inspect the
// `StartJobRun` ledger without re-listing through the AWS surface.
type GlueClient struct {
	fc *FakeCloud
}

// GetJobs returns every Glue Job the server knows about, across every
// account. Order is stable: by account, then job name.
func (c *GlueClient) GetJobs(ctx context.Context) (*GlueJobsResponse, error) {
	var out GlueJobsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/glue/jobs", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetJobRuns returns every JobRun across every account. If jobName is
// non-empty, only runs for that job are returned. Order: by account,
// then start time, then id.
func (c *GlueClient) GetJobRuns(ctx context.Context, jobName string) (*GlueJobRunsResponse, error) {
	path := "/_fakecloud/glue/job-runs"
	if jobName != "" {
		path += "?job_name=" + jobName
	}
	var out GlueJobRunsResponse
	if err := c.fc.doGet(ctx, path, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetCrawlers returns every Glue crawler the server knows about, across
// every account. Order is stable: by account, then crawler name.
func (c *GlueClient) GetCrawlers(ctx context.Context) (*GlueCrawlersResponse, error) {
	var out GlueCrawlersResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/glue/crawlers", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

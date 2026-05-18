package fakecloud

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
)

// ECSClient provides access to ECS introspection endpoints.
type ECSClient struct {
	fc *FakeCloud
}

// GetClusters lists every ECS cluster fakecloud has seen, across every
// account, sorted by cluster ARN.
func (c *ECSClient) GetClusters(ctx context.Context) (*EcsClustersResponse, error) {
	var out EcsClustersResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/ecs/clusters", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetTasks lists every task fakecloud has seen. Pass empty strings to
// skip the cluster / status filters.
func (c *ECSClient) GetTasks(ctx context.Context, cluster, status string) (*EcsTasksResponse, error) {
	path := "/_fakecloud/ecs/tasks"
	q := url.Values{}
	if cluster != "" {
		q.Set("cluster", cluster)
	}
	if status != "" {
		q.Set("status", status)
	}
	if enc := q.Encode(); enc != "" {
		path += "?" + enc
	}
	var out EcsTasksResponse
	if err := c.fc.doGet(ctx, path, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetTask returns a single task snapshot by task ID. Returns 404 if the
// task is unknown.
func (c *ECSClient) GetTask(ctx context.Context, taskID string) (*EcsTask, error) {
	var out EcsTask
	if err := c.fc.doGet(ctx, fmt.Sprintf("/_fakecloud/ecs/tasks/%s", taskID), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetTaskLogs returns the captured docker stdout/stderr for a task.
func (c *ECSClient) GetTaskLogs(ctx context.Context, taskID string) (*EcsTaskLogsResponse, error) {
	var out EcsTaskLogsResponse
	if err := c.fc.doGet(ctx, fmt.Sprintf("/_fakecloud/ecs/tasks/%s/logs", taskID), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ForceStopTask sends SIGTERM (then SIGKILL after 10s) to the task's
// running container via the runtime.
func (c *ECSClient) ForceStopTask(ctx context.Context, taskID string) (*EcsTask, error) {
	var out EcsTask
	if err := c.fc.doPost(ctx, fmt.Sprintf("/_fakecloud/ecs/tasks/%s/force-stop", taskID), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// MarkTaskFailed flips a task to STOPPED without killing the container —
// useful for simulating failed tasks deterministically in tests.
func (c *ECSClient) MarkTaskFailed(ctx context.Context, taskID string, req *EcsMarkFailedRequest) (*EcsTask, error) {
	var out EcsTask
	if err := c.fc.doPost(ctx, fmt.Sprintf("/_fakecloud/ecs/tasks/%s/mark-failed", taskID), req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetEvents replays the lifecycle event log.
func (c *ECSClient) GetEvents(ctx context.Context) (*EcsEventsResponse, error) {
	var out EcsEventsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/ecs/events", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetTaskMetadata returns the aggregated v4 metadata dump (the same shape
// `ECS_CONTAINER_METADATA_URI_V4` exposes to a container) for the task with
// the given full ARN. The ARN is URL-encoded before insertion into the path.
func (c *ECSClient) GetTaskMetadata(ctx context.Context, taskArn string) (*EcsTaskMetadataResponse, error) {
	var out EcsTaskMetadataResponse
	path := fmt.Sprintf("/_fakecloud/ecs/metadata/%s", url.PathEscape(taskArn))
	if err := c.fc.doGet(ctx, path, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetTaskCredentials returns the IAM task-role credentials that the ECS
// agent's metadata server would hand out at the path advertised in
// AWS_CONTAINER_CREDENTIALS_RELATIVE_URI.
func (c *ECSClient) GetTaskCredentials(ctx context.Context, taskID string) (*EcsTaskCredentials, error) {
	var out EcsTaskCredentials
	path := fmt.Sprintf("/_fakecloud/ecs/creds/%s", taskID)
	if err := c.fc.doGet(ctx, path, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetTaskMetadataV3 returns the raw v3 ECS task metadata JSON
// (ECS_CONTAINER_METADATA_URI). The shape is intentionally
// open-ended; callers can decode further as needed.
func (c *ECSClient) GetTaskMetadataV3(ctx context.Context, taskID string) (json.RawMessage, error) {
	var out json.RawMessage
	path := fmt.Sprintf("/_fakecloud/ecs/v3/%s", taskID)
	if err := c.fc.doGet(ctx, path, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// GetTaskMetadataV4 returns the raw v4 ECS task metadata JSON
// (ECS_CONTAINER_METADATA_URI_V4) as the agent serves it to a running
// container.
func (c *ECSClient) GetTaskMetadataV4(ctx context.Context, taskID string) (json.RawMessage, error) {
	var out json.RawMessage
	path := fmt.Sprintf("/_fakecloud/ecs/v4/%s", taskID)
	if err := c.fc.doGet(ctx, path, &out); err != nil {
		return nil, err
	}
	return out, nil
}

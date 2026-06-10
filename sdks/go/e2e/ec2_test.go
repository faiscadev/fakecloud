package e2e

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"

	fakecloud "github.com/faiscadev/fakecloud/sdks/go"
)

func TestE2EEC2(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cfg := awsConfig(t)

	ec2Client := ec2.NewFromConfig(cfg, func(o *ec2.Options) {
		o.BaseEndpoint = aws.String(fakecloudURL)
	})

	runOut, err := ec2Client.RunInstances(ctx, &ec2.RunInstancesInput{
		ImageId:      aws.String("ami-12345678"),
		InstanceType: ec2types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	if err != nil {
		t.Fatalf("RunInstances failed: %v", err)
	}
	if len(runOut.Instances) == 0 || runOut.Instances[0].InstanceId == nil {
		t.Fatal("expected RunInstances to return an instance with an id")
	}
	instanceID := *runOut.Instances[0].InstanceId

	fc := fakecloud.New(fakecloudURL)
	resp, err := fc.EC2().GetInstances(ctx)
	if err != nil {
		t.Fatalf("EC2().GetInstances() failed: %v", err)
	}

	found := false
	for _, instance := range resp.Instances {
		if instance.InstanceID == instanceID {
			found = true
			if instance.ImageID != "ami-12345678" {
				t.Fatalf("expected imageId ami-12345678, got %s", instance.ImageID)
			}
			if instance.InstanceType != "t3.micro" {
				t.Fatalf("expected instanceType t3.micro, got %s", instance.InstanceType)
			}
			switch instance.State {
			case "pending", "running":
				// expected freshly-launched states
			default:
				t.Fatalf("expected pending/running state, got %s", instance.State)
			}
		}
	}
	if !found {
		t.Fatalf("expected to find %s via introspection", instanceID)
	}
}

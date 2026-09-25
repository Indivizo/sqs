package queue

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnsure_CreatesDeadLetterQueueBeforeMainQueue(t *testing.T) {
	// given
	api := newFakeAPI()

	// when
	_, err := Ensure(context.Background(), api, "test", DefaultOptions())

	// then the dead-letter queue exists and its ARN is known before the
	// main queue that points at it is created
	require.NoError(t, err)
	assert.Equal(t, []string{
		"CreateQueue:test-deadMessages",
		"GetQueueAttributes:" + fakeQueueURL("test-deadMessages"),
		"CreateQueue:test",
	}, api.calls)
	require.Len(t, api.attributeRequests, 1)
	assert.Equal(t, []types.QueueAttributeName{types.QueueAttributeNameQueueArn}, api.attributeRequests[0].AttributeNames)
}

// CreateQueue on an existing queue fails when the requested attributes
// differ from the stored ones, so a service upgrading from v0.1.0 would
// refuse to boot on any drift here. These are v0.1.0's exact strings.
func TestEnsure_SendsTheSameAttributesAsV010(t *testing.T) {
	// given
	api := newFakeAPI()

	// when
	_, err := Ensure(context.Background(), api, "test", DefaultOptions())

	// then
	require.NoError(t, err)
	require.Len(t, api.createdQueues, 2)
	assert.Equal(t, map[string]string{
		"MessageRetentionPeriod": "1209600",
	}, api.createdQueues[0].Attributes)
	assert.Equal(t, map[string]string{
		"MessageRetentionPeriod": "1209600",
		"RedrivePolicy":          `{"maxReceiveCount":5,"deadLetterTargetArn":"` + api.queueArn + `"}`,
	}, api.createdQueues[1].Attributes)
}

func TestEnsure_ReturnsBothURLs(t *testing.T) {
	// given
	api := newFakeAPI()

	// when
	q, err := Ensure(context.Background(), api, "test", DefaultOptions())

	// then
	require.NoError(t, err)
	assert.Equal(t, "test", q.Name)
	assert.Equal(t, fakeQueueURL("test"), q.URL)
	assert.Equal(t, fakeQueueURL("test-deadMessages"), q.DeadLetterQueueURL)
}

func TestEnsure_DoesNotCreateMainQueueWhenDeadLetterCreationFails(t *testing.T) {
	// given
	api := newFakeAPI()
	denied := errors.New("access denied")
	api.createErrs["test-deadMessages"] = denied

	// when
	q, err := Ensure(context.Background(), api, "test", DefaultOptions())

	// then
	assert.ErrorIs(t, err, denied)
	assert.Nil(t, q)
	assert.Equal(t, []string{"CreateQueue:test-deadMessages"}, api.calls)
}

func TestEnsure_DoesNotCreateMainQueueWhenDeadLetterArnIsUnavailable(t *testing.T) {
	// given
	api := newFakeAPI()
	api.attributesErr = errors.New("throttled")

	// when
	q, err := Ensure(context.Background(), api, "test", DefaultOptions())

	// then
	assert.ErrorIs(t, err, api.attributesErr)
	assert.Nil(t, q)
	assert.NotContains(t, api.calls, "CreateQueue:test")
}

func TestEnsure_ReturnsMainQueueCreationError(t *testing.T) {
	// given
	api := newFakeAPI()
	exists := errors.New("queue already exists with different attributes")
	api.createErrs["test"] = exists

	// when
	q, err := Ensure(context.Background(), api, "test", DefaultOptions())

	// then
	assert.ErrorIs(t, err, exists)
	assert.Nil(t, q)
}

func TestEnsure_RejectsNameTooLongForDeadLetterSuffix(t *testing.T) {
	// given a name that fits SQS's 80-character limit on its own, but not
	// once the dead-letter suffix is appended
	api := newFakeAPI()
	name := strings.Repeat("a", 68)

	// when
	q, err := Ensure(context.Background(), api, name, DefaultOptions())

	// then nothing reaches AWS
	assert.ErrorIs(t, err, ErrQueueNameTooLong)
	assert.Nil(t, q)
	assert.Empty(t, api.calls)
}

func TestEnsure_AcceptsLongestNameThatFits(t *testing.T) {
	// given 67 + len("-deadMessages") == 80
	api := newFakeAPI()
	name := strings.Repeat("a", 67)

	// when
	_, err := Ensure(context.Background(), api, name, DefaultOptions())

	// then
	require.NoError(t, err)
}

func TestDefaultOptions_MatchV010(t *testing.T) {
	assert.Equal(t, Options{
		MaxReceiveCount:   5,
		RetentionSeconds:  1209600,
		VisibilityTimeout: 600,
		WaitTimeSeconds:   20,
	}, DefaultOptions())
}

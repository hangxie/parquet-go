package s3v2

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

// getConfig loads AWS config from shared config rather than explicit configuration.
var getConfig = sync.OnceValues(func() (aws.Config, error) {
	return config.LoadDefaultConfig(context.Background())
})

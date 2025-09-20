package s3v2

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

var (
	cfg  aws.Config
	once sync.Once
)

// Config from shared config rather than explicit configuration
func getConfig() aws.Config {
	once.Do(func() {
		c, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			panic(err)
		}
		cfg = c
	})
	return cfg
}

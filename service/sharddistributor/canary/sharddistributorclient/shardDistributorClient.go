package sharddistributorclient

import (
	"go.uber.org/fx"

	"github.com/uber/cadence/client/sharddistributor"
	"github.com/uber/cadence/client/wrappers/grpc"
	timeoutwrapper "github.com/uber/cadence/client/wrappers/timeout"
	sharddistributorv1 "github.com/uber/cadence/gen/proto/sharddistributor/v1"
)

// Params contains the dependencies needed to create a shard distributor client
type Params struct {
	fx.In

	YarpcClient sharddistributorv1.ShardDistributorAPIYARPCClient
}

// NewShardDistributorClient creates a new shard distributor client with GRPC and timeout wrappers
func NewShardDistributorClient(p Params) (sharddistributor.Client, error) {
	shardDistributorExecutorClient := grpc.NewShardDistributorClient(p.YarpcClient)
	shardDistributorExecutorClient = timeoutwrapper.NewShardDistributorClient(shardDistributorExecutorClient, timeoutwrapper.ShardDistributorExecutorDefaultTimeout)
	return shardDistributorExecutorClient, nil
}

package api

import (
	"context"
	"fmt"
	"time"

	"github.com/cosmostation/cvms/internal/common"
	"github.com/cosmostation/cvms/internal/common/types"
	"github.com/go-resty/resty/v2"
)

// GetBlockParallel fetches block data using parallel client if available
func GetBlockParallel(c common.CommonApp, height int64) (
	/* block height */ int64,
	/* block timestamp */ time.Time,
	/* proposer address */ string,
	/* block txs */ []types.Tx,
	/* last commit block height */ int64,
	/* block signatures */ []types.Signature,
	/* error */ error,
) {
	// Use parallel client if available
	if c.UseParallelClient() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Try parallel request first
		resp, err := c.ParallelClient.ParallelRequest(ctx, func(client *resty.Client) (*resty.Response, error) {
			return client.R().Get(fmt.Sprintf("/block?height=%d", height))
		}, true) // true for RPC

		if err == nil {
			return parseBlockResponse(resp)
		}

		// Log parallel failure and fall back to regular client
		c.Warnf("Parallel block request failed, falling back to regular client: %v", err)
	}

	// Fallback to regular GetBlock
	return GetBlock(c.CommonClient, height)
}

// GetValidatorsParallel fetches validators using parallel client if available
func GetValidatorsParallel(c common.CommonApp, height ...int64) ([]types.CosmosValidator, error) {
	// Use parallel client if available
	if c.UseParallelClient() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var path string
		if len(height) > 0 && height[0] > 0 {
			path = fmt.Sprintf("/validators?height=%d&per_page=200", height[0])
		} else {
			path = "/validators?per_page=200"
		}

		// Try parallel request first
		resp, err := c.ParallelClient.ParallelRequest(ctx, func(client *resty.Client) (*resty.Response, error) {
			return client.R().Get(path)
		}, true) // true for RPC

		if err == nil {
			return parseValidatorsResponse(resp)
		}

		// Log parallel failure and fall back to regular client
		c.Warnf("Parallel validators request failed, falling back to regular client: %v", err)
	}

	// Fallback to regular GetValidators
	return GetValidators(c.CommonClient, height...)
}

// GetStatusParallel fetches status using parallel client if available
func GetStatusParallel(c common.CommonApp) (
	/* block height */ int64,
	/* block timestamp */ time.Time,
	/* error */ error,
) {
	// Use parallel client if available
	if c.UseParallelClient() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Try parallel request first
		resp, err := c.ParallelClient.ParallelRequest(ctx, func(client *resty.Client) (*resty.Response, error) {
			return client.R().Get("/status")
		}, true) // true for RPC

		if err == nil {
			return parseStatusResponse(resp)
		}

		// Log parallel failure and fall back to regular client
		c.Warnf("Parallel status request failed, falling back to regular client: %v", err)
	}

	// Fallback to regular GetStatus
	return GetStatus(c.CommonClient)
}

// Helper function to parse block response (reuse existing parsing logic)
func parseBlockResponse(resp *resty.Response) (int64, time.Time, string, []types.Tx, int64, []types.Signature, error) {
	// This would contain the same parsing logic as the original GetBlock function
	// For now, we'll return an error to implement later
	return 0, time.Time{}, "", nil, 0, nil, fmt.Errorf("parseBlockResponse not implemented yet - use fallback")
}

// Helper function to parse validators response (reuse existing parsing logic)
func parseValidatorsResponse(resp *resty.Response) ([]types.CosmosValidator, error) {
	// This would contain the same parsing logic as the original GetValidators function
	// For now, we'll return an error to implement later
	return nil, fmt.Errorf("parseValidatorsResponse not implemented yet - use fallback")
}

// Helper function to parse status response (reuse existing parsing logic)
func parseStatusResponse(resp *resty.Response) (int64, time.Time, error) {
	// This would contain the same parsing logic as the original GetStatus function
	// For now, we'll return an error to implement later
	return 0, time.Time{}, fmt.Errorf("parseStatusResponse not implemented yet - use fallback")
}

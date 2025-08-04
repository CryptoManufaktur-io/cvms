package common

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestParallelRPCClient(t *testing.T) {
	// Create test logger
	logger := logrus.NewEntry(logrus.New())

	// Test endpoints (using akash endpoints)
	rpcEndpoints := []string{
		"https://rpc.lavenderfive.com:443/akash",
		"https://akash-rpc.polkachu.com:443",
	}
	apiEndpoints := []string{
		"https://akash-api.polkachu.com:443",
		"https://rest.lavenderfive.com:443/akash",
	}

	// Create parallel client
	client := NewParallelRPCClient(rpcEndpoints, apiEndpoints, "cosmos", logger)
	defer client.Stop()

	// Wait for initial health check
	t.Log("Waiting for initial health check...")
	time.Sleep(3 * time.Second)

	// Test getting healthy endpoints
	rpcCount, apiCount := client.GetHealthyEndpointsCount()
	t.Logf("Healthy endpoints: %d RPCs, %d APIs", rpcCount, apiCount)

	if rpcCount == 0 && apiCount == 0 {
		t.Skip("No healthy endpoints available for testing")
	}

	// Test multiple RPC calls to see load balancing
	if rpcCount > 0 {
		t.Log("Testing RPC load balancing...")
		for i := 0; i < 4; i++ {
			rpcClient, endpoint, err := client.GetNextRPCClient()
			if err != nil {
				t.Errorf("Failed to get RPC client on attempt %d: %v", i+1, err)
				continue
			}
			t.Logf("Attempt %d - Got RPC client for endpoint: %s", i+1, endpoint)

			// Test actual RPC call
			resp, err := rpcClient.R().Get("/status")
			if err != nil {
				t.Logf("RPC call failed for %s: %v", endpoint, err)
			} else {
				t.Logf("RPC call succeeded for %s (status: %d)", endpoint, resp.StatusCode())
			}
		}
	}

	// Test multiple API calls to see load balancing
	if apiCount > 0 {
		t.Log("Testing API load balancing...")
		for i := 0; i < 4; i++ {
			apiClient, endpoint, err := client.GetNextAPIClient()
			if err != nil {
				t.Errorf("Failed to get API client on attempt %d: %v", i+1, err)
				continue
			}
			t.Logf("Attempt %d - Got API client for endpoint: %s", i+1, endpoint)

			// Test actual API call
			resp, err := apiClient.R().Get("/cosmos/base/tendermint/v1beta1/node_info")
			if err != nil {
				t.Logf("API call failed for %s: %v", endpoint, err)
			} else {
				t.Logf("API call succeeded for %s (status: %d)", endpoint, resp.StatusCode())
			}
		}
	}

	// Test health check updates
	t.Log("Testing health check updates...")
	time.Sleep(1 * time.Second)

	// Force health check update
	client.updateHealthyEndpoints()

	newRpcCount, newApiCount := client.GetHealthyEndpointsCount()
	t.Logf("After manual health check: %d RPCs, %d APIs", newRpcCount, newApiCount)

	// Test random endpoint selection as well
	if rpcCount > 1 {
		t.Log("Testing random RPC selection...")
		endpoints := make(map[string]int)
		for i := 0; i < 6; i++ {
			_, endpoint, err := client.GetRandomRPCClient()
			if err != nil {
				t.Errorf("Failed to get random RPC client: %v", err)
				continue
			}
			endpoints[endpoint]++
		}
		t.Logf("Random RPC distribution: %v", endpoints)
	}
}

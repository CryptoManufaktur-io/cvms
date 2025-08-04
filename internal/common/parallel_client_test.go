package common

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestParallelRPCClient(t *testing.T) {
	// Create test logger
	logger := logrus.NewEntry(logrus.New())

	// Test endpoints (using public endpoints for testing)
	rpcEndpoints := []string{
		"https://rpc.cosmos.directory/cosmoshub",
		"https://cosmos-rpc.polkachu.com",
	}
	apiEndpoints := []string{
		"https://rest.cosmos.directory/cosmoshub",
		"https://cosmos-api.polkachu.com",
	}

	// Create parallel client
	client := NewParallelRPCClient(rpcEndpoints, apiEndpoints, "cosmos", logger)
	defer client.Stop()

	// Wait a moment for initial health check
	time.Sleep(2 * time.Second)

	// Test getting healthy endpoints
	rpcCount, apiCount := client.GetHealthyEndpointsCount()
	t.Logf("Healthy endpoints: %d RPCs, %d APIs", rpcCount, apiCount)

	if rpcCount == 0 && apiCount == 0 {
		t.Skip("No healthy endpoints available for testing")
	}

	// Test getting RPC client
	if rpcCount > 0 {
		rpcClient, endpoint, err := client.GetNextRPCClient()
		if err != nil {
			t.Fatalf("Failed to get RPC client: %v", err)
		}
		if rpcClient == nil {
			t.Fatal("RPC client is nil")
		}
		t.Logf("Got RPC client for endpoint: %s", endpoint)
	}

	// Test getting API client
	if apiCount > 0 {
		apiClient, endpoint, err := client.GetNextAPIClient()
		if err != nil {
			t.Fatalf("Failed to get API client: %v", err)
		}
		if apiClient == nil {
			t.Fatal("API client is nil")
		}
		t.Logf("Got API client for endpoint: %s", endpoint)
	}
}

func TestInjectiveEndpoints(t *testing.T) {
	// Create test logger
	logger := logrus.NewEntry(logrus.New())

	// Your specific Injective endpoints
	rpcEndpoints := []string{
		"http://57.129.140.17:26657",
		"https://injective-rpc.polkachu.com:443",
	}
	apiEndpoints := []string{
		"http://57.129.140.17:10337",
		"https://injective-api.polkachu.com:443",
	}

	// Create parallel client for Injective
	client := NewParallelRPCClient(rpcEndpoints, apiEndpoints, "cosmos", logger)
	defer client.Stop()

	// Wait for initial health check
	t.Log("Waiting for initial health check...")
	time.Sleep(3 * time.Second)

	// Test getting healthy endpoints
	rpcCount, apiCount := client.GetHealthyEndpointsCount()
	t.Logf("Injective healthy endpoints: %d RPCs, %d APIs", rpcCount, apiCount)

	if rpcCount == 0 && apiCount == 0 {
		t.Skip("No healthy Injective endpoints available for testing")
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
}

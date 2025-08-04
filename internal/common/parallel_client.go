package common

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cosmostation/cvms/internal/helper/healthcheck"
	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

// ParallelRPCClient manages multiple RPC endpoints and load balances requests
type ParallelRPCClient struct {
	rpcEndpoints []string
	apiEndpoints []string
	healthyRPCs  []string
	healthyAPIs  []string
	rpcIndex     int64
	apiIndex     int64
	clients      map[string]*resty.Client
	mu           sync.RWMutex
	logger       *logrus.Entry
	protocolType string

	// Health check interval
	healthCheckInterval time.Duration
	stopHealthCheck     chan bool
	isRunning          bool
}

// NewParallelRPCClient creates a new parallel RPC client
func NewParallelRPCClient(rpcEndpoints, apiEndpoints []string, protocolType string, logger *logrus.Entry) *ParallelRPCClient {
	client := &ParallelRPCClient{
		rpcEndpoints:        rpcEndpoints,
		apiEndpoints:        apiEndpoints,
		healthyRPCs:         make([]string, 0),
		healthyAPIs:         make([]string, 0),
		clients:             make(map[string]*resty.Client),
		logger:              logger,
		protocolType:        protocolType,
		healthCheckInterval: 30 * time.Second, // Check health every 30 seconds
		stopHealthCheck:     make(chan bool, 1),
		isRunning:          false,
	}

	// Create a logger that doesn't output to avoid spam
	restyLogger := logrus.New()
	restyLogger.SetOutput(logrus.StandardLogger().Out)
	restyLogger.SetLevel(logrus.ErrorLevel) // Only log errors

	// Initialize clients for each endpoint
	for _, endpoint := range rpcEndpoints {
		client.clients[endpoint] = resty.New().
			SetRetryCount(2).
			SetRetryWaitTime(100 * time.Millisecond).
			SetRetryMaxWaitTime(1 * time.Second).
			SetTimeout(5 * time.Second).
			SetBaseURL(endpoint).
			SetLogger(restyLogger)
	}

	for _, endpoint := range apiEndpoints {
		client.clients[endpoint] = resty.New().
			SetRetryCount(2).
			SetRetryWaitTime(100 * time.Millisecond).
			SetRetryMaxWaitTime(1 * time.Second).
			SetTimeout(5 * time.Second).
			SetBaseURL(endpoint).
			SetLogger(restyLogger)
	}

	// Initial health check
	client.updateHealthyEndpoints()

	// Start continuous health checking
	go client.continuousHealthCheck()

	return client
}

// GetNextRPCClient returns the next healthy RPC client using round-robin
func (p *ParallelRPCClient) GetNextRPCClient() (*resty.Client, string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.healthyRPCs) == 0 {
		return nil, "", errors.New("no healthy RPC endpoints available")
	}

	// Round-robin selection
	index := atomic.AddInt64(&p.rpcIndex, 1) % int64(len(p.healthyRPCs))
	endpoint := p.healthyRPCs[index]

	return p.clients[endpoint], endpoint, nil
}

// GetNextAPIClient returns the next healthy API client using round-robin
func (p *ParallelRPCClient) GetNextAPIClient() (*resty.Client, string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.healthyAPIs) == 0 {
		return nil, "", errors.New("no healthy API endpoints available")
	}

	// Round-robin selection
	index := atomic.AddInt64(&p.apiIndex, 1) % int64(len(p.healthyAPIs))
	endpoint := p.healthyAPIs[index]

	return p.clients[endpoint], endpoint, nil
}

// GetRandomRPCClient returns a random healthy RPC client
func (p *ParallelRPCClient) GetRandomRPCClient() (*resty.Client, string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.healthyRPCs) == 0 {
		return nil, "", errors.New("no healthy RPC endpoints available")
	}

	// Random selection
	index := rand.Intn(len(p.healthyRPCs))
	endpoint := p.healthyRPCs[index]

	return p.clients[endpoint], endpoint, nil
}

// GetRandomAPIClient returns a random healthy API client
func (p *ParallelRPCClient) GetRandomAPIClient() (*resty.Client, string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.healthyAPIs) == 0 {
		return nil, "", errors.New("no healthy API endpoints available")
	}

	// Random selection
	index := rand.Intn(len(p.healthyAPIs))
	endpoint := p.healthyAPIs[index]

	return p.clients[endpoint], endpoint, nil
}

// ParallelRequest executes the same request on multiple endpoints and returns the first successful response
func (p *ParallelRPCClient) ParallelRequest(ctx context.Context, requestFunc func(*resty.Client) (*resty.Response, error), useRPC bool) (*resty.Response, error) {
	p.mu.RLock()
	var endpoints []string
	if useRPC {
		endpoints = make([]string, len(p.healthyRPCs))
		copy(endpoints, p.healthyRPCs)
	} else {
		endpoints = make([]string, len(p.healthyAPIs))
		copy(endpoints, p.healthyAPIs)
	}
	p.mu.RUnlock()

	if len(endpoints) == 0 {
		return nil, errors.New("no healthy endpoints available")
	}

	// Use only first 2-3 endpoints to avoid overwhelming the network
	maxConcurrent := len(endpoints)
	if maxConcurrent > 3 {
		maxConcurrent = 3
	}

	type result struct {
		response *resty.Response
		err      error
		endpoint string
	}

	resultChan := make(chan result, maxConcurrent)

	// Start requests in parallel
	for i := 0; i < maxConcurrent; i++ {
		go func(endpoint string) {
			client := p.clients[endpoint]
			resp, err := requestFunc(client)
			resultChan <- result{response: resp, err: err, endpoint: endpoint}
		}(endpoints[i])
	}

	// Return first successful response
	var lastErr error
	for i := 0; i < maxConcurrent; i++ {
		select {
		case res := <-resultChan:
			if res.err == nil && res.response.StatusCode() < 400 {
				p.logger.Debugf("Parallel request succeeded using endpoint: %s", res.endpoint)
				return res.response, nil
			}
			lastErr = res.err
			p.logger.Debugf("Parallel request failed for endpoint %s: %v", res.endpoint, res.err)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, lastErr
}

// updateHealthyEndpoints checks health of all endpoints and updates healthy lists
func (p *ParallelRPCClient) updateHealthyEndpoints() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check RPC endpoints
	newHealthyRPCs := healthcheck.FilterHealthRPCEndpoints(p.rpcEndpoints, p.protocolType)

	// Check API endpoints
	newHealthyAPIs := healthcheck.FilterHealthEndpoints(p.apiEndpoints, p.protocolType)

	// Log changes
	if len(newHealthyRPCs) != len(p.healthyRPCs) {
		p.logger.Infof("Healthy RPC endpoints updated: %v -> %v", p.healthyRPCs, newHealthyRPCs)
	}
	if len(newHealthyAPIs) != len(p.healthyAPIs) {
		p.logger.Infof("Healthy API endpoints updated: %v -> %v", p.healthyAPIs, newHealthyAPIs)
	}

	p.healthyRPCs = newHealthyRPCs
	p.healthyAPIs = newHealthyAPIs
}

// continuousHealthCheck runs health checks in background
func (p *ParallelRPCClient) continuousHealthCheck() {
	p.isRunning = true
	ticker := time.NewTicker(p.healthCheckInterval)
	defer ticker.Stop()
	defer func() { p.isRunning = false }()

	for {
		select {
		case <-ticker.C:
			p.updateHealthyEndpoints()
		case <-p.stopHealthCheck:
			p.logger.Debug("Parallel RPC client health check stopped")
			return
		}
	}
}

// Stop stops the health checking goroutine
func (p *ParallelRPCClient) Stop() {
	if p.isRunning {
		p.stopHealthCheck <- true
	}
}

// GetHealthyEndpointsCount returns the count of healthy endpoints
func (p *ParallelRPCClient) GetHealthyEndpointsCount() (rpc int, api int) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.healthyRPCs), len(p.healthyAPIs)
}

// HasHealthyEndpoints returns true if there are healthy endpoints available
func (p *ParallelRPCClient) HasHealthyEndpoints() bool {
	rpcCount, apiCount := p.GetHealthyEndpointsCount()
	return rpcCount > 0 && apiCount > 0
}

// SetHealthCheckInterval allows customizing the health check interval
func (p *ParallelRPCClient) SetHealthCheckInterval(interval time.Duration) {
	p.healthCheckInterval = interval
}

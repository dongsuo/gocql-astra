// Copyright (c) DataStax, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gocqlastra

import (
	"net"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

const apacheAuthenticator = "org.apache.cassandra.auth.PasswordAuthenticator"
const dseAuthenticator = "com.datastax.bdp.cassandra.auth.DseAuthenticator"
const astraAuthenticator = "org.apache.cassandra.auth.AstraAuthenticator"

func NewClusterFromBundle(path, username, password string, timeout time.Duration) (*gocql.ClusterConfig, error) {
	dialer, err := NewDialerFromBundle(path, timeout)
	if err != nil {
		return nil, err
	}
	return NewCluster(dialer, username, password), nil
}

func NewClusterFromURL(url, databaseID, token string, timeout time.Duration) (*gocql.ClusterConfig, error) {
	dialer, err := NewDialerFromURL(url, databaseID, token, timeout)
	if err != nil {
		return nil, err
	}
	return NewCluster(dialer, "token", token), nil
}

func NewCluster(dialer gocql.HostDialer, username, password string) *gocql.ClusterConfig {
	// Add multiple fake contact points to make gocql call the dialer multiple times
	// The actual connection will be handled by the dialer, so these are just placeholders
	cluster := gocql.NewCluster("0.0.0.1", "0.0.0.2", "0.0.0.3")
	
	// Set the custom dialer that will handle the actual connection to Astra DB
	cluster.HostDialer = dialer

	// Keep the original IP addresses during translation
	cluster.AddressTranslator = gocql.AddressTranslatorFunc(func(addr net.IP, port int) (net.IP, int) {
		return addr, port
	})

	// Configure connection pool with round-robin policy
	cluster.PoolConfig = gocql.PoolConfig{HostSelectionPolicy: gocql.RoundRobinHostPolicy()}
	
	// Set up authentication with multiple allowed authenticators
	cluster.Authenticator = &gocql.PasswordAuthenticator{
		Username:              username,
		Password:              password,
		AllowedAuthenticators: []string{apacheAuthenticator, dseAuthenticator, astraAuthenticator},
	}
	
	// Explicitly set protocol version to avoid negotiation issues
	cluster.ProtoVersion = 4
	
	// Disable initial host lookup as we're using a custom dialer
	cluster.DisableInitialHostLookup = true
	
	// Set a reasonable reconnect interval
	cluster.ReconnectInterval = 30 * time.Second
	
	// Increase timeouts to handle potential network latency with Astra
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 30 * time.Second
	
	// Retry policy for better resilience
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 5}
	
	return cluster
}

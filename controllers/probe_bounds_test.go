/*
Copyright 2022 The sealos Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/labring/endpoints-operator/apis/network/v1beta1"
)

func TestClampProbeValue(t *testing.T) {
	tests := []struct {
		name         string
		value        int32
		defaultValue int32
		maximum      int32
		want         int32
	}{
		{name: "zero uses default", value: 0, defaultValue: 1, maximum: 60, want: 1},
		{name: "negative uses default", value: -7, defaultValue: 3, maximum: 60, want: 3},
		{name: "in range unchanged", value: 42, defaultValue: 1, maximum: 60, want: 42},
		{name: "too large truncated", value: math.MaxInt32, defaultValue: 1, maximum: 10, want: 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := clampProbeValue(tt.value, tt.defaultValue, tt.maximum); got != tt.want {
				t.Errorf("clampProbeValue(%d, %d, %d) = %d, want %d", tt.value, tt.defaultValue, tt.maximum, got, tt.want)
			}
		})
	}
}

func TestNormalizeProbeSettings(t *testing.T) {
	port := &v1beta1.ServicePort{
		TimeoutSeconds:   math.MaxInt32,
		SuccessThreshold: -1,
		FailureThreshold: math.MaxInt32,
	}
	normalizeProbeSettings(port)
	if port.TimeoutSeconds != maxTimeoutSeconds {
		t.Errorf("TimeoutSeconds = %d, want %d", port.TimeoutSeconds, maxTimeoutSeconds)
	}
	if port.SuccessThreshold != defaultSuccessThreshold {
		t.Errorf("SuccessThreshold = %d, want %d", port.SuccessThreshold, defaultSuccessThreshold)
	}
	if port.FailureThreshold != maxProbeThreshold {
		t.Errorf("FailureThreshold = %d, want %d", port.FailureThreshold, maxProbeThreshold)
	}

	zero := &v1beta1.ServicePort{}
	normalizeProbeSettings(zero)
	if zero.TimeoutSeconds != 1 || zero.SuccessThreshold != 1 || zero.FailureThreshold != 3 {
		t.Errorf("zero value normalization = %+v, want 1/1/3", zero)
	}
}

func TestNormalizePeriodSeconds(t *testing.T) {
	tests := []struct {
		in   int32
		want int32
	}{
		{in: -1, want: 0},
		{in: 0, want: 0},
		{in: 10, want: 10},
		{in: math.MaxInt32, want: maxPeriodSeconds},
	}
	for _, tt := range tests {
		if got := normalizePeriodSeconds(tt.in); got != tt.want {
			t.Errorf("normalizePeriodSeconds(%d) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

// TestConvertSubsetHugeThresholds ensures a large success threshold does not
// keep the probe loop running: the threshold is clamped to maxProbeThreshold.
func TestConvertSubsetHugeThresholds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse server url: %v", err)
	}
	port, err := strconv.Atoi(serverURL.Port())
	if err != nil {
		t.Fatalf("parse server port: %v", err)
	}

	cep := &v1beta1.ClusterEndpoint{
		Spec: v1beta1.ClusterEndpointSpec{
			PeriodSeconds: math.MaxInt32,
			Ports: []v1beta1.ServicePort{
				{
					Hosts: []string{"127.0.0.1"},
					Handler: v1beta1.Handler{
						HTTPGet: &v1beta1.HTTPGetAction{Path: "/", Scheme: "HTTP"},
					},
					TimeoutSeconds:   math.MaxInt32,
					SuccessThreshold: math.MaxInt32,
					FailureThreshold: math.MaxInt32,
					Name:             "default",
					Protocol:         "TCP",
					Port:             int32(port),
					TargetPort:       int32(port),
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	subsets, errs := clusterEndpointConvertEndpointSubset(ctx, cep, 1, nil)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("probe loop took %s, expected it to be bounded", elapsed)
	}
	if len(errs) != 0 {
		t.Fatalf("unexpected probe errors: %v", errs)
	}
	if len(subsets) != 1 {
		t.Fatalf("subsets = %d, want 1", len(subsets))
	}
}

// TestConvertSubsetNegativeValues ensures negative values are normalized and do
// not hang the probe loop.
func TestConvertSubsetNegativeValues(t *testing.T) {
	cep := &v1beta1.ClusterEndpoint{
		Spec: v1beta1.ClusterEndpointSpec{
			PeriodSeconds: -3,
			Ports: []v1beta1.ServicePort{
				{
					Hosts: []string{"127.0.0.1"},
					Handler: v1beta1.Handler{
						TCPSocket: &v1beta1.TCPSocketAction{Enable: true},
					},
					TimeoutSeconds:   -1,
					SuccessThreshold: -5,
					FailureThreshold: -2,
					Name:             "default",
					Protocol:         "TCP",
					Port:             1,
					TargetPort:       1,
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	subsets, errs := clusterEndpointConvertEndpointSubset(ctx, cep, 1, nil)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("probe loop took %s, expected it to be bounded", elapsed)
	}
	if len(errs) == 0 {
		t.Fatal("expected probe errors for a closed port")
	}
	if len(subsets) != 0 {
		t.Fatalf("subsets = %d, want 0", len(subsets))
	}
}

// TestConvertSubsetCanceledContext ensures a canceled reconcile context aborts
// probing quickly instead of blocking on unreachable hosts.
func TestConvertSubsetCanceledContext(t *testing.T) {
	cep := &v1beta1.ClusterEndpoint{
		Spec: v1beta1.ClusterEndpointSpec{
			Ports: []v1beta1.ServicePort{
				{
					Hosts: []string{"192.0.2.1"},
					Handler: v1beta1.Handler{
						TCPSocket: &v1beta1.TCPSocketAction{Enable: true},
					},
					TimeoutSeconds:   math.MaxInt32,
					SuccessThreshold: math.MaxInt32,
					FailureThreshold: math.MaxInt32,
					Name:             "default",
					Protocol:         "TCP",
					Port:             8443,
					TargetPort:       8443,
				},
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	subsets, errs := clusterEndpointConvertEndpointSubset(ctx, cep, 1, nil)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("canceled context probing took %s, expected immediate abort", elapsed)
	}
	if len(errs) == 0 {
		t.Fatal("expected a context error")
	}
	if len(subsets) != 0 {
		t.Fatalf("subsets = %d, want 0", len(subsets))
	}
}

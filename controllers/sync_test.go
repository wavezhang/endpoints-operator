/*
Copyright 2022 cuisongliu@qq.com.

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
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/labring/endpoints-operator/apis/network/v1beta1"
	"github.com/labring/endpoints-operator/utils/metrics"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Test_clusterEndpointConvertEndpointSubset(t *testing.T) {
	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	defer tcpListener.Close()
	go func() {
		for {
			conn, err := tcpListener.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	tcpPort := int32(tcpListener.Addr().(*net.TCPAddr).Port)

	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer httpServer.Close()
	httpPort := int32(httpServer.Listener.Addr().(*net.TCPAddr).Port)

	type args struct {
		cep         *v1beta1.ClusterEndpoint
		retry       int
		metricsinfo *metrics.MetricsInfo
	}
	tests := []struct {
		name  string
		args  args
		want  []corev1.EndpointSubset
		want1 []error
	}{
		{
			name: "default",
			args: args{
				cep: &v1beta1.ClusterEndpoint{
					TypeMeta: v1.TypeMeta{},
					ObjectMeta: v1.ObjectMeta{
						Name:      "",
						Namespace: "",
					},
					Spec: v1beta1.ClusterEndpointSpec{
						Ports: []v1beta1.ServicePort{
							{
								Hosts: []string{"127.0.0.1"},
								Handler: v1beta1.Handler{
									TCPSocket: &v1beta1.TCPSocketAction{Enable: true},
								},
								TimeoutSeconds:   1,
								SuccessThreshold: 1,
								FailureThreshold: 1,
								Name:             "default",
								Protocol:         "TCP",
								Port:             8080,
								TargetPort:       tcpPort,
							},
						},
						PeriodSeconds: 0,
					},
				},
				retry:       3,
				metricsinfo: nil,
			},
			want: []corev1.EndpointSubset{
				{
					Addresses: []corev1.EndpointAddress{{IP: "127.0.0.1"}},
					Ports:     []corev1.EndpointPort{{Name: "default", Port: tcpPort, Protocol: corev1.ProtocolTCP}},
				},
			},
			want1: nil,
		},
		{
			name: "endpoint",
			args: args{
				cep: &v1beta1.ClusterEndpoint{
					TypeMeta: v1.TypeMeta{},
					ObjectMeta: v1.ObjectMeta{
						Name:      "",
						Namespace: "",
					},
					Spec: v1beta1.ClusterEndpointSpec{
						Ports: []v1beta1.ServicePort{
							{
								Hosts: []string{"127.0.0.1"},
								Handler: v1beta1.Handler{
									HTTPGet: &v1beta1.HTTPGetAction{
										Path:   "/",
										Scheme: "HTTP",
									},
								},
								TimeoutSeconds:   1,
								SuccessThreshold: 1,
								FailureThreshold: 3,
								Name:             "default",
								Protocol:         "TCP",
								Port:             8848,
								TargetPort:       httpPort,
							},
						},
						PeriodSeconds: 0,
					},
				},
				retry:       3,
				metricsinfo: nil,
			},
			want: []corev1.EndpointSubset{
				{
					Addresses: []corev1.EndpointAddress{{IP: "127.0.0.1"}},
					Ports:     []corev1.EndpointPort{{Name: "default", Port: httpPort, Protocol: corev1.ProtocolTCP}},
				},
			},
			want1: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := clusterEndpointConvertEndpointSubset(context.Background(), tt.args.cep, tt.args.retry, tt.args.metricsinfo)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("clusterEndpointConvertEndpointSubset() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("clusterEndpointConvertEndpointSubset() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

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
	"fmt"
	"github.com/labring/endpoints-operator/utils/metrics"
	"strconv"
	"sync"

	"k8s.io/klog"

	"github.com/labring/endpoints-operator/apis/network/v1beta1"
	libv1 "github.com/labring/operator-sdk/api/core/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	defaultTimeoutSeconds   int32 = 1
	defaultSuccessThreshold int32 = 1
	defaultFailureThreshold int32 = 3

	maxTimeoutSeconds   int32 = 60
	maxProbeThreshold   int32 = 10
	maxPeriodSeconds    int32 = 3600
	maxProbeAttempts          = 10
	maxProbeConcurrency       = 16
)

// probeSemaphore bounds the total number of in-flight probes across all
// reconciles so a single large ClusterEndpoint cannot spawn an unbounded number
// of goroutines and sockets.
var probeSemaphore = make(chan struct{}, maxProbeConcurrency)

// clampProbeValue normalizes a user supplied probe value into a safe range.
// Zero and negative values fall back to the default, values above the upper
// bound are truncated to it.
func clampProbeValue(value, defaultValue, maximum int32) int32 {
	switch {
	case value <= 0:
		return defaultValue
	case value > maximum:
		return maximum
	default:
		return value
	}
}

// normalizeProbeSettings clamps the per-port probe settings copied from the CR.
func normalizeProbeSettings(port *v1beta1.ServicePort) {
	port.TimeoutSeconds = clampProbeValue(port.TimeoutSeconds, defaultTimeoutSeconds, maxTimeoutSeconds)
	port.SuccessThreshold = clampProbeValue(port.SuccessThreshold, defaultSuccessThreshold, maxProbeThreshold)
	port.FailureThreshold = clampProbeValue(port.FailureThreshold, defaultFailureThreshold, maxProbeThreshold)
}

// normalizePeriodSeconds clamps spec.periodSeconds. Zero keeps the historical
// "no periodic requeue" behaviour, negative values are treated as zero.
func normalizePeriodSeconds(period int32) int32 {
	if period <= 0 {
		return 0
	}
	if period > maxPeriodSeconds {
		return maxPeriodSeconds
	}
	return period
}

func (c *Reconciler) syncService(ctx context.Context, cep *v1beta1.ClusterEndpoint) {
	serviceCondition := v1beta1.Condition{
		Type:               v1beta1.SyncServiceReady,
		Status:             corev1.ConditionTrue,
		LastHeartbeatTime:  metav1.Now(),
		LastTransitionTime: metav1.Now(),
		Reason:             string(v1beta1.SyncServiceReady),
		Message:            "sync service successfully",
	}

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		svc := &corev1.Service{}
		svc.SetName(cep.Name)
		svc.SetNamespace(cep.Namespace)
		_, err := controllerutil.CreateOrUpdate(ctx, c.Client, svc, func() error {
			svc.Labels = cep.Labels
			svc.Annotations = cep.Annotations
			if err := controllerutil.SetControllerReference(cep, svc, c.scheme); err != nil {
				return err
			}
			if cep.Spec.ClusterIP == corev1.ClusterIPNone {
				svc.Spec.ClusterIP = corev1.ClusterIPNone
			}
			svc.Spec.Type = corev1.ServiceTypeClusterIP
			svc.Spec.SessionAffinity = corev1.ServiceAffinityNone
			svc.Spec.Ports = convertServicePorts(cep.Spec.Ports)
			return nil
		})
		return err
	}); err != nil {
		serviceCondition.LastHeartbeatTime = metav1.Now()
		serviceCondition.Status = corev1.ConditionFalse
		serviceCondition.Reason = "ServiceSyncError"
		serviceCondition.Message = err.Error()
		c.updateCondition(cep, serviceCondition)
		c.logger.V(4).Info("error updating service", "name", cep.Name, "msg", err.Error())
		return
	}
	if !isConditionTrue(cep, v1beta1.SyncServiceReady) {
		c.updateCondition(cep, serviceCondition)
	}
}
func (c *Reconciler) syncEndpoint(ctx context.Context, cep *v1beta1.ClusterEndpoint) {
	endpointCondition := v1beta1.Condition{
		Type:               v1beta1.SyncEndpointReady,
		Status:             corev1.ConditionTrue,
		LastHeartbeatTime:  metav1.Now(),
		LastTransitionTime: metav1.Now(),
		Reason:             string(v1beta1.SyncEndpointReady),
		Message:            "sync endpoint successfully",
	}
	var syncError error = nil
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {

		subsets, convertError := clusterEndpointConvertEndpointSubset(ctx, cep, c.RetryCount, c.MetricsInfo)

		if convertError != nil && len(convertError) != 0 {
			syncError = ToAggregate(convertError)
		}
		ep := &corev1.Endpoints{}
		ep.SetName(cep.Name)
		ep.SetNamespace(cep.Namespace)

		_, err := controllerutil.CreateOrUpdate(ctx, c.Client, ep, func() error {
			ep.Labels = map[string]string{}
			if err := controllerutil.SetControllerReference(cep, ep, c.scheme); err != nil {
				return err
			}
			ep.Subsets = subsets
			return nil
		})
		return err
	}); err != nil {
		endpointCondition.LastHeartbeatTime = metav1.Now()
		endpointCondition.Status = corev1.ConditionFalse
		endpointCondition.Reason = "EndpointSyncError"
		endpointCondition.Message = err.Error()
		c.updateCondition(cep, endpointCondition)
		c.logger.V(4).Info("error updating endpoint", "name", cep.Name, "msg", err.Error())
		return
	}
	if syncError != nil {
		endpointCondition.LastHeartbeatTime = metav1.Now()
		endpointCondition.Status = corev1.ConditionFalse
		endpointCondition.Reason = "EndpointSyncPortError"
		endpointCondition.Message = syncError.Error()
		c.updateCondition(cep, endpointCondition)
		c.logger.V(4).Info("error healthy endpoint", "name", cep.Name, "msg", syncError.Error())
		return
	}
	if !isConditionTrue(cep, v1beta1.SyncEndpointReady) {
		c.updateCondition(cep, endpointCondition)
	}
}

func clusterEndpointConvertEndpointSubset(ctx context.Context, cep *v1beta1.ClusterEndpoint, retry int, metricsinfo *metrics.MetricsInfo) ([]corev1.EndpointSubset, []error) {
	var wg sync.WaitGroup
	var mx sync.Mutex
	var data []corev1.EndpointSubset
	var errors []error
	var pointList []metrics.Point

	for _, p := range cep.Spec.Ports {
		for _, h := range p.Hosts {
			wg.Add(1)
			go func(port v1beta1.ServicePort, host string) {
				defer wg.Done()
				select {
				case probeSemaphore <- struct{}{}:
					defer func() { <-probeSemaphore }()
				case <-ctx.Done():
					mx.Lock()
					errors = append(errors, ctx.Err())
					mx.Unlock()
					return
				}
				normalizeProbeSettings(&port)
				pro := &libv1.Probe{
					TimeoutSeconds:   port.TimeoutSeconds,
					SuccessThreshold: port.SuccessThreshold,
					FailureThreshold: port.FailureThreshold,
				}
				if port.HTTPGet != nil {
					// add metrics point
					pointList = append(pointList, metrics.Point{
						Name:              cep.Name,
						Namespace:         cep.Namespace,
						TargetHostAndPort: host + ":" + strconv.Itoa(int(port.TargetPort)),
						ProbeType:         metrics.HTTP,
					})
					pro.HTTPGet = &libv1.HTTPGetAction{
						Path:        port.HTTPGet.Path,
						Port:        intstr.FromInt(int(port.TargetPort)),
						Host:        host,
						Scheme:      port.HTTPGet.Scheme,
						HTTPHeaders: port.HTTPGet.HTTPHeaders,
					}
				}
				if port.TCPSocket != nil && port.TCPSocket.Enable {
					// add metrics point
					pointList = append(pointList, metrics.Point{
						Name:              cep.Name,
						Namespace:         cep.Namespace,
						TargetHostAndPort: host + ":" + strconv.Itoa(int(port.TargetPort)),
						ProbeType:         metrics.TCP,
					})
					pro.TCPSocket = &libv1.TCPSocketAction{
						Port: intstr.FromInt(int(port.TargetPort)),
						Host: host,
					}
				}
				if port.UDPSocket != nil && port.UDPSocket.Enable {
					// add metrics point
					pointList = append(pointList, metrics.Point{
						Name:              cep.Name,
						Namespace:         cep.Namespace,
						TargetHostAndPort: host + ":" + strconv.Itoa(int(port.TargetPort)),
						ProbeType:         metrics.UDP,
					})
					pro.UDPSocket = &libv1.UDPSocketAction{
						Port: intstr.FromInt(int(port.TargetPort)),
						Host: host,
						Data: v1beta1.Int8ArrToByteArr(port.UDPSocket.Data),
					}
				}
				if port.GRPC != nil && port.GRPC.Enable {
					// add metrics point
					pointList = append(pointList, metrics.Point{
						Name:              cep.Name,
						Namespace:         cep.Namespace,
						TargetHostAndPort: host + ":" + strconv.Itoa(int(port.TargetPort)),
						ProbeType:         metrics.GRPC,
					})
					pro.GRPC = &libv1.GRPCAction{
						Port:    port.TargetPort,
						Host:    host,
						Service: port.GRPC.Service,
					}
				}
				// The number of attempts is capped by maxProbeAttempts so user
				// supplied thresholds cannot keep this loop running forever.
				w := &work{p: pro, retry: retry}
				attempts := int(pro.SuccessThreshold)
				if int(pro.FailureThreshold) > attempts {
					attempts = int(pro.FailureThreshold)
				}
				if attempts > maxProbeAttempts {
					attempts = maxProbeAttempts
				}
				keepGoing := true
				for i := 0; i < attempts; i++ {
					if err := ctx.Err(); err != nil {
						w.err = err
						keepGoing = false
						break
					}
					keepGoing = w.doProbe()
					if !keepGoing {
						break
					}
				}
				if keepGoing {
					// The attempt budget was exhausted before the configured
					// threshold was reached; fail closed instead of reporting
					// the host as healthy.
					w.err = fmt.Errorf("probe attempt budget of %d exceeded without reaching threshold", attempts)
				}
				mx.Lock()
				defer mx.Unlock()
				err := w.err

				var probe metrics.ProbeType
				if w.p.ProbeHandler.Exec != nil {
					probe = metrics.EXEC
				} else if w.p.ProbeHandler.HTTPGet != nil {
					probe = metrics.HTTP
				} else if w.p.ProbeHandler.TCPSocket != nil {
					probe = metrics.TCP
				} else if w.p.ProbeHandler.UDPSocket != nil {
					probe = metrics.UDP
				} else if w.p.ProbeHandler.GRPC != nil {
					probe = metrics.GRPC
				}
				klog.V(4).Info("[****] Probe is ", probe)

				if err != nil {
					// add metrics point
					if metricsinfo != nil {
						metricsinfo.RecordFailedCheck(cep.Name, cep.Namespace, host+":"+strconv.Itoa(int(port.TargetPort)), string(probe))
					}
					errors = append(errors, err)
				} else {
					// add metrics point
					if metricsinfo != nil {
						metricsinfo.RecordSuccessfulCheck(cep.Name, cep.Namespace, host+":"+strconv.Itoa(int(port.TargetPort)), string(probe))
					}
					data = append(data, port.ToEndpointSubset(host))
				}
			}(p, h)
		}
	}
	wg.Wait()
	for _, point := range pointList {
		if metricsinfo != nil {
			metricsinfo.RecordCheck(point.Name, point.Namespace, point.TargetHostAndPort, string(point.ProbeType))
		}
		//metricsinfo.RecordCeps(checkdata.NsName)
	}
	return data, errors
}

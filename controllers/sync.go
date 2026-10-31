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
	"errors"
	"fmt"

	"github.com/labring/endpoints-operator/utils/metrics"

	"strconv"
	"sync"

	"k8s.io/klog"

	"github.com/labring/endpoints-operator/apis/network/v1beta1"
	libv1 "github.com/labring/operator-sdk/api/core/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	// managedByLabelKey marks Service/Endpoints objects owned by this operator.
	// Objects without it are never adopted, so a ClusterEndpoint cannot hijack
	// manually maintained Services or Endpoints.
	managedByLabelKey   = "sealos.io/managed-by"
	managedByLabelValue = "endpoints-operator"
)

// errAdoptionRefused is returned when an existing Service or Endpoints object is
// not already managed by this operator.
var errAdoptionRefused = errors.New("refusing to adopt existing object")

// validateExistingObject fetches obj and reports whether it already exists. If
// it exists but is not managed by this operator (managed-by label plus the CEP
// as controller owner), adoption is refused.
func (c *Reconciler) validateExistingObject(ctx context.Context, obj client.Object, cep *v1beta1.ClusterEndpoint) (bool, error) {
	if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if obj.GetLabels()[managedByLabelKey] != managedByLabelValue {
		return true, fmt.Errorf("%w: existing %T %s/%s is missing label %s=%s", errAdoptionRefused,
			obj, obj.GetNamespace(), obj.GetName(), managedByLabelKey, managedByLabelValue)
	}
	if !metav1.IsControlledBy(obj, cep) {
		return true, fmt.Errorf("%w: existing %T %s/%s is not controlled by ClusterEndpoint %s/%s", errAdoptionRefused,
			obj, obj.GetNamespace(), obj.GetName(), cep.Namespace, cep.Name)
	}
	return true, nil
}

// reportSyncError records a failed sync condition and returns the error.
func (c *Reconciler) reportSyncError(cep *v1beta1.ClusterEndpoint, condition *v1beta1.Condition, reason string, err error, logMsg string) error {
	condition.LastHeartbeatTime = metav1.Now()
	condition.Status = corev1.ConditionFalse
	condition.Reason = reason
	condition.Message = err.Error()
	c.updateCondition(cep, *condition)
	c.logger.V(4).Info(logMsg, "name", cep.Name, "msg", err.Error())
	return err
}

func (c *Reconciler) syncService(ctx context.Context, cep *v1beta1.ClusterEndpoint) error {
	serviceCondition := v1beta1.Condition{
		Type:               v1beta1.SyncServiceReady,
		Status:             corev1.ConditionTrue,
		LastHeartbeatTime:  metav1.Now(),
		LastTransitionTime: metav1.Now(),
		Reason:             string(v1beta1.SyncServiceReady),
		Message:            "sync service successfully",
	}

	svc := &corev1.Service{}
	svc.SetName(cep.Name)
	svc.SetNamespace(cep.Namespace)
	existed, err := c.validateExistingObject(ctx, svc, cep)
	if err != nil {
		reason := "ServiceSyncError"
		if errors.Is(err, errAdoptionRefused) {
			reason = "AdoptionRefused"
			c.recorder.Eventf(cep, corev1.EventTypeWarning, "AdoptionRefused", "%v", err)
		}
		return c.reportSyncError(cep, &serviceCondition, reason, err, "error updating service")
	}
	if existed {
		c.logger.V(2).Info("adopting existing managed service", "namespace", cep.Namespace, "name", cep.Name)
		c.recorder.Eventf(cep, corev1.EventTypeNormal, "AdoptedExistingService", "Adopting existing Service %s/%s", cep.Namespace, cep.Name)
	}

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		_, err := controllerutil.CreateOrUpdate(ctx, c.Client, svc, func() error {
			labels := make(map[string]string, len(cep.Labels)+1)
			for k, v := range cep.Labels {
				labels[k] = v
			}
			labels[managedByLabelKey] = managedByLabelValue
			svc.Labels = labels
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
		return c.reportSyncError(cep, &serviceCondition, "ServiceSyncError", err, "error updating service")
	}
	if !isConditionTrue(cep, v1beta1.SyncServiceReady) {
		c.updateCondition(cep, serviceCondition)
	}
	return nil
}

func (c *Reconciler) syncEndpoint(ctx context.Context, cep *v1beta1.ClusterEndpoint) error {
	endpointCondition := v1beta1.Condition{
		Type:               v1beta1.SyncEndpointReady,
		Status:             corev1.ConditionTrue,
		LastHeartbeatTime:  metav1.Now(),
		LastTransitionTime: metav1.Now(),
		Reason:             string(v1beta1.SyncEndpointReady),
		Message:            "sync endpoint successfully",
	}

	ep := &corev1.Endpoints{}
	ep.SetName(cep.Name)
	ep.SetNamespace(cep.Namespace)
	existed, err := c.validateExistingObject(ctx, ep, cep)
	if err != nil {
		reason := "EndpointSyncError"
		if errors.Is(err, errAdoptionRefused) {
			reason = "AdoptionRefused"
			c.recorder.Eventf(cep, corev1.EventTypeWarning, "AdoptionRefused", "%v", err)
		}
		return c.reportSyncError(cep, &endpointCondition, reason, err, "error updating endpoint")
	}
	if existed {
		c.logger.V(2).Info("adopting existing managed endpoints", "namespace", cep.Namespace, "name", cep.Name)
		c.recorder.Eventf(cep, corev1.EventTypeNormal, "AdoptedExistingEndpoints", "Adopting existing Endpoints %s/%s", cep.Namespace, cep.Name)
	}

	var syncError error = nil
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {

		subsets, convertError := clusterEndpointConvertEndpointSubset(cep, c.RetryCount, c.MetricsInfo)

		if convertError != nil && len(convertError) != 0 {
			syncError = ToAggregate(convertError)
		}

		_, err := controllerutil.CreateOrUpdate(ctx, c.Client, ep, func() error {
			ep.Labels = map[string]string{managedByLabelKey: managedByLabelValue}
			if err := controllerutil.SetControllerReference(cep, ep, c.scheme); err != nil {
				return err
			}
			ep.Subsets = subsets
			return nil
		})
		return err
	}); err != nil {
		return c.reportSyncError(cep, &endpointCondition, "EndpointSyncError", err, "error updating endpoint")
	}
	if syncError != nil {
		return c.reportSyncError(cep, &endpointCondition, "EndpointSyncPortError", syncError, "error healthy endpoint")
	}
	if !isConditionTrue(cep, v1beta1.SyncEndpointReady) {
		c.updateCondition(cep, endpointCondition)
	}
	return nil
}

func clusterEndpointConvertEndpointSubset(cep *v1beta1.ClusterEndpoint, retry int, metricsinfo *metrics.MetricsInfo) ([]corev1.EndpointSubset, []error) {
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
				if port.TimeoutSeconds == 0 {
					port.TimeoutSeconds = 1
				}
				if port.SuccessThreshold == 0 {
					port.SuccessThreshold = 1
				}
				if port.FailureThreshold == 0 {
					port.FailureThreshold = 3
				}
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
				w := &work{p: pro, retry: retry}
				for w.doProbe() {
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

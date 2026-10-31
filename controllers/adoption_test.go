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
	"net"
	"testing"

	"github.com/go-logr/logr"
	"github.com/labring/endpoints-operator/apis/network/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newAdoptionTestReconciler(t *testing.T, objs ...client.Object) *Reconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 to scheme: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1 to scheme: %v", err)
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&v1beta1.ClusterEndpoint{}).
		Build()
	return &Reconciler{
		Client:   c,
		logger:   logr.Discard(),
		recorder: record.NewFakeRecorder(100),
		scheme:   scheme,
	}
}

func adoptionTestCEP(targetPort int32) *v1beta1.ClusterEndpoint {
	return &v1beta1.ClusterEndpoint{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mysql",
			Namespace: "default",
			UID:       types.UID("cep-uid"),
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
					Name:             "mysql",
					Protocol:         "TCP",
					Port:             3306,
					TargetPort:       targetPort,
				},
			},
		},
	}
}

func managedOwnerReference(cep *v1beta1.ClusterEndpoint) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{
		APIVersion: v1beta1.GroupVersion.String(),
		Kind:       "ClusterEndpoint",
		Name:       cep.Name,
		UID:        cep.UID,
		Controller: &controller,
	}
}

func findCondition(cep *v1beta1.ClusterEndpoint, conditionType v1beta1.ConditionType) (v1beta1.Condition, bool) {
	for _, cond := range cep.Status.Conditions {
		if cond.Type == conditionType {
			return cond, true
		}
	}
	return v1beta1.Condition{}, false
}

func TestSyncServiceRefusesManualService(t *testing.T) {
	cep := adoptionTestCEP(3306)
	manual := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: cep.Name, Namespace: cep.Namespace},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{Name: "mysql", Port: 3306}},
		},
	}
	r := newAdoptionTestReconciler(t, cep, manual)

	err := r.syncService(context.Background(), cep)
	if !errors.Is(err, errAdoptionRefused) {
		t.Fatalf("syncService() error = %v, want errAdoptionRefused", err)
	}

	got := &corev1.Service{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(manual), got); err != nil {
		t.Fatalf("get service: %v", err)
	}
	if len(got.OwnerReferences) != 0 {
		t.Errorf("manual service was adopted: ownerReferences = %v", got.OwnerReferences)
	}
	if len(got.Spec.Ports) != 1 || got.Spec.Ports[0].Port != 3306 {
		t.Errorf("manual service spec was modified: %+v", got.Spec.Ports)
	}
	cond, ok := findCondition(cep, v1beta1.SyncServiceReady)
	if !ok || cond.Reason != "AdoptionRefused" || cond.Status != corev1.ConditionFalse {
		t.Errorf("condition = %+v, want AdoptionRefused/False", cond)
	}
}

func TestSyncEndpointRefusesManualEndpoints(t *testing.T) {
	cep := adoptionTestCEP(3306)
	manual := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{Name: cep.Name, Namespace: cep.Namespace},
		Subsets: []corev1.EndpointSubset{
			{Addresses: []corev1.EndpointAddress{{IP: "10.0.0.1"}}},
		},
	}
	r := newAdoptionTestReconciler(t, cep, manual)

	err := r.syncEndpoint(context.Background(), cep)
	if !errors.Is(err, errAdoptionRefused) {
		t.Fatalf("syncEndpoint() error = %v, want errAdoptionRefused", err)
	}

	got := &corev1.Endpoints{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(manual), got); err != nil {
		t.Fatalf("get endpoints: %v", err)
	}
	if len(got.Subsets) != 1 || len(got.Subsets[0].Addresses) != 1 || got.Subsets[0].Addresses[0].IP != "10.0.0.1" {
		t.Errorf("manual endpoints subsets were modified: %+v", got.Subsets)
	}
	cond, ok := findCondition(cep, v1beta1.SyncEndpointReady)
	if !ok || cond.Reason != "AdoptionRefused" || cond.Status != corev1.ConditionFalse {
		t.Errorf("condition = %+v, want AdoptionRefused/False", cond)
	}
}

// TestReconcileSkipsEndpointWhenServiceRefused verifies that a refused Service
// sync short-circuits the reconcile so Endpoints are never written.
func TestReconcileSkipsEndpointWhenServiceRefused(t *testing.T) {
	cep := adoptionTestCEP(3306)
	manual := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: cep.Name, Namespace: cep.Namespace},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{Name: "mysql", Port: 3306}},
		},
	}
	r := newAdoptionTestReconciler(t, cep, manual)

	if _, err := r.reconcile(context.Background(), cep); !errors.Is(err, errAdoptionRefused) {
		t.Fatalf("reconcile() error = %v, want errAdoptionRefused", err)
	}

	ep := &corev1.Endpoints{}
	err := r.Get(context.Background(), types.NamespacedName{Name: cep.Name, Namespace: cep.Namespace}, ep)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("endpoints were written despite service sync failure: err = %v, subsets = %+v", err, ep.Subsets)
	}

	stored := &v1beta1.ClusterEndpoint{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(cep), stored); err != nil {
		t.Fatalf("get cluster endpoint: %v", err)
	}
	cond, ok := findCondition(stored, v1beta1.SyncServiceReady)
	if !ok || cond.Reason != "AdoptionRefused" {
		t.Errorf("persisted condition = %+v, want AdoptionRefused", cond)
	}
}

func TestSyncCreatesManagedObjects(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	cep := adoptionTestCEP(int32(listener.Addr().(*net.TCPAddr).Port))
	r := newAdoptionTestReconciler(t, cep)

	if err := r.syncService(context.Background(), cep); err != nil {
		t.Fatalf("syncService() error = %v", err)
	}
	svc := &corev1.Service{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(cep), svc); err != nil {
		t.Fatalf("get service: %v", err)
	}
	if svc.Labels[managedByLabelKey] != managedByLabelValue {
		t.Errorf("service labels = %v, want %s=%s", svc.Labels, managedByLabelKey, managedByLabelValue)
	}
	if !metav1.IsControlledBy(svc, cep) {
		t.Errorf("service is not controlled by the ClusterEndpoint")
	}

	if err := r.syncEndpoint(context.Background(), cep); err != nil {
		t.Fatalf("syncEndpoint() error = %v", err)
	}
	ep := &corev1.Endpoints{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(cep), ep); err != nil {
		t.Fatalf("get endpoints: %v", err)
	}
	if ep.Labels[managedByLabelKey] != managedByLabelValue {
		t.Errorf("endpoints labels = %v, want %s=%s", ep.Labels, managedByLabelKey, managedByLabelValue)
	}
	if !metav1.IsControlledBy(ep, cep) {
		t.Errorf("endpoints are not controlled by the ClusterEndpoint")
	}
}

func TestSyncAdoptsExistingManagedObjects(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	cep := adoptionTestCEP(int32(listener.Addr().(*net.TCPAddr).Port))
	managedSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            cep.Name,
			Namespace:       cep.Namespace,
			Labels:          map[string]string{managedByLabelKey: managedByLabelValue},
			OwnerReferences: []metav1.OwnerReference{managedOwnerReference(cep)},
		},
		Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "mysql", Port: 3306}}},
	}
	managedEp := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            cep.Name,
			Namespace:       cep.Namespace,
			Labels:          map[string]string{managedByLabelKey: managedByLabelValue},
			OwnerReferences: []metav1.OwnerReference{managedOwnerReference(cep)},
		},
		Subsets: []corev1.EndpointSubset{{Addresses: []corev1.EndpointAddress{{IP: "10.0.0.1"}}}},
	}
	r := newAdoptionTestReconciler(t, cep, managedSvc, managedEp)

	if err := r.syncService(context.Background(), cep); err != nil {
		t.Fatalf("syncService() error = %v", err)
	}
	if err := r.syncEndpoint(context.Background(), cep); err != nil {
		t.Fatalf("syncEndpoint() error = %v", err)
	}

	svc := &corev1.Service{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(cep), svc); err != nil {
		t.Fatalf("get service: %v", err)
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Name != "mysql" {
		t.Errorf("managed service was not reconciled: %+v", svc.Spec.Ports)
	}
	ep := &corev1.Endpoints{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(cep), ep); err != nil {
		t.Fatalf("get endpoints: %v", err)
	}
	if len(ep.Subsets) != 1 || len(ep.Subsets[0].Addresses) != 1 || ep.Subsets[0].Addresses[0].IP != "127.0.0.1" {
		t.Errorf("managed endpoints were not reconciled: %+v", ep.Subsets)
	}
}

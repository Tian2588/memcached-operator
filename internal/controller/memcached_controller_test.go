/*
Copyright 2026 Jenny.

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
package controller

import (
	"context"
	"testing"

	cachev1alpha1 "github.com/Tian2588/memcached-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	memcachedName      = "memcached-sample"
	memcachedNamespace = "default"
)

func TestMemcachedReconcileCreatesDeployment(t *testing.T) {
	s := runtime.NewScheme()
	_ = scheme.AddToScheme(s)
	_ = cachev1alpha1.AddToScheme(s)

	memcached := &cachev1alpha1.Memcached{
		ObjectMeta: metav1.ObjectMeta{
			Name:      memcachedName,
			Namespace: memcachedNamespace,
		},
		Spec: cachev1alpha1.MemcachedSpec{
			Size: 3,
		},
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(memcached).
		WithStatusSubresource(&cachev1alpha1.Memcached{}).
		Build()

	r := &MemcachedReconciler{
		Client: fakeClient,
		Scheme: s,
	}

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      memcachedName,
			Namespace: memcachedNamespace,
		},
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	dep := &appsv1.Deployment{}
	err := fakeClient.Get(context.Background(), types.NamespacedName{Name: memcachedName, Namespace: memcachedNamespace}, dep)
	if err != nil {
		t.Fatalf("cannot find deployment: %v", err)
	}
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 3 {
		t.Errorf("expected replicas 3, got %v", dep.Spec.Replicas)
	}
	if len(dep.OwnerReferences) == 0 {
		t.Error("expected owner reference on Deployment")
	}

	updated := &cachev1alpha1.Memcached{}
	if err := fakeClient.Get(context.Background(), req.NamespacedName, updated); err != nil {
		t.Fatalf("get memcached: %v", err)
	}
	if len(updated.Status.Conditions) == 0 {
		t.Error("expected status conditions to be set")
	}
}

func TestMemcachedReconcileNilReplicasDoesNotPanic(t *testing.T) {
	s := runtime.NewScheme()
	_ = scheme.AddToScheme(s)
	_ = cachev1alpha1.AddToScheme(s)

	memcached := &cachev1alpha1.Memcached{
		ObjectMeta: metav1.ObjectMeta{
			Name:      memcachedName,
			Namespace: memcachedNamespace,
		},
		Spec: cachev1alpha1.MemcachedSpec{
			Size: 2,
		},
	}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      memcachedName,
			Namespace: memcachedNamespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: nil,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "memcached",
						Image: "memcached:old",
					}},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(memcached, dep).
		WithStatusSubresource(&cachev1alpha1.Memcached{}).
		Build()

	r := &MemcachedReconciler{Client: fakeClient, Scheme: s}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: memcachedName, Namespace: memcachedNamespace}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	got := &appsv1.Deployment{}
	if err := fakeClient.Get(context.Background(), req.NamespacedName, got); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 2 {
		t.Errorf("expected replicas 2 after nil-pointer-safe update, got %v", got.Spec.Replicas)
	}
	if len(got.Spec.Template.Spec.Containers) == 0 || got.Spec.Template.Spec.Containers[0].Image != memcachedImage {
		t.Errorf("expected image %s, got %#v", memcachedImage, got.Spec.Template.Spec.Containers)
	}
}

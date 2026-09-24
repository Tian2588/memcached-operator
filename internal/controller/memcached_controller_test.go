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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types" // 新增
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	memcachedName      = "memcached-sample"
	memcachedNamespace = "default"
)

func TestMemcachedReconcile(t *testing.T) {
	// 1. 注册CRD scheme
	s := runtime.NewScheme()
	_ = scheme.AddToScheme(s)
	_ = cachev1alpha1.AddToScheme(s)

	// 2. 构造fake client
	memcached := &cachev1alpha1.Memcached{
		ObjectMeta: metav1.ObjectMeta{
			Name:      memcachedName,
			Namespace: memcachedNamespace,
		},
		Spec: cachev1alpha1.MemcachedSpec{
			Size: 3,
		},
	}
	// 预置资源到fake客户端
	fakeClient := fake.NewClientBuilder().WithObjects(memcached).WithScheme(s).Build()

	// 3. 构建Reconciler
	r := &MemcachedReconciler{
		Client: fakeClient,
		Scheme: s,
	}

	// 4. 执行Reconcile
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      memcachedName,
			Namespace: memcachedNamespace,
		},
	}
	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	// 5. 校验是否创建了Deployment
	dep := &appsv1.Deployment{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Name: memcachedName, Namespace: memcachedNamespace}, dep)
	if err != nil {
		t.Fatalf("cannot find deployment: %v", err)
	}
	if *dep.Spec.Replicas != 3 {
		t.Errorf("expected replicas 3, got %d", *dep.Spec.Replicas)
	}
}

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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/Tian2588/memcached-operator/api/v1alpha1"
)

const (
	TypeAvailable   = "Available"
	TypeProgressing = "Progressing"
)

// MemcachedReconciler reconciles a Memcached object
type MemcachedReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=cache.example.com,resources=memcacheds,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cache.example.com,resources=memcacheds/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cache.example.com,resources=memcacheds/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Memcached object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.0/pkg/reconcile
func (r *MemcachedReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 1. 获取Memcached CR实例
	memcached := &cachev1alpha1.Memcached{}
	if err := r.Get(ctx, req.NamespacedName, memcached); err != nil {
		if errors.IsNotFound(err) {
			log.Info("Memcached resource not found, ignore")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to fetch Memcached")
		return ctrl.Result{}, err
	}

	// 2. 查询对应的Deployment
	dep := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: memcached.Name, Namespace: memcached.Namespace}, dep)
	if err != nil {
		if errors.IsNotFound(err) {
			// Deployment不存在，新建
			dep = r.newDeploymentForMemcached(memcached)
			log.Info("Creating Deployment", "name", dep.Name)
			if err = r.Create(ctx, dep); err != nil {
				log.Error(err, "Failed to create Deployment")
				r.setCondition(memcached, TypeProgressing, metav1.ConditionFalse, "CreateFailed", "Create deployment failed")
				_ = r.Status().Update(ctx, memcached)
				return ctrl.Result{}, err
			}
			// 设置状态：正在创建
			r.setCondition(memcached, TypeProgressing, metav1.ConditionTrue, "Created", "Deployment created, waiting pods ready")
			_ = r.Status().Update(ctx, memcached)
			return ctrl.Result{Requeue: true}, nil
		}
		log.Error(err, "Failed get Deployment")
		return ctrl.Result{}, err
	}

	// 3. 副本数不一致，扩容/缩容
	wantSize := memcached.Spec.Size
	if *dep.Spec.Replicas != wantSize {
		log.Info("Updating Deployment", "name", dep.Name)
		dep.Spec.Replicas = &wantSize
		if err = r.Update(ctx, dep); err != nil {
			log.Error(err, "Update deployment replicas failed")
			return ctrl.Result{}, err
		}
		r.setCondition(memcached, TypeProgressing, metav1.ConditionTrue, "Scaling", "Scaling replicas")
		_ = r.Status().Update(ctx, memcached)
		return ctrl.Result{Requeue: true}, nil
	}

	// 4. 更新就绪副本状态
	memcached.Status.ReadyReplicas = dep.Status.ReadyReplicas

	// 5. 更新Conditions
	if dep.Status.ReadyReplicas == wantSize {
		r.setCondition(memcached, TypeAvailable, metav1.ConditionTrue, "Ready", "All replicas ready")
		r.setCondition(memcached, TypeProgressing, metav1.ConditionFalse, "Completed", "All replicas ready")
	} else {
		r.setCondition(memcached, TypeAvailable, metav1.ConditionFalse, "Pending", "Waiting for pods ready")
		r.setCondition(memcached, TypeProgressing, metav1.ConditionTrue, "Waiting", "Pods not fully ready")
	}

	// 提交Status更新
	if err = r.Status().Update(ctx, memcached); err != nil {
		log.Error(err, "Update status error")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// newDeploymentForMemcached 构造Deployment对象
func (r *MemcachedReconciler) newDeploymentForMemcached(m *cachev1alpha1.Memcached) *appsv1.Deployment {
	labels := map[string]string{
		"app":          "memcached",
		"memcached_cr": m.Name,
	}
	replicas := m.Spec.Size
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.Name,
			Namespace: m.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "memcached",
							Image: "memcached:1.6.26-alpine",
							Ports: []corev1.ContainerPort{
								{ContainerPort: 11211},
							},
						},
					},
				},
			},
		},
	}
	// 设置OwnerReference，级联删除
	ctrl.SetControllerReference(m, dep, r.Scheme)
	return dep
}

// setCondition 维护condition，避免重复添加同类型condition
func (r *MemcachedReconciler) setCondition(m *cachev1alpha1.Memcached, condType string, status metav1.ConditionStatus, reason, msg string) {
	newCond := metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            msg,
		LastTransitionTime: metav1.Now(),
	}
	foundIdx := -1
	for i, c := range m.Status.Conditions {
		if c.Type == condType {
			foundIdx = i
			break
		}
	}
	if foundIdx >= 0 {
		m.Status.Conditions[foundIdx] = newCond
	} else {
		m.Status.Conditions = append(m.Status.Conditions, newCond)
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *MemcachedReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1alpha1.Memcached{}).
		Owns(&appsv1.Deployment{}).
		Named("memcached").
		Complete(r)
}

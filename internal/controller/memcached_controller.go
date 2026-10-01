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
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/Tian2588/memcached-operator/api/v1alpha1"
)

const (
	TypeAvailable   = "Available"
	TypeProgressing = "Progressing"
	memcachedImage  = "memcached:1.6.26-alpine"
	memcachedPort   = 11211
	// 新增Finalizer标识
	memcachedFinalizer = "cache.example.com/finalizer"
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
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.0/pkg/reconcile
func (r *MemcachedReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	memcached := &cachev1alpha1.Memcached{}
	if err := r.Get(ctx, req.NamespacedName, memcached); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to fetch Memcached")
		return ctrl.Result{}, err
	}

	if memcached.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(memcached, memcachedFinalizer) {
			controllerutil.AddFinalizer(memcached, memcachedFinalizer)
			if err := r.Update(ctx, memcached); err != nil {
				logger.Error(err, "Failed to add finalizer")
				return ctrl.Result{}, err
			}
		}
	} else {
		// 父资源正在删除，执行清理逻辑
		if controllerutil.ContainsFinalizer(memcached, memcachedFinalizer) {
			logger.Info("Memcached CR is deleting, run cleanup logic")
			// 👉 这里可以放额外自定义清理逻辑（例如清理外部资源、自定义ConfigMap等）
			// test
			time.Sleep(15 * time.Second)
			logger.Info("Waiting for 15 seconds...")
			// Deployment 依靠OwnerReference由K8s GC自动回收，不需要手动Delete
			cm := &corev1.ConfigMap{}
			err := r.Get(ctx, types.NamespacedName{Name: memcached.Name, Namespace: memcached.Namespace}, cm)
			if err == nil {
				// 存在ConfigMap，则删除
				if err := r.Delete(ctx, cm); err != nil {
					logger.Error(err, "Failed to delete configmap during finalizer cleanup")
					return ctrl.Result{}, err
				}
				logger.Info("ConfigMap deleted successfully")
			} else if !errors.IsNotFound(err) {
				logger.Error(err, "Failed to get configmap for cleanup")
				return ctrl.Result{}, err
			}

			// 清理完成，移除finalizer，父资源才允许被删除
			latestMem := &cachev1alpha1.Memcached{}
			if err := r.Get(ctx, types.NamespacedName{Name: memcached.Name, Namespace: memcached.Namespace}, latestMem); err != nil {
				logger.Error(err, "Failed to fetch latest memcached before remove finalizer")
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(latestMem, memcachedFinalizer)
			if err := r.Update(ctx, latestMem); err != nil {
				logger.Error(err, "Failed to remove finalizer")
				return ctrl.Result{}, err
			}
			logger.Info("Finalizer removed, memcached CR can be deleted")
		}
		// finalizer移除后直接返回，不再创建/更新Deployment
		return ctrl.Result{}, nil
	}

	cm := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Name: memcached.Name, Namespace: memcached.Namespace}, cm)
	if err != nil && errors.IsNotFound(err) {
		cm, err = r.newConfigMapForMemcached(ctx, memcached)
		if err != nil {
			logger.Error(err, "Failed to build configmap")
			return ctrl.Result{}, err
		}
		logger.Info("Creating ConfigMap", "name", cm.Name)
		if err = r.Create(ctx, cm); err != nil {
			logger.Error(err, "Failed to create configmap")
			return ctrl.Result{}, err
		}
	} else if err != nil {
		logger.Error(err, "Failed to get configmap")
		return ctrl.Result{}, err
	}

	dep := &appsv1.Deployment{}
	err = r.Get(ctx, types.NamespacedName{Name: memcached.Name, Namespace: memcached.Namespace}, dep)
	if err != nil && errors.IsNotFound(err) {
		dep, err = r.newDeploymentForMemcached(ctx, memcached)
		if err != nil {
			logger.Error(err, "Failed to define Deployment for Memcached")
			if statusErr := r.patchStatus(ctx, memcached, nil, metav1.Condition{
				Type: TypeProgressing, Status: metav1.ConditionFalse, Reason: "CreateFailed", Message: err.Error(),
			}); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
			return ctrl.Result{}, err
		}
		logger.Info("Creating Deployment", "name", dep.Name)
		if err = r.Create(ctx, dep); err != nil {
			logger.Error(err, "Failed to create Deployment")
			if statusErr := r.patchStatus(ctx, memcached, dep, metav1.Condition{
				Type: TypeProgressing, Status: metav1.ConditionFalse, Reason: "CreateFailed", Message: "Failed to create Deployment",
			}); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.patchStatus(ctx, memcached, dep, metav1.Condition{
			Type: TypeProgressing, Status: metav1.ConditionTrue, Reason: "Created", Message: "Deployment created, waiting for Pods to become ready",
		})
	} else if err != nil {
		logger.Error(err, "Failed to get Deployment")
		return ctrl.Result{}, err
	}

	if needsUpdate(dep, memcached.Spec.Size) {
		logger.Info("Updating Deployment", "name", dep.Name)
		size := memcached.Spec.Size
		dep.Spec.Replicas = &size
		if len(dep.Spec.Template.Spec.Containers) > 0 {
			dep.Spec.Template.Spec.Containers[0].Image = memcachedImage
		}
		if err = r.Update(ctx, dep); err != nil {
			logger.Error(err, "Failed to update Deployment replicas")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.patchStatus(ctx, memcached, dep, metav1.Condition{
			Type: TypeProgressing, Status: metav1.ConditionTrue, Reason: "Scaling", Message: "Scaling replicas",
		})
	}

	if dep.Status.ReadyReplicas == memcached.Spec.Size {
		return ctrl.Result{}, r.patchStatus(ctx, memcached, dep,
			metav1.Condition{Type: TypeAvailable, Status: metav1.ConditionTrue, Reason: "Ready", Message: "All replicas ready"},
			metav1.Condition{Type: TypeProgressing, Status: metav1.ConditionFalse, Reason: "Completed", Message: "All replicas ready"},
		)
	}

	return ctrl.Result{}, r.patchStatus(ctx, memcached, dep,
		metav1.Condition{Type: TypeAvailable, Status: metav1.ConditionFalse, Reason: "Pending", Message: "Waiting for Pods to become ready"},
		metav1.Condition{Type: TypeProgressing, Status: metav1.ConditionTrue, Reason: "Waiting", Message: "Pods not fully ready"},
	)
}

func needsUpdate(dep *appsv1.Deployment, wantSize int32) bool {
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != wantSize {
		return true
	}
	if len(dep.Spec.Template.Spec.Containers) == 0 {
		return false
	}
	return dep.Spec.Template.Spec.Containers[0].Image != memcachedImage
}

func (r *MemcachedReconciler) patchStatus(ctx context.Context, m *cachev1alpha1.Memcached, dep *appsv1.Deployment, conditions ...metav1.Condition) error {
	// 先拷贝当前传入对象作为patch基准
	base := m.DeepCopy()

	if dep != nil {
		m.Status.ReadyReplicas = dep.Status.ReadyReplicas
	}
	for _, cond := range conditions {
		cond.ObservedGeneration = m.Generation
		meta.SetStatusCondition(&m.Status.Conditions, cond)
	}

	// MergeFrom 只提交差异部分，增量patch status
	patch := client.MergeFrom(base)
	if err := r.Status().Patch(ctx, m, patch); err != nil {
		log.FromContext(ctx).Error(err, "Failed to patch Memcached status")
		return err
	}
	return nil
}

// newDeploymentForMemcached constructs a Deployment owned by the Memcached CR.
func (r *MemcachedReconciler) newDeploymentForMemcached(ctx context.Context, m *cachev1alpha1.Memcached) (*appsv1.Deployment, error) {
	var memcached = "memcached"
	labels := map[string]string{
		"app":          memcached,
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
							Name:  memcached,
							Image: memcachedImage,
							Ports: []corev1.ContainerPort{
								{ContainerPort: memcachedPort, Name: memcached},
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("50m"),
									corev1.ResourceMemory: resource.MustParse("64Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("250m"),
									corev1.ResourceMemory: resource.MustParse("128Mi"),
								},
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(memcachedPort)},
								},
								InitialDelaySeconds: 5,
								PeriodSeconds:       10,
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(memcachedPort)},
								},
								InitialDelaySeconds: 15,
								PeriodSeconds:       20,
							},
						},
					},
				},
			},
		},
	}
	if err := ctrl.SetControllerReference(m, dep, r.Scheme); err != nil {
		log.FromContext(ctx).Error(err, "Failed to set controller reference")
		return nil, fmt.Errorf("set controller reference: %w", err)
	}
	return dep, nil
}

// newConfigMapForMemcached 创建附属ConfigMap，绑定ownerReference
func (r *MemcachedReconciler) newConfigMapForMemcached(ctx context.Context, m *cachev1alpha1.Memcached) (*corev1.ConfigMap, error) {
	labels := map[string]string{
		"app":          "memcached",
		"memcached_cr": m.Name,
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.Name,
			Namespace: m.Namespace,
			Labels:    labels,
		},
		Data: map[string]string{
			"memcached.conf": "max_memory=128m\n",
		},
	}
	// 设置属主引用，自动controller=true, blockOwnerDeletion=true
	if err := ctrl.SetControllerReference(m, cm, r.Scheme); err != nil {
		log.FromContext(ctx).Error(err, "Failed to set controller reference for configmap")
		return nil, fmt.Errorf("set controller reference cm: %w", err)
	}
	return cm, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MemcachedReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1alpha1.Memcached{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		Named("memcached").
		Complete(r)
}

# memcached-operator
Kubernetes Operator for Memcached, developed with Kubebuilder & Client-Go.

## Project Overview
Implement a custom Kubernetes Operator to manage Memcached workload lifecycle, based on Kubebuilder framework and Client-Go.
Define `Memcached` CRD (`cache.example.com/v1alpha1`), users can declare desired replica count via CR spec, the controller automatically creates & maintains Deployment for Memcached.

### Core capabilities:
1. Watch Memcached CR resources, reconcile to create/update underlying Deployment
2. Maintain resource status: ready replicas, conditions, etc.
3. OwnerReference management with `BlockOwnerDeletion=true`, demonstrate parent-child resource cascading deletion
4. Implement Finalizer to simulate cleanup logic for sub-resources, explore deletion blocking mechanism
5. Add controller unit test skeleton, golangci-lint code quality check, GitHub Actions CI pipeline
6. Develop & debug locally with Kind cluster, solve image pulling & architecture compatibility issues

## Tech Stack
- Golang, Client-Go
- Kubebuilder v4
- Kubernetes CRD / Custom Controller
- Kind (local K8s test cluster)
- golangci-lint
- Git & GitHub Actions

## Quick Start
### 1. Prerequisite
- Go 1.22+
- Kubebuilder
- Kind
- kubectl

### 2. Build & Install CRD
```bash
make generate
make manifests
make install
```

### 3. Run Controller locally
```bash
make run
```

### 4. Create Memcached CR
```bash
kubectl apply -f config/samples/cache_v1alpha1_memcached.yaml
```

### 5. Verify reosurce status
```bash
kubectl get memcacheds
kubectl get deploy,pod
```

### 6. Cleanup
```bash
kubectl delete -f config/samples/cache_v1alpha1_memcached.yaml
make uninstall
```


## Key Concepts Demonstrated
- Custom Resource Definition(CRD) and custom controller reconciliation logic
- `Owns()` to watch child resources (Deployment)
- OwnerReference, `BlockOwnerDeletion` cascading deletion
- Finalizer: intercept delete event, execute pre-clean logic
- Status subresource update & Condition design
- Controller error handling, RequeueAfter retry mechanism
- Go lint & unit test for controller

## Known Issues & Notes
- envtest may fail to download binaries in mainland network, can use FakeClient for unit test as alternative
- Kind cluster node image pull timeout: pre-load local images via `kind load docker-image`

## License
Apache License 2.0
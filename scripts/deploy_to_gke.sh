#!/bin/bash
# Deploy Kubernetes Job to GKE cluster and monitor execution
# Usage: ./deploy_to_gke.sh

set -e  # Exit on error

CLUSTER_NAME="gke-cluster-advanced"
CLUSTER_ZONE="us-central1-c"
PROJECT_ID="infrastructure-433717"

echo "========================================="
echo "Deploying to GKE cluster"
echo "========================================="
echo "Project: ${PROJECT_ID}"
echo "Cluster: ${CLUSTER_NAME}"
echo "Zone: ${CLUSTER_ZONE}"
echo ""

# Get cluster credentials
echo "Getting cluster credentials..."
gcloud container clusters get-credentials ${CLUSTER_NAME} \
  --zone ${CLUSTER_ZONE} \
  --project ${PROJECT_ID}

if [ $? -ne 0 ]; then
    echo "✗ Failed to get cluster credentials!"
    exit 1
fi

# Create GCP service account for the Kubernetes service account if it doesn't exist
echo "Creating GCP service account if needed..."
gcloud iam service-accounts create duckdb-analysis-sa \
  --display-name="DuckDB Analysis Service Account" \
  --project=${PROJECT_ID} 2>/dev/null || {
    echo "Service account already exists or creation failed (continuing...)"
}

# Grant GCS object viewer role to the service account
echo "Granting GCS permissions..."
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:duckdb-analysis-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer" \
  --condition=None 2>/dev/null || {
    echo "Permissions already granted or grant failed (continuing...)"
}

# Workload Identity binding (if using Workload Identity)
echo "Setting up Workload Identity binding..."
gcloud iam service-accounts add-iam-policy-binding \
  duckdb-analysis-sa@${PROJECT_ID}.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:${PROJECT_ID}.svc.id.goog[default/duckdb-analysis-sa]" \
  --project=${PROJECT_ID} 2>/dev/null || {
    echo "Workload Identity binding may already exist or failed (continuing...)"
}

# Apply Kubernetes manifests
echo ""
echo "Applying Kubernetes manifests..."
kubectl apply -f k8s/serviceaccount.yaml
kubectl apply -f k8s/job.yaml

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Job deployed successfully!"
    echo ""
    echo "Monitoring job execution..."
    echo "Press Ctrl+C to stop monitoring (job will continue running)"
    echo ""
    
    # Wait for job to start
    sleep 5
    
    # Monitor job
    kubectl wait --for=condition=ready pod -l app=duckdb-analysis --timeout=300s || true
    
    # Follow pod logs
    POD_NAME=$(kubectl get pods -l app=duckdb-analysis -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    if [ -n "$POD_NAME" ]; then
        echo "Following logs from pod: ${POD_NAME}"
        kubectl logs -f ${POD_NAME} || true
    else
        echo "Waiting for pod to be created..."
        kubectl get pods -l app=duckdb-analysis -w
    fi
else
    echo ""
    echo "✗ Job deployment failed!"
    exit 1
fi

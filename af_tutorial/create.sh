#!/bin/bash
vm_name="$1"
zone="$2"
machine_type="$3"

gcloud beta compute --project=figure-development-data instances create "$vm_name" \
    --zone="$zone" \
    --machine-type="$machine_type"\
    --subnet=default \
    --network-tier=PREMIUM \
    --maintenance-policy=MIGRATE \
    --service-account=747334062945-compute@developer.gserviceaccount.com \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --tags=http-server,https-server \
    --image-family=pytorch-1-8-cpu-ubuntu-1804 \
    --image-project=deeplearning-platform-release \
    --boot-disk-size=1000 \
    --boot-disk-type=pd-ssd \
    --boot-disk-device-name="$vm_name" \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --reservation-affinity=any \
    --metadata startup-script-url=gs://vm-setup/vm/startup.sh \
    --no-address

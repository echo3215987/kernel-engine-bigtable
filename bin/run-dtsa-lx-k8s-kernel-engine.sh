#!/usr/bin/env bash

set -x

log_dir=/mnt/fuse_cephfs/logs/rca/kernel-engine/$1

log_file=$log_dir/$HOSTNAME.log

mkdir -p $log_dir

/mnt/fuse_cephfs/data/spark-2.4.0/bin/spark-submit \
 --master k8s://192.168.1.63:6443 \
 --deploy-mode cluster \
 --name kernel-engine-$1 \
 --jars local:///mnt/fuse_cephfs/data/rca/kernel-engine/$1/jars/rcaKernelEngine.jar \
 --conf spark.driver.userClassPathFirst=true \
 --conf spark.executor.userClassPathFirst=true \
 --conf spark.executor.instances=3 \
 --conf spark.driver.memory=4g \
 --conf spark.executor.memory=4g \
 --conf spark.memory.offHeap.enabled=true \
 --conf spark.memory.offHeap.size=4g \
 --conf spark.kubernetes.container.image=10.57.232.169:8800/rca/kernel-engine:v1 \
 --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
 --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-$1 \
 --conf spark.kubernetes.namespace=rca-$1 \
 --conf spark.kubernetes.driver.volumes.hostPath.m1.mount.path=/mnt/fuse_cephfs/data \
 --conf spark.kubernetes.driver.volumes.hostPath.m1.mount.readOnly=false \
 --conf spark.kubernetes.driver.volumes.hostPath.m1.options.path=/mnt/fuse_cephfs/data \
 --conf spark.kubernetes.driver.volumes.hostPath.m1.options.type=Directory \
 --conf spark.kubernetes.executor.volumes.hostPath.m1.mount.path=/mnt/fuse_cephfs/data \
 --conf spark.kubernetes.executor.volumes.hostPath.m1.mount.readOnly=false \
 --conf spark.kubernetes.executor.volumes.hostPath.m1.options.path=/mnt/fuse_cephfs/data \
 --conf spark.kubernetes.executor.volumes.hostPath.m1.options.type=Directory \
 --conf spark.eventLog.dir=/mnt/fuse_cephfs/data/spark/eventLog \
 --conf spark.eventLog.enabled=true \
 --conf spark.files=/mnt/fuse_cephfs/data/rca/kernel-engine/$1/conf/default.yaml,/mnt/fuse_cephfs/data/rca/kernel-engine/$1/conf/config_rca-${1}.yaml \
 --conf spark.kubernetes.node.selector.spark=enabled \
 --class com.foxconn.iisd.bd.rca.KernelEngine local:///mnt/fuse_cephfs/data/rca/kernel-engine/$1/jars/rcaKernelEngine.jar /mnt/fuse_cephfs/data/rca/kernel-engine/$1/conf/default.yaml
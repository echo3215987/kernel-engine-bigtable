#!/usr/bin/env bash

set -x

#java -cp /opt/spark/extraJars/*:/opt/spark/jars/*:local:///mnt/fuse_cephfs/data/rca/kernel-engine/$1/jars/rcaKernelEngine.jar com.foxconn.iisd.bd.rca.KernelEngine /mnt/fuse_cephfs/data/rca/kernel-engine/$1/conf/default.yaml

/opt/spark/bin/spark-submit \
 --master k8s://192.168.1.63:6443 \
 --deploy-mode cluster \
 --conf spark.kubernetes.container.image=10.57.232.169:8800/spark/spark:2.4 \
 --conf spark.kubernetes.container.image.pullPolicy=Always \
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
 --conf spark.kubernetes.node.selector.spark=enabled \
 --conf spark.driver.extraClassPath=/opt/spark/extraJars/rca-dependencies-1.0.jar \
 --conf spark.executor.extraClassPath=/opt/spark/extraJars/rca-dependencies-1.0.jar \
 --class com.foxconn.iisd.bd.rca.KernelEngine local:///mnt/fuse_cephfs/data/rca/kernel-engine/$1/jars/rcaKernelEngine.jar /mnt/fuse_cephfs/data/rca/kernel-engine/$1/conf/default.yaml
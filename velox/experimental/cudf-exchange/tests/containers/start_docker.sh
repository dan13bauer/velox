#!/bin/bash

IMG=velox-dev-ucx-test-1.19.0.img:latest
NAME=velox-dev-ucx-test

docker run -d --rm -it --gpus all  --network=host --device /dev/infiniband/rdma_cm  \
       --device=/dev/infiniband/uverbs0 --device=/dev/infiniband/uverbs1 \
       --device=/dev/infiniband/uverbs2 --device=/dev/infiniband/uverbs3 \
       --device=/dev/infiniband/uverbs4 --device=/dev/infiniband/uverbs5 \
       --device=/dev/infiniband/uverbs6 --device=/dev/infiniband/uverbs7 \
       --device=/dev/infiniband/uverbs8 --device=/dev/infiniband/uverbs9 \
       --cap-add=IPC_LOCK \
       --shm-size=1g \
       --ulimit memlock=-1 \
       --ulimit stack=67108864 \
       --pid host \
       --name ${NAME} \
       --entrypoint='' \
       -v /gpfs/zc2/data/tpch/tpch-sf1-parquet/one_brc_parquet:/data \
       ${IMG} \
       tail -f /dev/null

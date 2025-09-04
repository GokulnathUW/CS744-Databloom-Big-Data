# -- Software Stack Version

SPARK_VERSION="3.3.1"
HADOOP_VERSION="3"
JUPYTERLAB_VERSION="3.6.1"

# -- Building the Images

sudo docker build \
  -f cluster-base.Dockerfile \
  -t cluster-base .

sudo docker build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg hadoop_version="${HADOOP_VERSION}" \
  -f spark-base.Dockerfile \
  -t spark-base .

sudo docker build \
  -f spark-master.Dockerfile \
  -t spark-master .

sudo docker build \
  -f spark-worker.Dockerfile \
  -t spark-worker .

sudo docker build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg jupyterlab_version="${JUPYTERLAB_VERSION}" \
  -f jupyterlab.Dockerfile \
  -t jupyterlab .

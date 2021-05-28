from airflow import DAG

# pylint: disable=E0401,C0411
import plutil as util


dag = DAG(
    "andrew-tutorial",
    default_args=util.default_args,
    description="tutorial",
    schedule_interval=None
)

cluster = util.RayGpuCluster(dag=dag, name="andrew-gpu-tmp", num_worker=2)

cstart = cluster.initialize(task_id="cluster-start")
cdebug = cluster.exec_cmd("/opt/conda/bin/python -m plnet.main --action=debug", task_id="exec-debug")
cdelete = cluster.delete(task_id="cluster-delete")

# pylint: disable=W0104
cstart >> cdebug >> cdelete

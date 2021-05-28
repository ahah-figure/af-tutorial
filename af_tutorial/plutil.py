import datetime
import os
from typing import Union

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

# pylint: disable=C0411
import vm_utils.main as vmain
import vm_utils.vm as vutil
import vm_utils.ray as cutil

REGION = "us-east4"
ZONE = REGION + "-b"

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'Data Team',
    'depends_on_past': False,
    'email': ['ahah@figure.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 4,
    'retry_delay': datetime.timedelta(minutes=3),
    'start_date': yesterday,
}


def await_start(task_id: str, dag: DAG, vm_name: str) -> PythonOperator:
    task = PythonOperator(
        task_id=task_id,
        python_callable=vmain.await_instance_up,
        op_kwargs={"vm_name": vm_name, "vm_zone": ZONE},
        dag=dag
    )
    return task


class ComputeBlock:

    def __init__(
        self,
        manager: Union[vutil.VmManager, cutil.ClusterManager],
        dag: DAG
    ):
        self.manager = manager
        self.dag = dag

    def initialize(self, task_id: str) -> PythonOperator:
        """
        Initialize compute block.
        """
        task = PythonOperator(
            task_id=task_id,
            python_callable=self.manager.initialize,
            dag=self.dag
        )
        return task

    def exec_cmd(self, cmd: str, task_id: str) -> PythonOperator:
        """
        Execute bash command on compute block.
        """
        task = PythonOperator(
            task_id=task_id,
            python_callable=self.manager.exec_cmd,
            op_kwargs={"cmd": cmd},
            dag=self.dag
        )
        return task

    def delete(self, task_id: str) -> PythonOperator:
        """
        Delete compute block.
        """
        task = PythonOperator(
            task_id=task_id,
            python_callable=self.manager.delete,
            dag=self.dag
        )
        return task


class RayCluster(ComputeBlock):

    def __init__(self, dag: DAG, name: str, num_gpu: int, num_worker: int):
        manager = cutil.ClusterManager(
            num_gpu=num_gpu,
            num_worker=num_worker,
            # ray_pwd=Variable.get("RAY_REDIS_PWD"),
            ray_pwd=os.getenv("RAY_REDIS_PWD"),
            name=name,
            region=REGION,
            zone=ZONE,
            initialize_env=False
        )
        super().__init__(manager=manager, dag=dag)


class RayComputeCluster(RayCluster):

    def __init__(self, dag: DAG, name: str, num_worker=10):
        super().__init__(dag=dag, name=name, num_gpu=0, num_worker=num_worker)


class RayGpuCluster(RayCluster):
    def __init__(self, dag: DAG, name: str, num_worker=4):
        super().__init__(dag=dag, name=name, num_gpu=4, num_worker=num_worker)


class SparkCluster(RayComputeCluster):
    def __init__(self, dag: DAG, name: str):
        super().__init__(dag=dag, name=name)


class ComputeVM(ComputeBlock):
    def __init__(self, dag: DAG, name: str):
        manager = vutil.VmManager(
            name=name,
            zone=ZONE,
            machine_type="n1-highmem-96",
            boot_disk_size=2000,
            initialize_env=False
        )
        super().__init__(manager=manager, dag=dag)

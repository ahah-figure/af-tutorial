import datetime
import os
import pathlib

import vm_utils.vm.instance_init as vu
import vm_utils.main as vmain

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

PROJ_DIR = pathlib.Path(__file__).parent.absolute()
CREATE_SCRIPT_PATH = os.path.join(PROJ_DIR, "create.sh")

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'Data Team',
    'depends_on_past': False,
    'email': ['ahah@figure.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=10),
    'start_date': yesterday,
}


class VM:

    def __init__(self, name: str, instance_type: str, zone: str, dag: DAG = None):
        self.name = name
        self.instance_type = instance_type
        self.zone = zone
        self.dag = dag

        self.vm_builder = vu.VmBuilder(
            name=self.name,
            zone=self.zone,
            machine_type=self.instance_type,
            boot_disk_size=1000
        )

    def start(self) -> PythonOperator:
        """
        Start VM and run startup script.
        Returns
        -------
        BashOperator
        """
        task_id = "start_instance"
        task = PythonOperator(
            task_id=task_id,
            python_callable=self.vm_builder.create_instance,
            dag=self.dag
        )
        return task

    def await_instance_up(self) -> PythonOperator:
        task_id = "await_instance"
        task = PythonOperator(
            task_id=task_id,
            python_callable=vmain.await_instance_up,
            op_kwargs={"vm_name": self.name, "vm_zone": self.zone},
            dag=self.dag
        )
        return task

    def execute_cmd(self, cmd: str, task_id: str) -> BashOperator:
        """
        Execute helocnet action on vm.
        Parameters
        ----------
        cmd: str

        Returns
        -------
        BashOperator
        """
        user = "andrewhah"
        cmd = f'gcloud compute ssh {user}@{self.name} --zone {self.zone} --tunnel-through-iap ' + \
            f'--command  "{cmd}"'
        return BashOperator(task_id=task_id, bash_command=cmd, dag=self.dag)

    def delete(self) -> BashOperator:
        """
        Delete a VM.
        Returns
        -------
        BashOperator
        """
        cmd = f"gcloud compute instances delete {self.name} --zone={self.zone} --quiet"
        task_id = "delete_instance"
        return BashOperator(task_id=task_id, bash_command=cmd, dag=self.dag)

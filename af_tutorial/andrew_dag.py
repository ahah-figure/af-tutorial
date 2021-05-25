import af_tutorial.util as util

from airflow import DAG


dag = DAG(
    "andrew-tutorial",
    default_args=util.default_args,
    description="tutorial",
    schedule_interval=None
)


vm = util.VM(
    name="andrew-tmp",
    instance_type="n1-standard-4",
    zone="us-central1-a",
    dag=dag
)

vm_start = vm.start()
vm_await = vm.await_instance_up()
vm_do_something = vm.execute_cmd(cmd="echo 'HELLOOOOOOOOOOOOOOOOOOOOOOOOO'", task_id="do_something")
vm_delete = vm.delete()

# pylint: disable=W0104
vm_start >> vm_await >> vm_do_something >> vm_delete

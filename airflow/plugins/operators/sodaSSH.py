import uuid
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.ssh.operators.ssh import SSHOperator

class SSHSodaOperator(BaseOperator):
    template_fields = ("check_path",)
    def __init__(self, task_id:str, 
                 check_path:str, 
                 ssh_conn_id:str, 
                 do_xcom_push:bool=False, 
                 *args,
                 **kwargs):
        super(SSHSodaOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.ssh_conn_id    = ssh_conn_id
        self.check_path     = check_path
        self.do_xcom_push   = do_xcom_push

    def execute(self, context):
        ssh_command = f"""
        /opt/spark/bin/spark-submit {self.check_path}
        """
        ssh_task = SSHOperator(
            task_id         =f"{self.task_id}_ssh_{uuid.uuid4().hex}",
            ssh_conn_id     =self.ssh_conn_id,
            command         =ssh_command,
            cmd_timeout     =None,
            dag             =self.dag,
            do_xcom_push    =self.do_xcom_push
        )
        ssh_task.execute(context)
import uuid
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.models import Variable

class SSHSparkOperator(BaseOperator):
    @apply_defaults
    def __init__(self, task_id:str, application_path:str, ssh_conn_id:str, *args, **kwargs):
        super(SSHSparkOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.application_path = application_path

    def execute(self, context):
        ssh_command = f"""
        source /etc/profile
        /opt/spark/bin/spark-submit {self.application_path}
        """
        ssh_task = SSHOperator(
            task_id=f"{self.task_id}_ssh_{uuid.uuid4().hex}",
            ssh_conn_id=self.ssh_conn_id,
            command=ssh_command,
            cmd_timeout=None,
            dag=self.dag
        )
        ssh_task.execute(context)
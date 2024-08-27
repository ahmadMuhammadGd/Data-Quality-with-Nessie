import uuid
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.models import Variable

class SSHSparkOperator(BaseOperator):
    template_fields = ("python_args",)
    def __init__(self, task_id:str, application_path:str, ssh_conn_id:str, python_args:str=None, *args, **kwargs):
        super(SSHSparkOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.application_path = application_path
        if python_args:
            self.python_args = python_args
        else:
            self.python_args = ''
    
    def execute(self, context):
        print(self.python_args)
        ssh_command = f"""
        export AWS_ACCESS_KEY_ID={Variable.get('MINIO_ACCESS_KEY')}
        export AWS_SECRET_ACCESS_KEY={Variable.get('MINIO_SECRET_KEY')}
        export AWS_REGION=us-east-1
        export AWS_DEFAULT_REGION=us-east-1
        /opt/spark/bin/spark-submit {self.application_path} {self.python_args}
        """
        ssh_task = SSHOperator(
            task_id=f"{self.task_id}_ssh_{uuid.uuid4().hex}",
            ssh_conn_id=self.ssh_conn_id,
            command=ssh_command,
            cmd_timeout=None,
            dag=self.dag,
        )
        ssh_output = ssh_task.execute(context)
        if ssh_output:
            context['task_instance'].xcom_push(key='output', value=ssh_output)

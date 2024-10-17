from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.ssh.hooks.ssh import SSHHook
from jinja2 import Template
import json

class SSHSparkOperator(BaseOperator):
    """
    SSHSparkOperator is an Airflow custom operator that allows for the execution of a Spark job on a remote machine 
    via SSH. It uses `SSHHook` to establish an SSH connection and submits a Spark job using the `spark-submit` command.

    Attributes:
    -----------
    ssh_conn_id : str
        The connection ID for the SSH connection as defined in Airflow's connections.
    application_path : str
        The path to the Spark application (Python script) that will be submitted.
    python_args : str, optional
        Arguments to be passed to the Spark job. Defaults to an empty string.

    Methods:
    --------
    execute(context):
        Executes the Spark job via SSH, constructs the SSH command, and runs it on the remote machine.
        Logs the output, error, and exit status of the command.

    Template Fields:
    ----------------
    template_fields : tuple
        The fields that support Jinja templating. In this case, the `python_args` field is templated.
    """
    template_fields = ("python_args",)
    
    @apply_defaults
    def __init__(self, task_id: str, application_path: str, ssh_conn_id: str, python_args: str = '', *args, **kwargs):
        super().__init__(task_id=task_id, *args, **kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.application_path = application_path
        self.python_args = python_args
    
    def execute(self, context):
        self.log.info("Preparing to execute Spark job with arguments: %s", self.python_args)
        
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        conn = ssh_hook.get_connection(self.ssh_conn_id)
        extras = conn.extra_dejson if conn.extra_dejson else {}
        
        env_vars_template = """
        {% for key, value in env_vars.items() %}
        export {{ key }}="{{ value }}"
        {% endfor %}
        """
        
        template = Template(env_vars_template)
        env_vars_command = template.render(env_vars=extras)
        
        spark_submit = f"/opt/spark/bin/spark-submit {self.application_path} {self.python_args}".replace("\n", " ")
        ssh_command = f"""
        {env_vars_command}

        export PYTHONPATH=/spark-container/spark:/config:/spark-container:/spark-container/soda$PYTHONPATH
        
        {spark_submit}
        """
        self.log.info("command: %s", ssh_command)
        
        try:
            with ssh_hook.get_conn() as ssh_client:
                _, stdout, stderr = ssh_client.exec_command(ssh_command)
                exit_status = stdout.channel.recv_exit_status()
                
                # Read the command output
                output = stdout.read().decode('utf-8')
                error_output = stderr.read().decode('utf-8')
                
                self.log.info("SSH command output: %s", output)
                # context['task_instance'].xcom_push(key='output', value=output)
                
                self.log.info("Exit status: %s", exit_status)
                if exit_status == 0:
                    self.log.info("Spark job completed successfully.")
                else:
                    self.log.error("Spark job failed.", error_output)
                    raise Exception(f"SSH command failed with exit status {exit_status}")
                
                return output
            
        except Exception as e:
            self.log.error("An error occurred: %s", str(e))
            raise

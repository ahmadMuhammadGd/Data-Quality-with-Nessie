from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.ssh.hooks.ssh import SSHHook
from jinja2 import Template
import json

class SSHSparkOperator(BaseOperator):
    template_fields = ("python_args",)
    @apply_defaults
    def __init__(self, task_id: str, application_path: str, ssh_conn_id: str, python_args: str = None, *args, **kwargs):
        super().__init__(task_id=task_id, *args, **kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.application_path = application_path
        self.python_args = python_args or ''
    
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
        
        self.log.info("env_vars_template: %s", env_vars_command)
        
        ssh_command = f"""
        {env_vars_command}
        export PYTHONPATH=/spark-container/spark:/config:/spark-container:/spark-container/soda$PYTHONPATH
        /opt/spark/bin/spark-submit {self.application_path} {self.python_args}
        """
        
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

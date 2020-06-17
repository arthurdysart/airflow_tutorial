# REQUIRED MODULES
import airflow

from airflow.operators.bash_operator import BashOperator
from datetime import datetime

from lib.create_json import Create
from lib.export_json import Export
from lib.itemize_s3_bucket import Itemize


# DAG DEFINITION
dag_context = {"dag_id": "transport_json_object_using_s3",

               "schedule_interval": "* 16 * * *",

               "default_args": {
                   "owner": "Airflow admin",
                   "start_date": datetime(2020, 6, 19),
                   "retries": 3,
                   "retry_delay": 1,
                   "max_active_runs": 1,
                   "depends_on_past": False,
                   "provide_context": True
               }
   }


# TASK DEFINITIONS
with airflow.DAG(**dag_context) as dag:

    # Create tasks
    create_task = BashOperator(task_id="create_json",
                               bash_command=Create().bash_command)

    export_task = BashOperator(task_id="export_json_to_s3",
                               bash_command=Export().bash_command)

    itemize_task = BashOperator(task_id="itemize_s3_bucket",
                                bash_command=Itemize().bash_command)

    # Set task dependencies
    create_task >> export_task >> itemize_task

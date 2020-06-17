import getpass
import json
import uuid

from datetime import datetime
from typing import *


class Create(object):

    def __init__(self, count=None):
        self.count = count or 1
        self.command_template = """
            mkdir -p $AIRFLOW_HOME/input \
                && echo '{all_json_objects}' >> $AIRFLOW_HOME/json/{filename}.json
            ;
        """

    @property
    def bash_command(self, count=None) -> str:
        """ Generate bash command to create new JSON object. """
        json_strings = []
        for num in range(0, count or self.count, 1):
            json_object = self.create_json_object()
            json_string = json.dumps(json_object)
            json_strings.append(json_string)

        return self.command_template.format(all_json_objects="\n".join(json_strings),
                                            filename=datetime.now().isoformat())

    @staticmethod
    def create_json_object() -> Dict[Any]:
        """ Generate new JSON object properties. """
        return {
            "datetime_now": datetime.now().isoformat(),
            "user_name": getpass.getuser(),
            "json_id": str(uuid.uuid4())
        }

class Itemize(object):

    def __init__(self, local_path=None, remote_path=None):
        self.local_path = local_path or "$AIRFLOW_HOME/json_manifest.txt"
        self.remote_path = remote_path or "s3://my_airflow_json_bucket/json/"
        self.command_template = """
            aws s3 ls --recursive {remote_path} | awk '{{print $4}}' > {local_path}
            ;
        """

    @property
    def bash_command(self, local_path=None, remote_path=None) -> str:
        """ Generate bash command to sync local directory to S3 bucket. """
        return self.command_template.format(local_path=local_path or self.local_path,
                                            remote_path=remote_path or self.remote_path)

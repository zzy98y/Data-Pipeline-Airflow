from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS json'{}'
    """
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 s3_json_key="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_json_key = s3_json_key
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info("Clearing Redshift table..")
        redshift.run("DELETE FROM {}".format(self.table))   
        

        self.log.info("Copying data from S3 to Redshift..")
        path_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, path_key)
        if self.s3_json_key != "auto":
            s3_json_path = "s3://{}/{}".format(self.s3_bucket, self.s3_json_key)
        else:
            s3_json_path = "auto"
        formatted_table = StageToRedshiftOperator(
            
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            s3_json_path
        )
        redshift.run(formatted_table)
        self.log.info(f"Table {self.table} has been staged.")





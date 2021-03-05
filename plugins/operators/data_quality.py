from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 table = [],
                 test = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table,
        self.test = test, 
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        
        # Connect to redshift 
        self.log.info("Establishing Redshift Connections") 
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id) 
        
        # Check for zero rows 
        for table in self.table:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"{table} table Data Quality Check - FAILED.")

            if records[0][0] < 1:
                raise ValueError(f"{table} table Data Quality Check - FAILED.")
            self.log.info(f"{table} table Data Quality Check - PASSED.")
            
            
            # custom test case
            for num, test in enumerate(self.test):
                name=test['name']
                sql=test['sql']
                exp_result=test['exp_result']
                  
            records = redshift.get_records(sql)
            actual_result = records[0][0]
            if actual_result != exp_result:
                self.log.error(f"Data quality test {name} FAILED. Expected result = {exp_result}, Actual result = {actual_result}")
                raise ValueError(f"Data quality test {name} FAILED. Expected result = {exp_result}, Actual result = {actual_result}")
            
            self.log.info(f"Data quality test {name} PASSED. Expected result = {exp_result}, Actual result = {actual_result}")
            
            
            
            
        
        
        
        
        
        
        
        
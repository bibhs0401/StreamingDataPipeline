import boto3

def lambda_handler(event, context):
    try:
        
        glue_job_name = 'bibhusha-load-redshift'
        
        # Set up the Glue client
        glue_client = boto3.client('glue')

        # Trigger the Glue job
        response = glue_client.start_job_run(JobName=glue_job_name)

        # Log the run ID for reference
        run_id = response['JobRunId']
        print(f"Started Glue job run with ID: {run_id}")

        return {
            'statusCode': 200,
            'body': f"Started Glue job run with ID: {run_id}"
        }
    except Exception as e:
        print(f"Error triggering Glue job: {e}")
        return {
            'statusCode': 500,
            'body': f"Error triggering Glue job: {e}"
        }


"""

    This is a simple stand-alone script (it requires only Bauplan installed and the relevant credentials)
    that showcase the WAP pattern in uploading parquet files to a lakehouse, i.e. Iceberg table backed by a catalog. 
    In particular, the script will:
    
    * Ingest data from an S3 source into an Iceberg table
    * Run quality checks on the data using Bauplan as a query engine and Arrow operations
    * Merge the branch into the main branch
    
    If dependencies are installed, just run: 
    
    python wap_flow.py --table_name <table_name> --branch_name <branch_name> --s3_path <s3_path>
    
    Note how much lighter the integration is compared to other datalake tools ;-)

"""


### IMPORTS
from datetime import datetime
import bauplan
from dbos import DBOS
import threading
import os


# initialize DBOS
DBOS()


@DBOS.step()
def source_to_iceberg_table(
    bauplan_client: bauplan.Client,
    table_name: str,
    namespace: str,
    source_s3_pattern: str,
    bauplan_ingestion_branch: str
):
    """
    
    Wrap the table creation and upload process in Bauplan.
    
    """
    # if the branch already exists, we delete it and create a new one
    if bauplan_client.has_branch(bauplan_ingestion_branch):
        bauplan_client.delete_branch(bauplan_ingestion_branch)
    
    # create the branch from main
    bauplan_client.create_branch(bauplan_ingestion_branch, from_ref='main')
    # we check if the branch is there (and learn a new API method ;-))
    assert bauplan_client.has_branch(bauplan_ingestion_branch), "Branch not found"
    # create namespace if it doesn't exist
    if not bauplan_client.has_namespace(namespace, ref=bauplan_ingestion_branch):
        bauplan_client.create_namespace(namespace, branch=bauplan_ingestion_branch)
    
    # now we create the table in the branch
    bauplan_client.create_table(
        table=table_name,
        search_uri=source_s3_pattern,
        namespace=namespace,
        branch=bauplan_ingestion_branch,
        # just in case the test table is already there for other reasons
        replace=True  
    )
    
    # we check if the table is there (and learn a new API method ;-))
    fq_name = f"{namespace}.{table_name}"
    assert bauplan_client.has_table(table=fq_name, ref=bauplan_ingestion_branch), "Table not found"
    is_imported = bauplan_client.import_data(
        table=table_name,
        search_uri=source_s3_pattern,
        namespace=namespace,
        branch=bauplan_ingestion_branch
    )

    return is_imported


@DBOS.step()
def run_quality_checks(
    bauplan_client: bauplan.Client,
    bauplan_ingestion_branch: str,
    namespace: str,
    table_name: str
) -> bool:
    """
    
    We check the data quality by running the checks in-process: we use 
    Bauplan SDK to query the data as an Arrow table, and check if the 
    target column is not null through vectorized PyArrow operations.
    
    """
    # we retrieve the data and check if the table is column has any nulls
    # make sure the column you're checking is in the table, so change this appropriately
    # if you're using a different dataset
    column_to_check = 'passenger_count'
    # NOTE:  you can interact with the lakehouse in pure Python (no SQL)
    # and still back an Arrow table (in this one column) through a performant scan.
    DBOS.logger.info("Perform a S3 columnar scan on the column {}".format(column_to_check))
    wap_table = bauplan_client.scan(
        table=table_name,
        ref=bauplan_ingestion_branch,
        namespace=namespace,
        columns=[column_to_check]
    )
    DBOS.logger.info("Read the table successfully!")
    # we return a boolean, True if the quality check passed, False otherwise
    return wap_table[column_to_check].null_count > 0


@DBOS.step()
def merge_branch(
    bauplan_client: bauplan.Client,
    bauplan_ingestion_branch: str
):
    """
    
    We merge the ingestion branch into the main branch. If this succeed,
    the transaction itself is considered successful.
    
    """
    # we merge the branch into the main branch
    return bauplan_client.merge_branch(
        source_ref=bauplan_ingestion_branch,
        into_branch='main'
    )


@DBOS.step()
def delete_branch(
    bauplan_client: bauplan.Client, 
    ingestion_branch: str
):
    """
    
    We delete the branch to avoid clutter!
    
    """
    return bauplan_client.delete_branch(ingestion_branch)


@DBOS.scheduled("*/1 * * * *")
@DBOS.workflow()
def wap_with_bauplan(
    scheduled_time, actual_time
):
    """
    Run the WAP ingestion pipeline using Bauplan in a DBOS workflow
    leveraging the new concept of transactions:
    
    """
    table_name = os.getenv("TABLE_NAME")
    branch_name = os.getenv("BRANCH_NAME")
    s3_path = os.getenv("S3_PATH")
    namespace = os.getenv("NAMESPACE")
    
    DBOS.logger.info(f"Starting the WAP flow with the following parameters: {table_name}, {branch_name}")
    DBOS.logger.info(f"We scheduled the flow for {scheduled_time} and run it at {actual_time}")
    bauplan_client = bauplan.Client()
    ### THIS IS THE WRITE
    # first, ingest data from the s3 source into a table the Bauplan branch
    source_to_iceberg_table(
        bauplan_client,
        table_name, 
        namespace,
        s3_path,
        branch_name
    )
    ### THIS IS THE AUDIT
    # we query the table in the branch and check we have no nulls
    is_check_passed = run_quality_checks(
        bauplan_client,
        branch_name,
        namespace=namespace,
        table_name=table_name
    )
    # NOTE: we use normal Python flow to define what happens in case of failure
    if not is_check_passed:
        # as an example of "roll-back", we clean up the branch
        # and immediately get back to the state of the lakehouse
        # before the ingestion!
        DBOS.logger.info("Check failed, cleaning up the branch")
        delete_branch(
            bauplan_client,
            branch_name
        )
        DBOS.logger.error(f"Flow failed, clean-up done at {datetime.now()}!")
        return
    # THIS IS THE PUBLISH 
    # finally, we merge the branch into the main branch if the quality checks passed
    merge_branch(
        bauplan_client,
        branch_name
    )
    # clean up the branch
    delete_branch(
        bauplan_client,
        branch_name
    )
    # say goodbye
    DBOS.logger.info(f"All done at {datetime.now()}, see you, space cowboy.")

    return


if __name__ == "__main__":
    DBOS.launch()
    threading.Event().wait()
    

import configparser
import sys


import uuid
from lib.Utils import *
from lib.dataloader import *
from lib.transformations import *
from lib.logger import *

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]
    job_run_id = "SBDL-" + str(uuid.uuid4())
    print("Initializing SBDL Job in " + job_run_env + " Job ID: " + job_run_id)

    conf = get_app_config(job_run_env)
    enable_hive = True if conf.get("enable.hive") == "true" else False
    hive_db = conf.get("hive.database")

    print("creating spark session")

    spark = get_spark_session(job_run_env)
    logger = Log4j(spark)

    logger.info("Finished creating Spark Session")

    logger.info("Reading SBDL Accounts DF")
    accounts_df = load_accounts(spark, job_run_env, enable_hive, hive_db)
    logger.info("Applying Transformations to SBDL Accounts DF")
    contract_df = get_contract(accounts_df)

    logger.info("Reading SBDL Party DF")
    party_df = load_parties(spark, job_run_env, enable_hive, hive_db)
    logger.info("Applying Transformations to SBDL Party DF")
    relations_df = get_party(party_df)

    logger.info("Reading SBDL Party Address DF")
    party_address_df = load_address(spark, job_run_env, enable_hive, hive_db)
    logger.info("Applying Transformations to SBDL Party Address DF")
    party_address_tr_df = get_party_address(party_address_df)

    logger.info("Joining Party and Address DFs")
    party_address_join_df = join_party_address(relations_df, party_address_tr_df)

    logger.info("Joining Party and contract DFs")
    party_contract_join_df = join_account_party(contract_df, party_address_join_df)

    logger.info("create payload")
    payload_df = create_payload(spark, party_contract_join_df)

    payload_df.show()

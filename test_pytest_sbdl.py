from datetime import datetime, date

import pytest
from pyspark import Row
from pyspark.sql.types import StructType, StructField, StringType, NullType, TimestampType, ArrayType, DateType

from lib.Utils import get_spark_session, get_app_config
from lib.dataloader import load_accounts, load_parties, load_address
from lib.transformations import get_contract, get_party, get_party_address, join_party_address, join_account_party, \
    create_payload
from chispa import assert_df_equality


@pytest.fixture(scope='session')
def spark():
    return get_spark_session("LOCAL")

@pytest.fixture(scope='session')
def expected_party_rows():
    return [Row(load_date=date(2022, 8, 2), account_id='6982391060', party_id='9823462810', relation_type='F-N', relation_start_date=datetime(2019, 7, 29, 2, 51, 32)),
            Row(load_date=date(2022, 8, 2), account_id='6982391061', party_id='9823462811', relation_type='F-N', relation_start_date=datetime(2018, 8, 31, 1, 57, 22)),
            Row(load_date=date(2022, 8, 2), account_id='6982391062', party_id='9823462812', relation_type='F-N', relation_start_date=datetime(2018, 8, 25, 12, 20, 29)),
            Row(load_date=date(2022, 8, 2), account_id='6982391063', party_id='9823462813', relation_type='F-N', relation_start_date=datetime(2018, 5, 11, 3, 53, 28)),
            Row(load_date=date(2022, 8, 2), account_id='6982391064', party_id='9823462814', relation_type='F-N', relation_start_date=datetime(2019, 6, 6, 10, 48, 12)),
            Row(load_date=date(2022, 8, 2), account_id='6982391065', party_id='9823462815', relation_type='F-N', relation_start_date=datetime(2019, 5, 4, 1, 42, 37)),
            Row(load_date=date(2022, 8, 2), account_id='6982391066', party_id='9823462816', relation_type='F-N', relation_start_date=datetime(2019, 5, 15, 7, 9, 29)),
            Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462817', relation_type='F-N', relation_start_date=datetime(2018, 5, 16, 6, 23, 4)),
            Row(load_date=date(2022, 8, 2), account_id='6982391068', party_id='9823462818', relation_type='F-N', relation_start_date=datetime(2017, 11, 26, 20, 50, 12)),
            Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462820', relation_type='F-S', relation_start_date=datetime(2017, 11, 20, 9, 48, 5)),
            Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462821', relation_type='F-S', relation_start_date=datetime(2018, 7, 19, 15, 26, 57))]

@pytest.fixture(scope='session')
def expected_contract_df(spark):
    schema = StructType([StructField('account_id', StringType()),
                         StructField('contractIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('sourceSystemIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contactStartDateTime',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', TimestampType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractTitle',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             ArrayType(StructType(
                                                                 [StructField('contractTitleLineType', StringType()),
                                                                  StructField('contractTitleLine', StringType())]))),
                                                 StructField('oldValue', NullType())])),
                         StructField('taxIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             StructType([StructField('taxIdType', StringType()),
                                                                         StructField('taxId', StringType())])),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractBranchCode',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractCountry',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())]))])
    return spark.read.format("json").schema(schema).load("test_data/results/contract_df.json")

@pytest.fixture(scope='session')
def expected_final_df(spark):
    schema = StructType(
        [StructField('keys',
                     ArrayType(StructType([StructField('keyField', StringType()),
                                           StructField('keyValue', StringType())])),
                    ),
         StructField('payload',
                     StructType([
                         StructField('contractIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('sourceSystemIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contactStartDateTime',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', TimestampType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractTitle',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', ArrayType(
                                                     StructType([StructField('contractTitleLineType', StringType()),
                                                                 StructField('contractTitleLine', StringType())]))),
                                                 StructField('oldValue', NullType())])),
                         StructField('taxIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             StructType([StructField('taxIdType', StringType()),
                                                                         StructField('taxId', StringType())])),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractBranchCode',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractCountry',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('partyRelations',
                                     ArrayType(StructType([
                                         StructField('partyIdentifier',
                                                     StructType([
                                                         StructField('operation', StringType()),
                                                         StructField('newValue', StringType()),
                                                         StructField('oldValue', NullType())])),
                                         StructField('partyRelationshipType',
                                                     StructType([
                                                         StructField('operation', StringType()),
                                                         StructField('newValue', StringType()),
                                                         StructField('oldValue', NullType())])),
                                         StructField('partyRelationStartDateTime',
                                                     StructType([
                                                         StructField('operation', StringType()),
                                                         StructField('newValue', TimestampType()),
                                                         StructField('oldValue', NullType())])),
                                         StructField('partyAddress',
                                                     StructType([StructField('operation', StringType()),
                                                                 StructField(
                                                                     'newValue',
                                                                     StructType(
                                                                         [StructField('addressLine1', StringType()),
                                                                          StructField('addressLine2', StringType()),
                                                                          StructField('addressCity', StringType()),
                                                                          StructField('addressPostalCode',
                                                                                      StringType()),
                                                                          StructField('addressCountry', StringType()),
                                                                          StructField('addressStartDate', DateType())
                                                                          ])),
                                                                 StructField('oldValue', NullType())]))])))]))])
    return spark.read.format("json").schema(schema).load("test_data/results/final_df.json") \
        .select("keys", "payload")


def test_get_config():
    conf_local = get_app_config("LOCAL")
    conf_qa = get_app_config("QA")

    assert conf_local["kafka.topic"] == "sbdl_kafka_cloud"
    assert conf_qa["kafka.topic"] == "sbdl_kafka_qa"


def test_blank_test(spark):
    print(spark.version)
    assert spark.version == "3.4.4"

def test_read_accounts(spark):
    accounts_df = load_accounts(spark, "LOCAL", False, None)
    assert accounts_df.count() == 9

def test_read_parties(spark, expected_party_rows):
    actual_party_rows = load_parties(spark, "LOCAL", False, None).collect()
    assert actual_party_rows == expected_party_rows

def test_get_contract(spark, expected_contract_df):
    contract_df = load_accounts(spark, "LOCAL", False, None)
    actual_contract_df = get_contract(contract_df)
    assert actual_contract_df.collect() == expected_contract_df.collect()
    assert_df_equality(expected_contract_df, actual_contract_df, ignore_nullable=True)

def test_final_df(spark, expected_final_df):
    accounts_df = load_accounts(spark, "LOCAL", False, None)
    contract_df = get_contract(accounts_df)
    party_df = load_parties(spark, "LOCAL", False, None)
    relations_df = get_party(party_df)
    party_address_df = load_address(spark, "LOCAL", False, None)
    address_df = get_party_address(party_address_df)
    party_address_join_df = join_party_address(relations_df, address_df)
    join_account_df = join_account_party(contract_df, party_address_join_df)
    actual_final_df = create_payload(spark, join_account_df) \
                .select("keys", "payload")
    assert_df_equality(actual_final_df, expected_final_df, ignore_nullable=True)

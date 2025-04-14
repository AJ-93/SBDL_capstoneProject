
from pyspark.sql.functions import *


def get_insert_function(column, alias):
    return struct(lit("INSERT").alias("operation"),
                  column.alias("newValue"),
                  lit(None).alias("oldValue")
                  ).alias(alias)

def get_contract(df):
    contract_title = array(when(~isnull("legal_title_1"),
                                struct(
                                    lit("lgl_ttl_ln_1").alias("contractTitleLineType"),
                                    col("legal_title_1").alias("contractTitleLine"),
                                ).alias("contractTitle")
                                ),
                           when(~isnull("legal_title_2"),
                                struct(
                                    lit("lgl_ttl_ln_2").alias("contractTitleLineType"),
                                    col("legal_title_2").alias("contractTitleLine")
                                ).alias("contractTitle")
                                ),

    )

    contract_title_non_null_line = filter(contract_title, lambda x: ~isnull(x))

    tax_identifier = struct(col("tax_id_type").alias("taxIdType"),
                            col("tax_id").alias("taxId")
                            ).alias("taxIdentifier")

    return df.select(
                "account_id",get_insert_function(col("account_id"), "contractIdentifier"),
                get_insert_function(col("source_sys"), "sourceSystemIdentifier"),
                get_insert_function(col("account_start_date"), "contactStartDateTime"),
                get_insert_function(contract_title_non_null_line, "contractTitle"),
                get_insert_function(tax_identifier, "taxIdentifier"),
                get_insert_function(col("branch_code"), "contractBranchCode"),
                get_insert_function(col("country"), "contractCountry"),

    )

def get_party(df):
    return df.select(
        "account_id","party_id",
        get_insert_function(col("party_id"), "partyIdentifier"),
        get_insert_function(col("relation_type"), "partyRelationshipType"),
        get_insert_function(col("relation_start_date"), "partyRelationStartDateTime"),
    )

def get_party_address(df):
    party_address_struct = struct(col("address_line_1").alias("addressLine1"),
                                  col("address_line_2").alias("addressLine2"),
                                  col("city").alias("addressCity"),
                                  col("postal_code").alias("addressPostalCode"),
                                  col("country_of_address").alias("addressCountry"),
                                  col("address_start_date").alias("addressStartDate"),
    ).alias("partyAddress")
    return df.select(
        "party_id",
        get_insert_function(party_address_struct, "partyAddress"),
    )

def join_account_party(acc_df, party_df):
    return acc_df.join(party_df, "account_id", "left_outer")

def join_party_address(party_df, address_df):
    return party_df.join(address_df, "party_id", "left_outer") \
            .groupBy("account_id") \
            .agg(collect_list(struct("partyIdentifier",
                                 "partyRelationshipType",
                                 "partyRelationStartDateTime",
                                 "partyAddress"
                                 ).alias("partyDetails")
                          ).alias("partyRelations"))

def create_payload(spark, df):
    header_info = [("SBDL-Contract", 1, 0)]
    header_df = spark.createDataFrame(header_info) \
                .toDF("eventType", "majorSchemaVersion", "minorSchemaVersion")

    payload_df = header_df.crossJoin(df) \
                .select(struct(expr("uuid()").alias("eventIdentifier"),
                        col("eventType"),col("majorSchemaVersion"),col("minorSchemaVersion"),
                        lit(date_format(current_timestamp(), "yyyy-mm-dd'T'HH:mm:ssZ")).alias("eventDateTime")
                        ).alias("eventHeader"),
                        array(lit("contractIdentifier").alias("keyField"),
                                col("account_id").alias("keyValue"),
                              ).alias("keys"),
                        struct(col("contractIdentifier"),
                               col("sourceSystemIdentifier"),
                               col("contactStartDateTime"),
                               col("contractTitle"),
                               col("taxIdentifier"),
                               col("contractBranchCode"),
                               col("contractCountry"),
                               col("partyRelations")
                               ).alias("payload")


                        )
    return payload_df

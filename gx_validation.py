import great_expectations as gx
from great_expectations.checkpoint import Checkpoint

# Initialize GX context
context = gx.get_context(context_root_dir="C:/Users/LENOVO/Desktop/Sh/demo/demo/demo-project/gx")

# Load existing datasource
data_source = context.get_datasource("non_financial_source")

# Get the existing asset
data_asset = data_source.get_asset("non_financial_asset")

# Build batch request
batch_request = data_asset.build_batch_request()

# Create or retrieve Expectation Suite
expectation_suite_name = "non_financial_suite"
try:
    context.add_expectation_suite(expectation_suite_name=expectation_suite_name)
except Exception:
    pass  # Suite already exists

# Create validator
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

# Table-level checks
validator.expect_table_row_count_to_be_between(min_value=1)
validator.expect_table_column_count_to_equal(value=33)

# Column-level expectations
validator.expect_column_values_to_not_be_null(column="OBJECT_ID")
validator.expect_column_values_to_match_regex(column="OBJECT_ID", regex=r"^\d{24}$")
validator.expect_column_values_to_not_be_null(column="SYSTEM_ID")
validator.expect_column_values_to_match_regex(column="SYSTEM_ID", regex=r"^\d{24}$")
validator.expect_column_values_to_not_be_null(column="ACCOUNT_TYPE_ACCOUNT_CLASS")
validator.expect_column_values_to_be_in_set(column="ACCOUNT_TYPE_ACCOUNT_CLASS", value_set=["ABC_0"])
validator.expect_column_values_to_not_be_null(column="ID")
validator.expect_column_values_to_match_regex(column="ID", regex=r"^\d{20}$")
validator.expect_column_values_to_not_be_null(column="S_IN")
validator.expect_column_values_to_be_between(column="S_IN", min_value=-1000000000000, max_value=0)
validator.expect_column_values_to_not_be_null(column="S_OUT")
validator.expect_column_values_to_be_between(column="S_OUT", min_value=-1000000000000, max_value=0)
validator.expect_column_values_to_not_be_null(column="DT")
validator.expect_column_values_to_be_in_set(column="DT", value_set=[0])
validator.expect_column_values_to_not_be_null(column="CT")
validator.expect_column_values_to_be_in_set(column="CT", value_set=[0])
validator.expect_column_values_to_not_be_null(column="BS_IN")
validator.expect_column_values_to_be_between(column="BS_IN", min_value=-1000000000000, max_value=0)
validator.expect_column_values_to_not_be_null(column="BS_OUT")
validator.expect_column_values_to_be_between(column="BS_OUT", min_value=-1000000000000, max_value=0)
validator.expect_column_values_to_not_be_null(column="BDT")
validator.expect_column_values_to_be_in_set(column="BDT", value_set=[0])
validator.expect_column_values_to_not_be_null(column="BCT")
validator.expect_column_values_to_be_in_set(column="BCT", value_set=[0])
validator.expect_column_values_to_not_be_null(column="DEBIT_COUNT")
validator.expect_column_values_to_be_in_set(column="DEBIT_COUNT", value_set=[0])
validator.expect_column_values_to_not_be_null(column="CREDIT_COUNT")
validator.expect_column_values_to_be_in_set(column="CREDIT_COUNT", value_set=[0])
validator.expect_column_values_to_not_be_null(column="GENERATED_CODE")
validator.expect_column_values_to_not_be_null(column="POST_ADDRESS")
validator.expect_column_values_to_match_regex(column="POST_ADDRESS", regex=r"^City Street House Home \d+$")
validator.expect_column_values_to_match_regex(column="CONDITIONAL_FIRST_DATE", regex=r"^\d{2}\.\d{2}\.\d{4}$|^$")
validator.expect_column_values_to_not_be_null(column="FIRST_DATE")
validator.expect_column_values_to_match_regex(column="FIRST_DATE", regex=r"^\d{2}\.\d{2}\.\d{4}$|^$")
validator.expect_column_values_to_not_be_null(column="QUALITY_CATEGORY")
validator.expect_column_values_to_be_in_set(column="QUALITY_CATEGORY", value_set=["ККПНА_1"])
validator.expect_column_values_to_not_be_null(column="CURRENCY")
validator.expect_column_values_to_be_in_set(column="CURRENCY", value_set=["000"])
validator.expect_column_values_to_not_be_null(column="REFERENCE_VALUE")
validator.expect_column_values_to_be_in_set(column="REFERENCE_VALUE", value_set=["КВК_9", "КВК_10", "КВК_12", "КВК_13", "КВК_14"])
validator.expect_column_values_to_not_be_null(column="RISK_GROUP_ACCOUNT_CLASS")
validator.expect_column_values_to_be_in_set(column="RISK_GROUP_ACCOUNT_CLASS", value_set=["КГРДБА_1", "КГРДБА_5"])
validator.expect_column_values_to_not_be_null(column="REG_CHECK")
validator.expect_column_values_to_be_in_set(column="REG_CHECK", value_set=["YES", "NO"])
validator.expect_column_values_to_not_be_null(column="NAME")
validator.expect_column_values_to_match_regex(column="NAME", regex=r"^Ativity \d+$")
validator.expect_column_values_to_not_be_null(column="FIXED_VALUE")
validator.expect_column_values_to_be_in_set(column="FIXED_VALUE", value_set=[26])
validator.expect_column_values_to_not_be_null(column="MAXSOUT")
validator.expect_column_values_to_be_between(column="MAXSOUT", min_value=-888888964, max_value=-888888888)

# Save expectation suite
validator.save_expectation_suite(discard_failed_expectations=False)

# Define and run checkpoint
context.add_or_update_checkpoint(
    name="non_financial_checkpoint",
    config_version=1.0,
    class_name="Checkpoint",
    run_name_template="%Y%m%d-%H%M%S-non-financial-run",
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
    action_list=[
        {
            "name": "store_results_in_openmetadata",
            "action": {
                "module_name": "metadata.great_expectations.action",
                "class_name": "OpenMetadataValidationAction",
                "config_file_path": "C:/Users/LENOVO/Desktop/Sh/demo/demo/demo-project/config",
                "database_service_name": "DEMO",
                "database_name": "demo",
                "table_name": "demo",
                "schema_name": "public",
            },
        }
    ],
)

checkpoint_result = context.run_checkpoint(checkpoint_name="non_financial_checkpoint")

print("Checkpoint run completed. Results ingested into OpenMetadata.")

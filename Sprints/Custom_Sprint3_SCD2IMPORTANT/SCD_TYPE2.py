from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_timestamp, expr

# Configuration
target_path = '/tmp/delta/people-10m'
source_path = '/tmp/delta/people-10m-updates'
FUTURE_DATE = "3000-01-01 00:00:00"

trgt = DeltaTable.forPath(spark, target_path)
updatesDF = spark.read.load(source_path) # Your new incoming data

# --- STEP 1: Prepare the "staged" updates ---
# We need to find which records in the source already exist in the target.
# For those, we need TWO rows in our merge source:
# 1. The row that will match and UPDATE (deactivate) the existing target record.
# 2. The row that will NOT match and INSERT (as the new active record).

column_names = updatesDF.columns

# Rows that will be inserted (New records + New versions of changed records)
staged_updates = updatesDF.select(*[col(c) for c in column_names]) \
    .withColumn("mergeKey", col("id")) \
    .withColumn("begin_time", current_timestamp()) \
    .withColumn("end_time", lit(FUTURE_DATE).cast("timestamp")) \
    .withColumn("isActive", lit(True))

# Rows that will trigger the "deactivation" of old records
# We set mergeKey to NULL so it forces an 'Update' on the existing record 
# but allows the insert logic to handle the new version separately.
existing_records_to_deactivate = updatesDF.join(trgt.toDF(), "id") \
    .where("trgt.isActive = true") \
    .select(updatesDF["id"], *[updatesDF[c] for c in column_names if c != 'id']) \
    .withColumn("mergeKey", lit(None)) \
    .withColumn("begin_time", current_timestamp()) \
    .withColumn("end_time", lit(FUTURE_DATE).cast("timestamp")) \
    .withColumn("isActive", lit(False))

# Combine them
final_source_df = staged_updates.unionByName(existing_records_to_deactivate, allowMissingColumns=True)

# --- STEP 2: The Merge ---
trgt.alias('people') \
  .merge(
    final_source_df.alias('updates'),
    'people.id = updates.mergeKey AND people.isActive = true' 
  ) \
  .whenMatchedUpdate(set = {
    # If the ID matches and the row is currently active, we "expire" it
    "isActive": "false",
    "end_time": "current_timestamp()"
  }) \
  .whenNotMatchedInsert(values = {
    # New records (or the "new version" of a changed record) get inserted as Active
    "id": "updates.id",
    "firstName": "updates.firstName",
    "lastName": "updates.lastName",
    "salary": "updates.salary",
    "begin_time": "updates.begin_time",
    "end_time": "updates.end_time",
    "isActive": "true"
  }) \
  .execute()
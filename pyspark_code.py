import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.job import Job

# Get Glue job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Glue and Spark contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the push-down predicate
predicate_pushdown = "region in ('ca', 'gb', 'us')"

# Create a DynamicFrame from the Glue catalog table with push-down predicate
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_raw",
    table_name="raw_statistics",
    transformation_ctx="datasource0",
    push_down_predicate=predicate_pushdown
)

# Define mappings for ApplyMapping
mappings = [
    ("video_id", "string", "video_id", "string"),
    ("trending_date", "string", "trending_date", "string"),
    ("title", "string", "title", "string"),
    ("channel_title", "string", "channel_title", "string"),
    ("category_id", "long", "category_id", "long"),
    ("publish_time", "string", "publish_time", "string"),
    ("tags", "string", "tags", "string"),
    ("views", "long", "views", "long"),
    ("likes", "long", "likes", "long"),
    ("dislikes", "long", "dislikes", "long"),
    ("comment_count", "long", "comment_count", "long"),
    ("thumbnail_link", "string", "thumbnail_link", "string"),
    ("comments_disabled", "boolean", "comments_disabled", "boolean"),
    ("ratings_disabled", "boolean", "ratings_disabled", "boolean"),
    ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"),
    ("description", "string", "description", "string"),
    ("region", "string", "region", "string")
]

# Apply mapping, resolve choice, and drop null fields in one step
transformed_df = ApplyMapping.apply(
    frame=datasource0,
    mappings=mappings,
    transformation_ctx="applymapping1"
).resolveChoice(
    choice="make_struct",
    transformation_ctx="resolvechoice2"
).dropNullFields(
    transformation_ctx="dropnullfields3"
)

# Write the transformed data to S3
glueContext.write_dynamic_frame.from_options(
    frame=transformed_df,
    connection_type="s3",
    connection_options={
        "path": "s3://de-on-youtube-cleansed-useast1-dev/youtube/raw_statistics/",
        "partitionKeys": ["region"]
    },
    format="parquet",
    transformation_ctx="datasink4"
)

# Commit the job
job.commit()

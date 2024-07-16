import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql.functions import col, udf
from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
from pyspark.sql.types import BooleanType


def is_english_only(text):
    if text is None:
        return False
    for char in text:
        if not ('\u0000' <= char <= '\u007F'):
            return False
    return True

is_english_only_udf = udf(is_english_only, BooleanType())

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    df = dfc.select(list(dfc.keys())[0]).toDF()
    
    df = df.select(
        col("query"),
        col("kind"),
        col("etag"),
        col("id"),
        col('snippet.title').alias("title"),
        col('snippet.description').substr(1,300).alias("description"),
        col('snippet.publishedat').alias("publishedat"),
        col('snippet.country').alias("country"),
        col('snippet.defaultlanguage').alias("defaultlanguage"),
        col('contentdetails.relatedplaylists.likes').alias("likes"),
        col('contentdetails.relatedplaylists.uploads').alias("uploads"),
        col('statistics.viewCount').alias("viewCount"),
        col('statistics.subscriberCount').alias("subscriberCount"),
        col('statistics.hiddenSubscriberCount').alias("hiddenSubscriberCount"),
        col('statistics.videoCount').alias("videoCount")
    )
    
    df = df.filter(is_english_only_udf(df["description"]))
    

    dyf = DynamicFrame.fromDF(df,glueContext,'mod_df')
    dyfc = DynamicFrameCollection({'dyf':dyf},glueContext)
    return dyfc
args = getResolvedOptions(sys.argv, ['JOB_NAME','BUCKET_NAME', 'OBJECT_KEY'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket_name = args['BUCKET_NAME']
object_key = args['OBJECT_KEY']

path = f's3://{bucket_name}/{object_key}'

# Script generated for node Amazon S3
AmazonS3_node1720146242667 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": [path]}, transformation_ctx="AmazonS3_node1720146242667")

# Script generated for node Custom Transform
CustomTransform_node1720146247973 = MyTransform(glueContext, DynamicFrameCollection({"AmazonS3_node1720146242667": AmazonS3_node1720146242667}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1720146252906 = SelectFromCollection.apply(dfc=CustomTransform_node1720146247973, key=list(CustomTransform_node1720146247973.keys())[0], transformation_ctx="SelectFromCollection_node1720146252906")

# Script generated for node Amazon Redshift
AmazonRedshift_node1720146258039 = glueContext.write_dynamic_frame.from_options(
    frame=SelectFromCollection_node1720146252906,
    connection_type="redshift",
    connection_options={
        "postactions": """
            BEGIN;
            MERGE INTO public.youtube
            USING public.youtube_temp_vl1j0e
            ON youtube.kind = youtube_temp_vl1j0e.kind
            AND youtube.etag = youtube_temp_vl1j0e.etag
            AND youtube.id = youtube_temp_vl1j0e.id
            WHEN MATCHED THEN UPDATE SET
                query = youtube_temp_vl1j0e.query,
                kind = youtube_temp_vl1j0e.kind,
                etag = youtube_temp_vl1j0e.etag,
                id = youtube_temp_vl1j0e.id,
                title = youtube_temp_vl1j0e.title,
                description = youtube_temp_vl1j0e.description,
                publishedat = youtube_temp_vl1j0e.publishedat,
                country = youtube_temp_vl1j0e.country,
                defaultlanguage = youtube_temp_vl1j0e.defaultlanguage,
                likes = youtube_temp_vl1j0e.likes,
                uploads = youtube_temp_vl1j0e.uploads,
                viewCount = youtube_temp_vl1j0e.viewCount,
                subscriberCount = youtube_temp_vl1j0e.subscriberCount,
                hiddenSubscriberCount = youtube_temp_vl1j0e.hiddenSubscriberCount,
                videoCount = youtube_temp_vl1j0e.videoCount
            WHEN NOT MATCHED THEN INSERT VALUES (
                youtube_temp_vl1j0e.query,
                youtube_temp_vl1j0e.kind,
                youtube_temp_vl1j0e.etag,
                youtube_temp_vl1j0e.id,
                youtube_temp_vl1j0e.title,
                youtube_temp_vl1j0e.description,
                youtube_temp_vl1j0e.publishedat,
                youtube_temp_vl1j0e.country,
                youtube_temp_vl1j0e.defaultlanguage,
                youtube_temp_vl1j0e.likes,
                youtube_temp_vl1j0e.uploads,
                youtube_temp_vl1j0e.viewCount,
                youtube_temp_vl1j0e.subscriberCount,
                youtube_temp_vl1j0e.hiddenSubscriberCount,
                youtube_temp_vl1j0e.videoCount
            );
            DROP TABLE public.youtube_temp_vl1j0e;
            END;
        """,
        "redshiftTmpDir": "s3://aws-glue-assets-590183870342-us-east-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.youtube_temp_vl1j0e",
        "connectionName": "RedshiftConnection",
        "preactions": """
            CREATE TABLE IF NOT EXISTS public.youtube (
                query VARCHAR,
                kind VARCHAR,
                etag VARCHAR,
                id VARCHAR,
                title VARCHAR,
                description VARCHAR(300),
                publishedat VARCHAR,
                country VARCHAR,
                defaultlanguage VARCHAR,
                likes VARCHAR,
                uploads VARCHAR,
                viewCount VARCHAR,
                subscriberCount VARCHAR,
                hiddenSubscriberCount BOOLEAN,
                videoCount VARCHAR
            );
            DROP TABLE IF EXISTS public.youtube_temp_vl1j0e;
            CREATE TABLE IF NOT EXISTS public.youtube_temp_vl1j0e (
                query VARCHAR,
                kind VARCHAR,
                etag VARCHAR,
                id VARCHAR,
                title VARCHAR,
                description VARCHAR(300),
                publishedat VARCHAR,
                country VARCHAR,
                defaultlanguage VARCHAR,
                likes VARCHAR,
                uploads VARCHAR,
                viewCount VARCHAR,
                subscriberCount VARCHAR,
                hiddenSubscriberCount BOOLEAN,
                videoCount VARCHAR
            );
        """
    },
    transformation_ctx="AmazonRedshift_node1720146258039"
)

job.commit()
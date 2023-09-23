from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, TimestampType, MapType, \
    BooleanType, FloatType

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder\
        .master("local")\
        .appName("ecommerce example")\
        .getOrCreate()

    # 1. load input data
    input_root_path = "/Users/jaesuk/PyCharm/sparkTutorial/practise_ex/input"

    # 1.1 products
    product_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("category_ids", ArrayType(IntegerType()), False),
    ])

    # product data
    products_df = ss.read\
        .option("inferSchema", False)\
        .json(f"{input_root_path}/products.json")\
        .withColumnRenamed("name", "product_name")


    # 1.2 items
    items_schema = StructType([
        StructField("item_id", IntegerType(), False),
        StructField("product_id", IntegerType(), False),
        StructField("seller_id", IntegerType(), False),
        StructField("promotion_ids", ArrayType(IntegerType()), False),
        StructField("original_title", StringType(), False),
        StructField("search_tag", ArrayType(StringType()), False),
        StructField("price", IntegerType(), False),
        StructField("create_timestamp", TimestampType(), False),
        StructField("update_timestamp", TimestampType(), False),
        StructField("attrs", MapType(StringType(), StringType()), False),
        StructField("free_shipping", BooleanType(), False)
    ])

    items_df = ss.read.option("inferSchema", False)\
        .schema(items_schema).json(f"{input_root_path}/items.json")\
        .withColumnRenamed("create_timestamp", "item_create_timestamp")\
        .withColumnRenamed("update_timestamp", "item_update_timestamp") \
        .withColumnRenamed("original_title", "original_item_title")

    # items_df.show()

    # 1.3 categories (product-level)
    categories_schema = StructType([
        StructField("category_id", IntegerType(), False),
        StructField("category_name", StringType(), False),
    ])

    categories_df = ss.read.option("inferSchema", False)\
        .schema(categories_schema).json(f"{input_root_path}/categories.json")

    # 1.4 review (product-level)
    reviews_schema = StructType([
        StructField("review_id", IntegerType()),
        StructField("product_id", IntegerType()),
        StructField("content", StringType()),
        StructField("score", IntegerType()),
    ])

    reviews_df = ss.read.option("inferSchema", False)\
        .schema(reviews_schema).json(f"{input_root_path}/reviews.json")\
        .withColumnRenamed("score", "review_score")\
        .withColumnRenamed("content", "review_content")


    # 1.5 promotions (item-level)
    promotions_schema = StructType([
        StructField("promotion_id", IntegerType()),
        StructField("name", StringType()),
        StructField("discount_rate", FloatType()),
        StructField("start_date", StringType()),
        StructField("end_date", StringType())
    ])

    promotions_df = ss.read.option("inferSchema", False)\
        .schema(promotions_schema)\
        .json(f"{input_root_path}/promotions.json")
    promotions_df.printSchema()


    # 1.6 sellers (item-level)
    sellers_schema = StructType([
        StructField("seller_id", IntegerType()),
        StructField("name", StringType())
    ])

    sellers_df = ss.read.option("inferSchema", False)\
        .json(f"{input_root_path}/sellers.json")\
        .withColumnRenamed("name", "seller_name")
    # sellers.show()
    sellers_df.printSchema()

    categories_df.show()
    reviews_df.show()
    promotions_df.show()
    sellers_df.show()
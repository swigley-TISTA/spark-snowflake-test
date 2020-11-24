package main

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME

case class Item(
                 I_ITEM_SK: BigDecimal,
                 I_ITEM_ID: String,
                 I_REC_START_DATE: String,
                 I_REC_END_DATE: String,
                 I_ITEM_DESC: String,
                 I_CURRENT_PRICE: BigDecimal,
                 I_WHOLESALE_COST: BigDecimal,
                 I_BRAND_ID: BigDecimal,
                 I_BRAND: String,
                 I_CLASS_ID: BigDecimal,
                 I_CLASS: String,
                 I_CATEGORY_ID: BigDecimal,
                 I_CATEGORY: String,
                 I_MANUFACT_ID: BigDecimal,
                 I_MANUFACT: String,
                 I_SIZE: String,
                 I_FORMULATION: String,
                 I_COLOR: String,
                 I_UNITS: String,
                 I_CONTAINER: String,
                 I_MANAGER_ID: BigDecimal,
                 I_PRODUCT_NAME: String
            )

object main {
  def main(args: Array[String]): Unit = {
    val appName = "spark snowflake test"
    val snowFlakeSource = SNOWFLAKE_SOURCE_NAME

    // Get account environment variables
    val snowFlakeAccountId = System.getenv("SNOWFLAKE_ACCT")
    val snowFlakeRegion = System.getenv("SNOWFLAKE_REGION")
    val snowFlakeUser = System.getenv("SNOWFLAKE_USER")
    val snowFlakePw = System.getenv("SNOWFLAKE_PW")

    val snowFlakeAccount = s"$snowFlakeAccountId.$snowFlakeRegion.snowflakecomputing.com"

    val snowFlakeWh = "COMPUTE_WH"
    val snowFlakeDbInput = "SNOWFLAKE_SAMPLE_DATA"
    val showFlakeSchemaInput = "TPCDS_SF10TCL"
    val snowFlakeDbOutput = "TEST_DB"
    val showFlakeSchemaOutput = "TEST_SCHEMA"
    val snowFlakeTableInput = "ITEM"
    val snowFlakeTableOutput = "BRANDS_COUNTS_G"

    lazy val spark: SparkSession = {
      SparkSession
        .builder()
        .master("local")
        .appName(appName)
        .getOrCreate()
    }
    import spark.implicits._

    val sfOptionsInput = Map(
      "sfURL" -> snowFlakeAccount,
      "sfUser" -> snowFlakeUser,
      "sfPassword" -> snowFlakePw,
      "sfDatabase" -> snowFlakeDbInput,
      "sfSchema" -> showFlakeSchemaInput,
      "sfWarehouse" -> snowFlakeWh
    )

    val sfOptionsOutput = Map(
      "sfURL" -> snowFlakeAccount,
      "sfUser" -> snowFlakeUser,
      "sfPassword" -> snowFlakePw,
      "sfDatabase" -> snowFlakeDbOutput,
      "sfSchema" -> showFlakeSchemaOutput,
      "sfWarehouse" -> snowFlakeWh
    )

    // Read input table
    val testDS: Dataset[Item] = spark
      .read
      .format(snowFlakeSource)
      .options(sfOptionsInput)
      .option("dbtable", snowFlakeTableInput)
      .load().as[Item]

    // Do whatever
    val count: Long = testDS.count()
    println("Count is: " + count + ".")

    val brandsDF = testDS.groupBy("I_brand").count().sort()
    val brands = brandsDF.map(row => (row.getString(0),row.getLong(1)))
    brands.foreach(row => println(row.toString))

    // Write output table
    brandsDF
      .write
      .format(snowFlakeSource)
      .options(sfOptionsOutput)
      .option("dbtable", snowFlakeTableOutput)
      .mode(SaveMode.Overwrite)
      .save()
  }
}

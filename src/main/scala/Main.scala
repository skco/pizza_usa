import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, DateType, DoubleType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.countDistinct

object pizza {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("netflix")
      .master("local")
      .getOrCreate()

    var pizza_dataDF:DataFrame = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .csv("pizza_data.csv")

    pizza_dataDF.show()
    pizza_dataDF.printSchema()

    var pizza_cleanedDF:DataFrame = pizza_dataDF.withColumn("Price",regexp_replace(col("Price"),"[$,]","").cast(DataTypes.DoubleType))

    pizza_cleanedDF.show()
    pizza_cleanedDF.printSchema()

    pizza_cleanedDF.select(max(col("Price"))).show()
    pizza_cleanedDF.select(min(col("Price"))).show()
    pizza_cleanedDF.select(avg(col("Price"))).show()

  }
}

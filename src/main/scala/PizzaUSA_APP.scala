
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes}

object PizzaUSA_APP {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("PizzaUSA_APP")
      .master("local")
      .getOrCreate()

    val pizza_dataDF:DataFrame = spark.read
                                      .option("header", true)
                                      .option("delimiter", ",")
                                      .csv("pizza_data.csv")

    pizza_dataDF.show()
    pizza_dataDF.printSchema()

    val pizza_cleanedDF:DataFrame = pizza_dataDF.withColumn("Price",regexp_replace(col("Price"),"[$,]","").cast(DataTypes.DoubleType))

    pizza_cleanedDF.show()
    pizza_cleanedDF.printSchema()

    pizza_cleanedDF.select(max(col("Price"))).show()
    pizza_cleanedDF.select(min(col("Price"))).show()
    pizza_cleanedDF.select(avg(col("Price"))).show()

  }
}

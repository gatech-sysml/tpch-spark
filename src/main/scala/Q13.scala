package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q13 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    import spark.implicits._
    import schemaProvider._

    val special = udf { (x: String) => x.matches(".*special.*requests.*") }

    val intermediateResult = customer.join(order, $"c_custkey" === order("o_custkey")
      && !special(order("o_comment")), "left_outer")
      .groupBy($"o_custkey")
      .agg(count($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(count($"o_custkey").as("custdist"))
      // .sort($"custdist".desc, $"c_count".desc)
    
    // Repartition the data based on the grouping keys
    val repartitionedResult = intermediateResult.repartition($"custdist", $"c_count")

    // Group by a constant column to shuffle the data
    val groupedResult1 = repartitionedResult.groupBy(lit(1)).agg(count($"*"))

    // Group by another constant column to shuffle the data further
    // val groupedResult2 = groupedResult1.groupBy(lit(1)).agg(count($"*"))

    // Perform an additional transformation to introduce another stage
    val finalResult = groupedResult1.filter($"1" === 1)

    finalResult
  }

}

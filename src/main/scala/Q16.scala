package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q16 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    import spark.implicits._
    import schemaProvider._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val complains = udf { (x: String) => x.matches(".*Customer.*Complaints.*") }
    val polished = udf { (x: String) => x.startsWith("MEDIUM POLISHED") }
    val numbers = udf { (x: Int) => x.toString().matches("49|14|23|45|19|3|36|9") }

    val fparts = part.filter(($"p_brand" !== "Brand#45") && !polished($"p_type") &&
      numbers($"p_size"))
      .select($"p_partkey", $"p_brand", $"p_type", $"p_size")

    val intermediateResult = supplier.filter(!complains($"s_comment"))
      // .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", $"ps_suppkey")
      .join(fparts, $"ps_partkey" === fparts("p_partkey"))
      .groupBy($"p_brand", $"p_type", $"p_size")
      .agg(countDistinct($"ps_suppkey").as("supplier_count"))
      // .sort($"supplier_count".desc, $"p_brand", $"p_type", $"p_size")

    // Repartition the data based on the grouping keys
    val repartitionedResult = intermediateResult.repartition($"supplier_count", $"p_brand", $"p_type", $"p_size")

    // Group by a constant column to shuffle the data
    val groupedResult1 = repartitionedResult.groupBy(lit(1)).agg(count($"*"))

    // Group by another constant column to shuffle the data further
    // val groupedResult2 = groupedResult1.groupBy(lit(1)).agg(count($"*"))

    // Perform an additional transformation to introduce another stage
    val finalResult = groupedResult1.filter($"1" === 1)

    finalResult
  }

}

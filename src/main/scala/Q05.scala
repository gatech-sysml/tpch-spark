package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q05 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    import spark.implicits._
    import schemaProvider._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val forders = order.filter($"o_orderdate" < "1995-01-01" && $"o_orderdate" >= "1994-01-01")

    val intermediateResult = region.filter($"r_name" === "ASIA")
      .join(nation, $"r_regionkey" === nation("n_regionkey"))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(lineitem, $"s_suppkey" === lineitem("l_suppkey"))
      .select($"n_name", $"l_extendedprice", $"l_discount", $"l_orderkey", $"s_nationkey")
      .join(forders, $"l_orderkey" === forders("o_orderkey"))
      .join(customer, $"o_custkey" === customer("c_custkey") && $"s_nationkey" === customer("c_nationkey"))
      .select($"n_name", decrease($"l_extendedprice", $"l_discount").as("value"))
      .groupBy($"n_name")
      .agg(sum($"value").as("revenue"))
      // .sort($"revenue".desc)

    // Repartition the data based on the grouping keys
    val repartitionedResult = intermediateResult.repartition($"revenue")

    // Group by a constant column to shuffle the data
    val groupedResult1 = repartitionedResult.groupBy(lit(1)).agg(count($"*"))

    // Group by another constant column to shuffle the data further
    // val groupedResult2 = groupedResult1.groupBy(lit(1)).agg(count($"*"))

    // Perform an additional transformation to introduce another stage
    val finalResult = groupedResult1.filter($"1" === 1)

    finalResult
  }

}

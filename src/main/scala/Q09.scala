package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q09 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    import spark.implicits._
    import schemaProvider._

    val getYear = udf { (x: String) => x.substring(0, 4) }
    val expr = udf { (x: Double, y: Double, v: Double, w: Double) => x * (1 - y) - (v * w) }

    val linePart = part.filter($"p_name".contains("green"))
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"))

    val natSup = nation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))

    val intermediateResult = linePart.join(natSup, $"l_suppkey" === natSup("s_suppkey"))
      .join(partsupp, $"l_suppkey" === partsupp("ps_suppkey")
        && $"l_partkey" === partsupp("ps_partkey"))
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"n_name", getYear($"o_orderdate").as("o_year"),
        expr($"l_extendedprice", $"l_discount", $"ps_supplycost", $"l_quantity").as("amount"))
      .groupBy($"n_name", $"o_year")
      .agg(sum($"amount"))
      // .sort($"n_name", $"o_year".desc)

    // Repartition the data based on the grouping keys
    val repartitionedResult = intermediateResult.repartition($"n_name", $"o_year")

    // Group by a constant column to shuffle the data
    val groupedResult1 = repartitionedResult.groupBy(lit(1)).agg(count($"*"))

    // Group by another constant column to shuffle the data further
    val groupedResult2 = groupedResult1.groupBy(lit(1)).agg(count($"*"))

    // Perform an additional transformation to introduce another stage
    val finalResult = groupedResult2.filter($"1" === 1)

    finalResult
  }

}

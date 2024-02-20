package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q04 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    import spark.implicits._
    import schemaProvider._

    val forders = order.filter($"o_orderdate" >= "1993-07-01" && $"o_orderdate" < "1993-10-01")
    val flineitems = lineitem.filter($"l_commitdate" < $"l_receiptdate")
      .select($"l_orderkey")
      .distinct

    val intermediateResult = flineitems.join(forders, $"l_orderkey" === forders("o_orderkey"))
      .groupBy($"o_orderpriority")
      .agg(count($"o_orderpriority"))
      .sort($"o_orderpriority")

    // Repartition the data based on the grouping keys
    val repartitionedResult = intermediateResult.repartition($"o_orderpriority")

    // Group by a constant column to shuffle the data
    val groupedResult1 = repartitionedResult.groupBy(lit(1)).agg(count($"*"))

    // Group by another constant column to shuffle the data further
    val groupedResult2 = groupedResult1.groupBy(lit(1)).agg(count($"*"))

    // Perform an additional transformation to introduce another stage
    val finalResult = groupedResult2.filter($"1" === 1)

    finalResult
  }

}

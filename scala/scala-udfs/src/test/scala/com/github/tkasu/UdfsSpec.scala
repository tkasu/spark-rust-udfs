package com.github.tkasu

import com.github.tkasu.udfs.SqrtAndMolUDF
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.DataTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class UdfsSpec extends AnyFlatSpec with Matchers {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("UdfsSpec")
    .getOrCreate()

  import spark.implicits._

  spark.udf.register("sqrtAndMolUDF", new SqrtAndMolUDF(), DataTypes.DoubleType)

  "sqrtAndMol" should "return 42 when 0 is passed" in {
    SqrtAndMolUDF.sqrtAndMol(0) mustBe 42.0
  }

  "sqrtAndMol" should "return 44 when 4 is passed" in {
    SqrtAndMolUDF.sqrtAndMol(4) mustBe 44.0
  }

  "sqrtAndMolUdf" should "return 42 when 0 is passed" in {
    val df = Seq(0L).toDF("value")
    df.select(expr("sqrtAndMolUDF(value)"))
      .as[Double]
      .collect()
      .head mustBe 42.0
  }

  "sqrtAndMolUdf" should "return 44 when 4 is passed" in {
    val df = Seq(4L).toDF("value")
    df.select(expr("sqrtAndMolUDF(value)"))
      .as[Double]
      .collect()
      .head mustBe 44.0
  }
}

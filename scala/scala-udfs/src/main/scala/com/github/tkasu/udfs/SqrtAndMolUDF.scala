package com.github.tkasu.udfs

import org.apache.spark.sql.api.java.UDF1

class SqrtAndMolUDF extends UDF1[Long, Double] {
  override def call(x: Long): Double = SqrtAndMolUDF.sqrtAndMol(x)
}

object SqrtAndMolUDF {
    def sqrtAndMol(x: Long): Double = {
        Math.sqrt(x) + 42
    }
}
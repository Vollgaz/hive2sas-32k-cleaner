package org.vollgaz.spark.analysis

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object Udf {

    /**
      *
      */
    val udfMapHiveType: UserDefinedFunction = udf[String, String, Int](Udf.mapHiveType)

    def mapHiveType(sparkType: String, byteLength: Int): String = {
        (Option(sparkType), Option(byteLength)) match {
            case (Some("BinaryType"), _)                                        => s"BINARY"
            case (Some("BooleanType"), _)                                       => s"BOOLEAN"
            case (Some("DateType"), _)                                          => s"DATE"
            case (Some("DoubleType"), _)                                        => s"DOUBLE"
            case (Some("IntegerType"), _)                                       => s"INT"
            case (Some("LongType"), _)                                          => s"BIGINT"
            case (Some("StringType"), Some(`byteLength`)) if byteLength < 65535 => s"VARCHAR(" + (byteLength - (byteLength % 10) + 10) + ")"
            case (Some("StringType"), Some(`byteLength`))                       => s"STRING"
            case (Some("DecimalType(38,18)"), _)                                => s"DECIMAL(38,18)"
            case (Some("DecimalType(18,2)"), _)                                 => s"DECIMAL(18,2)"
            case (Some("DecimalType"), _)                                       => s"DECIMAL"
            case (Some("TimestampType"), _)                                     => s"TIMESTAMP"
            case _                                                              => throw new Exception(s"$sparkType : Type non pris en charge.")
        }
    }
}
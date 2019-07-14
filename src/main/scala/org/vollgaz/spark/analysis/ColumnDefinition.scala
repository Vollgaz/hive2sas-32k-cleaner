package org.vollgaz.spark.analysis

case class ColumnDefinition(colName      : String,
                            colType      : String,
                            colByteLength: Int,
                            hiveType     : String)

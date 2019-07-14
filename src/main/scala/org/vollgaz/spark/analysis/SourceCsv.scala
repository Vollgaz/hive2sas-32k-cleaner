package org.vollgaz.spark.analysis

import org.apache.spark.sql.{Dataset, SparkSession}

class SourceCsv(filepath: String, columnseparator: String) extends SourceFile(filepath) {

    override def process(): Dataset[ColumnDefinition] = {
        val df = SparkSession
            .builder()
            .getOrCreate()
            .read
            .option("sep", columnseparator)
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(filepath)
            .transform(sanitizeColumnName)
        analyseFileContent(df)
    }
}

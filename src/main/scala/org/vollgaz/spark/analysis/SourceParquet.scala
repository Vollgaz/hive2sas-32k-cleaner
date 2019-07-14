package org.vollgaz.spark.analysis

import org.apache.spark.sql.{Dataset, SparkSession}

class SourceParquet(filepath: String) extends SourceFile(filepath) {
    /**
      * Lance l'analyse d'un fichier parquet
      *
      * @return Un dataframe décrivant chaque colonne du dataframe d'entrée.
      *         Fournie le type et la longueur en utf-8 .
      */
    override def process(): Dataset[ColumnDefinition] = {
        val df = SparkSession.builder().getOrCreate().read.parquet(filepath)
        analyseFileContent(df)
    }
}

package org.vollgaz.spark.analysis

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

abstract class SourceFile(filepath: String) {

    val columnDefinitionEncoder: Encoder[ColumnDefinition] = Encoders.product[ColumnDefinition]
    val spark: SparkSession = SparkSession.builder().getOrCreate()

    import spark.implicits._

    def process(): Dataset[ColumnDefinition] = ???

    /**
      * Analyse la longueur maximale de chaque colonnes du dataframe.
      *
      * @param df
      * @return
      */
    def analyseFileContent(df: Dataset[Row]): Dataset[ColumnDefinition] = {

        df.schema
            .fields
            .filter(field => !field.dataType.toString.contains("MapType") &&
                !field.dataType.toString.contains("ArrayMap") &&
                !field.dataType.toString.contains("StructType"))
            .map(x => {
                val data = df
                    .select(x.name)
                    .filter(col(x.name).isNotNull)
                    .distinct()
                    .collect()
                    .map(_ (0))

                val bytelength = getByteLength(data)
                // Make a new tuple
                ColumnDefinition(x.name, x.dataType.toString, bytelength, Udf.mapHiveType(x.dataType.toString, bytelength))
            }).toSeq.toDS().as(columnDefinitionEncoder)
    }

    /**
      * Pour une array de type quelconque , retourne la longueure maximale de la représentation string.
      * La longueure est celle du nombre d'octets de la réprésentation en UTF-8
      *
      * @param dataArray La liste de toutes les valeurs de la colonnes
      * @return La longueur maximale en octet avec l'encodage UTF-8
      */
    def getByteLength(dataArray: Array[Any]): Int = {
        if (dataArray.isEmpty) 0
        else dataArray.map(_.toString.getBytes("UTF-8").length).maxBy(x => x)
    }

    /**
      * Retourne la longueur maximal en nombre de caractères
      *
      * @param dataArray La liste de toutes les valeurs de la colonnes
      * @return La longueur maximale en nombre de caractères.
      */
    def getCharLength(dataArray: Array[Any]): Int = {
        if (dataArray.isEmpty) 0
        else dataArray.map(_.toString.length).maxBy(x => x)
    }

    /**
      * Corrige les noms de colonnes avec des points
      * Les points sont remplacés par des underscores.
      *
      * @param df
      * @return
      */
    def sanitizeColumnName(df: Dataset[Row]): Dataset[Row] = {
        val brokenColumn = df.columns.filter(_.contains(".")).toList
        brokenColumn.foldLeft(df) {
            (accu: Dataset[Row], colname: String) =>
                accu.withColumnRenamed(colname, colname.replaceAll("\\.", "_"))
        }
    }


    /**
      * Retire les colonnes qui ne peuvent pas être écrite en CSV (Array et Map)
      *
      * @param df
      * @return
      */
    def removeUnwrittableColumns(df: DataFrame): DataFrame = {
        val columnsToPrint = df.schema.fields
            .filter(field => !field.dataType.toString.contains("MapType") && !field.dataType.toString.contains("ArrayMap"))
            .map(x => x.name)

        df.select(columnsToPrint.head, columnsToPrint.tail: _*)
    }
}

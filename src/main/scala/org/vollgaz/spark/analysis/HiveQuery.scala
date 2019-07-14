package org.vollgaz.spark.analysis

class HiveQuery(hivedb: String, filepath: String) {

    /**
      * Genere la query pour dropper la table hive si elle existe.
      *
      * @return
      */
    def queryDropTable(): String = s"DROP TABLE IF EXISTS $hivedb.$getHiveTableName"

    /**
      * Genere la query de creation d'une table hive.
      *
      * @param colList
      * @return
      */
    def queryCreateTable(colList: Array[ColumnDefinition]): String = {
        val queryStringBuffer = new StringBuilder(s"CREATE EXTERNAL TABLE $hivedb.$getHiveTableName(")
        queryStringBuffer.append(s"`${colList.head.colName}` ${colList.head.hiveType}")

        colList.tail.foreach(col => queryStringBuffer.append(s", `${col.colName}` ${col.hiveType}"))

        queryStringBuffer.append(s") STORED AS PARQUET LOCATION '$filepath'")
        queryStringBuffer.mkString
    }

    /**
      * Retourne le nom de la table hive à partir du nom du fichier
      *
      * @return
      */
    def getHiveTableName: String = {
        filepath
            .split("/")
            .last
            .replace("_PARQUET", "")
            .split("\\.")
            .head
    }
}

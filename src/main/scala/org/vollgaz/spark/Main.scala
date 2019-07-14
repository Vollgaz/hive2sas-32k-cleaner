package org.vollgaz.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}
import org.vollgaz.spark.analysis.{ColumnDefinition, HiveQuery, SourceCsv, SourceParquet}

object Main {
    val spark: SparkSession = SparkSession
        .builder()
        .appName("parquet2hive")
        .enableHiveSupport()
        .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    /**
      *
      * @param args
      */
    def main(args: Array[String]): Unit = {
        val argMap: Map[String, String] = args.toSeq
            .map { arg =>
                var arr = arg.split('=')
                (arr(0).trim, arr(1).trim)
            }
            .toMap[String, String]

        parseArgs(argMap)
        startAnalysis()
    }

    /**
      *
      * @param args
      * --file = Le path global du fichier
      * --type = csv ou parquet
      * --hiveDb = (Depend de --updateHive) la db cible
      * --sep = (Depend de --type = csv) le separateur des csv.
      */
    def parseArgs(args: Map[String, String]): Unit = {
        Settings.fileToProcess = args("--file")
        Settings.typeToProcess = args("--type")
        Settings.hiveDB = args("--hiveDb")
        Settings.fileSeparator = Settings.typeToProcess match {
            case "csv" => args("--sep")
            case _     => s";"
        }
        println(
            s"${Settings.fileToProcess}\t\t${Settings.typeToProcess}\t\t${Settings.fileSeparator}")
    }

    def startAnalysis(): Unit = {

        val df: Dataset[ColumnDefinition] = Settings.typeToProcess.toString match {
            case "csv"     => new SourceCsv(Settings.fileToProcess, Settings.fileSeparator).process()
            case "parquet" => new SourceParquet(Settings.fileToProcess).process()
            case _         => throw new Exception(s"${Settings.typeToProcess} : Type de fichier non pris en charge")
        }
        df.show(false)

        val hiveCaller = new HiveQuery(Settings.hiveDB, Settings.fileToProcess)

        val queryDrop = hiveCaller.queryDropTable()
        println(queryDrop)
        spark.sql(queryDrop)

        val queryCreate = hiveCaller.queryCreateTable(df.collect())
        println(queryCreate)
        spark.sql(queryCreate)
        println("Job done")
    }
}

package org.bigdata.nlp.sql.scala.test

import com.johnsnowlabs.nlp.DocumentAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, explode}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import com.johnsnowlabs.nlp.annotator._



object NLPtoSQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("NLP to SQL").master("local[*]").getOrCreate()

    val nlpPipeline = new Pipeline().setStages(Array(
      new DocumentAssembler().setInputCol("text").setOutputCol("document"),
      new Tokenizer().setInputCols("document").setOutputCol("tokens"),
      new RegexMatcher()
        .setInputCols("document") // 更新这里的输入列
        .setOutputCol("matches")
        .setStrategy("MATCH_ALL")
    ))

    import spark.implicits._
    val text = "Show me the sales for the last quarter by region."
    val df = Seq(text).toDF("text")
    val result = nlpPipeline.fit(df).transform(df)

    val tokens = result.select(explode(col("matches.result")).as("token"))
    val sql = generateSQL(tokens)
    println(s"Generated SQL: $sql")

    val salesDF = Seq(
      ("North", "last", 100),
      ("South", "last", 200),
      ("East", "last", 300),
      ("West", "last", 400),
      ("North", "this", 500),
      ("South", "this", 600),
      ("East", "this", 700),
      ("West", "this", 800)
    ).toDF("region", "quarter", "sales")

    val queryResult = salesDF.sparkSession.sql(sql)
    println("Query Result:")
    queryResult.show()
  }

  def generateSQL(tokens: DataFrame): String = {
    implicit val stringEncoder = Encoders.STRING
    val quarter = tokens.filter(col("token").isin("last", "this"))
      .select(concat_ws(" ", collect_list("token")).as("quarter"))
      .as[String]
      .head

    val region = tokens.filter(!col("token").isin("last", "this"))
      .select(concat_ws(" ", collect_list("token")).as("region"))
      .as[String]
      .head

    s"SELECT * FROM sales WHERE quarter = '$quarter' AND region = '$region'"
  }
}

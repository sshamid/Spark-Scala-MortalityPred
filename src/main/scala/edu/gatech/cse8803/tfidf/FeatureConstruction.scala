/**
 * @author Hang Su
 */
package edu.gatech.cse8803.tfidf

//import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.SparkContext
//import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.Row
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions.max

/*

object tfIDF(notes: RDD[NoteEvent]) : Double = {

  val indexer = new StringIndexer()
    .setInputCol("term")
    .setOutputCol("termIndexed")

  val indexed = indexer.fit(df)
    .transform(df)
    .drop("term")
    .withColumn("termIndexed", $"termIndexed".cast("integer"))
    .groupBy($"doc", $"termIndexed")
    .agg(count(lit(1)).alias("cnt").cast("double"))


  val pairs = indexed.map{case Row(doc: Long, term: Int, cnt: Double) =>
  (doc, (term, cnt))}
  val docs = pairs.groupByKey

  val n = indexed.select(max($"termIndexed")).first.getInt(0) + 1

  val docsWithFeatures = docs.mapValues(vs => Vectors.sparse(n, vs.toSeq))

}

*/
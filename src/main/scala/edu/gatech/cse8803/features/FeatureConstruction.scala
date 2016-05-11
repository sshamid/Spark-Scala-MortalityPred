/**
 * @author Hang Su
 */
package edu.gatech.cse8803.features

import edu.gatech.cse8803.model.{NoteEvent, ICUevent}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


object FeatureConstruction {

  /**
   * ((patient-id, feature-name), feature-value)
   */
  type FeatureTuple = ((String, String), Double)

  /**
   * Aggregate feature tuples from ICU event
    *
    * @param  RDD of ICU event
   * @return RDD of feature tuples
   */
  def constructICUFeatureTuple(icuEvents: RDD[ICUevent]): RDD[FeatureTuple] = {

    val age = icuEvents.map(r => ((r.patientID, "age"), r.age.toString.toDouble))
    val gender = icuEvents.map(r => ((r.patientID, "gender"), if (r.gender == "F") 0.0 else 1.0))
    val sapadm = icuEvents.map(r => ((r.patientID, "sapadm"), r.sapsi_first.toString.toDouble))
    val sapmin = icuEvents.map(r => ((r.patientID, "sapmin"), r.sapsi_min.toString.toDouble))
    val sapmax = icuEvents.map(r => ((r.patientID, "sapmax"), r.sapsi_max.toString.toDouble))
    val sofaadm = icuEvents.map(r => ((r.patientID, "sofaadm"), r.sofa_first.toString.toDouble))
    val sofamin = icuEvents.map(r => ((r.patientID, "sofamin"), r.sofa_min.toString.toDouble))
    val sofamax = icuEvents.map(r => ((r.patientID, "sofamax"), r.sofa_max.toString.toDouble))

    val p = age.union(gender).union(sapadm).union(sapmin).union(sapmax).union(sofaadm).union(sofamin).union(sofamax)
    p
  }

  /**
   * Given a feature tuples RDD, construct features in vector
   * format for each patient. feature name should be mapped
   * to some index and convert to dense feature format.
    *
    * @param sc SparkContext to run
   * @param feature RDD of input feature tuples
   * @return
   */
  def construct(sc: SparkContext, feature: RDD[FeatureTuple]): RDD[(String, Vector)] = {

    /** save for later usage */
    //feature.take(10).foreach(println)

    /** create a feature name to id map*/

    /* FeatureTuple: ((patient-id, feature-name), feature-value) */

    val feat_names = feature.map(_._1._2).distinct()
    val feat_count = feat_names.count().toInt
    val feat_name_to_index_map = feat_names.zipWithIndex

   /** transform input feature into dense vector*/

    val result = feature
        .map(r => (r._1._1,(r._1._2, r._2)))
      .groupByKey()
      .map(r => (r._1, Vectors.dense(r._2.map(r => r._2).toArray)))
    //result.take(10).foreach(println)

    val scaler = new StandardScaler(withMean = true, withStd = true).fit(result.map(_._2))

    result.map({ case (patientID, featureVector) => (patientID, scaler.transform(Vectors.dense(featureVector.toArray)))})

  }

}


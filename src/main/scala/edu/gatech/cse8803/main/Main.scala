/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.main

import java.text.SimpleDateFormat

import edu.gatech.cse8803.features.FeatureConstruction
import org.apache.spark.mllib.classification.{SVMWithSGD,LogisticRegressionWithSGD}
import org.apache.spark.mllib.tree.RandomForest

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.storage.StorageLevel
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.model.{NoteEvent, ICUevent}

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Vectors, Vector}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source


object Main {
  def main(args: Array[String]) {

    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)


    val sc = createContext
    val sqlContext = new SQLContext(sc)
    /** initialize loading of data */
    //val (icuEvent = loadRddRawData(sqlContext)
    var (icuEvents, noteEvent) = loadRddRawData(sqlContext)


   icuEvents = icuEvents.map(r => (r.patientID, r)).reduceByKey( (v1,v2) => { if( v1.intime.getTime() > v2.intime.getTime() ) v1 else v2 })
      .map(_._2)

    //noteEvent.take(10).foreach(println)
    println("total noteevents:" + noteEvent.count())
    println("total icuevents:" + icuEvents.count())
    println("total patient in icuevents :" + icuEvents.map(r => r.patientID).distinct().count())
    println("total patient in noteevents:" + noteEvent.map(r => r.patientID).distinct().count())

        val featureTuples = FeatureConstruction.constructICUFeatureTuple(icuEvents)

    val rawFeatures = FeatureConstruction.construct(sc, featureTuples)
        //rawFeatures.take(10).foreach(println)
        // (label, vector)

        val rawFeaturesWithLabel = icuEvents.map(r => (r.patientID, r.flag)).join(rawFeatures).map{
        case (pid, (label, vector)) => LabeledPoint(if (label == "Y") 1.0 else 0.0, vector)
          }

        //rawFeaturesWithLabel.take(10).foreach(println)
        val splits = rawFeaturesWithLabel.randomSplit(Array(0.7, 0.3), seed = 11L)
        val train  = splits(0)
        val test   = splits(1)
        //test_svm(train,test)
        println("Result for imbalanced class")
        val (auroc1,aupr1)=run_SVM(train,test,100)
        val (auroc2,aupr2)=run_lr(train,test,100)
        val (auroc3,aupr3)=run_rf(train,test,numTrees=500)
        //test_lr(train,test)
        //test_RF(train,test)

        val dead=rawFeaturesWithLabel.filter(p=>p.label.toInt==1)
        val count=dead.count()
        val alive=sc.parallelize(rawFeaturesWithLabel.filter(p=>p.label.toInt==0).takeSample(withReplacement = true,count.toInt,11L))
        val balanced_features=sc.union(dead,alive)
        val ba_splits = balanced_features.randomSplit(Array(0.7, 0.3), seed = 11L)
        val ba_train  = ba_splits(0)
        val ba_test   = ba_splits(1)

        println("Result for balanced class")
        run_SVM(ba_train,ba_test,100)
        run_lr(ba_train,ba_test,100)
        run_rf(ba_train,ba_test,numTrees=500)

  }

  def run_SVM(train: RDD[LabeledPoint], test:RDD[LabeledPoint], numIterations: Int):(Double,Double)={
    //svm with sgd
    train.persist(StorageLevel.MEMORY_ONLY)
    val model = SVMWithSGD.train(train, numIterations)
    model.clearThreshold()
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    //scoreAndLabels.take(5).foreach(println)
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    val auPR  = metrics.areaUnderPR()
    train.unpersist()
    println("Area under ROC of SVM is  = " + auROC)
    println("Area under PR of SVM is  = " + auPR)
    (auROC,auPR)
  }


  def run_lr(train: RDD[LabeledPoint], test:RDD[LabeledPoint], numIterations: Int):(Double,Double)={
    //svm with sgd
    train.persist(StorageLevel.MEMORY_ONLY)
    val model = LogisticRegressionWithSGD.train(train, numIterations)
    model.clearThreshold()
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    //scoreAndLabels.take(5).foreach(println)
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    val auPR  = metrics.areaUnderPR()
    train.unpersist()
    println("Area under ROC of LR is  = " + auROC)
    println("Area under PR of LR is  = " + auPR)
    (auROC,auPR)
  }


  def run_rf(train: RDD[LabeledPoint], test:RDD[LabeledPoint], numTrees: Int):(Double,Double)={
    //svm with sgd
    train.persist(StorageLevel.MEMORY_ONLY)
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    //val numTrees = 100 // Use more in practice.
    val featureSubsetStrategy = "auto"
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32
    val model = RandomForest.trainClassifier(train, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    //scoreAndLabels.take(5).foreach(println)
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    val auPR  = metrics.areaUnderPR()
    train.unpersist()
    println("Area under ROC of rf is  = " + auROC)
    println("Area under PR of rf is  = " + auPR)
    (auROC,auPR)
  }

  def loadRddRawData(sqlContext: SQLContext): (RDD[ICUevent],RDD[NoteEvent]) = {
      val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm")
      val dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")


    /** load data using Spark SQL into three RDDs */
      //CSVUtils.loadCSVAsTable(sqlContext, "data/medication_orders_INPUT.csv")
      CSVUtils.loadCSVAsTable(sqlContext, "data/icustay_detail.csv", "icuevents")
      CSVUtils.loadCSVAsTable(sqlContext, "data/cln_note_events0.csv", "noteevents")
      //CSVUtils.loadCSVAsTable(sqlContext, "data/encounter_dx_INPUT.csv")

      var icustay = sqlContext.sql(
        """
          |SELECT icustay_id, subject_id, gender, hospital_expire_flg, icustay_intime, icustay_outtime, icustay_admit_age,
          |sapsi_first,sapsi_min,sapsi_max,sofa_first,sofa_min,sofa_max
          |FROM icuevents
        """.stripMargin)
        .map(r =>
        ICUevent(r(0).toString, r(1).toString, r(2).toString, r(3).toString, dateFormat2.parse(r(4).toString),
          dateFormat2.parse(r(5).toString), r(6).toString.toDouble,
          CSVUtils.parseDouble(r(7).toString), CSVUtils.parseDouble(r(8).toString),CSVUtils.parseDouble(r(9).toString),
            CSVUtils.parseDouble(r(10).toString), CSVUtils.parseDouble(r(11).toString),CSVUtils.parseDouble(r(12).toString)))
      icustay = icustay.filter( r => r.sapsi_first != -999999.0 & r.sofa_first != -999999.0)

      val notes = sqlContext.sql(
        """
          |SELECT subject_id,hadm_id,charttime,text
          |FROM noteevents
          |WHERE text IS NOT NULL and text <> ''
        """.stripMargin)
        .map(r => NoteEvent(r(0).toString,r(1).toString, dateFormat.parse(r(2).toString),r(3).toString))

    (icustay,notes)
    }


    def createContext(appName: String, masterUrl: String): SparkContext = {
      val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
      new SparkContext(conf)
    }

    def createContext(appName: String): SparkContext = createContext(appName, "local")

    def createContext: SparkContext = createContext("CSE 8803 Final Project", "local")

}

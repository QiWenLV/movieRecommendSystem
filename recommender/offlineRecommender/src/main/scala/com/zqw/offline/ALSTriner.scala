package com.zqw.offline

import breeze.numerics.sqrt
import com.zqw.config.{RecommenderConfig => RC}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

//case class MovieRating(val uid: Int, val mid: Int, val score: Double, val timestamp: Long)

//参数调优
object ALSTriner {

  def main(args: Array[String]): Unit = {

    //测试数据
    val config = Map (
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop24:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setAppName("ALSTrainer").setMaster(config("spark.cores"))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    //加载数据
    val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", RC.MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => Rating(rating.uid, rating.mid, rating.score)).cache()

    //输出最优参数
    adjustALSParams(ratingRDD)

    //最优参数：(70, 0.01, 0.16171316791926257)
    spark.close()
  }



  //输出最优参数
  def adjustALSParams(trainData: RDD[Rating]): Unit = {
    val (rank, iterations, lambda) = (50, 10, 0.01)

    //设定多个训练参数
    val result = for(rank <- Array(30, 40 ,50, 60, 70); lambda <- Array(1, 0.1, 0.01)) yield{
      //进行训练
      val model = ALS.train(trainData, rank, 5, lambda)
      //计算均方根误差
      val rmse = gerRmse(model, trainData)
      (rank, lambda, rmse)
    }

    println("最优参数：" + result.minBy(_._3))

  }


  def gerRmse(model: MatrixFactorizationModel, trainData: RDD[Rating]): Double = {

    //需要构造一个userProducts: RDD[(Int, Int)]
    val userMovies = trainData.map(item => (item.user, item.product))
    //矩阵计算
    val predictRating = model.predict(userMovies)

    //真实值
    val real = trainData.map(item => ((item.user, item.product), item.rating))
    //预测值
    val predict = predictRating.map(item => ((item.user, item.product), item.rating))

    //求方差，(mean是求平均数)
    val avg = real.join(predict).map{case ((uid, mid), (real, pre)) =>

      //真实值和预测值之间的差值
      val err = real - pre
      err * err
    }.mean()

    //方差为
    sqrt(avg)
  }


}

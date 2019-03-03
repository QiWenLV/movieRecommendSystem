package com.zqw.offline

import com.zqw.config.{RecommenderConfig => RC}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix


//原始数据样例类
case class Movie(val mid: Int, val name: String, val describe: String,
                 val timelong: String, val issue: String, val shoot: String,
                 val language: String, val genres: String, val actors: String,
                 val directors: String)

case class MovieRating(val uid: Int, val mid: Int, val score: Double, val timestamp: Long)

case class Tag(val uid: Int, val mid: Int, val tag: String, val timestamp: Long)


/**
  * MongoDB的连接配置
  * @param uri  链接
  * @param db   数据库
  */
case class MongoConfig(val uri: String, val db: String)


object OfflineRecommender {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("OfflineRecommender").setMaster(RC.config("spark.cores"))
      .set("spark.executor.memory", "3G").set("spark.driver.memory", "2G")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    //加载数据
    val mongoConfig = MongoConfig(RC.config("mongo.uri"), RC.config("mongo.db"))
    //将数据转化为一个三元组
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", RC.MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid, rating.mid, rating.score))

    //user数据(只有uid，用于模型计算)
    val userRDD = ratingRDD.map(_._1).distinct()
    //movie数据(只有mid，用于模型计算)
    val movieRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", RC.MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load().as[Movie].rdd.map(_.mid)

    //训练ALS模型
    println("数据读取完毕")

    //创建训练数据集
    val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))
    val (rank, iterations, kambda) = (50, 10, 0.01)

    val model = ALS.train(trainData, rank, iterations, kambda)

    //计算用户推荐矩阵
    //构造一个 usersProducts RDD
//    val userMovies = userRDD.cartesian(movieRDD)
//    //矩阵计算
//    val preRatings = model.predict(userMovies)
//
//    //(uid, (mid, 相似度评分))
//    val userRecs = preRatings.map(rating => (rating.user, (rating.product, rating.rating)))
//      .groupByKey()
//      .map{
//        case(uid, recs) =>
//          UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(RC.USER_MAX_RECOMMENDATION)
//            .map(x => Recommendation(x._1, x._2)))
//      }.toDF()
//
//    userRecs.write
//      .option("uri", mongoConfig.uri)
//      .option("collection", RC.USER_RECS)
//      .mode("overwrite")
//      .format("com.mongodb.spark.sql")
//      .save()

    //计算电影相似度矩阵
    //相似度矩阵
    val movieFeatures = model.productFeatures.map {
      case (mid, freatures) => (mid, new DoubleMatrix(freatures))
    }
    //矩阵相乘，求笛卡尔积(过滤相同电影，余弦计算，0.6以上才认为是相似，转化为样例类)
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter{case (a, b) => a._1 != b._1}
      .map{case (a,b) => (a._1, (b._1, consinSim(a._2, b._2)))}
      .filter(_._2._2 > 0.6)
      .groupByKey()
      .map{case (mid, items) => MovieRecs(mid, items.toList.map(x => Recommendation(x._1, x._2)))}
      .toDF()

    movieRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", RC.MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //关闭
    spark.close()
  }

  //余弦相似度计算方法, 传入两个Double 计算余弦，该结果就是两个电影的相似度
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
    //向量点击公式
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }

}

//推荐
case class Recommendation(rid: Int, r: Double )

//用户推荐
case class UserRecs(uid: Int, recs: Seq[Recommendation])

//电影的相似度
case class MovieRecs(mid: Int, recs: Seq[Recommendation])
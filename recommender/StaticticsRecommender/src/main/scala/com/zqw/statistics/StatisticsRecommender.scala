package com.zqw.statistics

import java.text.SimpleDateFormat
import java.util.Date

import com.zqw.config.{RecommenderConfig => RC}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


//原始数据样例类
case class Movie(val mid: Int, val name: String, val describe: String,
                 val timelong: String, val issue: String, val shoot: String,
                 val language: String, val genres: String, val actors: String,
                 val directors: String)

case class Rating(val uid: Int, val mid: Int, val score: Double, val timestamp: Long)
case class Tag(val uid: Int, val mid: Int, val tag: String, val timestamp: Long)


/**
  * MongoDB的连接配置
  * @param uri  链接
  * @param db   数据库
  */
case class MongoConfig(val uri: String, val db: String)


object StatisticsRecommender {



  def main(args: Array[String]): Unit = {
    

    val sparkConf = new SparkConf().setAppName("StaticticsRecommender").setMaster(RC.config("spark.cores"))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._


    //从MongoDB加载数据
    val mongoConfig = MongoConfig(RC.config("mongo.uri"), RC.config("mongo.db"))
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", RC.MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", RC.MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    //创建表
    ratingDF.createOrReplaceTempView("ratings")
    movieDF.createOrReplaceTempView("movies")


    //1. 统计所有历史数据中每个电影的评分数
    val rateMoreMoviesDf = spark.sql("select mid, count(mid) as count from ratings group by mid")
    rateMoreMoviesDf.write
      .option("uri", mongoConfig.uri)
      .option("collection", RC.RATE_MORE_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //2. 统计以月为单位每个电影的评分数
    // ->> mid, count, time
    //自定义udf函数，将秒时间转化为
    val sdf = new SimpleDateFormat("yyyyMM")
    spark.udf.register("changeData", (x: Int) => sdf.format(new Date(x * 1000L)). toInt)
    //进行转换
    val ratingOfYeahMouth = spark.sql("select mid, score, changeData(timestamp) as year_mouth from ratings")
    ratingOfYeahMouth.createOrReplaceTempView("ratingOfMouth")

    val rateMoreRecentlyMovies = spark.sql("select mid, count(mid) as count, year_mouth from ratingOfMouth group by year_mouth, mid")
    rateMoreRecentlyMovies.write
      .option("uri", mongoConfig.uri)
      .option("collection", RC.RATE_MORE_RECENTLY_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //3. 统计每个电影的平均评分
    val averageMoviesDF = spark.sql("select mid, avg(score) as avg from ratings group by mid")
    averageMoviesDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", RC.AVERAGE_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //4. 统计每种电影类别中评分最高的10个电影
    val movieWithScore = movieDF.join(averageMoviesDF, Seq("mid", "mid"))
    //所有的电影类别
    val genres = List("Action","Adventure","Animation","Comedy","Ccrime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
      ,"Romance","Science","Tv","Thriller","War","Western")
    //转换为RDD
    val genresRDD = spark.sparkContext.makeRDD(genres)
    //两个RDD做笛卡尔积的拼接，然后过滤
    val genresTopMovies = genresRDD.cartesian(movieWithScore.rdd)
      .filter{
        case (genres, row) => row.getAs[String]("genres").toLowerCase().contains(genres.toLowerCase())
      }
      .map{
        case (genres, row) => {
          (genres, (row.getAs[Int]("mid"), row.getAs[Double]("avg")))
        }
      }
      .groupByKey()
      .map{
        case (genres, items) =>
          GenresRecommendation(genres, items.toList.sortWith(_._2 > _._2).take(10).map(item => Recommendation(item._1, item._2)))
      }.toDF()

    genresTopMovies.write
      .option("uri", mongoConfig.uri)
      .option("collection", RC.GENRES_TOP_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    spark.stop()
  }
}

/**
  * 推荐对象
  * @param rid    mid
  * @param r      评分
  */
case class Recommendation(rid: Int, r: Double )

/**
  * 电影类别的推荐
  * @param genres   电影的类别
  * @param recs      top10的电影
  */
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])
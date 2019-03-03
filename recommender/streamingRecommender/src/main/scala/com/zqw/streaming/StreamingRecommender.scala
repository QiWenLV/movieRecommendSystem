package com.zqw.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.zqw.config.{RecommenderConfig => RC}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import scala.collection.mutable


/**
  * MongoDB的连接配置
  *
  * @param uri  链接
  * @param db   数据库
  */
case class MongoConfig(val uri: String, val db: String)


//推荐
case class Recommendation(rid: Int, r: Double )
//电影的相似度
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object ConnHelper extends Serializable{
  lazy val jedis = new Jedis(RC.config("jedis.uri"))
  lazy val mongoClient = MongoClient(MongoClientURI(RC.config("mongo.uri")))
}

object StreamingRecommender {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setAppName("StreamingRecommender").setMaster(RC.config("spark.cores")).set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(RC.config("mongo.uri"), RC.config("mongo.db"))




    //获取电影相似度矩阵----------广播变量(mid, (rid, r))

    val movieRecs = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", RC.MOVIE_RECS)
      .option("spark.mongodb.output.localThreshold",1500)
      .format("com.mongodb.spark.sql")
      .load()
      .select($"mid",$"recs")
      .as[MovieRecs]
      .rdd
      .map{recs => (recs.mid, recs.recs.map(x => (x.rid, x.r)).toArray)}
      .persist(StorageLevel.MEMORY_AND_DISK)
//
//          .option("spark.mongodb.output.maxBatchSize",64)
//
    println("相似度矩阵加载完成")
    //获取所有相似电影的Mid, 收集为Map[Int, Array[Int]] => Map[mid, 相似电影mid数组]
//    val simMoviesMatrix = movieRecs.map(rec=>(rec._1, rec._2.keys.toArray)).collectAsMap()


    //每个电影与其他电影的相似度，HASH[电影Id, HASH[电影Id2, Id1与Id2相似度]]
    val movie2movieSimilarity =  movieRecs.take(6000).toMap

    //每个电影的最相似的K个电影(只包含电影ID，把相似度去掉了)，HASH[电影Id, 相似的K个电影Ids]
    val topKMostSimilarMovies = movieRecs.map(rec=>(rec._1, rec._2.map(_._1).toArray)).take(6000).toMap


    println("Map转换完成")

    //电影相似度矩阵广播变量
    val bMovie2movieSimilarity = sc.broadcast(movie2movieSimilarity)
    //最相似电影HASH的广播变量
    val bTopKMostSimilarMovies = ssc.sparkContext.broadcast(topKMostSimilarMovies)

    //为了在核心任务执行前将广播变量提前pull到各个worker，所以这里做了一堆故意引用了广播的任务
    val firstRDD = ssc.sparkContext.parallelize(1 to 10000, 1000)
    val useless = firstRDD.map{ i =>
      val pullTopK = bTopKMostSimilarMovies.value.contains(i)
      val pullSim = bMovie2movieSimilarity.value.contains(i)
      (i, pullTopK, pullSim)
    }.count()
    println(useless)

    println("广播变量加载完成")


    //创建Kafka对象
    val kafkaParams = Map(
      "bootstrap.servers" -> "hadoop24:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(RC.config("kafka.topic")), kafkaParams)
    )
    //kafka的数据：Uid|Mid|Score|Timestamp
    val ratingStream = kafkaStream.map(msg => {
      val attr = msg.value().split("\\|")
      (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).cache()
    print("收到Kafka消息：--------------------------")
//    ratingStream.print()
//    println()

    ratingStream.foreachRDD{rdd =>
      if(!rdd.isEmpty) {
        rdd.map { case (uid, mid, score, timestamp) =>
          println(">>>>>>>>>>>>>>>>>>>>>" + uid + "--" + mid)

          //从Redis中获取该用户最近M次评分
          //val userRecentlyRatings = getUserRecentlyRating(RC.MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)
          val userRecentlyRatings = Array((4005, 4.2), (129, 3.4), (32381, 2.1))

          //获取电影P 最相似的K个电影(待选电影)
          val simMovies = getTopSimMovies(bTopKMostSimilarMovies.value, RC.MAX_SIM_MOVIES_NUM, mid, uid)

          //计算待选电影的推荐优先级
          val streamRecs = computeMovieScores(bMovie2movieSimilarity.value, userRecentlyRatings, simMovies)


        }.count()
      }
    }


    //启动Streaming程序
    ssc.start()
    ssc.awaitTermination()
  }


  /**
    * 从Redis中获取该用户最近M次评分
    * @param num 获取数量
    * @param uid 用户ID
    * @return
    */
  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {

    //使用Jedis，返回(uid, score)
    jedis.lrange("uid:" + uid.toString, 0, num).map{item =>
      val attr = item.split("\\|")
      (attr(0).trim.toInt, attr(1).trim.toDouble)
    }.toArray
  }


  def getTopSimMovies2(num: Int, mid: Int, uid: Int, allSimMovies:  Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Array[Int] = {

    //从广播变量的电影相似度矩阵中获取当前电影所有的相似电影
//    val allSimMovies = simMovies.filter(_._1 == mid).collectAsMap()(mid).toArray

    //获取用户已经观看过的电影(在MongoDB中的评分表找查询用户ID，返回该用户评分了的所有电影)
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(RC.MONGODB_RATING_COLLECTION).find(MongoDBObject("uid" -> uid)).toArray.map{item =>
      item.get("mid").toString.toInt
    }

    //过滤掉已经评分过的电影，并且按相似度排序，只留下mid
    allSimMovies.filter(x => !ratingExist.contains(x._1))
      .sortWith(_._2 > _._2).take(num)
      .map(_._1)
  }

  def computeMovieScores2(movieSimRecs:  Array[(Int, Array[(Int, Double)])], userRecentlyRatings: Array[(Int, Double)], topSimMovies: Array[Int]): Array[(Int, Double)] = {

    //用于保存每一个待选电影和最近评分的每个电影的权重得分
    val score = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    //用于保存每个电影的增强因子数
    val increMap = mutable.HashMap[Int, Int]()
    //用于保存每个电影的减弱因子数
    val decreMap = mutable.HashMap[Int, Int]()
    var simScore = 0.0
    for(cMovieId <- topSimMovies; (rMovieId, rate) <- userRecentlyRatings){

      val (smallerId, biggerId) = if (cMovieId < rMovieId) (cMovieId, rMovieId) else (rMovieId, cMovieId)

      if(smallerId == biggerId) {
        simScore = 0.0
      }else {
        //在电影相似度矩阵中查找两个电影的相识读
        simScore = movieSimRecs.toMap.get(smallerId) match {
          case Some(subSimHash) =>
            subSimHash.toMap.get(biggerId) match{
              case Some(sim) => sim
              case None => 0.0
            }
          case None => 0.0
        }
      }

      //计算相似度

      //相似度大于0.6，才算相似
      if(simScore > 0.6){
        score += ((cMovieId, simScore * rate))
        if(rate >= 3.0){
          increMap(cMovieId) = increMap.getOrDefault(cMovieId, 0) + 1
        }else {
          decreMap(cMovieId) = decreMap.getOrDefault(cMovieId, 0) + 1
        }
      }
    }

    //返回数据Array(mid, score)
    score.toArray.groupBy(_._1).map{case (mid, sims) =>
      //取平均值，然后计算因子的影响(因子取对数，避免影响过大)
      (mid, sims.map(_._2).sum / sims.length + log(increMap.getOrElse[Int](mid, 1)) - log(decreMap.getOrElse[Int](mid, 1)))
    }.toArray
  }

//  def getMoviesSimScore2(simMovies: RDD[ Map[Int, Double]], userRatingMovie: Int): Double = {
//    //在矩阵中找到待选电影
//    simMovies match {
//      //找到之后，在待选电影的相似度列表找寻找用户评分电影
//      case Some() => sim.get(userRatingMovie) match {
//        case Some(score) => score
//        case None => 0.0
//      }
//      case None => 0.0
//    }
//  }

  //----------------------------------------------------------------------------------------------------------

  /**
    * 获取电影P 最相似的K个电影
    * @param num            相似电影的数量
    * @param mid            当前电影的ID
    * @param uid            当前的评分用户
    * @param simMovies      电影相似度矩阵的广播变量值
    * @param mongoConfig    MongoDB的配置
    * @return
    */
  def getTopSimMovies(simMovies:  scala.collection.Map[Int, Array[Int]], num: Int, mid: Int, uid: Int)(implicit mongoConfig: MongoConfig): Array[Int] = {

    //从广播变量的电影相似度矩阵中获取当前电影所有的相似电影
    val similarMoviesBeforeFilter = simMovies.getOrElse(mid, Array[Int]())

    //获取用户已经观看过的电影(在MongoDB中的评分表找查询用户ID，返回该用户评分了的所有电影)
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(RC.MONGODB_RATING_COLLECTION).find(MongoDBObject("uid" -> uid)).toArray.map{item =>
      item.get("mid").toString.toInt
    }

    //过滤掉已经评分过的电影，并且按相似度排序，只留下mid
    similarMoviesBeforeFilter.filter(ratingExist.contains(_) == false)
  }


  /**
    * 让每一个待选电影与用户近期的喜欢的电影进行相识度比较，然后计算增强因子和减弱因子，最后的得出所有待选电影的推荐分数
    * @param simMovies              电影相似度矩阵
    * @param userRecentlyRatings    用户近期的N条评分(mid, score)
    * @param topSimMovies           电影的相似度TopN(待选电影)
    * @return
    */
  def computeMovieScores(simMovies: collection.Map[Int, Map[Int, Double]], userRecentlyRatings: Array[(Int, Double)], topSimMovies: Array[Int]): Array[(Int, Double)] = {

    //用于保存每一个待选电影和最近评分的每个电影的权重得分
    val score = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    //用于保存每个电影的增强因子数
    val increMap = mutable.HashMap[Int, Int]()
    //用于保存每个电影的减弱因子数
    val decreMap = mutable.HashMap[Int, Int]()

    for(topSimMovie <- topSimMovies; userRecentlyRating <- userRecentlyRatings){
      //计算相似度
      val simScore = getMoviesSimScore(simMovies, userRecentlyRating._1, topSimMovie)
      //相似度大于0.6，才算相似
      if(simScore > 0.6){
        score += ((topSimMovie, simScore * userRecentlyRating._2))
        if(userRecentlyRating._2 > 3){
          increMap(topSimMovie) = increMap.getOrDefault(topSimMovie, 0) + 1
        }else {
          decreMap(topSimMovie) = decreMap.getOrDefault(topSimMovie, 0) + 1
        }
      }
    }

    //返回数据Array(mid, score)
    score.groupBy(_._1).map{case (mid, sims) =>
      //取平均值，然后计算因子的影响(因子取对数，避免影响过大)
      (mid, sims.map(_._2).sum / sims.length + log(increMap(mid)) - log(decreMap(mid)))
    }.toArray
  }

  def log(m: Int): Double = {
    math.log(m) / math.log(2)
  }

  /**
    * 获取两个电影之间的相似度
    * @param simMovies        电影相似度矩阵
    * @param userRatingMovie  用户最近评分电影
    * @param topSimMovie      待选电影
    * @return
    */
  def getMoviesSimScore(simMovies: collection.Map[Int, Map[Int, Double]], userRatingMovie: Int, topSimMovie: Int): Double = {
    //在矩阵中找到待选电影
    simMovies.get(topSimMovie) match {
        //找到之后，在待选电影的相似度列表找寻找用户评分电影
        case Some(sim) => sim.get(userRatingMovie) match {
          case Some(score) => score
          case None => 0.0
        }
        case None => 0.0
      }
  }

  /**
    * 将数据保存到MongoDB  数据格式：uid -> 1, recs -> 22:4.5 | 45:3.8
    * @param streamRecs     流式的推荐结果
    * @param mongoConfig    MongoDB的配置
    */
  def saveRecsToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
    //连接对象
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(RC.MONGODB_STREAM_RECS_COLLECTION)
    //删除原来的推荐信息
    streamRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))
    //新增推荐列表
    streamRecsCollection.insert(MongoDBObject("uid" -> uid, "recs" -> streamRecs.map(x => x._1 + ":" + x._2).mkString("|")))

    println("数据计算完成：" + uid + "---" + streamRecs.map(x => x._1 + ":" + x._2).mkString("|"))
  }
}

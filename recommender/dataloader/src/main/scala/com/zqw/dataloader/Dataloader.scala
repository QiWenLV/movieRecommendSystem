package com.zqw.dataloader


import java.net.InetAddress

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.casbah.commons.MongoDBObject
import com.zqw.config.{RecommenderConfig => RC}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

//数据分析


/**
  * Movies 数据集通过 ^ 符合分割
  *
  * 151^                      //电影ID
  * Rob Roy (1995)^           //电影的名称
  * In the  honour.^          //电影的描述
  * 139 minutes^              //电影时长
  * August 26, 1997^          //发行日期
  * 1995^                     //拍摄日期
  * English ^                 //语言
  * Action|Drama|Romance ^    //电影类型
  * Liam Neeson| McCardie|    //电影的演员
  * Michael Caton-Jones       //电影的导演
  **/
case class Movie(val mid: Int, val name: String, val describe: String,
                 val timelong: String, val issue: String, val shoot: String,
                 val language: String, val genres: String, val actors: String,
                 val directors: String)

/**
  * Roting数据集，用户对电影的评分数据集，用逗号分割
  *
  * 1,          //用户的ID
  * 1263,       //电影的ID
  * 2.0,        //用户对于电影的评分
  * 1260759151  //用户对电影评分的时间
  */
case class Rating(val uid: Int, val mid: Int, val score: Double, val timestamp: Long)

/**
  * tag数据据，用户对电影的标签数据集，用逗号分割
  *
  * 15,             //用户ID
  * 100365,         //电影ID
  * documentary,    //标签具体内容
  * 1425876220      //打标签的时间
  */
case class Tag(val uid: Int, val mid: Int, val tag: String, val timestamp: Long)

/**
  * MongoDB的连接配置
  * @param uri  链接
  * @param db   数据库
  */
case class MongoConfig(val uri: String, val db: String)

/**
  * ES的连接配置
  * @param httpHosts      http的主机列表  以逗号分割
  * @param transportHosts Transport主机列表，以逗号分割
  * @param index          需要操作的索引
  * @param clustername    ES集群的名称
  */
case class ESConfig(val httpHosts: String, val transportHosts: String, val index: String, val clustername: String)

/**
  * 数据的主加载服务
  */
object Dataloader {



  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(RC.config("spark.cores"))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    //加载数据集
    val movieRDD = spark.sparkContext.textFile(RC.MOVIE_DATA_PATH)
    val ratingRDD = spark.sparkContext.textFile(RC.RATING_DATA_PATH)
    val tagRDD = spark.sparkContext.textFile(RC.TAG_DATA_PATH)

    //将RDD转化为DataFrame
    val movieDF = movieRDD.map(item => {
      val attr = item.split("\\^")
      Movie(attr(0).toInt,
        attr(1).trim,
        attr(2).trim,
        attr(3).trim,
        attr(4).trim,
        attr(5).trim,
        attr(6).trim,
        attr(7).trim,
        attr(8).trim,
        attr(9).trim
      )
    }).toDF()

    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toLong)
    }).toDF()

    val tagDF = tagRDD.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toLong)
    }).toDF()


    implicit val mongoConfig = MongoConfig(RC.config("mongo.uri"), RC.config("mongo.db"))
    implicit val esConfig = ESConfig(RC.config("es.httpHosts"), RC.config("es.transportHosts"), RC.config("es.index"), RC.config("es.cluster.name"))


    //需要将数据保存在MongoDB中
    storeDataToMongoDB(movieDF, ratingDF, tagDF)

    //在将数据导入ES之前要对数据进行处理，需要将Tag数据加入到Movie中，方便ES查询
    import org.apache.spark.sql.functions._
    //按mid进行聚合，然后tag合并成一个字段
    val newTag = tagDF.groupBy($"mid").agg(concat_ws("|", collect_set($"tag")).as("tags")).select("mid", "tags")
    //必须用left join
    val movieWithTagsDF = movieDF.join(newTag, Seq("mid", "mid"), "left")
   // storeDataTOES(movieWithTagsDF)

    spark.stop()
  }


  def storeDataToMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {

    //建立连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    //如果Mongo有对应的数据库，则删除(直接删除)
    mongoClient(mongoConfig.db)(RC.MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(RC.MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(RC.MONGODB_TAG_COLLECTION).dropCollection()

    //将当前数据写入
    movieDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", RC.MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", RC.MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", RC.MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表建立索引
    mongoClient(mongoConfig.db)(RC.MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(RC.MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(RC.MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(RC.MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(RC.MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    //关闭连接
    mongoClient.close()
  }

  def storeDataTOES(movieDF: DataFrame)(implicit eSConfig: ESConfig): Unit = {
    //新建连接
    val settings: Settings = Settings.builder().put("cluster.name", eSConfig.clustername).build()
    val esClient = new PreBuiltTransportClient(settings)

    //将TransportHosts添加到esClient连接中
    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    eSConfig.transportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host: String, port: String) =>{
        println(host + "-------" + port)
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
      }
    }

    //需要清除ES中遗留的数据
    if(esClient.admin().indices().exists(new IndicesExistsRequest(eSConfig.index)).actionGet().isExists){
      //存在则删除
      esClient.admin().indices().delete(new DeleteIndexRequest(eSConfig.index))
    }

    //新建索引库
    esClient.admin().indices().create(new CreateIndexRequest(eSConfig.index))

    //将数据写入到ES
    movieDF.write
      .option("es.nodes", eSConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(eSConfig.index + "/" + RC.ES_MOVIE_INDEX)

  }
}

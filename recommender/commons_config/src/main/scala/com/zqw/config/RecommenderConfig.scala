package com.zqw.config

object RecommenderConfig {

  //测试数据
  val config = Map(
    "spark.cores" -> "local[2]",
    "mongo.uri" -> "mongodb://localhost:27017/recommender",
    "mongo.db" -> "recommender",
    "es.httpHosts" -> "hadoop24:9200",
    "es.transportHosts" -> "hadoop24:9300",
    "es.index" -> "recommender",
    "es.cluster.name" -> "hadoop24_es",
    "kafka.topic" -> "recommender",
    "jedis.uri" -> "hadoop24"
  )

  //MongoDB的数据表名
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"

  //统计表的名称
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  val MOVIE_DATA_PATH = "F:\\develop\\idea_hadoop\\movieRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\small\\movies.csv"
  val RATING_DATA_PATH = "F:\\develop\\idea_hadoop\\movieRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\small\\ratings.csv"
  val TAG_DATA_PATH = "F:\\develop\\idea_hadoop\\movieRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\small\\tags.csv"


  //ES 索引库名
  val ES_MOVIE_INDEX = "Movie"

  //为每个用户推荐电影的数量
  val USER_MAX_RECOMMENDATION = 20

  //用户电影推荐表
  val USER_RECS = "UserRecs"
  //电影相似度矩阵
  val MOVIE_RECS = "MovieRecs"

  //实时推荐表
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"

  //实时推荐，取最近M次评分纪录
  val MAX_USER_RATINGS_NUM = 20
  //实时推荐，取目标电影相似度最高的N部电影
  val MAX_SIM_MOVIES_NUM = 20

}

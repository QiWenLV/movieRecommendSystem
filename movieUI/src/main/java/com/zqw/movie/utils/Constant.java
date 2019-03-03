package com.zqw.movie.utils;

/**
 * 定义业务系统的常量
 */
public class Constant {

    //返回信息
    public static final String RESULT_KEY_S = "success";
    public static final String RESULT_KEY_F = "fail";
    public static final String RESULT_CODE_SUCCCESS = "200";


    public static final String MONGO_DATABASE = "recommender";

    //用户表
    public static final String MONGODB_USER_COLLECTION = "User";


    //MongoDB的数据表名
    public static final String MONGODB_MOVIE_COLLECTION = "Movie";
    public static final String MONGODB_RATING_COLLECTION = "Rating";
    public static final String MONGODB_TAG_COLLECTION = "Tag";

    //统计表的名称
    //优质电影表
    public static final String RATE_MORE_MOVIES = "RateMoreMovies";
    //最热电影表
    public static final String RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies";
    //电影平均评分表
    public static final String AVERAGE_MOVIES = "AverageMovies";
    //电影类别Top10表
    public static final String GENRES_TOP_MOVIES = "GenresTopMovies";

    //用户电影推荐矩阵表
    public static final String USER_RECS = "UserRecs";
    //电影相似度矩阵
    public static final String MOVIE_RECS = "MovieRecs";

    //实时推荐表
    public static final String MONGODB_STREAM_RECS_COLLECTION = "StreamRecs";


    //ES的索引库
    public static final String ES_INDEX = "recommend";

    //使用的Type
    public static final String ES_TYPE = "Movie";
    public static final int USER_RATING_QUEUE_SIZE = 20;
    public static final String USER_RATING_LOG_PREFIX = "USER_RATING_LOG_PREFIX:";


}

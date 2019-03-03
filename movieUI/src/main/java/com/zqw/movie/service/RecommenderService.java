package com.zqw.movie.service;

import com.zqw.movie.model.domain.Movie;
import com.zqw.movie.model.recom.Recommendation;
import com.zqw.movie.model.request.GetHybridRecommendationRequest;
import com.zqw.movie.utils.Constant;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

//推荐服务
@Service
public class RecommenderService {

    @Resource
    private MongoTemplate mongoTemplate;

    public List<Recommendation> getHybridRecommendation(GetHybridRecommendationRequest request){

        //获得实时推荐结果

        //获得ALS的离线推荐结果

        //获得基于内容推荐的推荐结果

        //返回结果

        return null;
    }

    //
    public List<Recommendation> getUserCFMovies(int uid, int sum){

        return null;
    }


    //离线推荐信息接口
    public List<Recommendation> getStreamRecsMovies(int uid, int num){
        Query query = new Query(Criteria.where("uid").is(uid));
        Movie movieStream = mongoTemplate.findOne(query, Movie.class, Constant.MONGODB_STREAM_RECS_COLLECTION);

        List<Recommendation> result = new ArrayList<>();
        //进行数据切割
//        for (String s : movieStream.getGenres().split("\\|")) {
//            result.add(new Recommendation(s()))
//        }

        return result;
    }
}

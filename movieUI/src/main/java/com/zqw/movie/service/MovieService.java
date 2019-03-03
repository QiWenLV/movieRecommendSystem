package com.zqw.movie.service;

import com.zqw.movie.model.domain.Movie;
import com.zqw.movie.model.recom.Recommendation;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

@Service
public class MovieService {

    @Resource
    private MongoTemplate mongoTemplate;

    public List<Movie> getMoviesSyMids(List<Integer> ids) {

        Query query = new Query(new Criteria("mid").all(ids));

        return mongoTemplate.find(query, Movie.class);
    }

}

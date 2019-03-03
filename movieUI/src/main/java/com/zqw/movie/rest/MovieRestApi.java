package com.zqw.movie.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@Controller
@RequestMapping("/rect/movies")
public class MovieRestApi {



    //-----------数据接口

    //实时推荐信息接口(混合推荐)
    //http://localhost:8088/rest/movie/stream?username=zqw1&num=20
    //
    @RequestMapping(path = "/stream")
    public void getRealtimeRecommendations(String username, int num) {


//        return null;
    }

    //离线推荐信息接口
    public void offLineMovie(){

    }

    //热门推荐信息接口

    //优质电影信息接口

    //最新电影信息接口

    //------------模糊检索

    //基于名称或者描述的模糊检索

    //------------电影的详细页面

    //单个电影信息

    //给电影打标签

    //获取电影的所有标签

    //获取一个电影的相似的电影推荐

    //提给电影打分功能

    //-----------电影的类别页面

    //提供电影类别查找

    //-----------用户空间页面

    //提供用户的所有电影评分记录


}

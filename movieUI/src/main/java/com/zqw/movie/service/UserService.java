package com.zqw.movie.service;

import com.zqw.movie.model.domain.User;
import com.zqw.movie.model.request.LoginRequest;
import com.zqw.movie.model.request.UpdateUserGenresRequest;
import com.zqw.movie.model.result.ResultStatus;
import com.zqw.movie.utils.Constant;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

//用户服务
@Service
public class UserService {

    @Resource
    private MongoTemplate mongoTemplate;

    public boolean registerUser(LoginRequest request) {

        //判断是否已经注册
        if(registerCheck(request.getUsername()) != null){
            return false;
        }

        //创建一个用户
        User user = new User();
        user.setUsername(request.getUsername());
        user.setPassword(request.getPassword());
        user.setFirst(true);

        //插入一个用户
        mongoTemplate.save(user);

        return true;
    }

    public ResultStatus loginUser(LoginRequest request){

        //判断是否已经注册
        User byId = registerCheck(request.getUsername());
        if(byId == null){
            return new ResultStatus("未注册", "201", false);
        }

        //做密码校验
        if(request.getPassword().equals(byId.getPassword())){
            return new ResultStatus(Constant.RESULT_KEY_S, Constant.RESULT_CODE_SUCCCESS, true);
        }else {
            return new ResultStatus("用户名密码不正确", "202", false);
        }
    }

    public ResultStatus updateUserGenres(UpdateUserGenresRequest request) {

        User user = registerCheck(request.getUsername());
        if(user == null){
            return new ResultStatus("未注册", "201", false);
        }

        Query query = new Query(Criteria.where("username").is(user.getUsername()));
        Update update = new Update();
        update.set("prefGenres", request.getGenres());

        mongoTemplate.updateFirst(query, update, User.class);

        return new ResultStatus(Constant.RESULT_KEY_S, Constant.RESULT_CODE_SUCCCESS, true);
    }

    //判断用户是否注册
    private User registerCheck(String username){
        if (!username.isEmpty()){
            Query query = new Query(Criteria.where("username").is(username));
            List<User> users = mongoTemplate.find(query, User.class);
            if(users.size() == 0){
                return null;
            } else {
                return users.get(0);
            }
        }
        return null;
    }

}

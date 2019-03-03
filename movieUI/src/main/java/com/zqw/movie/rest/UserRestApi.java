package com.zqw.movie.rest;

import com.zqw.movie.model.request.LoginRequest;
import com.zqw.movie.model.request.UpdateUserGenresRequest;
import com.zqw.movie.model.result.ResultStatus;
import com.zqw.movie.service.UserService;
import com.zqw.movie.utils.Constant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;

//处理用户相关操作
@RestController
@RequestMapping("/rest/users")
public class UserRestApi {

    @Autowired
    private UserService userService;

    //用户注册
    // http://localhost:8088/rest/users/register?username=zqw&password=123
    //{
    //    "key": "success",
    //    "code": "200",
    //    "status": true
    //}
    @RequestMapping(path = "/register", produces = "application/json", method = RequestMethod.GET)
    public ResultStatus registerUser(@RequestParam("username") String user, @RequestParam("password")String pass){

        return new ResultStatus(Constant.RESULT_KEY_S, Constant.RESULT_CODE_SUCCCESS, userService.registerUser(new LoginRequest(user, pass)));
    }

    //用户登录
    //http://localhost:8088/rest/users/login?username=zqw&password=123
    @RequestMapping(path = "/login", produces = "application/json", method = RequestMethod.GET)
    public ResultStatus loginUser(@RequestParam("username") String user, @RequestParam("password")String pass){

        return userService.loginUser(new LoginRequest(user, pass));
    }


    //用户注册
    // http://localhost:8088/rest/users/genres?username=zqw&genres=a|b|c|d
    //{
    //    "key": "success",
    //    "code": "200",
    //    "status": true
    //}
    //添加用户偏爱影片类别
    @RequestMapping(path = "/genres", produces = "application/json", method = RequestMethod.GET)
    public ResultStatus addGenres(String username, String genres){
        return userService.updateUserGenres(new UpdateUserGenresRequest(username, Arrays.asList(genres.split("#"))));
    }
}

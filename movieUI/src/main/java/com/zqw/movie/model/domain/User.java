package com.zqw.movie.model.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.zqw.movie.utils.Constant;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Document(collection = Constant.MONGODB_USER_COLLECTION)
public class User {
    @Transient
    @JsonIgnore
    private String _id;

    @Id
    private int uid;

    private String username;

    private String password;

    //是否首次登录
    private boolean first;

    //注册时间
    private long timestamp;

    //保存用户首选喜欢电影的类别
    private List<String> prefGenres = new ArrayList<>();

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.uid = username.hashCode();
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isFirst() {
        return first;
    }

    public void setFirst(boolean first) {
        this.first = first;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean passwordMatch(String password) {
        return this.password.compareTo(password) == 0;
    }

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public List<String> getPrefGenres() {
        return prefGenres;
    }

    public void setPrefGenres(List<String> prefGenres) {
        this.prefGenres = prefGenres;
    }
}
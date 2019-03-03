package com.zqw.movie.model.result;

public class ResultStatus {

    private String key;
    private String code;
    private boolean status;

    public ResultStatus() {
    }

    public ResultStatus(String key, String code, boolean status) {
        this.key = key;
        this.code = code;
        this.status = status;
    }

    @Override
    public String toString() {
        return "ResultStatus{" +
                "key='" + key + '\'' +
                ", code='" + code + '\'' +
                ", status=" + status +
                '}';
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public boolean isStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }
}

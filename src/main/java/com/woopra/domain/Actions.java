package com.woopra.domain;

/**
 * @author : anudeep on 2/15/18
 * @project : MapReduce
 */
public class Actions {

   public String id;
    public long time;

    @Override
    public String toString() {
        return "{" +
                "\"id\":\"" + id + '\"' +
                ", \"time\":" + time +
                '}';
    }
}

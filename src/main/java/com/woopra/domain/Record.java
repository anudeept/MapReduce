package com.woopra.domain;

import java.util.List;

/**
 * @author : anudeep on 2/15/18
 * @project : MapReduce
 */
public class Record {

   public String pid;
   public List<Actions> actions;

    @Override
    public String toString() {
        return "{" +
                "\"pid\":\"" + pid + '\"' +
                ", \"actions\":" + actions +
                '}';
    }
}

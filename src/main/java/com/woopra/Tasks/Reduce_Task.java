package com.woopra.Tasks;

import com.woopra.callback.CallBack;

import java.util.*;

/**
 * @author : anudeep on 2/16/18
 * @project : MapReduce
 */
public class Reduce_Task implements Task{

    Map<String, List<Long>> data;
    CallBack reduceCallBack;

    public Reduce_Task(Object data, CallBack reduceCallBack) {
        this.data =(Map<String, List<Long>>) data;
        this.reduceCallBack = reduceCallBack;
    }

    @Override
    public Integer call() throws Exception {
        Iterator<String> itr = data.keySet().iterator();
        Map<String, Long> records = new HashMap<>();
        while ((itr.hasNext())) {
            String key = itr.next();
            long val = 0;
            if (!data.get(key).isEmpty()) {
                val = Collections.max(data.get(key));
            }
            records.put(key, val);
        }
        reduceCallBack.completed(records);
        return records.size();
    }
}

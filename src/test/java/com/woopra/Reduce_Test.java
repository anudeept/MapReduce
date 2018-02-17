package com.woopra;

import com.woopra.Tasks.TaskExecutor;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * @author : anudeep on 2/16/18
 * @project : MapReduce
 */
public class Reduce_Test {

    @Test
    public void reduce_Test() {
        Map<String, List<Long>> map = new HashMap<>();
        List<Long> list = Arrays.asList(1491033515000l, 1491033511000l, 1491033511000l, 1491034106000l);
        map.put("tVs45iwHpQ5i", list);
        TaskExecutor taskExecutor = new TaskExecutor();
        MapReduce mapReduce = new MapReduce();
        List<Object> arrList = new ArrayList<>();
        arrList.add(map);
        Map<String, Long> totalReduceValues= mapReduce.reduce(arrList, taskExecutor);
        Assert.assertEquals(1491034106000l,totalReduceValues.get("tVs45iwHpQ5i").longValue());
    }
}

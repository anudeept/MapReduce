package com.woopra;

import com.woopra.Tasks.TaskExecutor;
import com.woopra.domain.Record;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author : anudeep on 2/16/18
 * @project : MapReduce
 */
public class Group_Test {
    List<Record> totalRecords;
    List<Map<String, List<Long>>> groupRecords;
    @Before
    public void setup() {
        List<Object> files = new ArrayList<>();
        files.add("file.txt");
        TaskExecutor taskExecutor = new TaskExecutor();
        MapReduce mapReduce = new MapReduce();
        totalRecords = mapReduce.map(files, taskExecutor);
        groupRecords=mapReduce.groupByPID(totalRecords,500);
    }

    @Test
    public void group_Test(){
        Assert.assertEquals(3,groupRecords.size());
        Assert.assertEquals(500,groupRecords.get(0).size());
        Assert.assertEquals(500,groupRecords.get(1).size());
        Assert.assertEquals(463,groupRecords.get(2).size());
    }


}

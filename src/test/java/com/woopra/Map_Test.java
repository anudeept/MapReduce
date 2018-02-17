package com.woopra;

import com.woopra.Tasks.Map_Task;
import com.woopra.Tasks.Task;
import com.woopra.Tasks.TaskExecutor;
import com.woopra.domain.Record;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * @author : anudeep on 2/16/18
 * @project : MapReduce
 */
public class Map_Test {

    List<Future<Integer>> futures;
    List<Record> totalRecords = new ArrayList<>();

    @Before
    public void setup() {
        String fileName = "file.txt";
        List<Object> files = new ArrayList<>();
        files.add(fileName);
        TaskExecutor taskExecutor = new TaskExecutor();
        //Map Function
        List<Task> map_Tasks = files.stream().map(o -> {
            Task task = new Map_Task(o, values -> {
                List<Record> records = (List<Record>) values;
                totalRecords.addAll(records);
            });
            return task;
        }).collect(Collectors.toList());
        futures = taskExecutor.createPool(map_Tasks);
    }

    @Test
    public void task_Test() {
        Assert.assertEquals(1, futures.size());
    }

    @Test
    public void records_Test() {
        Assert.assertEquals(3864, totalRecords.size());
    }

    @Test
    public void futures_Test()  {
        try {
            Assert.assertEquals(3864, futures.get(0).get().intValue());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}

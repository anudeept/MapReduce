package com.woopra;

import com.woopra.Tasks.TaskExecutor;
import com.woopra.domain.Record;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author : anudeep on 2/16/18
 * @project : MapReduce
 */
public class SortAndLimit_Test {
    Map<String, Long> reduceFinalRecords;

    @Before
    public void setup() {
        MapReduce mapReduce = new MapReduce();
        List<Object> files = new ArrayList<>();
        files.add("file.txt");
        TaskExecutor taskExecutor = new TaskExecutor();
        //Map Function
        List<Record> totalRecords = mapReduce.map(files, taskExecutor);
        //Grouping Function
        List<Map<String, List<Long>>> groupRecords = mapReduce.groupByPID(totalRecords, 500);
        //Reduce Function
        Map<String, Long> totalReduceValues = mapReduce.reduce(new ArrayList<>(groupRecords), taskExecutor);
        reduceFinalRecords = mapReduce.sortAndLimit(totalReduceValues, 100);

    }

    @Test
    public void limit_Test() {
        Assert.assertEquals(100, reduceFinalRecords.size());
    }
    @Test
    public void sort_Test() {
        List<Long> values = new ArrayList<>(reduceFinalRecords.values());
        List<Long> values_sort = new ArrayList<>(reduceFinalRecords.values());
        Collections.sort(values_sort, Collections.reverseOrder());
        Assert.assertEquals(values, values_sort);
    }
}

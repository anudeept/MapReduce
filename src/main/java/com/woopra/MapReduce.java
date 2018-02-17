package com.woopra;

import com.woopra.Tasks.Map_Task;
import com.woopra.Tasks.TaskExecutor;
import com.woopra.Tasks.Reduce_Task;
import com.woopra.Tasks.Task;
import com.woopra.callback.CallBack;
import com.woopra.domain.Actions;
import com.woopra.domain.Record;
import org.apache.log4j.Logger;

import javax.annotation.Resources;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author : anudeep on 2/15/18
 * @project : MapReduce
 */
public class MapReduce {
    final static Logger log = Logger.getLogger(MapReduce.class);

    public static void main(String[] args) {
        String fileName = "file.txt";
        List<Object> files = new ArrayList<>();
        files.add(fileName);
        MapReduce mapReduce = new MapReduce();
        mapReduce.processData(files);
    }

    /**
     * processData- Process data from a txt file and find recent 100 visitors based on Max action time
     *
     * @param fileList
     */
    public void processData(List<Object> fileList) {
        try {

            TaskExecutor taskExecutor = new TaskExecutor();
            //Map Function
            List<Record> totalRecords = map(fileList, taskExecutor);
            //Grouping Function
            List<Map<String, List<Long>>> groupRecords = groupByPID(totalRecords, 500);
            //Reduce Function
            Map<String, Long> totalReduceValues = reduce(new ArrayList<>(groupRecords), taskExecutor);
            //Sort And Limit
            LinkedHashMap<String, Long> reduceFinalRecords = sortAndLimit(totalReduceValues, 100);
            System.out.println("No of Records=" + reduceFinalRecords.size());
            System.out.println(reduceFinalRecords);
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
    }

    /**
     * map - Process mutiple files parallel using mThread pool
     *
     * @param files
     * @param taskExecutor
     * @return - Record data with PID and list of Activity Times.
     */
    public List<Record> map(List<Object> files, TaskExecutor taskExecutor) {
        //Map Function
        List<Record> totalRecords = new ArrayList<>();
        CallBack callBack = new CallBack() {
            @Override
            public void completed(Object values) {
                List<Record> records = (List<Record>) values;
                totalRecords.addAll(records);
            }
        };
        List<Task> map_Tasks = files.stream().map(o -> {
            Task task = new Map_Task(o, callBack);
            return task;
        }).collect(Collectors.toList());
        boolean sucess = taskExecutor.execute(map_Tasks);
        if (!sucess) {
            log.error("Failed to process Map Tasks");
            System.exit(0);
        }
        return totalRecords;
    }

    /**
     * groupByPID - Groups records based on PID and splits into multiple Maps.
     *
     * @param records
     * @param mapLimit
     * @return - List of Maps with Pid and activity times.
     */
    public List<Map<String, List<Long>>> groupByPID(List<Record> records, int mapLimit) {
        Map<String, List<Long>> currentMap = new HashMap<>();
        List<Map<String, List<Long>>> maps = new ArrayList<>();
        try {
            maps.add(currentMap);
            for (Record record : records) {
                int index = -1;
                for (int i = 0; i < maps.size(); i++) {
                    if (maps.get(i).containsKey(record.pid)) {
                        index = i;
                        break;
                    }
                }
                List<Long> actions;
                if (index != -1) {
                    actions = maps.get(index).get(record.pid);
                } else {
                    actions = new ArrayList<>();
                    if (currentMap.size() >= mapLimit) {
                        Map<String, List<Long>> map = new HashMap<>();
                        maps.add(map);
                        currentMap = map;
                    }
                    currentMap.put(record.pid, actions);
                }
                for (Actions action : record.actions) {
                    actions.add(action.time);
                }

            }
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
        return maps;
    }

    /**
     * reduce - Find max activity time for each PID.
     *
     * @param groupRecords
     * @param taskExecutor
     * @return - return Map with PID and Max Activity Time.
     */
    public Map<String, Long> reduce(List<Object> groupRecords, TaskExecutor taskExecutor) {
        Map<String, Long> totalReduceValues = new Hashtable<>();
        CallBack callBack = new CallBack() {
            @Override
            public void completed(Object values) {
                Map<String, Long> records = (Map<String, Long>) values;
                totalReduceValues.putAll(records);
            }
        };
        List<Task> reduce_Tasks = new ArrayList<>(groupRecords).stream().map(listMap -> {
            Task task = new Reduce_Task(listMap, callBack);
            return task;
        }).collect(Collectors.toList());
        boolean sucess = taskExecutor.execute(reduce_Tasks);
        if (!sucess) {
            log.error("Failed to process Reduce Tasks");
            System.exit(0);
        }
        return totalReduceValues;
    }

    /**
     * sortAndLimit - Sorts PID based on Activity Time Descending order and limit out to 100
     *
     * @param records
     * @param limit
     * @return - LinkedHashMap (to keep order) PID and Recent Time of Activity.
     */
    public LinkedHashMap<String, Long> sortAndLimit(Map<String, Long> records, int limit) {
        List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(records.entrySet());
        LinkedHashMap<String, Long> sortedHashMap = new LinkedHashMap<>();
        //Descending Order
        try {
            Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });
            for (int i = 0; i < limit; i++) {
                Map.Entry entry = list.get(i);
                sortedHashMap.put((String) entry.getKey(), (long) entry.getValue());
            }
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
        return sortedHashMap;
    }
}

package com.woopra.Tasks;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;
import com.woopra.callback.CallBack;
import com.woopra.domain.Record;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author : anudeep on 2/16/18
 * @project : MapReduce
 */
public class Map_Task implements Task {

    Object data;
    CallBack mapCallBack;

    public Map_Task(Object data, CallBack mapCallBack) {
        this.data = data;
        this.mapCallBack = mapCallBack;
    }

    @Override
    public Integer call() throws Exception {
        List<Record> records = new ArrayList<>();
        try {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream(data.toString());
            Reader r = new InputStreamReader(is, "UTF-8");
            Gson gson = new GsonBuilder().create();
            JsonStreamParser p = new JsonStreamParser(r);
            while (p.hasNext()) {
                JsonElement e = p.next();
                if (e.isJsonObject()) {
                    Record m = gson.fromJson(e, Record.class);
                    records.add(m);
                }
            }
        } /*catch (FileNotFoundException ex) {
            System.out.println(ex);
        } */catch (Exception ex) {
            System.out.println(ex);
        }
        mapCallBack.completed(records);
        return records.size();
    }
}

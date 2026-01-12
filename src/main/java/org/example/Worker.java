package org.example;

import org.mapdb.DB;

import java.util.Map;

public class Worker implements Runnable {
    private final String taskName;
    private final int taskId;
    private Map<String, String> map;
    private Integer N;
    private Integer batch;

    public Worker(String taskName, int taskId, Map<String,String> map, Integer N, Integer batch) {
        this.taskName = taskName;
        this.taskId = taskId;
        this.map = map;
        this.N = N;
        this.batch = batch;
    }

    @Override
    public void run() {
        System.out.println("线程 " + Thread.currentThread().getName()
                + " 正在执行任务: " + taskName + " (ID: " + taskId + ")");

        long mapCost = 0L;
        for(int i = 0 ;i<batch ;i++) {
            // 测试 MapDB
            long mapdbTime = testMapDB(N);
            System.out.println(this.taskName+"================================");
            System.out.println(this.taskName+"MapDB put " + N + " entries耗时: " + mapdbTime + " ms");
            mapCost += mapdbTime;
        }
        System.out.println( "================================");
        System.out.println(this.taskName+"MapDB put " + N + " entries耗时: " + mapCost/batch + " ms");
        System.out.println("任务 " + taskName + " 执行完成");
    }


    private  long testMapDB(int N) {

        long start = System.nanoTime();
        for (int i = 0; i < N; i++) {
            map.put("key" + i, "value" + i+"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
        }
        long end = System.nanoTime();
        return (end - start) / 1_000_000;
    }

}

package org.example;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.util.Map;

public class DBPerfTestMutilThread {
    public static DB db;
    public static Map<String, String> map;
    static {
         db = DBMaker.newFileDB(new File("mapdb-test.db"))
                .mmapFileEnable()
                .closeOnJvmShutdown()
                .transactionDisable()
                .deleteFilesAfterClose()
                .make();

         map =  db.getHashMap("map");
    }

    public static void main(String[] args) throws Exception {
        int N = 1_00_000; // 写入 10 万条数据，可自行调整
        int batch = 3;
       int threadNum = 2;
       long startTime = System.nanoTime();
       for(int i = 0 ;i< threadNum;i++) {
           new Thread(new Worker("work"+i,i, map, N, batch)).start();
       }


    }

}

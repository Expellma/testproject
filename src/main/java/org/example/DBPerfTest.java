package org.example;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class DBPerfTest {
    static {
        RocksDB.loadLibrary(); // RocksDB 必须先加载
    }

    public static void main(String[] args) throws Exception {
        int N = 1_00_000; // 写入 10 万条数据，可自行调整
        int batch = 10;
        long mapCost = 0L;
        long rocksdbCost = 0L;
        for(int i = 0 ;i<batch ;i++) {
            // 测试 MapDB
            long mapdbTime = testMapDB(N);
            // 测试 RocksDB
//            long rocksdbTime = testRocksDB(N);
            System.out.println("================================");
            System.out.println("MapDB put " + N + " entries耗时: " + mapdbTime + " ms");
//            System.out.println("RocksDB put " + N + " entries耗时: " + rocksdbTime + " ms");
            mapCost += mapdbTime;
//            rocksdbCost += rocksdbTime;
        }
        System.out.println( "================================");
        System.out.println("MapDB put " + N + " entries耗时: " + mapCost/batch + " ms");

    }

    private static long testMapDB(int N) {
        DB db = DBMaker.newFileDB(new File("mapdb-test.db"))
                .mmapFileEnable()
                .closeOnJvmShutdown()
                .transactionDisable()
                .deleteFilesAfterClose()
                .make();

        Map<String, String> map =  db.getHashMap("map");

        long start = System.nanoTime();
        for (int i = 0; i < N; i++) {
            map.put("key" + i, "value" + i+"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
        }
        long end = System.nanoTime();

        db.close();
        return (end - start) / 1_000_000;
    }

    private static long testRocksDB(int N) throws RocksDBException {
        Options options = new Options()
                .setCreateIfMissing(true)
                .setWriteBufferSize(128*1024*1024)
                .setMaxWriteBufferNumber(3);
//                .setTargetFileSizeBase(128*1024*1024);
//                .setAllowMmapWrites(true);

        // ---------- Compaction ----------
        options.setLevel0FileNumCompactionTrigger(8);      // L0 文件数到8就触发合并
        options.setLevel0SlowdownWritesTrigger(20);        // 到20个文件减速写
        options.setLevel0StopWritesTrigger(36);            // 到36个文件完全停写
        options.setMaxBackgroundJobs(12);                   // 后台线程数（建议=CPU核数）
        options.setMaxSubcompactions(4);                   // 单次合并拆4个子任务

        try (RocksDB db = RocksDB.open(options, "rocksdb-test")) {
            WriteOptions writeOptions = new WriteOptions().setDisableWAL(true);
            long start = System.nanoTime();
            for (int i = 0; i < N; i++) {
                db.put(writeOptions,("key" + i).getBytes(),
                        ("value" + i+"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx").getBytes());
            }
            long end = System.nanoTime();
            db.close();
//            RocksDB.destroyDB("rocksdb-test",options);
            return (end - start) / 1_000_000;
        }
    }
}

package ro.midra.stream.infrastructure;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.*;

import java.util.Map;

/**
 * Shares native cache and write-buffer budgets across stores; avoids multiplying defaults per task.
 */
public class BoundedRocksDb implements RocksDBConfigSetter {
    private static final Cache CACHE = new LRUCache(128 * 1024 * 1024L);
    private static final WriteBufferManager BUFFERS =
            new WriteBufferManager(64 * 1024 * 1024L, CACHE);

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        var table = (BlockBasedTableConfig) options.tableFormatConfig();
        table.setBlockCache(CACHE).setCacheIndexAndFilterBlocks(true);
        options
                .setTableFormatConfig(table)
                .setWriteBufferManager(BUFFERS)
                .setWriteBufferSize(8 * 1024 * 1024L)
                .setMaxWriteBufferNumber(2)
                .setMaxBackgroundJobs(2);
    }

    @Override
    public void close(String storeName, Options options) {
        // Shared native budgets live for the process lifetime, not the lifetime of one task.
    }
}

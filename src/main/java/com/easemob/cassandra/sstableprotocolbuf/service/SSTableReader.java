package com.easemob.cassandra.sstableprotocolbuf.service;

import com.easemob.cassandra.sstable.SSTableModel;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableIdentityIteratorPatched;
import org.apache.cassandra.io.sstable.SSTableReaderPatched;
import org.apache.cassandra.io.sstable.SSTableScannerPatched;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

@Slf4j
public class SSTableReader {
    public static final String SSTABLE_DATA_FILE_SURFIX = "-Data.db";
    private static final CFMetaData cfMetaData = CFMetaData.compile("CREATE TABLE \"Entity_Properties\" (\n" +
            "  key blob,\n" +
            "  column1 blob,\n" +
            "  value blob,\n" +
            "  PRIMARY KEY ((key), column1)\n" +
            ") WITH COMPACT STORAGE AND\n" +
            "  bloom_filter_fp_chance=1.0 AND\n" +
            "  caching='NONE' AND\n" +
            "  comment='' AND\n" +
            "  dclocal_read_repair_chance=0.100000 AND\n" +
            "  gc_grace_seconds=864000 AND\n" +
            "  index_interval=128 AND\n" +
            "  read_repair_chance=0.100000 AND\n" +
            "  replicate_on_write='true' AND\n" +
            "  populate_io_cache_on_flush='false' AND\n" +
            "  default_time_to_live=0 AND\n" +
            "  speculative_retry='NONE' AND\n" +
            "  memtable_flush_period_in_ms=0 AND\n" +
            "  compaction={'class': 'SizeTieredCompactionStrategy'} AND\n" +
            "  compression={'sstable_compression': 'LZ4Compressor'};", "\"Usergrid_Applications\"");


    public Flux<SSTableModel.Row> parse(String sstableDataFilePath){
        Path path = Paths.get(sstableDataFilePath);

        boolean isDataFileExist =  Files.exists(path);
        if(!isDataFileExist){
            throw new IllegalArgumentException("cassandra data file "+sstableDataFilePath+" doesn't exist");
        }
        Flux<SSTableIdentityIteratorPatched> flux = Flux.just(sstableDataFilePath)
                .map(Descriptor::fromFilename)
                .map(descriptor -> {
                            try {
                                return SSTableReaderPatched.open(descriptor, cfMetaData);
                            } catch (IOException e) {
                                throw Exceptions.propagate(e);
                            }
                        }

                )
                .map(SSTableReaderPatched::getScanner)
                .flatMap(scanner -> {
                    return Flux.create(sink -> {
                        while (scanner.hasNext()) {
                            SSTableIdentityIteratorPatched tableRow = (SSTableIdentityIteratorPatched) scanner.next();
                            sink.next(tableRow);
                        }
                        try {
                            scanner.close();
                        } catch (IOException e) {
                            sink.error(e);
                        }
                        sink.complete();
                    });
                });
        return flux.filter(isLive)
                .map(rowMapper);
    }

    public static void main(String[] args) throws Exception {
//        String ssTableFileName = "/Volumes/stliu/tools/apache-cassandra-2.0.9/data/Usergrid_Applications/Entity_Properties/Usergrid_Applications-Entity_Properties-jb-11-Data.db";
        String ssTableFileName = "/Users/stliu/tools/apache-cassandra-2.0.9/data/Usergrid_Applications/Entity_Properties/Usergrid_Applications-Entity_Properties-jb-1-Data.db";
//        Descriptor descriptor = Descriptor.fromFilename(ssTableFileName);




        Path location = Paths.get("test-output.zip");


//        flux.filter(isLive)
//                .map(rowMapper)
//
////                .publishOn(Schedulers.elastic())
//
//                .reduceWith(() -> {
//                    try {
//                        return Files.newOutputStream(location);
//                    } catch (IOException e) {
//                        throw Exceptions.propagate(e);
//                    }
//                }, (out, bytes) -> {
//                    try {
//                        bytes.writeDelimitedTo(out);
//                        return out;
//                    } catch (IOException e) {
//                        throw Exceptions.propagate(e);
//                    }
//                })
//                .doOnSuccessOrError((out, t) -> {
//                    try {
//                        out.close();
//                    } catch (IOException e) {
//                        throw Exceptions.propagate(e);
//                    }
//                })
//                .map(out -> location)
//                .subscribe(System.out::println, (e) -> {
//                    System.out.println("------- error ------");
//                    e.printStackTrace();
//                }, () -> {
//                    System.out.println("--------completed ------");
//                });
        ;
    }


    private static Predicate<SSTableIdentityIteratorPatched> isLive = (row) -> {
        return row.getColumnFamily().deletionInfo().isLive();
    };

    private static Function<SSTableIdentityIteratorPatched, SSTableModel.Row> rowMapper = (cassandraRow) -> {
        SSTableModel.Row.Builder builder = SSTableModel.Row.newBuilder()
                .setKey(ByteString.copyFrom(cassandraRow.getKey().key));
        while (cassandraRow.hasNext()) {
            OnDiskAtom atom = cassandraRow.next();
            if (atom instanceof Column) {
                Column column = (Column) atom;
                if (column instanceof DeletedColumn) {
                    //忽略被删除的column
                    continue;
                } else if (column instanceof ExpiringColumn) {
                    //忽略过期的column
                    continue;
                } else if (column instanceof CounterColumn) {
                    //忽略counter column
                    continue;
                }
                SSTableModel.Column column1 = ColumnMapper.INSTANCE.apply(column);
                builder.addColumns(column1);
            }
        }
        return builder.build();
    };
}

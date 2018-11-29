package com.easemob.cassandra.sstableprotocolbuf;

import com.easemob.cassandra.sstable.SSTableModel;
import com.easemob.cassandra.sstableprotocolbuf.service.SSTableReader;
import com.google.protobuf.AbstractMessageLite;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.stereotype.Component;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.awt.image.DataBuffer;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

@Component
public class Runner implements CommandLineRunner {

    private final SSTableReader ssTableReader = new SSTableReader();

    @Override
    public void run(String... args) throws Exception {

        if (args.length != 1) {
            System.err.println("用法： java -jar sstable-protocolbuf.jar {cassandra sstable文件的路径或者包含sstable文件的目录(包含-Data.db的文件)}");
            System.exit(-1);
        }

        String sstableLocation = args[0];

        Path sstableLocationPath = Paths.get(sstableLocation);
        if (!Files.exists(sstableLocationPath)) {
            System.err.println("指定的目录" + sstableLocation + "不存在!!!");
            System.exit(-1);
        }
        if (Files.isDirectory(sstableLocationPath)) {
            Files.walk(sstableLocationPath)
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(SSTableReader.SSTABLE_DATA_FILE_SURFIX))
                    .forEach(this::process);
            ;
        } else {
            process(sstableLocationPath);
        }
    }

    private void process(Path path) {

        Path targetPath = getTargetFileLocation(path);
        System.out.println("开始处理" + path + "处理后的文件会生成在" + targetPath);
        Flux<SSTableModel.Row> rows = ssTableReader.parse(path.toAbsolutePath().toString());


        rows.publishOn(Schedulers.elastic())

//                .map(AbstractMessageLite::toByteString)
//                .map(DataBufferUtils.)

                .reduceWith(() -> {
                    try {
                        return Files.newOutputStream(targetPath);
                    } catch (IOException e) {
                        throw Exceptions.propagate(e);
                    }
                }, (out, bytes) -> {
                    try {
                        bytes.writeDelimitedTo(out);
                        return out;
                    } catch (IOException e) {
                        throw Exceptions.propagate(e);
                    }
                })
                .doOnSuccessOrError((out, t) -> {
                    try {
                        out.close();
                    } catch (IOException e) {
                        throw Exceptions.propagate(e);
                    }
                })
                .map(out -> targetPath)
                .subscribe(System.out::println, (e) -> {
                    System.out.println("------- error ------");
                    e.printStackTrace();
                }, () -> {
                    System.out.println("--------completed ------");
                })


        ;

    }

    private AsynchronousFileChannel createFile(Path path) {
        try {
            return AsynchronousFileChannel.open(path, StandardOpenOption.TRUNCATE_EXISTING,StandardOpenOption.WRITE);
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }

    private Path getTargetFileLocation(Path source) {
        return Paths.get(source.toAbsolutePath().toString()+".proto");
    }
}

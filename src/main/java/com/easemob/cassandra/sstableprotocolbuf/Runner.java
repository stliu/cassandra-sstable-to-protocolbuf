package com.easemob.cassandra.sstableprotocolbuf;

import com.easemob.cassandra.sstable.SSTableModel;
import com.easemob.cassandra.sstableprotocolbuf.service.SSTableReader;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdOutputStream;
import com.google.protobuf.AbstractMessageLite;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FileUtils;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.stereotype.Component;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.awt.image.DataBuffer;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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

        long start = System.currentTimeMillis();
        long sourceFileSize = getFileSizeSafly(path);

        Path targetPath = getTargetFileLocation(path);
        System.out.println("开始处理" + path + "处理后的文件会生成在" + targetPath);
        Flux<SSTableModel.Row> rows = ssTableReader.parse(path.toAbsolutePath().toString());

        rows.reduceWith(new OutputStreamSupplier(targetPath), new WriteData())
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

                    long end = System.currentTimeMillis();
                    long processTimeInSecond = (end - start) / 1000;
                    long targetFileSize = getFileSizeSafly(targetPath);

                    System.out.println("处理完成" + Thread.currentThread());
                    System.out.println("\t源文件: " + path + " 大小: " + FileUtils.byteCountToDisplaySize(sourceFileSize));
                    System.out.println("\t目标文件: " + targetPath + "大小: " + FileUtils.byteCountToDisplaySize(targetFileSize) + ", 减少了 " + FileUtils.byteCountToDisplaySize(sourceFileSize - targetFileSize));
                    System.out.println("\t处理时间: " + processTimeInSecond + "秒");

                })


        ;

    }


    public static long getFileSizeSafly(Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }
    }

    public static class WriteData implements BiFunction<OutputStream, SSTableModel.Row, OutputStream> {
        @Override
        public OutputStream apply(OutputStream out, SSTableModel.Row row) {
            try {
                row.writeDelimitedTo(out);
                return out;
            } catch (IOException e) {
                throw Exceptions.propagate(e);
            }
        }
    }

    private static final String compressor = CompressorStreamFactory.ZSTANDARD;

    public static class OutputStreamSupplier implements Supplier<OutputStream> {
        private final Path targetPath;

        public OutputStreamSupplier(Path targetPath) {
            this.targetPath = targetPath;
        }

        @Override
        public OutputStream get() {
            try {
                OutputStream os = Files.newOutputStream(targetPath);
                return new CompressorStreamFactory()
                        .createCompressorOutputStream(compressor, os);
            } catch (Exception e) {
                throw Exceptions.propagate(e);
            }
        }
    }

    private static Path getTargetFileLocation(Path source) {
        return Paths.get(source.toAbsolutePath().toString() + ".proto." + getCompressedFileExt(compressor));
    }

    private static String getCompressedFileExt(String compressor) {
        if (CompressorStreamFactory.ZSTANDARD.equals(compressor)) {
            return "zst";
        }
        return compressor;
    }

}

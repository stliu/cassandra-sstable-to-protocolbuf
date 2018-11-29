package org.apache.cassandra.io.sstable;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.io.compress.CompressedRandomAccessReader;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.cassandra.db.Directories.SECONDARY_INDEX_NAME_SEPARATOR;

public class SSTableReaderPatched extends SSTable implements Closeable {


    /**
     * SSTableReaders are open()ed by Keyspace.onStart; after that they are created by SSTableWriter.renameAndOpen.
     * Do not re-call open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
     */

    private static final Logger logger = LoggerFactory.getLogger(SSTableReaderPatched.class);


    // indexfile and datafile: might be null before a call to load()
    private SegmentedFile ifile;
    private SegmentedFile dfile;

    private IndexSummary indexSummary;
    private IFilter bf;


    private final AtomicBoolean isSuspect = new AtomicBoolean(false);
    //    private final SSTableDeletingTask deletingTask;
    // not final since we need to be able to change level on a file.
    private volatile SSTableMetadata sstableMetadata;


    public static SSTableReaderPatched open(Descriptor desc, CFMetaData metadata) throws IOException {
        IPartitioner p = desc.cfname.contains(SECONDARY_INDEX_NAME_SEPARATOR)
                ? new LocalPartitioner(metadata.getKeyValidator())
                : StorageService.getPartitioner();
        return open(desc, componentsFor(desc), metadata, p);
    }


    private static SSTableReaderPatched open(Descriptor descriptor, Set<Component> components, CFMetaData metadata, IPartitioner partitioner) throws IOException {
        long start = System.nanoTime();
        SSTableMetadata sstableMetadata = openMetadata(descriptor, components, partitioner);

        SSTableReaderPatched sstable = new SSTableReaderPatched(descriptor,
                components,
                metadata,
                partitioner,
                sstableMetadata);
        sstable.load();
        sstable.validate();
        logger.debug("INDEX LOAD TIME for {}: {} ms.", descriptor, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        return sstable;
    }

    private static SSTableMetadata openMetadata(Descriptor descriptor, Set<Component> components, IPartitioner partitioner) throws IOException {
        assert partitioner != null;
        // Minimum components without which we can't do anything
        assert components.contains(Component.DATA) : "Data component is missing for sstable" + descriptor;
        assert components.contains(Component.PRIMARY_INDEX) : "Primary index component is missing for sstable " + descriptor;

        logger.info("Opening {} ({} bytes)", descriptor, new File(descriptor.filenameFor(COMPONENT_DATA)).length());

        SSTableMetadata sstableMetadata = SSTableMetadata.serializer.deserialize(descriptor).left;

        // Check if sstable is created using same partitioner.
        // Partitioner can be null, which indicates older version of sstable or no stats available.
        // In that case, we skip the check.
        String partitionerName = partitioner.getClass().getCanonicalName();
        if (sstableMetadata.partitioner != null && !partitionerName.equals(sstableMetadata.partitioner)) {
            logger.error(String.format("Cannot open %s; partitioner %s does not match system partitioner %s.  Note that the default partitioner starting with Cassandra 1.2 is Murmur3Partitioner, so you will need to edit that to match your old partitioner if upgrading.",
                    descriptor, sstableMetadata.partitioner, partitionerName));
            System.exit(1);
        }
        return sstableMetadata;
    }


    private SSTableReaderPatched(final Descriptor desc,
                                 Set<Component> components,
                                 CFMetaData metadata,
                                 IPartitioner partitioner,
                                 SSTableMetadata sstableMetadata) {
        super(desc, components, metadata, partitioner);
        this.sstableMetadata = sstableMetadata;
    }


    /**
     * Clean up all opened resources.
     */
    public void close() throws IOException {
        // Force finalizing mmapping if necessary
        ifile.cleanup();
        dfile.cleanup();
        // close the BF so it can be opened later.
        bf.close();
        indexSummary.close();
    }


    private void load() throws IOException {
        bf = FilterFactory.AlwaysPresent;
        SegmentedFile.Builder ibuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
        SegmentedFile.Builder dbuilder = compression
                ? SegmentedFile.getCompressedBuilder()
                : SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode());

        boolean summaryLoaded = loadSummary(this, ibuilder, dbuilder, metadata);
        if (!summaryLoaded)
            buildSummary(ibuilder, dbuilder, summaryLoaded);

        ifile = ibuilder.complete(descriptor.filenameFor(Component.PRIMARY_INDEX));
        dfile = dbuilder.complete(descriptor.filenameFor(Component.DATA));
    }

    private void buildSummary(SegmentedFile.Builder ibuilder, SegmentedFile.Builder dbuilder, boolean summaryLoaded) throws IOException {
        // we read the positions in a BRAF so we don't have to worry about an entry spanning a mmap boundary.
        RandomAccessReader primaryIndex = RandomAccessReader.open(new File(descriptor.filenameFor(Component.PRIMARY_INDEX)));

        try {
            long indexSize = primaryIndex.length();
            long histogramCount = sstableMetadata.estimatedRowSize.count();
            long estimatedKeys = histogramCount > 0 && !sstableMetadata.estimatedRowSize.isOverflowed()
                    ? histogramCount
                    : estimateRowsFromIndex(primaryIndex); // statistics is supposed to be optional

            IndexSummaryBuilder summaryBuilder = null;
            if (!summaryLoaded)
                summaryBuilder = new IndexSummaryBuilder(estimatedKeys, metadata.getIndexInterval());

            long indexPosition;
            while ((indexPosition = primaryIndex.getFilePointer()) != indexSize) {
                ByteBuffer key = ByteBufferUtil.readWithShortLength(primaryIndex);
                RowIndexEntry indexEntry = RowIndexEntry.serializer.deserialize(primaryIndex, descriptor.version);
                DecoratedKey decoratedKey = partitioner.decorateKey(key);
                if (first == null)
                    first = decoratedKey;
                last = decoratedKey;

                // if summary was already read from disk we don't want to re-populate it using primary index
                if (!summaryLoaded) {
                    summaryBuilder.maybeAddEntry(decoratedKey, indexPosition);
                    ibuilder.addPotentialBoundary(indexPosition);
                    dbuilder.addPotentialBoundary(indexEntry.position);
                }
            }

            if (!summaryLoaded)
                indexSummary = summaryBuilder.build(partitioner);
        } finally {
            FileUtils.closeQuietly(primaryIndex);
        }

        first = getMinimalKey(first);
        last = getMinimalKey(last);
    }

    private static boolean loadSummary(SSTableReaderPatched reader, SegmentedFile.Builder ibuilder, SegmentedFile.Builder dbuilder, CFMetaData metadata) {
        File summariesFile = new File(reader.descriptor.filenameFor(Component.SUMMARY));
        if (!reader.descriptor.version.offHeapSummaries || !summariesFile.exists())
            return false;

        DataInputStream iStream = null;
        try {
            iStream = new DataInputStream(new FileInputStream(summariesFile));
            reader.indexSummary = IndexSummary.serializer.deserialize(iStream, reader.partitioner);
            if (reader.indexSummary.getIndexInterval() != metadata.getIndexInterval()) {
                iStream.close();
                logger.debug("Cannot read the saved summary for {} because Index Interval changed from {} to {}.",
                        reader.toString(), reader.indexSummary.getIndexInterval(), metadata.getIndexInterval());
                FileUtils.deleteWithConfirm(summariesFile);
                return false;
            }
            reader.first = reader.partitioner.decorateKey(ByteBufferUtil.readWithLength(iStream));
            reader.last = reader.partitioner.decorateKey(ByteBufferUtil.readWithLength(iStream));
            ibuilder.deserializeBounds(iStream);
            dbuilder.deserializeBounds(iStream);
        } catch (IOException e) {
            logger.debug("Cannot deserialize SSTable Summary: ", e);
            // corrupted; delete it and fall back to creating a new summary
            FileUtils.closeQuietly(iStream);
            FileUtils.deleteWithConfirm(summariesFile);
            return false;
        } finally {
            FileUtils.closeQuietly(iStream);
        }

        return true;
    }

    private void validate() {
        if (this.first.compareTo(this.last) > 0)
            throw new IllegalStateException(String.format("SSTable first key %s > last key %s", this.first, this.last));
    }

    /**
     * get the position in the index file to start scanning to find the given key (at most indexInterval keys away)
     */
    long getIndexScanPosition(RowPosition key) {
        int index = indexSummary.binarySearch(key);
        if (index < 0) {
            // binary search gives us the first index _greater_ than the key searched for,
            // i.e., its insertion position
            int greaterThan = (index + 1) * -1;
            if (greaterThan == 0)
                return -1;
            return indexSummary.getPosition(greaterThan - 1);
        } else {
            return indexSummary.getPosition(index);
        }
    }

    /**
     * Returns the compression metadata for this sstable.
     *
     * @throws IllegalStateException if the sstable is not compressed
     */
    private CompressionMetadata getCompressionMetadata() {
        if (!compression)
            throw new IllegalStateException(this + " is not compressed");

        return ((ICompressedFile) dfile).getMetadata();
    }


    void markSuspect() {
        if (logger.isDebugEnabled())
            logger.debug("Marking " + getFilename() + " as a suspect for blacklisting.");

        isSuspect.getAndSet(true);
    }

    /**
     * I/O SSTableScanner
     *
     * @return A Scanner for seeking over the rows of the SSTable.
     */

    public SSTableScannerPatched getScanner() {
        return new SSTableScannerPatched(this, DataRange.allData(partitioner));
    }


    RandomAccessReader openDataReader() {
        return compression
                ? CompressedRandomAccessReader.open(getFilename(), getCompressionMetadata())
                : RandomAccessReader.open(new File(getFilename()));
    }

    RandomAccessReader openIndexReader() {
        return RandomAccessReader.open(new File(getIndexFilename()));
    }


}

package org.apache.cassandra.io.sstable;


import com.google.common.collect.AbstractIterator;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.compaction.ICompactionScanner;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SSTableScannerPatched implements ICompactionScanner {
    private final RandomAccessReader dfile;
    private final RandomAccessReader ifile;
    private final SSTableReaderPatched sstable;

    private final Iterator<AbstractBounds<RowPosition>> rangeIterator;
    private AbstractBounds<RowPosition> currentRange;

    private Iterator<OnDiskAtomIterator> iterator;

    /**
     * @param sstable   SSTable to scan; must not be null
     * @param dataRange a single range to scan; must not be null
     */
    SSTableScannerPatched(SSTableReaderPatched sstable, DataRange dataRange) {
        assert sstable != null;

        this.dfile =  sstable.openDataReader();
        this.ifile = sstable.openIndexReader();
        this.sstable = sstable;

        List<AbstractBounds<RowPosition>> boundsList = new ArrayList<>(2);
        if (dataRange.isWrapAround() && !dataRange.stopKey().isMinimum(sstable.partitioner)) {
            // split the wrapping range into two parts: 1) the part that starts at the beginning of the sstable, and
            // 2) the part that comes before the wrap-around
            boundsList.add(new Bounds<>(sstable.partitioner.getMinimumToken().minKeyBound(), dataRange.stopKey(), sstable.partitioner));
            boundsList.add(new Bounds<>(dataRange.startKey(), sstable.partitioner.getMinimumToken().maxKeyBound(), sstable.partitioner));
        } else {
            boundsList.add(new Bounds<>(dataRange.startKey(), dataRange.stopKey(), sstable.partitioner));
        }
        this.rangeIterator = boundsList.iterator();
    }


    private void seekToCurrentRangeStart() {
        if (currentRange.left.isMinimum(sstable.partitioner))
            return;

        long indexPosition = sstable.getIndexScanPosition(currentRange.left);
        // -1 means the key is before everything in the sstable. So just start from the beginning.
        if (indexPosition == -1) {
            // Note: this method shouldn't assume we're at the start of the sstable already (see #6638) and
            // the seeks are no-op anyway if we are.
            ifile.seek(0);
            dfile.seek(0);
            return;
        }

        ifile.seek(indexPosition);
        try {

            while (!ifile.isEOF()) {
                indexPosition = ifile.getFilePointer();
                DecoratedKey indexDecoratedKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                int comparison = indexDecoratedKey.compareTo(currentRange.left);
                // because our range start may be inclusive or exclusive, we need to also contains()
                // instead of just checking (comparison >= 0)
                if (comparison > 0 || currentRange.contains(indexDecoratedKey)) {
                    // Found, just read the dataPosition and seek into index and data files
                    long dataPosition = ifile.readLong();
                    ifile.seek(indexPosition);
                    dfile.seek(dataPosition);
                    break;
                } else {
                    RowIndexEntry.serializer.skip(ifile);
                }
            }
        } catch (IOException e) {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.getFilename());
        }
    }

    public void close() throws IOException {
        FileUtils.close(dfile, ifile);
    }

    public long getLengthInBytes() {
        return dfile.length();
    }

    public long getCurrentPosition() {
        return dfile.getFilePointer();
    }

    public String getBackingFiles() {
        return sstable.toString();
    }

    public boolean hasNext() {
        if (iterator == null)
            iterator = createIterator();
        return iterator.hasNext();
    }

    public OnDiskAtomIterator next() {
        if (iterator == null)
            iterator = createIterator();
        return iterator.next();
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    private Iterator<OnDiskAtomIterator> createIterator() {
        return new KeyScanningIterator();
    }

    protected class KeyScanningIterator extends AbstractIterator<OnDiskAtomIterator> {
        private DecoratedKey nextKey;
        private RowIndexEntry nextEntry;
        private DecoratedKey currentKey;
        private RowIndexEntry currentEntry;

        protected OnDiskAtomIterator computeNext() {
            try {
                if (nextEntry == null) {
                    do {
                        // we're starting the first range or we just passed the end of the previous range
                        if (!rangeIterator.hasNext())
                            return endOfData();

                        currentRange = rangeIterator.next();
                        seekToCurrentRangeStart();

                        if (ifile.isEOF())
                            return endOfData();

                        currentKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                        currentEntry = RowIndexEntry.serializer.deserialize(ifile, sstable.descriptor.version);
                    } while (!currentRange.contains(currentKey));
                } else {
                    // we're in the middle of a range
                    currentKey = nextKey;
                    currentEntry = nextEntry;
                }

                long readEnd;
                if (ifile.isEOF()) {
                    nextEntry = null;
                    nextKey = null;
                    readEnd = dfile.length();
                } else {
                    // we need the position of the start of the next key, regardless of whether it falls in the current range
                    nextKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                    nextEntry = RowIndexEntry.serializer.deserialize(ifile, sstable.descriptor.version);
                    readEnd = nextEntry.position;

                    if (!currentRange.contains(nextKey)) {
                        nextKey = null;
                        nextEntry = null;
                    }
                }

                    dfile.seek(currentEntry.position);
                    ByteBufferUtil.readWithShortLength(dfile); // key
                    if (sstable.descriptor.version.hasRowSizeAndColumnCount)
                        dfile.readLong();
                    long dataSize = readEnd - dfile.getFilePointer();
                    return new SSTableIdentityIteratorPatched(sstable, dfile, currentKey, dataSize);
            } catch (IOException e) {
                sstable.markSuspect();
                throw new CorruptSSTableException(e, sstable.getFilename());
            }
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" +
                "dfile=" + dfile +
                " ifile=" + ifile +
                " sstable=" + sstable +
                ")";
    }
}

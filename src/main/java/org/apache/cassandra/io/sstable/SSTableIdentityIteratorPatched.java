package org.apache.cassandra.io.sstable;


import java.io.*;
import java.util.Iterator;

import org.apache.cassandra.serializers.MarshalException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.io.util.RandomAccessReader;

public class SSTableIdentityIteratorPatched implements Comparable<SSTableIdentityIteratorPatched>, OnDiskAtomIterator {

    private final DecoratedKey key;

    private final ColumnFamily columnFamily;

    private final Iterator<OnDiskAtom> atomIterator;

    private final boolean validateColumns;
    private final String filename;

    /**
     * Used to iterate through the columns of a row.
     *
     * @param sstable  SSTable we are reading ffrom.
     * @param file     Reading using this file.
     * @param key      Key of this row.
     * @param dataSize length of row data
     */
    SSTableIdentityIteratorPatched(SSTableReaderPatched sstable, RandomAccessReader file, DecoratedKey key, long dataSize) {
        this(sstable, file, key, dataSize, false);
    }

    /**
     * Used to iterate through the columns of a row.
     *
     * @param sstable   SSTable we are reading ffrom.
     * @param file      Reading using this file.
     * @param key       Key of this row.
     * @param dataSize  length of row data
     * @param checkData if true, do its best to deserialize and check the coherence of row data
     */
    private SSTableIdentityIteratorPatched(SSTableReaderPatched sstable, RandomAccessReader file, DecoratedKey key, long dataSize, boolean checkData) {
        this(sstable.metadata, file, file.getPath(), key, dataSize, checkData, sstable, ColumnSerializer.Flag.LOCAL);
    }

    // sstable may be null *if* checkData is false
    // If it is null, we assume the data is in the current file format
    private SSTableIdentityIteratorPatched(CFMetaData metadata,
                                           DataInput in,
                                           String filename,
                                           DecoratedKey key,
                                           long dataSize,
                                           boolean checkData,
                                           SSTableReaderPatched sstable,
                                           ColumnSerializer.Flag flag) {
        assert !checkData || (sstable != null);
        this.filename = filename;
        this.key = key;
        // Used by lazilyCompactedRow, so that we see the same things when deserializing the first and second time
        int expireBefore = (int) (System.currentTimeMillis() / 1000);
        this.validateColumns = checkData;
        Descriptor.Version dataVersion = sstable == null ? Descriptor.Version.CURRENT : sstable.descriptor.version;

        try {
            columnFamily = EmptyColumns.factory.create(metadata);
            columnFamily.delete(DeletionTime.serializer.deserialize(in));
            int columnCount = dataVersion.hasRowSizeAndColumnCount ? in.readInt() : Integer.MAX_VALUE;
            atomIterator = columnFamily.metadata().getOnDiskIterator(in, columnCount, flag, expireBefore, dataVersion);
        } catch (IOException e) {
            if (sstable != null)
                sstable.markSuspect();
            throw new CorruptSSTableException(e, filename);
        }
    }

    public DecoratedKey getKey() {
        return key;
    }

    public ColumnFamily getColumnFamily() {
        return columnFamily;
    }

    public boolean hasNext() {
        try {
            return atomIterator.hasNext();
        } catch (IOError e) {
            // catch here b/c atomIterator is an AbstractIterator; hasNext reads the value
            if (e.getCause() instanceof IOException)
                throw new CorruptSSTableException((IOException) e.getCause(), filename);
            else
                throw e;
        }
    }

    public OnDiskAtom next() {
        try {
            OnDiskAtom atom = atomIterator.next();
            if (validateColumns)
                atom.validateFields(columnFamily.metadata());
            return atom;
        } catch (MarshalException me) {
            throw new CorruptSSTableException(me, filename);
        }
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public void close() {
        // creator is responsible for closing file when finished
    }



    public int compareTo(SSTableIdentityIteratorPatched o) {
        return key.compareTo(o.key);
    }

}

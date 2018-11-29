package com.easemob.cassandra.sstableprotocolbuf.service;

import com.easemob.cassandra.sstable.SSTableModel;
import com.google.protobuf.ByteString;
import org.apache.cassandra.db.Column;

import java.util.function.Function;

public class ColumnMapper implements Function<Column, SSTableModel.Column> {

    public static final ColumnMapper INSTANCE = new ColumnMapper();

    @Override
    public SSTableModel.Column apply(Column column) {
        return SSTableModel.Column.newBuilder()
                .setName(ByteString.copyFrom(column.name()))
                .setValue(ByteString.copyFrom(column.value()))
                .setWriteTime(column.timestamp())
                .build();
    }
}

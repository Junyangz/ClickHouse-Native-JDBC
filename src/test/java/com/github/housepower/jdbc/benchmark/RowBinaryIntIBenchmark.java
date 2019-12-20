package com.github.housepower.jdbc.benchmark;

import com.google.common.base.Strings;

import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;

import java.sql.PreparedStatement;

import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.domain.ClickHouseFormat;

/**
 */
public class RowBinaryIntIBenchmark extends AbstractIBenchmark{
    private String columnType = "Int32";

    @Benchmark
    @Test
    public void benchInsertNative() throws Exception {
        withConnection(connection -> {
            wideColumnPrepare(connection, columnType);
            String params = Strings.repeat("?, ", columnNum);
            PreparedStatement
                pstmt = connection.prepareStatement("INSERT INTO " + getTableName() + " values(" + params.substring(0, params.length() - 2) + ")");
            for (int i = 0; i < batchSize; i++) {
                for (int j = 0; j < columnNum; j++ ) {
                    pstmt.setObject(j + 1, j + 1);
                }
                pstmt.addBatch();
            }
            int []res = pstmt.executeBatch();
            Assert.assertEquals(res.length, batchSize);

            wideColumnAfter(connection);
        }, ConnectionType.NATIVE);
    }

    @Benchmark
    @Test
    public void benchInsertHttpRowBinary() throws Exception {
        withConnection(connection -> {
            wideColumnPrepare(connection, columnType);
            ClickHouseStatement sth = (ClickHouseStatement) connection.createStatement();
            sth.write().send("INSERT INTO " + getTableName(), stream -> {
                for (int i = 0; i < batchSize; i++) {
                    for (int j = 0; j < columnNum; j++ ) {
                        stream.writeInt32(j + 1);
                    }
                }
            }, ClickHouseFormat.RowBinary);

            wideColumnAfter(connection);
        }, ConnectionType.HTTP);
    }

}

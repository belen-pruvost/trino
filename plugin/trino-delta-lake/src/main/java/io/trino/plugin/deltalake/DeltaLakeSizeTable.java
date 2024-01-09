/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.util.PageListBuilder;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.addExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DeltaLakeSizeTable
        implements SystemTable
{
    private static final String SIZE_COLUMN = "size_bytes";
    private static final List<ColumnMetadata> COLUMNS = ImmutableList.of(new ColumnMetadata(SIZE_COLUMN, BIGINT));

    private final SchemaTableName tableName;
    private final String tableLocation;
    private final TransactionLogAccess transactionLogAccess;
    private final ConnectorTableMetadata tableMetadata;

    public DeltaLakeSizeTable(SchemaTableName tableName, String tableLocation, TransactionLogAccess transactionLogAccess)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"), COLUMNS);
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        SchemaTableName baseTableName;
        Stream<AddFileEntry> activeFiles;

        try {
            baseTableName = new SchemaTableName(tableName.getSchemaName(), DeltaLakeTableName.tableNameFrom(tableName.getTableName()));
            TableSnapshot tableSnapshot = transactionLogAccess.loadSnapshot(session, baseTableName, tableLocation, Optional.empty());
            MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(session, tableSnapshot);
            ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntry(session, tableSnapshot);
            activeFiles = transactionLogAccess.getActiveFiles(session, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), ImmutableSet.of());
        }
        catch (IOException e) {
            throw new TrinoException(DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA, "Unable to load table metadata from location: " + tableLocation, e);
        }

        return new FixedPageSource(sizePage(activeFiles, baseTableName));
    }

    private List<Page> sizePage(Stream<AddFileEntry> activeFiles, SchemaTableName baseTableName)
    {
        Long cumulativeSizeBytes = activeFiles.map(AddFileEntry::getSize).reduce(0L, (subtotal, fileSize) -> {
            try {
                return addExact(subtotal, fileSize);
            }
            catch (ArithmeticException e) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Size in bytes for table %s overflows long", baseTableName), e);
            }
        });
        PageListBuilder pagesBuilder = PageListBuilder.forTable(tableMetadata);
        pagesBuilder.beginRow();
        pagesBuilder.appendBigint(cumulativeSizeBytes);
        pagesBuilder.endRow();
        return pagesBuilder.build();
    }
}

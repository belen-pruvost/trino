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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.Session;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableMetadataScheduler;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_LOCATION;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperationsCheckpointFilteringDisabled.FileType.CDF_DATA;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperationsCheckpointFilteringDisabled.FileType.CHECKPOINT;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperationsCheckpointFilteringDisabled.FileType.DATA;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperationsCheckpointFilteringDisabled.FileType.DELETION_VECTOR;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperationsCheckpointFilteringDisabled.FileType.LAST_CHECKPOINT;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperationsCheckpointFilteringDisabled.FileType.STARBURST_EXTENDED_STATS_JSON;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperationsCheckpointFilteringDisabled.FileType.TRANSACTION_LOG_JSON;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperationsCheckpointFilteringDisabled.FileType.TRINO_EXTENDED_STATS_JSON;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

// single-threaded as DistributedQueryRunner.spans is shared mutable state
@Execution(ExecutionMode.SAME_THREAD)
public class TestDeltaLakeFileOperationsCheckpointFilteringDisabled
        extends AbstractTestQueryFramework
{
    private static final int MAX_PREFIXES_COUNT = 10;

    private HiveMetastore metastore;
    // TODO: Consider waiting for scheduled task completion instead of manual triggering
    private DeltaLakeTableMetadataScheduler metadataScheduler;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path catalogDir = Files.createTempDirectory("catalog-dir");
        closeAfterClass(() -> deleteRecursively(catalogDir, ALLOW_INSECURE));

        DistributedQueryRunner queryRunner = DeltaLakeQueryRunner.builder()
                .addCoordinatorProperty("optimizer.experimental-max-prefetched-information-schema-prefixes", Integer.toString(MAX_PREFIXES_COUNT))
                .addDeltaProperty("hive.metastore.catalog.dir", catalogDir.toUri().toString())
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                .addDeltaProperty("delta.checkpoint-filtering.enabled", "false")
                .addDeltaProperty("delta.register-table-procedure.enabled", "true")
                .addDeltaProperty("delta.metastore.store-table-metadata", "true")
                .addDeltaProperty("delta.metastore.store-table-metadata-threads", "0") // Use the same thread to make the test deterministic
                .addDeltaProperty("delta.metastore.store-table-metadata-interval", "30m") // Use a large interval to avoid interference with the test
                .build();
        metastore = TestingDeltaLakeUtils.getConnectorService(queryRunner, HiveMetastoreFactory.class).createMetastore(Optional.empty());
        metadataScheduler = TestingDeltaLakeUtils.getConnectorService(queryRunner, DeltaLakeTableMetadataScheduler.class);
        return queryRunner;
    }

    @Test
    public void testCheckpointFileOperations()
    {
        assertUpdate("DROP TABLE IF EXISTS test_checkpoint_file_operations");
        assertUpdate("CREATE TABLE test_checkpoint_file_operations(key varchar, data varchar) with (checkpoint_interval = 2, partitioned_by=ARRAY['key'])");
        assertUpdate("INSERT INTO test_checkpoint_file_operations VALUES ('p1', '1-abc')", 1);
        assertUpdate("INSERT INTO test_checkpoint_file_operations VALUES ('p2', '2-xyz')", 1);
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_checkpoint_file_operations')");
        assertFileSystemAccesses(
                "SELECT * FROM test_checkpoint_file_operations",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.newStream"), 1)
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000002.checkpoint.parquet", "InputFile.newInput"), 2)
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000002.checkpoint.parquet", "InputFile.length"), 2)
                        .addCopies(new FileOperation(DATA, "key=p1/", "InputFile.newInput"), 1)
                        .addCopies(new FileOperation(DATA, "key=p2/", "InputFile.newInput"), 1)
                        .build());
        // reads of checkpoint and commits _are_ cached
        assertFileSystemAccessesNoMetadataCacheFlush(
                getSession(),
                "SELECT * FROM test_checkpoint_file_operations",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.newStream"), 1)
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000002.checkpoint.parquet", "InputFile.newInput"), 0)
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000002.checkpoint.parquet", "InputFile.length"), 0)
                        .addCopies(new FileOperation(DATA, "key=p1/", "InputFile.newInput"), 1)
                        .addCopies(new FileOperation(DATA, "key=p2/", "InputFile.newInput"), 1)
                        .build());
        assertUpdate("INSERT INTO test_checkpoint_file_operations VALUES ('p3', '3-xyz')", 1);
        assertFileSystemAccessesNoMetadataCacheFlush(
                getSession(),
                "SELECT * FROM test_checkpoint_file_operations",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.newStream"), 1)
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000002.checkpoint.parquet", "InputFile.newInput"), 0)
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000002.checkpoint.parquet", "InputFile.length"), 0)
                        .addCopies(new FileOperation(DATA, "key=p1/", "InputFile.newInput"), 1)
                        .addCopies(new FileOperation(DATA, "key=p2/", "InputFile.newInput"), 1)
                        .addCopies(new FileOperation(DATA, "key=p3/", "InputFile.newInput"), 1)
                        .build());
        assertFileSystemAccessesNoMetadataCacheFlush(
                getSession(),
                "SELECT * FROM test_checkpoint_file_operations",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.newStream"), 1)
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000002.checkpoint.parquet", "InputFile.newInput"), 0)
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000002.checkpoint.parquet", "InputFile.length"), 0)
                        .addCopies(new FileOperation(DATA, "key=p1/", "InputFile.newInput"), 1)
                        .addCopies(new FileOperation(DATA, "key=p2/", "InputFile.newInput"), 1)
                        .addCopies(new FileOperation(DATA, "key=p3/", "InputFile.newInput"), 1)
                        .build());
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<FileOperation> expectedAccesses)
    {
        assertFileSystemAccesses(getSession(), query, expectedAccesses);
    }

    private void assertFileSystemAccesses(Session session, @Language("SQL") String query, Multiset<FileOperation> expectedAccesses)
    {
        assertUpdate("CALL system.flush_metadata_cache()");
        assertFileSystemAccessesNoMetadataCacheFlush(session, query, expectedAccesses);
    }

    private void assertFileSystemAccessesNoMetadataCacheFlush(Session session, @Language("SQL") String query, Multiset<FileOperation> expectedAccesses)
    {
        // TODO FIXME
        // Is this still working??
        // trackingFileSystemFactory.reset();
        getDistributedQueryRunner().executeWithPlan(session, query);
        List<SpanData> spanData = getDistributedQueryRunner().getSpans();
        assertMultisetsEqual(getOperations(spanData), expectedAccesses);
    }

    private Multiset<FileOperation> getOperations(List<SpanData> spanData)
    {
        return spanData.stream()
                .filter(span -> span.getName().startsWith("InputFile.") || span.getName().startsWith("OutputFile."))
                .filter(span -> {
                    String path = requireNonNull(span.getAttributes().get(FILE_LOCATION), "FILE_LOCATION attribute is empty");
                    return !path.endsWith(".trinoSchema") && !path.contains(".trinoPermissions");
                })
                .map(span -> FileOperation.create(span.getAttributes().get(FILE_LOCATION), span.getName()))
                .collect(toCollection(HashMultiset::create));
    }

    private record FileOperation(FileType fileType, String fileId, String operationType)
    {
        public static FileOperation create(String path, String operationType)
        {
            String fileName = path.replaceFirst(".*/", "");
            if (path.matches(".*/_delta_log/_last_checkpoint")) {
                return new FileOperation(LAST_CHECKPOINT, fileName, operationType);
            }
            if (path.matches(".*/_delta_log/.*.checkpoint.*")) {
                return new FileOperation(CHECKPOINT, fileName, operationType);
            }
            if (path.matches(".*/_delta_log/\\d+\\.json")) {
                return new FileOperation(TRANSACTION_LOG_JSON, fileName, operationType);
            }
            if (path.matches(".*/_delta_log/\\d+\\.checkpoint.parquet")) {
                return new FileOperation(CHECKPOINT, fileName, operationType);
            }
            if (path.matches(".*/_delta_log/_trino_meta/extended_stats.json")) {
                return new FileOperation(TRINO_EXTENDED_STATS_JSON, fileName, operationType);
            }
            if (path.matches(".*/_delta_log/_starburst_meta/extendeded_stats.json")) {
                return new FileOperation(STARBURST_EXTENDED_STATS_JSON, fileName, operationType);
            }
            if (path.contains("/deletion_vector_")) {
                return new FileOperation(DELETION_VECTOR, fileName, operationType);
            }
            Pattern dataFilePattern = Pattern.compile(".*?/(?<partition>key=[^/]*/)?[^/]+");
            if (path.matches(".*/_change_data/.*")) {
                Matcher matcher = dataFilePattern.matcher(path);
                if (matcher.matches()) {
                    return new FileOperation(CDF_DATA, matcher.group("partition"), operationType);
                }
            }
            if (!path.contains("_delta_log")) {
                Matcher matcher = dataFilePattern.matcher(path);
                if (matcher.matches()) {
                    return new FileOperation(DATA, firstNonNull(matcher.group("partition"), "no partition"), operationType);
                }
            }
            throw new IllegalArgumentException("File not recognized: " + path);
        }

        public FileOperation
        {
            requireNonNull(fileType, "fileType is null");
            requireNonNull(fileId, "fileId is null");
            requireNonNull(operationType, "operationType is null");
        }
    }

    enum FileType
    {
        LAST_CHECKPOINT,
        CHECKPOINT,
        TRANSACTION_LOG_JSON,
        TRINO_EXTENDED_STATS_JSON,
        STARBURST_EXTENDED_STATS_JSON,
        DATA,
        CDF_DATA,
        DELETION_VECTOR,
        /**/;
    }
}

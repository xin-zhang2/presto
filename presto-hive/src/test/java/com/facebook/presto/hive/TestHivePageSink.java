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
package com.facebook.presto.hive;

import com.facebook.presto.GroupByHashPageIndexerFactory;
import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePageSinkMetadata;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.testing.TestingNodeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.airlift.slice.Slices;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.LineItemColumn;
import io.airlift.tpch.LineItemGenerator;
import io.airlift.tpch.TpchColumnType;
import io.airlift.tpch.TpchColumnTypes;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.hive.AbstractTestHiveClient.TEST_HIVE_PAGE_SINK_CONTEXT;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.CacheQuotaRequirement.NO_CACHE_REQUIREMENT;
import static com.facebook.presto.hive.HiveCompressionCodec.NONE;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveQueryRunner.METASTORE_CONTEXT;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.PAGE_SORTER;
import static com.facebook.presto.hive.HiveTestUtils.ROW_EXPRESSION_SERVICE;
import static com.facebook.presto.hive.HiveTestUtils.createTestHdfsEnvironment;
import static com.facebook.presto.hive.HiveTestUtils.getAllSessionProperties;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveAggregatedPageSourceFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveBatchPageSourceFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveFileWriterFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveRecordCursorProvider;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveSelectivePageSourceFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultOrcFileWriterFactory;
import static com.facebook.presto.hive.HiveType.HIVE_DATE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.LocationHandle.TableType.NEW;
import static com.facebook.presto.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY;
import static com.facebook.presto.hive.TestHiveUtil.createTestingFileHiveMetastore;
import static com.facebook.presto.spi.SplitContext.NON_CACHEABLE;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertTrue;

public class TestHivePageSink
{
    private static final int NUM_ROWS = 1000;
    private static final String SCHEMA_NAME = "test";
    private static final String TABLE_NAME = "test";

    @Test
    public void testAllFormats()
            throws Exception
    {
        HiveClientConfig config = new HiveClientConfig();
        SortingFileWriterConfig sortingFileWriterConfig = new SortingFileWriterConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        File tempDir = Files.createTempDir();
        try {
            ExtendedHiveMetastore metastore = createTestingFileHiveMetastore(new File(tempDir, "metastore"));
            for (HiveStorageFormat format : getSupportedHiveStorageFormats()) {
                config.setHiveStorageFormat(format);
                config.setCompressionCodec(NONE);
                long uncompressedLength = writeTestFile(config, metastoreClientConfig, metastore, makeFileName(tempDir, config), sortingFileWriterConfig);
                assertGreaterThan(uncompressedLength, 0L);

                for (HiveCompressionCodec codec : HiveCompressionCodec.values()) {
                    if (codec == NONE || !codec.isSupportedStorageFormat(format)) {
                        continue;
                    }
                    config.setCompressionCodec(codec);
                    long length = writeTestFile(config, metastoreClientConfig, metastore, makeFileName(tempDir, config), sortingFileWriterConfig);
                    assertTrue(uncompressedLength > length, format("%s with %s compressed to %s which is not less than %s", format, codec, length, uncompressedLength));
                }
            }
        }
        finally {
            deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
        }
    }

    protected List<HiveStorageFormat> getSupportedHiveStorageFormats()
    {
        // CSV supports only unbounded VARCHAR type, and Alpha does not support DML yet
        return Arrays.stream(HiveStorageFormat.values())
                .filter(format -> format != HiveStorageFormat.CSV && format != HiveStorageFormat.ALPHA)
                .collect(toImmutableList());
    }

    private static String makeFileName(File tempDir, HiveClientConfig config)
    {
        return tempDir.getAbsolutePath() + "/" + config.getHiveStorageFormat().name() + "." + config.getCompressionCodec().name();
    }

    private static long writeTestFile(HiveClientConfig config, MetastoreClientConfig metastoreClientConfig, ExtendedHiveMetastore metastore, String outputPath, SortingFileWriterConfig sortingFileWriterConfig)
    {
        HiveTransactionHandle transaction = new HiveTransactionHandle();
        HiveWriterStats stats = new HiveWriterStats();
        ConnectorPageSink pageSink = createPageSink(transaction, config, metastoreClientConfig, metastore, new Path("file:///" + outputPath), stats, sortingFileWriterConfig);
        List<LineItemColumn> columns = getTestColumns();
        List<Type> columnTypes = columns.stream()
                .map(LineItemColumn::getType)
                .map(TestHivePageSink::getHiveType)
                .map(hiveType -> hiveType.getType(FUNCTION_AND_TYPE_MANAGER))
                .collect(toList());

        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        int rows = 0;
        for (LineItem lineItem : new LineItemGenerator(0.01, 1, 1)) {
            rows++;
            if (rows >= NUM_ROWS) {
                break;
            }
            pageBuilder.declarePosition();
            for (int i = 0; i < columns.size(); i++) {
                LineItemColumn column = columns.get(i);
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                switch (column.getType().getBase()) {
                    case IDENTIFIER:
                        BIGINT.writeLong(blockBuilder, column.getIdentifier(lineItem));
                        break;
                    case INTEGER:
                        INTEGER.writeLong(blockBuilder, column.getInteger(lineItem));
                        break;
                    case DATE:
                        DATE.writeLong(blockBuilder, column.getDate(lineItem));
                        break;
                    case DOUBLE:
                        DOUBLE.writeDouble(blockBuilder, column.getDouble(lineItem));
                        break;
                    case VARCHAR:
                        createUnboundedVarcharType().writeSlice(blockBuilder, Slices.utf8Slice(column.getString(lineItem)));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported type " + column.getType());
                }
            }
        }
        Page page = pageBuilder.build();
        pageSink.appendPage(page);
        getFutureValue(pageSink.finish());

        File outputDir = new File(outputPath);
        List<File> files = ImmutableList.copyOf(outputDir.listFiles((dir, name) -> !name.endsWith(".crc")));
        File outputFile = getOnlyElement(files);
        long length = outputFile.length();

        ConnectorPageSource pageSource = createPageSource(transaction, config, metastoreClientConfig, outputFile);

        List<Page> pages = new ArrayList<>();
        while (!pageSource.isFinished()) {
            Page nextPage = pageSource.getNextPage();
            if (nextPage != null) {
                pages.add(nextPage.getLoadedPage());
            }
        }
        MaterializedResult expectedResults = toMaterializedResult(getSession(config, new HiveCommonClientConfig()), columnTypes, ImmutableList.of(page));
        MaterializedResult results = toMaterializedResult(getSession(config, new HiveCommonClientConfig()), columnTypes, pages);
        assertEquals(results, expectedResults);
        assertEquals(stats.getInputPageSizeInBytes().getAllTime().getMax(), page.getRetainedSizeInBytes());
        return length;
    }

    public static MaterializedResult toMaterializedResult(ConnectorSession session, List<Type> types, List<Page> pages)
    {
        // materialize pages
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(session, types);
        for (Page outputPage : pages) {
            resultBuilder.page(outputPage);
        }
        return resultBuilder.build();
    }

    private static ConnectorPageSource createPageSource(HiveTransactionHandle transaction, HiveClientConfig config, MetastoreClientConfig metastoreClientConfig, File outputFile)
    {
        HiveFileSplit fileSplit = new HiveFileSplit(
                "file:///" + outputFile.getAbsolutePath(),
                0,
                outputFile.length(),
                outputFile.length(),
                outputFile.lastModified(),
                Optional.empty(),
                ImmutableMap.of(),
                0);

        HiveSplit split = new HiveSplit(
                fileSplit,
                SCHEMA_NAME,
                TABLE_NAME,
                "",

                new Storage(
                        StorageFormat.create(config.getHiveStorageFormat().getSerDe(), config.getHiveStorageFormat().getInputFormat(), config.getHiveStorageFormat().getOutputFormat()),
                        "location",
                        Optional.empty(),
                        false,
                        ImmutableMap.of(),
                        ImmutableMap.of()),
                ImmutableList.of(),
                ImmutableList.of(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                NO_PREFERENCE,
                getColumnHandles().size(),
                TableToPartitionMapping.empty(),
                Optional.empty(),
                false,
                NO_CACHE_REQUIREMENT,
                Optional.empty(),
                ImmutableSet.of(),
                SplitWeight.standard(),
                Optional.empty());

        HiveTableLayoutHandle layoutHandle = new HiveTableLayoutHandle.Builder()
                .setSchemaTableName(new SchemaTableName(SCHEMA_NAME, TABLE_NAME))
                .setTablePath("path")
                .setPartitionColumns(ImmutableList.of())
                .setDataColumns(getColumnHandles().stream().map(column -> new Column(column.getName(), column.getHiveType(), Optional.empty(), Optional.empty())).collect(toImmutableList()))
                .setTableParameters(ImmutableMap.of())
                .setDomainPredicate(TupleDomain.all())
                .setRemainingPredicate(TRUE_CONSTANT)
                .setPredicateColumns(ImmutableMap.of())
                .setPartitionColumnPredicate(TupleDomain.all())
                .setBucketHandle(Optional.empty())
                .setBucketFilter(Optional.empty())
                .setPushdownFilterEnabled(false)
                .setLayoutString("layout")
                .setRequestedColumns(Optional.empty())
                .setPartialAggregationsPushedDown(false)
                .setAppendRowNumberEnabled(false)
                .setPartitions(Optional.empty())
                .setHiveTableHandle(Optional.empty())
                .build();
        TableHandle tableHandle = new TableHandle(
                new ConnectorId(HIVE_CATALOG),
                new HiveTableHandle(SCHEMA_NAME, TABLE_NAME),
                transaction,
                Optional.of(layoutHandle));
        HivePageSourceProvider provider = new HivePageSourceProvider(
                config,
                createTestHdfsEnvironment(config, metastoreClientConfig),
                getDefaultHiveRecordCursorProvider(config, metastoreClientConfig),
                getDefaultHiveBatchPageSourceFactories(config, metastoreClientConfig),
                getDefaultHiveSelectivePageSourceFactories(config, metastoreClientConfig),
                getDefaultHiveAggregatedPageSourceFactories(config, metastoreClientConfig),
                FUNCTION_AND_TYPE_MANAGER,
                ROW_EXPRESSION_SERVICE);
        return provider.createPageSource(transaction, getSession(config, new HiveCommonClientConfig()), split, tableHandle.getLayout().get(), ImmutableList.copyOf(getColumnHandles()), NON_CACHEABLE, new RuntimeStats());
    }

    private static ConnectorPageSink createPageSink(HiveTransactionHandle transaction, HiveClientConfig config, MetastoreClientConfig metastoreClientConfig, ExtendedHiveMetastore metastore, Path outputPath, HiveWriterStats stats, SortingFileWriterConfig sortingFileWriterConfig)
    {
        LocationHandle locationHandle = new LocationHandle(outputPath, outputPath, Optional.empty(), NEW, DIRECT_TO_TARGET_NEW_DIRECTORY);
        HiveOutputTableHandle handle = new HiveOutputTableHandle(
                SCHEMA_NAME,
                TABLE_NAME,
                getColumnHandles(),
                new HivePageSinkMetadata(new SchemaTableName(SCHEMA_NAME, TABLE_NAME), metastore.getTable(METASTORE_CONTEXT, SCHEMA_NAME, TABLE_NAME), ImmutableMap.of()),
                locationHandle,
                config.getHiveStorageFormat(),
                config.getHiveStorageFormat(),
                config.getHiveStorageFormat(),
                config.getCompressionCodec(),
                ImmutableList.of(),
                Optional.empty(),
                ImmutableList.of(),
                "test",
                ImmutableMap.of(),
                Optional.empty());
        HdfsEnvironment hdfsEnvironment = createTestHdfsEnvironment(config, metastoreClientConfig);
        HivePageSinkProvider provider = new HivePageSinkProvider(
                getDefaultHiveFileWriterFactories(config, metastoreClientConfig),
                hdfsEnvironment,
                PAGE_SORTER,
                metastore,
                new GroupByHashPageIndexerFactory(new JoinCompiler(MetadataManager.createTestMetadataManager())),
                FUNCTION_AND_TYPE_MANAGER,
                config,
                metastoreClientConfig,
                sortingFileWriterConfig,
                new HiveLocationService(hdfsEnvironment),
                HiveTestUtils.PARTITION_UPDATE_CODEC,
                HiveTestUtils.PARTITION_UPDATE_SMILE_CODEC,
                new TestingNodeManager("fake-environment"),
                new HiveEventClient(),
                new HiveSessionProperties(config, new OrcFileWriterConfig(), new ParquetFileWriterConfig(), new CacheConfig()),
                stats,
                getDefaultOrcFileWriterFactory(config, metastoreClientConfig),
                HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
        return provider.createPageSink(transaction, getSession(config, new HiveCommonClientConfig()), handle, TEST_HIVE_PAGE_SINK_CONTEXT);
    }

    private static TestingConnectorSession getSession(HiveClientConfig config, HiveCommonClientConfig hiveCommonClientConfig)
    {
        return new TestingConnectorSession(getAllSessionProperties(config, hiveCommonClientConfig));
    }

    public static List<HiveColumnHandle> getColumnHandles()
    {
        ImmutableList.Builder<HiveColumnHandle> handles = ImmutableList.builder();
        List<LineItemColumn> columns = getTestColumns();
        for (int i = 0; i < columns.size(); i++) {
            LineItemColumn column = columns.get(i);
            HiveType hiveType = getHiveType(column.getType());
            handles.add(new HiveColumnHandle(column.getColumnName(), hiveType, hiveType.getTypeSignature(), i, REGULAR, Optional.empty(), Optional.empty()));
        }
        return handles.build();
    }

    private static List<LineItemColumn> getTestColumns()
    {
        return Stream.of(LineItemColumn.values())
                // Not all the formats support DATE
                .filter(column -> !column.getType().equals(TpchColumnTypes.DATE))
                .collect(toList());
    }

    private static HiveType getHiveType(TpchColumnType type)
    {
        switch (type.getBase()) {
            case IDENTIFIER:
                return HIVE_LONG;
            case INTEGER:
                return HIVE_INT;
            case DATE:
                return HIVE_DATE;
            case DOUBLE:
                return HIVE_DOUBLE;
            case VARCHAR:
                return HIVE_STRING;
            default:
                throw new UnsupportedOperationException();
        }
    }
}

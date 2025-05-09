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
package com.facebook.presto.orc;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.orc.StripeReader.StripeId;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;
import com.facebook.presto.orc.stream.OrcInputStream;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public interface StripeMetadataSource
{
    Slice getStripeFooterSlice(OrcDataSource orcDataSource, StripeId stripeId, long footerOffset, int footerLength, boolean cacheable, long fileModificationTime)
            throws IOException;

    Map<StreamId, OrcDataSourceInput> getInputs(
            OrcDataSource orcDataSource,
            StripeId stripeId,
            Map<StreamId, DiskRange> diskRanges,
            boolean cacheable,
            long fileModificationTime)
            throws IOException;

    List<RowGroupIndex> getRowIndexes(
            MetadataReader metadataReader,
            HiveWriterVersion hiveWriterVersion,
            StripeId stripeId,
            StreamId streamId,
            OrcInputStream inputStream,
            List<HiveBloomFilter> bloomFilters,
            RuntimeStats runtimeStats,
            long fileModificationTime)
            throws IOException;

    class CacheableSlice
    {
        private final Slice slice;
        private final long fileModificationTime;

        CacheableSlice(Slice slice, long fileModificationTime)
        {
            this.slice = requireNonNull(slice, "slice is null");
            this.fileModificationTime = fileModificationTime;
        }

        public Slice getSlice()
        {
            return slice;
        }

        public long getFileModificationTime()
        {
            return fileModificationTime;
        }
    }

    class CacheableRowGroupIndices
    {
        private final List<RowGroupIndex> rowGroupIndices;
        private final long fileModificationTime;

        public CacheableRowGroupIndices(List<RowGroupIndex> rowGroupIndices, long fileModificationTime)
        {
            this.rowGroupIndices = requireNonNull(rowGroupIndices, "rowGroupIndices is null");
            this.fileModificationTime = fileModificationTime;
        }
        public List<RowGroupIndex> getRowGroupIndices()
        {
            return rowGroupIndices;
        }

        public long getFileModificationTime()
        {
            return fileModificationTime;
        }
    }
}

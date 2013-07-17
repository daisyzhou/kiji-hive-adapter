/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
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

package org.kiji.hive;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.hive.io.KijiRowDataWritable;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ResourceUtils;

/**
 * Writes key-value records from a KijiTableInputSplit (usually 1 region in an HTable).
 */
public class KijiTableRecordWriter
    implements FileSinkOperator.RecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(KijiTableRecordWriter.class);

  private final Kiji mKiji;
  private final KijiTable mKijiTable;
  private final KijiTableWriter mKijiTableWriter;

  /**
   * Constructor.
   *
   * @param conf The job configuration.
   * @throws java.io.IOException If the input split cannot be opened.
   */
  public KijiTableRecordWriter(Configuration conf)
      throws IOException {
    String kijiURIString = conf.get(KijiTableOutputFormat.CONF_KIJI_TABLE_URI);
    LOG.info("FIXME configuring KijiTableRecordWriter with URI {}", kijiURIString);
    KijiURI kijiURI = KijiURI.newBuilder(kijiURIString).build();
    mKiji = Kiji.Factory.open(kijiURI);
    mKijiTable = mKiji.openTable(kijiURI.getTable());
    mKijiTableWriter = mKijiTable.openTableWriter();
  }

  @Override
  public void write(Writable w) throws IOException {
    Preconditions.checkArgument(w instanceof KijiRowDataWritable,
        "KijiTableRecordWriter can only operate on KijiRowDataWritable objects.");
    KijiRowDataWritable kijiRowDataWritable = (KijiRowDataWritable) w;
    LOG.info("Dropping {} on the floor.", w.toString());
  }

  @Override
  public void close(boolean abort) throws IOException {
    LOG.info("Closing KijiTableRecordWriter");
    ResourceUtils.closeOrLog(mKijiTableWriter);
    ResourceUtils.releaseOrLog(mKijiTable);
    ResourceUtils.releaseOrLog(mKiji);
  }
}

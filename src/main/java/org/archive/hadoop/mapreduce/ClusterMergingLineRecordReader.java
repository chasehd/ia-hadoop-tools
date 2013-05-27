/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.archive.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.compress.GzipCodec;

// ALL INTERNETARCHIVE CHANGES INCLUDE A COMMENT STARTING "// IA "

// IA Use IA-custom code to read catenated gzip envelopes.
import org.archive.util.zip.OpenJDK7GZIPInputStream;

/**
 * IA modified version of Hadoop LineRecordReader.  The files read by
 * this class are made up of catenated gzip envelopes.  The default
 * LineRecordReader will select the built-in GzipCodec to uncompress
 * these files, but the built-in GzipCodec stops reading after the
 * first catenated gzip envelope :( So, we have this modified version
 * of LineRecordReader that uses the IA-custom OpenJDK7GZIPInputStream
 * which will read through all of the catenated gzip envelopes,
 * i.e. the whole input file.
 *
 * Otherwise, this is a cut/pase/copy of Hadoop LineRecordReader.  If
 * the input file is not compressed, then it will be read as normal.
 * Also, if the compression is something other than gzip, then the
 * corresponding codec will be used based on the file extension (e.g.
 * "bz2" would use Bzip2 codec).
 *
 * If you really want to use the built-in gzip codec, then set
 * the configuration property
 *
 *  clusterMergingLineRecordReader.zipnum = false
 *
 * and the built-in gzip codec will be used for ".gz" files. 
 *
 * Treats keys as offset in file and value as line. 
 */
public class ClusterMergingLineRecordReader extends RecordReader<LongWritable, Text> {
  private static final Log LOG = LogFactory.getLog(ClusterMergingLineRecordReader.class);

  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  private LineReader in;
  private int maxLineLength;
  private LongWritable key = null;
  private Text value = null;
  private byte[] recordDelimiterBytes;

  public ClusterMergingLineRecordReader() {
  }

  public ClusterMergingLineRecordReader(byte[] recordDelimiter) {
    this.recordDelimiterBytes = recordDelimiter;
  }

  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                                    Integer.MAX_VALUE);
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(file);

    // IA -- start code modifications
    boolean zipnum = false;
    if ( codec instanceof GzipCodec )
      {
        // If the file to be read ends in ".gz" then the CompressionCodecFactory will automatically
        // select the built-in gzip codec.  Unfortunately, that codec stops after reading the
        // first catenated gzip envelope :(  So, we have this property here (default true) which
        // forces us to use the IA OpenJDK7GZIPInputStream for reading ".gz" files.
        if ( context.getConfiguration().getBoolean( "clusterMergingLineRecordReader.zipnum", true ) )
          {
            zipnum = true;
          }
      }

    // open the file and seek to the start of the split
    FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(split.getPath());
    boolean skipFirstLine = false;
    if (codec != null) {
      if (null == this.recordDelimiterBytes) {
        if ( zipnum )
          {
            in = new LineReader(new OpenJDK7GZIPInputStream(fileIn), job);
          }
        else
          {
            in = new LineReader(codec.createInputStream(fileIn), job);
          }
      } else {
        if ( zipnum )
          {
            in = new LineReader(new OpenJDK7GZIPInputStream(fileIn), job, this.recordDelimiterBytes);
          }
        else
          {
            in = new LineReader(codec.createInputStream(fileIn), job, this.recordDelimiterBytes);
          }
      }
      // IA -- end code modifications
      end = Long.MAX_VALUE;
    } else {
      if (start != 0) {
        skipFirstLine = true;
        --start;
        fileIn.seek(start);
      }
      if (null == this.recordDelimiterBytes) {
        in = new LineReader(fileIn, job);
      } else {
        in = new LineReader(fileIn, job, this.recordDelimiterBytes);
      }
    }
    if (skipFirstLine) {  // skip first line and re-establish "start".
      start += in.readLine(new Text(), 0,
                           (int)Math.min((long)Integer.MAX_VALUE, end - start));
    }
    this.pos = start;
  }
  
  public boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new LongWritable();
    }
    key.set(pos);
    if (value == null) {
      value = new Text();
    }
    int newSize = 0;
    while (pos < end) {
      newSize = in.readLine(value, maxLineLength,
                            Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
                                     maxLineLength));
      if (newSize == 0) {
        break;
      }
      pos += newSize;
      if (newSize < maxLineLength) {
        break;
      }

      // line too long. try again
      LOG.info("Skipped line of size " + newSize + " at pos " + 
               (pos - newSize));
    }
    if (newSize == 0) {
      key = null;
      value = null;
      return false;
    } else {
      return true;
    }
  }

  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  public Text getCurrentValue() {
    return value;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }
  
  public synchronized void close() throws IOException {
    if (in != null) {
      in.close(); 
    }
  }
}

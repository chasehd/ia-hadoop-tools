/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


/**
 * RecordReader which does a uniq-merge of already sorted
 * input splits.
 *
 * Since this is written for merging Wayback CDX clusters, we make
 * some assumptions about the inputs, primarily, that they are already
 * sorted lines of plain text.
 *
 * Another assumption is that lines are terminated with a single
 * newline (\n) character.  Carraige-return (\r) are
 * <strong>not</strong> treated as line terminators.
 *
 * This RecordReader always returns an empty key, and the value
 * contains the entire line read from the input file.  This is
 * similar to the Hadoop LineRecordReader except that we don't
 * bother putting the line count into the key since it's not
 * really that useful.
 */
public class ClusterMergingRecordReader extends RecordReader<Text,Text>
{
  RecordReader[] partReaders;
  
  // Always return an empty key.  See note in getCurrentKey()
  Text currentKey   = new Text();
  Text currentValue;

  // For CDX files we *only* use \n to terminate lines, not \r!
  byte[] eol = new byte[] { '\n' };

  /**
   * Iterate through the splits and initialize a ClusterMergingLineRecordReader
   * for each one.
   */
  @Override
  public void initialize( InputSplit genericSplit,
                          TaskAttemptContext context )
    throws IOException, InterruptedException
  {
    ClusterMergingInputSplit split = (ClusterMergingInputSplit) genericSplit;
    
    List<FileSplit> parts = split.getSplits();
    
    partReaders = new RecordReader[parts.size()];
    
    for ( int i = 0; i < partReaders.length ; i++ )
      {
        partReaders[i] = new ClusterMergingLineRecordReader( );
        partReaders[i].initialize( parts.get(i), context );
      }
  }

  /**
   * Return the next key/value pair from the underlying group of RecordReaders.
   *
   * This method is where the magic happens.  It reads the values from
   * the RecordReaders and keeps the lowest one (plain old string
   * comparison) and gobbles up any duplicate values.
   */
  @Override
  public boolean nextKeyValue()
    throws IOException, InterruptedException
  {
    Text candidate = null;

    for ( int i = 0 ; i < partReaders.length ; i++ )
      {
        // If a partReader is null, then we've already closed it,
        // having reached the end.
        if ( partReaders[i] != null )
          {
            Text partValue = (Text) partReaders[i].getCurrentValue();
            
            while ( partValue == null ||
                    (currentValue != null && partValue.compareTo( currentValue ) <= 0 ) )
              {
                if ( ! partReaders[i].nextKeyValue() )
                  {
                    // We've reached the end of this partReader, so close it and
                    // null-out the reference to it in the array.
                    partValue = null;
                    partReaders[i].close();
                    partReaders[i] = null;
                    break; 
                  }
                
                partValue = (Text) partReaders[i].getCurrentValue();
              }
            
            // The partValue can be null if we hit the end of the
            // partReader and bailed-out of the while loop above.  In
            // that case, just go on to the next reader.
            if ( partValue == null ) continue;
            
            if ( candidate == null )
              {
                candidate = partValue;
              }
            else
              {
                candidate = candidate.compareTo( partValue ) <= 0 ? candidate : partValue;
              }
          }
      }
    
    // If there was no candidate value from any of the parts, then
    // that means that we're all done.
    if ( candidate == null ) return false;
    
    // Only create a new Text the first time around, after that,
    // just copy the value from the candidate.
    if ( currentValue == null )
      {
        currentValue = new Text(candidate);
      }
    else
      {
        currentValue.set( candidate );
      }
    
    return true;
  }
  
  /**
   * The currentKey is <strong>always</strong> empty.  We don't care
   * about it at all.  Only the value matters.
   */
  @Override
  public Text getCurrentKey()
    throws IOException, InterruptedException
  {
    return currentKey;
  }

  /**
   * Return the currentValue.
   */
  @Override
  public Text getCurrentValue()
    throws IOException, InterruptedException
  {
    return currentValue;
  }

  /**
   * Return 0.0f as the progress.
   *
   * I leave it as an exercise to the reader to measure the progress of multiple
   * underlying RecordReaders which are reading from possibly compressed files.
   */
  @Override
  public float getProgress()
    throws IOException, InterruptedException
  {
    return 0.0f;
  }

  /**
   * Close the underlying RecordReaders.
   *
   * FIXME: Does it matter if the first one throws an IOException and
   *        we don't try to close the rest?
   */
  @Override
  public synchronized void close() 
    throws IOException
  {
    for ( int i = 0 ; i < partReaders.length ; i++ )
      {
        if ( partReaders[i] != null )
          {
            partReaders[i].close();
          }
      }
  }

}

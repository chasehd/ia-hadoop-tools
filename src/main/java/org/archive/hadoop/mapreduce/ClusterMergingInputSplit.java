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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * InputSplit which contains a list of FileSplits corresponding to the
 * matching "part-*" files of input CDX clusters.
 */
public class ClusterMergingInputSplit extends InputSplit implements Writable
{
  List<FileSplit> splits = new ArrayList<FileSplit>(4);
  
  long length;

  /**
   * Add split to the end of the list.  Accumulate the length of the added split.
   */
  public void add( FileSplit split )
  {
    splits.add( split );
    length += split.getLength();
  }
  
  /**
   * Return internal list of splits.  Not a good idea to modify the returned list.
   */
  public List<FileSplit> getSplits( )
  {
    return this.splits;
  }

  /**
   * Return the accumulated length of all the splits.
   */
  @Override
  public long getLength() throws IOException, InterruptedException
  {
    return length;
  }
  
  /**
   * Currently, eturns empty string[].  I leave as an exercise to the
   * reader some complex logic to analyze the inner FileSplits and
   * collate their locations into a meaningful representation of this
   * split.
   *
   * Note that by returning the empty string[] here, Hadoop cannot
   * perform data-local task scheduling.  That is, a task will be run
   * on any node in the cluster, it won't try to schedule it on a node
   * that has a local copy of the data.
   */
  @Override
  public String[] getLocations() throws IOException, InterruptedException
  {
    return new String[0];      
  }
  
  /**
   * Serialize out this split.
   */
  @Override
  public void write( DataOutput out ) throws IOException
  {
    out.writeLong(length);
    out.writeInt(splits.size());
    for ( FileSplit split : splits )
      {
        split.write(out);
      }
  }
  
  /**
   * Serialize in this split.
   */
  @Override
  public void readFields( DataInput in ) throws IOException
  {
    length = in.readLong();
    int count = in.readInt();
    for ( int i = 0 ; i < count ; i++ )
      {
        // Strangely the default FileSplit constructor is hidden, so
        // we have to pass in null/0 values to create an empty one.
        FileSplit split = new FileSplit(null,0,0,null);
        split.readFields(in);
        
        splits.add(split);
      }
  }
  
  /**
   * Return a human-friendly representation of the split.
   */
  @Override
  public String toString( )
  {
    StringBuilder buf = new StringBuilder();
    buf.append( "length=" );
    buf.append( length );
    buf.append( " splits=[" );
    for ( int i = 0 ; i < splits.size() ; i++ )
      {
        FileSplit split = splits.get(i);
        buf.append( split.toString() );
        if ( i < (splits.size()-1) ) buf.append( "," );
      }
    buf.append( "]" );
    
    return buf.toString();
  }
  
}

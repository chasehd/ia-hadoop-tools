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

package org.archive.hadoop.pig;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.archive.hadoop.mapreduce.ClusterMergingInputFormat;


/**
 * Pig LoadFunc that just wraps a ClusterMergingInputFormat.
 *
 * This loader can be used to merge-uniq Wayback CDX clusters as long as they
 * adhere to the requirements of ClusterMergingInputFormat.
 */
public class ClusterMergingLoader extends LoadFunc
{
  TupleFactory factory = TupleFactory.getInstance();

  RecordReader reader;

  /**
   * Set the config property for ClusterMergingInputFormat which contains the
   * input file glob.
   */
  @Override
  public void setLocation( String location, Job job ) throws IOException
  {
    job.getConfiguration().set( "clusterMergingInputFormat.input.dir", location );
  }
  
  /**
   * Return a new ClusterMergingInputFormat instance.
   */
  @Override
  public InputFormat getInputFormat( ) throws IOException
  {
    return new ClusterMergingInputFormat( );
  }

  /**
   * Save the reader for later.
   */
  @Override
  public void prepareToRead( RecordReader reader, PigSplit split ) throws IOException
  {
    this.reader = reader;
  }

  /**
   * Return the next value from the ClusterMergingRecordReader.  We
   * only care about the value.  We ignore the key, because the
   * ClusterMergingRecordReader always returns '' for the key, it's
   * bogus.  See comments in that class for more details.
   */
  @Override
  public Tuple getNext( ) throws IOException
  {
    try
      {
        if ( ! reader.nextKeyValue() )
          {
            return null;
          }
        
        Tuple t = factory.newTuple(reader.getCurrentValue().toString());

        return t;
      }
    catch ( InterruptedException ie )
      {
        throw new IOException( ie );
      }
  }

}

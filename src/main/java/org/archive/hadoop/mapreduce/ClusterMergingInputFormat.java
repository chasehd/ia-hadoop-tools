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
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.util.StringUtils;


/**
 * InputFormat that merges already sorted matching partitions from one
 * or more Wayback CDX clusters as they are being read.
 *
 * This class traverses the directories of the given CDX clusters and
 * bundles the matching <code>part-*</code> files together into
 * ClusterMergingInputSplit.  That is,
 * <code>cluster-A/part-00000.gz</code> and
 * <code>cluster-B/part-00000.gz</code> are bundled together into a
 * single split, then <code>cluster-A/part-00001.gz</code> and
 * <code>cluster-B/part-00001.gz</code> are bundled together into
 * another split, and so forth.
 *
 * A simple ClusterPathFilter nested class provides a simple filtering
 * of the paths inside the cluster.  We keep all the "part-*" files,
 * but filter-out the '-idx" files.
 *
 * The merge-uniq'ing of the vlaues is performed in the
 * ClusterMergingRecordReader.
 */
public class ClusterMergingInputFormat extends InputFormat<Text,Text>
{
  /**
   * Get the list of splits for the input CDX clusters.
   */
  @Override
  public List<InputSplit> getSplits( JobContext context ) 
    throws IOException, InterruptedException
  {
    List<IOException> errors = new ArrayList<IOException>();

    ClusterMergingInputSplit[] splits = null;  // Initialize on-demand

    String dirs = context.getConfiguration().get( "clusterMergingInputFormat.input.dir", "" );
    String [] list = StringUtils.split(dirs);
    Path[] result = new Path[list.length];

    // Iterate through the paths listed in clusterMergingInputFormat.input.dir (separated by ',').
    // Each of these paths can have a file glob (e.g. "/path/foo*"), which we expand, then
    // iterate through the list of paths that result from the glob expansion.
    for (int i = 0; i < list.length; i++) 
      {
        Path p = new Path(StringUtils.unEscapeString(list[i]));

        FileSystem fs = p.getFileSystem( context.getConfiguration() );
        FileStatus[] clusters = fs.globStatus(p);
        
        if ( clusters == null ) 
          {
            errors.add(new IOException("Input path does not exist: " + p));
          } 
        else if ( clusters.length == 0 )
          {
            errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
          }
        else 
          {
            for ( FileStatus cluster: clusters )
              {
                if ( cluster.isDir() )
                  {
                    FileStatus[] partfiles = fs.listStatus( cluster.getPath(), new ClusterPathFilter() );

                    if ( splits == null ) splits = new ClusterMergingInputSplit[partfiles.length];
                    
                    for ( int part = 0; part < partfiles.length; part++ )
                      {
                        FileStatus partfile = partfiles[part];

                        long length = partfile.getLen();

                        FileSplit split = new FileSplit( partfile.getPath(), 0, length, null );

                        if ( splits[part] == null ) splits[part] = new ClusterMergingInputSplit();

                        splits[part].add( split );
                      }
                  } 
                else 
                  {
                    errors.add( new IOException( "Input cluster is not a directory: " + cluster ) );
                  }
              }
          }
      }

    if ( splits == null ) errors.add( new IOException( "No splits!" ) );

    if ( !errors.isEmpty() ) throw new InvalidInputException(errors);

    List<InputSplit> splitList = Arrays.asList( (InputSplit[]) splits );
    
    return splitList;
  }


  /**
   * Return a new ClusterMergingRecordReader instance.
   */
  @Override
  public RecordReader<Text,Text> createRecordReader( InputSplit genericSplit, TaskAttemptContext context ) 
    throws IOException, InterruptedException
  {
    return new ClusterMergingRecordReader( );
  }

  /**
   * Simple path filter to only accept 'part-*' files and exclude the '-idx' files.
   * I leave this as an exercise to the reader to make fancier.
   */
  public static class ClusterPathFilter implements PathFilter
  {
    public boolean accept( Path path )
    {
      return path.getName().startsWith( "part-" ) && ! path.getName().endsWith( "-idx" );
    }
  }

}

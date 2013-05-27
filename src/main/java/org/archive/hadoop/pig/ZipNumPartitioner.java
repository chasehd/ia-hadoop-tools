package org.archive.hadoop.pig;

import org.apache.hadoop.io.Writable;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableText;
import org.apache.pig.impl.io.PigNullableWritable;

/**
 * A Partitioner for Pig that simply unwinds the Pig-provided key and
 * then calls the parent Hadoop Partitioner to calculate its
 * partition.
 *
 * If the Pig-provided key is a Tuple (is it ever not?), then we grab
 * the first element from the tuple, convert it to a string and use
 * that string as the key.
 */
public class ZipNumPartitioner extends org.archive.hadoop.mapreduce.ZipNumPartitioner<PigNullableWritable, Writable> {

	@Override
	public int getPartition( PigNullableWritable key, Writable value, int numSplits )
        {
          String skey;
          Object pobject = key.getValueAsPigType();
          if ( pobject instanceof Tuple )
            {
              try
                {
                  skey = ((Tuple)pobject).get(0).toString();
                }
              catch ( Exception e )
                {
                  return 0;
                }
            }
          else
            {
              skey = pobject.toString();
            }
          
          return super.getPartition( skey, numSplits );
	}
}

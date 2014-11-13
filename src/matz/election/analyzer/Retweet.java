/**
 * 
 */
package matz.election.analyzer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

/**Collecting/Clustering of retweet
 * @author Yu
 *
 */
public class Retweet {
	
	/**公式RTを収集するMap．出力はRTStatusをKey，RTしたユーザをVal。
	 * @author Yu
	 *
	 */
	public static class RetweetMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			try {
				Status tweet = TwitterObjectFactory.createStatus(value.toString());
				if (tweet.isRetweet()) {
					String retweet = TwitterObjectFactory.getRawJSON(tweet.getRetweetedStatus());
				}
			} catch (TwitterException e) {
				//do nothing
				e.printStackTrace();
			}
			
		}
		
	}

}

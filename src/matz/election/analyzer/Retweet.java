/**
 * 
 */
package matz.election.analyzer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

/**Collecting/Clustering of retweet
 * @author Yu
 *
 */
public class Retweet {
	
	/**公式RTを収集するMap．出力はRT<s>Status</s>文面をKey、RTしたユーザをVal。文面はBase64エンコする。
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
					//retweet
					Status retweet = tweet.getRetweetedStatus();
//					String text = retweet.getText().replaceAll("[\t|\n]", "_");
					String text = retweet.getText();
					byte[] textByte = text.getBytes();
					byte[] encodedByte = Base64.encodeBase64(textByte);
					String encoded = new String(encodedByte);
					output.collect(new Text(encoded), new LongWritable(tweet.getUser().getId()));
				} else if (tweet.getRetweetCount() > 0) {
					//original
					String text = tweet.getText();
					byte[] textByte = text.getBytes();
					byte[] encodedByte = Base64.encodeBase64(textByte);
					String encoded = new String(encodedByte);
					output.collect(new Text(encoded), new LongWritable(tweet.getUser().getId()));
				}
			} catch (TwitterException e) {
				//do nothing
				e.printStackTrace();
			}
			
		}
	}
	
	/**Mapされたペアから、文面のKey、ユーザリストのcsvをつくるReducer。
	 * @author YuMatsuzawa
	 *
	 */
	public static class RetweetReduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, Text>, JobConfigurable {
		private static int threshold = 10;

		public void configure(JobConf job) {
			String extraArg = job.get("arg3");
			if (extraArg != null) {
				try {
					threshold = Integer.parseInt(extraArg);
				} catch (NumberFormatException e) {
					//do nothing. default value will be kept.
				}
			}
		};
		
		@Override
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Set<String> users = new HashSet<String>();
			while(values.hasNext()) users.add(values.next().toString());
			if (users.size() >= threshold) {
				String csv = "";
				for (String user : users) {
					if (!csv.isEmpty()) csv += ",";
					csv += user;
				}
				output.collect(key, new Text(csv));
			}
		}
	}
	
	/**RT-usrの全ての組合せペアを作り出すMap
	 * @author YuMatsuzawa
	 *
	 */
	public static class RTCrossJoinMap extends MapReduceBase implements Mapper<Text, Text, IntWritable, Text> {
		private static IntWritable one = new IntWritable(1);
		@Override
		public void map(Text key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			String concat = key.toString() + "," + value.toString();
			output.collect(one, new Text(concat));
		}	
	}
	
	/**既存のCrossJoinアルゴと同じだが、入力時点でCSVに加工済みなので多分こっちのほうが速い。
	 * @author YuMatsuzawa
	 *
	 */
	public static class RTCrossJoinReduce extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text> {

		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			ArrayList<String> total = new ArrayList<String>();
			while(values.hasNext()) total.add(values.next().toString());
			
			int i = 0;
			for (String pair : total) {
				int j = 0, cur = i;
				for (String innerPair : total) {
					if (j > cur) {
						output.collect(new Text(pair), new Text(innerPair));
					}
					j++;
				}
				i++;
			}
		}
		
	}
}

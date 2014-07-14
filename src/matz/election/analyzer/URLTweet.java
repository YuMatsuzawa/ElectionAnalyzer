package matz.election.analyzer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.URLEntity;

/**ツイートに含まれるURLを元にして内容の傾向を判定し、分析するためのクラス。
 * @author Matsuzawa
 *
 */
public class URLTweet extends matz.election.analyzer.TweetCount {
	
	/**URLを含むツイートと、各URLごとの出現回数を数え上げるMapper。<br>
	 * SeqFile<LongWritable, Text>を読み、Val内のJSONをパースし、添付されたURLの有無を確認する。<br>
	 * 面倒なのでパースエラーが出ていたツイート(Keyが0のもの)は最初からスキップする。
	 * @author Matsuzawa
	 *
	 */
	public static class URLCountMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		private static Text urlText = new Text("noURL");

		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			if (key.get() != 0) {
				Status tweet = null;
				try {
					tweet = TwitterObjectFactory.createStatus(value.toString());
					for (URLEntity url : tweet.getURLEntities()) { //もしURL添付がなければ配列は空である。よってループは1回も回らずに抜ける。すると初期化時の"noURL"キーがそのまま残る。
						String urlStr = url.getExpandedURL(); //展開済みURLを使う。
						if (urlStr == null) urlStr = url.getURL();
						urlText.set(urlStr);
						output.collect(urlText, one);
						
						urlText.set("withURL"); //ループが回ったということはURLがあったということなので、最後にこのキーを代入しておく。
					}
				} catch (TwitterException e) {
					e.printStackTrace();
					System.out.println(value.toString());
 				}
				output.collect(urlText, one); //URLがなかった場合は"noURL"のまま、あった場合は"withURL"になっている。
			}
		}
		
	}
	
	public static class TextIntReduce extends TweetCount.TextIntReduce {};
}

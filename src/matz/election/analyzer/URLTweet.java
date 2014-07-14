package matz.election.analyzer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.URLEntity;

/**ツイートに含まれるURLを元にして内容の傾向を判定し、分析するためのクラス。
 * @author Matsuzawa
 *
 */
public class URLTweet extends matz.election.analyzer.TweetCount {
	protected static final int BUZZ_THRESHOLD = 100;
	
	/**URLを含むツイートと、各URLごとの出現回数を数え上げるMapper。<br>
	 * SeqFile<LongWritable, Text>を読み、Val内のJSONをパースし、添付されたURLの有無を確認する。<br>
	 * 面倒なのでパースエラーが出ていたツイート(Keyが0のもの)は最初からスキップする。
	 * @author Matsuzawa
	 *
	 */
	public static class URLCountMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		private Text urlText = new Text("noURL");

		
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
						if (urlStr == null) urlStr = url.getURL(); //展開済みが使えなければURLを使うが、
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
	
	/**URLCountで得た結果を元に言及された回数ごとに頻度を数える。<br>
	 * 入力はTextなのでLong,Text、出力はInt,Int.
	 * @author Matsuzawa
	 *
	 */
	public static class URLFreqMap extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		private IntWritable count = new IntWritable();
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
				throws IOException {
			String[] splitLine = value.toString().split("\\s");
			if (splitLine.length == 2) {
				try {
					count.set(Integer.parseInt(splitLine[1]));
				} catch (NumberFormatException e) {
					e.printStackTrace();
					System.err.println(splitLine);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			output.collect(count, one);
		}
	}
	
	/**URLCountで得た結果から、そこそこ話題になった（多くのツイートで言及された）URL=BuzzURLを抽出し、言及回数ごとにCSVとしてまとめる。<br>
	 * 後にこのデータを元にクラスタリングとかしたらいいんじゃない？<br>
	 * Buzzの閾値は定数BUZZ_THRESHOLDで定める。デフォルト100。引数で変更可能。<br>
	 * 入力はTextなのでLong,Text。出力はInt,Text。Keyには言及回数、Valueにはスペース区切りで該当URLがappendされたもの。
	 * @author Matsuzawa
	 *
	 */
	public static class BuzzExtractMap extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>, JobConfigurable {
		private int buzzThreshold = URLTweet.BUZZ_THRESHOLD;
		
		private IntWritable count = new IntWritable();
		private Text url = new Text();
		
		private String[] excludedKeyArray = {"withURL","noURL"};
		private List<String> excludedKeys = Arrays.asList(excludedKeyArray);
		
		private String extraArg = new String();
		public void configure(JobConf job) {
			extraArg = job.get("arg3");
			if (extraArg != null) {
				try {
					buzzThreshold = Integer.parseInt(extraArg);
				} catch (NumberFormatException e) {
					//do nothing
				}
			}
		};

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			String[] splitLine = value.toString().split("\\s");
			if (!excludedKeys.contains(splitLine[0])) {
				try {
					int intCount = Integer.parseInt(splitLine[1]);
					if (intCount > this.buzzThreshold) {
						count.set(intCount);
						url.set(splitLine[0]);
						output.collect(count, url);
					}
				} catch (NumberFormatException e) {
					e.printStackTrace();
					System.err.println(splitLine);
				}
			}
		}
	}
	
	/**BuzzExtractMapのためのReducer。Iteratorが返すTextのURLを、スペース区切りを入れて次々AppendしていったものをValueに投入する。
	 * @author Matsuzawa
	 *
	 */
	public static class BuzzExtractReduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {

		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			String urls = values.next().toString();
			while(values.hasNext()) {
				urls += " " + values.next().toString();
			}
			output.collect(key, new Text(urls));
		}
		
	}
	
	public static class TextIntReduce extends TweetCount.TextIntReduce {};
	public static class LongIntReduce extends TweetCount.LongIntReduce {};
	public static class IntIntReduce extends TweetCount.IntIntReduce {};
}

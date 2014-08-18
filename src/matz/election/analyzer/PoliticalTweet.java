package matz.election.analyzer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

/**政治的な内容のツイートを抽出するためのクラス。<br>
 * 元データは政治的な内容のツイートを行ったアカウントについて、その時期に行ったその他のツイートも集める、という方式で収集した。<br>
 * ぶっちゃけそれで取得した「その他ツイート」はとりあえずいらんわ、となったので改めて抽出し直す。<br>
 * クエリキーワードはqueries.txtに含まれている。
 * @author Matsuzawa
 *
 */
public class PoliticalTweet extends URLTweet {
	protected static final Path queryFile = new Path("/user/matsuzawa/queries.txt");
	
	/**政治的な内容のツイートを抽出するMap。元データのSeqFileを読み、抽出された同形式のSeqFileとして吐く。<br>
	 * パース不可能なツイートはこの時点で排除する。よって出力されたSeqFileはより扱いやすいものになっている。<br>
	 * Reducerはなしでもいい。が、なしというのの指定方法を定義してない気がするので適当に無内容なReducerを用意する。
	 * @author Matsuzawa
	 *
	 */
	public static class PoliticalTweetMap extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text>,JobConfigurable {
		private List<String> queryList = new ArrayList<String>();
		
		public void configure(JobConf job) {
			// configureを使ってqueryListを初期化する。うまく行けばこれでいいし、うまくいかなければ何か別の方法を考える必要がある。
			BufferedReader br = null;
			FSDataInputStream is = null;
			try {
				FileSystem fs = FileSystem.get(job);
				is = fs.open(queryFile);
				br = new BufferedReader(new InputStreamReader(is));
				String line = new String();
				while((line = br.readLine()) != null) {
					queryList.add(line);
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			} finally {
				try {
					br.close();
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			if (key.get() != 0) {
				boolean isPolitical = false;
				Status tweet = null;
				try {
					tweet = TwitterObjectFactory.createStatus(value.toString());
					for(String keyword : queryList) {
						// キーワードリストの中に合致する語が一つでもあれば該当。なければ破棄
						if (tweet.getText().contains(keyword)) {
							isPolitical = true;
							break;
						}
					}
					if (isPolitical) {
						output.collect(key, value);
					}
				} catch (TwitterException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**単なる元データの絞り込みがしたいだけなので、Reducerは無内容でいい。
	 * @author Matsuzawa
	 *
	 */
	public static class PoliticalTweetReduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {

		@Override
		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			while(values.hasNext()) {
				output.collect(key, values.next());
			}
		}
	}
	
	/**政党名ごとに、期間中にどれだけツイートされたかカウントするMap。実際の支持率や、議席割合との比較に用いる。<br>
	 * 
	 * @author Yu
	 *
	 */
	public static class PartyBuzzMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO 自動生成されたメソッド・スタブ
			
		}
		
	}

}

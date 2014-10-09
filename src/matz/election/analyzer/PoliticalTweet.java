package matz.election.analyzer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.URLEntity;

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
	 * PoloticalTweetMapで絞り込んだデータを入力に使えばいい。Key:TweetID,Value:RawJSON.<br>
	 * TextIntReduceが使える。
	 * @author Yu
	 *
	 */
	public static class PartyBuzzMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private String[] partyNames = {"自民党","民主党","日本維新の会","公明党","みんなの党","生活の党","共産党","社民党","新党改革","みどりの風"};
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			Status tweet = null;
			try {
				tweet = TwitterObjectFactory.createStatus(value.toString());
				for (String party : partyNames) {
					if (tweet.getText().contains(party)) {
						output.collect(new Text(party), new IntWritable(1));
					}
				}
			} catch (TwitterException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**政策に関する話題＝Topicに関連するツイートで、URLを含むものをカウントする。<br>
	 * インプットはPoliticalTweetMapで抽出された政治関連ツイートのSeqファイル。<br>
	 * インプットのKeyにはLongのユーザID、ValueにはTextでツイートStatusが格納されている。<br>
	 * Topicはコマンドラインから取得するようにするので、インターフェイスJobConfigurableを実装する。<br>
	 * 最終的なアウトプットは、URLをTextKey、言及数をIntValueとして持つ<s>Text</s>SeqFileとする。SeqFileのほうが扱いが良いので変更。TextIntReduceを使う。<br>
	 * さらにそのアウトプットをインプットとして、URLをKey，そのリンク先ページのタイトルをValueとするようなマップを用意すると、簡単なチェックに使えるだろう。
	 * @author YuMatsuzawa
	 *
	 */
	public static class TopicURLCountMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>, JobConfigurable {
		//引数など、設定情報をコマンドラインやmain内から得たい場合は、JobCinfigurableをimplementしてconfigureを実装する。
		private List<String> topicQueries = new ArrayList<String>();
		private Text urlText = new Text();
		private IntWritable one = new IntWritable(1);
		
		public void configure(JobConf job) {
			String extraArg = null;
			int argIndex = 3;
			while(true) {
				extraArg = job.get(String.format("arg%d", argIndex));
				if (extraArg != null) {
					topicQueries.add(extraArg);
					argIndex++;
				} else {
					break;
				}
			}
		}
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			Status tweet = null;
			try {
				tweet = TwitterObjectFactory.createStatus(value.toString());
				boolean isRelated = false;
				for (String query : topicQueries){ //クエリに合致する語を含む（＝関連ツイートである）かどうかを調べる。
					if (tweet.getText().contains(query)) {
						isRelated = true;
						break;
					}
				}
				
				if (isRelated) { //関連しているなら添付URLを数える。
					for (URLEntity url : tweet.getURLEntities()) { //もしURL添付がなければ配列は空である。よってループは1回も回らずに抜ける。
						String urlStr = url.getExpandedURL(); //展開済みURLを使う。
						if (urlStr == null) urlStr = url.getURL(); //展開済みが使えなければURLを使うが、ここには外部の短縮サービスで短縮されたURLが入っていることもある。
						urlText.set(urlStr);
						output.collect(urlText, one);
					}
				}
			} catch (TwitterException e) {
				e.printStackTrace();
			}
			
		}
	}
	
	/**TopicURLCountMapで取得した特定話題のURL群について、適当な閾値よりも多く言及されているURLにコネクションを開き、 記事のタイトルを取得してくるMap。<br>
	 * <s>TextFile</s>SeqFileをインプットとし、<s>1行は空白区切りで、URL-CountNumペアが格納されている。</s>Text-IntWritableペアが格納されている<br>
	 * コマンドラインから閾値を取得する。デフォルトは10。
	 * @author YuMatsuzawa
	 *
	 */
	public static class TopicURLTitleMap extends MapReduceBase implements Mapper<Text, IntWritable, Text, IntWritable>, JobConfigurable {
		private int threshold = 10;
		private IntWritable one = new IntWritable(1);
		private Text title = new Text();

		public void configure(JobConf job) {
			String extraArg = job.get("arg3");
			if (extraArg != null) {
				try {
					threshold = Integer.parseInt(extraArg);
				} catch (NumberFormatException e) {
					//do nothing. default value will be kept.
				}
			}
		}
		
		@Override
		public void map(Text key, IntWritable value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
//			String[] pair = value.toString().split("//s");
//			int keyIdx = 0, valIdx = 1;
			URL link = null;
//			if (Integer.parseInt(pair[valIdx]) >= threshold) {
			if (Integer.parseInt(value.toString()) >= threshold) {
				link = new URL(key.toString());
				HttpURLConnection conn = null;
				try {
					conn = (HttpURLConnection) link.openConnection();
		    		conn.setConnectTimeout(10*1000);
		    		conn.setInstanceFollowRedirects(true); // get actual contents
		    		conn.connect();
		    		
		    		Document doc = Jsoup.parse(conn.getContent().toString());
		    		Element titleElement = doc.getElementsByTag("title").first();
		    		title.set(titleElement.text());
		    		
		    		output.collect(title, one);
		    		
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	
	public static class TextIntReduce extends TweetCount.TextIntReduce {};

}

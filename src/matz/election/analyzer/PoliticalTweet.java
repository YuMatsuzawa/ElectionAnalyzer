package matz.election.analyzer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.Map.Entry;

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
	 * さらにそのアウトプットをインプットとして、URLをKey，そのリンク先ページのタイトルをValueとするようなマップを用意すると、簡単なチェックに使えるだろう。<br>
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
	
	/**話題関連URLと、その言及ユーザとのペア（リンク）を出力する。その後の集計の基礎とすべきデータベースとするため、Reducerは何もしなくていい。<br>
	 * つまり、URLをKey，言及ユーザをValueとするMapperから送られてくるペアをそのまま再現して送り出すReducerでよい。<br>
	 * <s>ここでいきなりURLの展開と、クエリパラメータの除去を行う。以後の分析はこのMapで作られたデータを参照して行えば良い（元のTweetログに立ち返る必要がない）.</s><br>
	 * なんかうまくいかないので展開はやめる。パラメータ除去も、あとで別のMapを設けてそこでやる。まずはもとURLとUserIDとのリンクを作ってしまう。<br>
	 * 必要ならこのMapReduceの出力を複製してMetastoreテーブルとしてインポートしておけばHive/Impala分析もできる。
	 * @author YuMatsuzawa
	 *
	 */
	public static class TopicURLUserMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable>, JobConfigurable {
		//引数など、設定情報をコマンドラインやmain内から得たい場合は、JobCinfigurableをimplementしてconfigureを実装する。
		private List<String> topicQueries = new ArrayList<String>();
		private Text urlText = new Text();
//		private int MAX_HOP = 10;
		
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
				OutputCollector<Text, LongWritable> output, Reporter reporter)
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
						
						//ここで末尾のアンカー/クエリで不要なものを除去。
//						String tmp = URLExpander.trimURL(urlStr), destStr = null;
						
						//open connection to fetch Location, while considering redirect loop
//						int hopNum = 0;
//						while(tmp!=null && hopNum < MAX_HOP) {
//							hopNum++;
//							destStr = tmp;
//							tmp = URLExpander.connectWithoutRedirect(tmp);
//						}
//						
//						if (hopNum >= MAX_HOP) { // assumed redirect loop. keep initial URL
//							destStr = urlStr;
//						}
						
//						urlText.set(destStr);
						urlText.set(urlStr);
						output.collect(urlText, key); // collect URL-UserID pair
					}
				}
			} catch (TwitterException e) {
				e.printStackTrace();
			}
			
		}
	}
	
	public static class TopicURLUserReduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			while(values.hasNext()) {
				output.collect(key, values.next()); // doing nothing, just passing source pair
			}
		}
		
	}
	
	/**URLのKeyに対し、言及ユーザのValueを集計するMapR。Mapperは単にテキストからKey-Valを読む。
	 * @author YuMatsuzawa
	 *
	 */
	public static class FilterURLMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String[] pair = value.toString().split(",");
			output.collect(new Text(pair[0]), new Text(pair[1]));
		}
		
	}
	
	/**Reducer内で、Valリストを結合し、CSV化する。出力はKeyにURL，Valueに対応する言及ユーザのCSVが入る。
	 * @author YuMatsuzawa
	 *
	 */
	public static class FilterURLReduce extends MapReduceBase implements Reducer<Text,Text,Text,Text> {
//		private int threshold = 1;

//		public void configure(JobConf job) {
//			String extraArg = job.get("arg3");
//			if (extraArg != null) {
//				try {
//					threshold = Integer.parseInt(extraArg);
//				} catch (NumberFormatException e) {
//					//do nothing. default value will be kept.
//				}
//			}
//		}
		
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
//			ArrayList<String> valueList = new ArrayList<String>();
//			while(values.hasNext()) valueList.add(values.next().toString());
			if (!key.toString().startsWith("\"")) {
				String valStr = "";
				while(values.hasNext()) {
					if (!valStr.isEmpty()) valStr += ",";
					valStr += values.next().toString();
				}
				Text valText = new Text(valStr);
				output.collect(key, valText);
			}
			
		}
	}
	
	/**FilterURLで作ったURLごとの言及ユーザリストに対し、閾値を設けて定数以下の言及回数のURLを除外する。<br>
	 * 出力はFilterURLと同形式、URLとそれに対応するユーザリスト。
	 * @author YuMatsuzawa
	 *
	 */
	public static class ThresholdURLMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>, JobConfigurable {
		private int threshold = 1;

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
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String[] pair = value.toString().split("\\s");
			String URL = pair[0];
			String[] users = pair[1].split(",");
			if (users.length >= threshold) {
				output.collect(new Text(URL), new Text(pair[1]));
			}
		}
	}
	
	public static class ThresholdURLReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			while(values.hasNext()) output.collect(key, values.next());
		}
	}
	
	/**FilterURLまたはThresholdURLで作成したURL-ユーザリストのデータから、任意の2URLについてレコードを接合したペアを出力する。<br>
	 * 従ってこのMapRは、入力データが100件のレコードを持っていれば、出力は100x99=9900件のレコードを持つ。<br>
	 * 入力に対し2乗オーダーで計算時間及びメモリ使用量を要するので、重いJOIN処理である。<br>
	 * また、全組み合わせを網羅する必要上、combineが不可能で、単一のReducer内で処理するしかないため、MapR向きの処理ではない。<br>
	 * Mapper内では、統一されたInt値のKeyに対し、入力レコードをValueとして添付してemitする。これによって単一のReducerに全てのレコードを集結させる。
	 * @author YuMatsuzawa
	 *
	 */
	public static class PairedURLMap extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
		private IntWritable one = new IntWritable(1);
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			output.collect(one, value);
		}
	}
	
	/**Reducer内で入力レコードのJOIN処理を行う。KeyはIntの1に統一してあるため、入力の全レコードが単一のReducerに集結する。<br>
	 * 入力はURL\tUserid,userid,userid,...という形式だが、この\tはReducerからText形式のデータに出力される際に自動で付けられたものである。<br>
	 * このタブを保持してしまった場合、出力のテキストを読む際に問題が起きるような気がするので、これをカンマにしておく。即ち出力は以下の形式である。<br>
	 * <code>URL1,userid,userid,userid,...\tURL2,userid,userid,userid,...</code>
	 * @author YuMatsuzawa
	 *
	 */
	public static class PairedURLReduce extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text> {

		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			LinkedHashMap<String, String[]> pairs = new LinkedHashMap<String, String[]>();
			while(values.hasNext()) {
				String[] pair = values.next().toString().split("\t");
				String[] users = pair[1].split(",");
				pairs.put(pair[0], users);
			}
			
			// currently we need combination of records, that means we consider a reversed pair as the same to the original pair.
			// to eliminate unneeded pairs, we drop 'already processed' record from iterating Map.
			int i=0;
			for (Entry<String, String[]> pair : pairs.entrySet()) {
//				Entry<String, String[]> keyPair = pair;
				String keyStr = pair.getKey()+","+join(pair.getValue());
//				pairs.remove(pair.getKey()); 													//removing picked entry from Map.
				int j=0,cur=i;
				for (Entry<String, String[]> valPair : pairs.entrySet()) {
					if (j > cur) {
						String valStr = valPair.getKey()+","+join(valPair.getValue());
						output.collect(new Text(keyStr), new Text(valStr));
					}
					j++;
				}
				i++;
			}
		}

		private static String join(String[] strings) {
			String ret = "";
			for(String str : strings) {
				if (!ret.isEmpty()) ret += ",";
				ret += str;
			}
			return ret;
		}
	}
	
	/**FIXME 未完成。<br>
	 * TopicURLCountMapで取得した特定話題のURL群について、適当な閾値よりも多く言及されているURLにコネクションを開き、 記事のタイトルを取得してくるMap。<br>
	 * <s>TextFile</s>SeqFileをインプットとし、<s>1行は空白区切りで、URL-CountNumペアが格納されている。</s>Text-IntWritableペアが格納されている<br>
	 * コマンドラインから閾値を取得する。デフォルトは10。HTMLパースエンジン・Jsoupを使ってtitleタグの中身を取得する。<br>
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

package matz.election.analyzer;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import matz.election.analyzer.util.URLExpander;

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
						if (urlStr == null) urlStr = url.getURL(); //展開済みが使えなければURLを使うが、ここには外部の短縮サービスで短縮されたURLが入っていることもある。
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
	
	/**短縮URL展開とアンカー・クエリ除去したURLをKey、言及ユーザをValueとするペアでエッジを表現するレコードを出力するマップ。<br>
	 * Reducer内で短縮URL展開すれば、1URLに対し（1+Combine回数）コネクションで済むので軽いはず。アンカー・クエリ除去はMapper内でやる。
	 * @author YuMatsuzawa
	 *
	 */
	public static class URLReferMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			try {
				Status tweet = TwitterObjectFactory.createStatus(value.toString());
				for (URLEntity url : tweet.getURLEntities()) {
					String urlStr = url.getExpandedURL();
					if (urlStr == null) urlStr = url.getURL();
					urlStr = URLExpander.trimURL(urlStr);
					
					output.collect(new Text(urlStr), new LongWritable(tweet.getId()));
				}
			} catch (TwitterException e) {
				e.printStackTrace();
			}
			
		}
	}
	
	public static class URLReferReduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			String urlStr = key.toString();
			String tmpStr = key.toString();
			int count = 0;
			boolean isLoop = false, isMalformed = false;
			while (tmpStr!=null) {
				count++;
				if (count >= 10) {
					isLoop = true;
					break;
				}
				try {
					tmpStr = URLExpander.connectWithoutRedirect(tmpStr);
				} catch (MalformedURLException e) {
					isMalformed = true;
					e.printStackTrace();
					break;
				}
			}
			if (!isLoop && !isMalformed) urlStr = tmpStr;
			
			Text urlText = new Text(urlStr);
			while(values.hasNext()) {
				output.collect(urlText, values.next());
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
	 * Buzzの閾値は定数BUZZ_THRESHOLDで定める。デフォルト100。inpath/outpathの後の第3引数で変更可能。<br>
	 * 入力はTextなのでLong,Text。出力はInt,Text。Keyには言及回数、Valueにはスペース区切りで該当URLがappendされたもの。
	 * @author Matsuzawa
	 *
	 */
	public static class BuzzExtractMap extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>, JobConfigurable {
		//引数など、設定情報をコマンドラインやmain内から得たい場合は、JobCinfigurableをimplementしてconfigureを実装する。
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
	
	/**BuzzExtractMapのためのReducer。Iteratorが返すTextのURLを、スペース区切りを入れて次々AppendしていったものをValueに投入する。<br>
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
	
	/**リストアップされたBuzzURLに実際にコネクションを開き、URLを取得する。結果、転送先が同一のURLだったものについてはマージ(言及数を合計)する。<br>
	 * 出力としてはURLCountと同じ、Text,Int形式になる。このMapReduceの結果を再びBuzzExtractに飲ませれば、URLが展開されたBuzzリストが得られる。<br>
	 * ReducerはTextIntReduceでよい。出力はTextFileとする。<br>
	 * 多分このマップはHTTPコネクションを開きまくるので非常にネットワーク負荷・メモリ負荷が高い。<br>
	 * 多少なりとも抑制するためにwaitを入れた方がいい？
	 * @author Matsuzawa
	 *
	 */
	public static class BuzzURLExpandMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
//		private IntWritable count = new IntWritable();
		private Text url = new Text();
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String[] urls = value.toString().split("\\s");
			int count = Integer.parseInt(urls[0]);
			for (String shortOrLongUrl : urls) {
				String longUrlOrNull = URLExpander.expand(shortOrLongUrl);		// let expand() return null if given URL cannot be opened (Exception'ed)
				try {
					Thread.sleep(50); //waitを入れてみる
				} catch (InterruptedException e) {
					//do nothing
				}
				if (longUrlOrNull != null) {
//					count.set();
					url.set(longUrlOrNull);
//					url.set("hoge");
					output.collect(url, new IntWritable(count));
				}
			}
//			output.collect(new Text("soto"),new IntWritable(count));
		}
	}
	
	/**Seq形式のText-Intファイルで、KeyにURLが入っているものに対し、そのURLの実際のリンク先（展開URL）を取得するためのMap。<br>
	 * このMapでは、元KeyのURLを展開したものを新Keyとしたペア（Valueは元の値のまま）を生成し、それをReducerに投入することで短縮URLによって別個のものとされていた同一のリンクの頻度を合計する。<br>
	 * 最終的なアウトプットはインプットと同形式のText-Intファイルとなる。Seq形式で出力するかText形式で出力するかはその後の用途によるので、AnalyzerMainのテーブルで指定する。<br>
	 * URL展開のための再帰的なリダイレクト参照のスニペットはURLExpanderのmain関数内にある。リダイレクトループを抜けるための処理を設けること。<br>
	 * また、URL展開のAPIが失効しているような場合には、初期ホップの時点でHTTPアクセスに対する返り値がない場合がある。このようなURLを除去するか、そのままにするかも用途による。
	 * @author YuMatsuzawa
	 *
	 */
	public static class URLExpandMap extends MapReduceBase implements Mapper<Text, IntWritable, Text, IntWritable> {
		private static int MAX_HOP = 10; 
		
		@Override
		public void map(Text key, IntWritable value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String tmp = key.toString(), destURL = null;
			int hopNum = 0;
			while(tmp!=null && hopNum < MAX_HOP) { //loop until find some reachable destination. but be carful of redirection loop.
				hopNum++;
				destURL = tmp;
				tmp = connectWithoutRedirect(tmp);
			}
			
			//At this point, destURL always have the same String as initial tmp, or acquired URL String.
			//Take note: The acquired ones might indicate error page or something meaningless.
			if (hopNum < MAX_HOP) {
				//if destURL was the same as the initial key or reached some URL within MAX_HOP window, replace the key with it.
				output.collect(new Text(destURL), value);
			} else {
				//if the URL deemed as causation of redirection loop, simply keep the initial URL.
				output.collect(key, value);
			}
		}
		private static String connectWithoutRedirect(String args) {
			URL inputUrl = null;
			HttpURLConnection conn = null;
			String ret = null;
			try {
				inputUrl = new URL(args);
				conn = (HttpURLConnection) inputUrl.openConnection();
				conn.setInstanceFollowRedirects(false);
				conn.setConnectTimeout(10*1000);
				
//				for (Entry<String, List<String>> headers : conn.getHeaderFields().entrySet()) {
//					System.out.print(headers.getKey() + " :");
//					for (String value : headers.getValue()) {
//						System.out.println("\t"+value);
//					}
//				}
//				System.out.println();
				
				ret = (conn.getHeaderField("Location") != null)? conn.getHeaderField("Location") : null;
				
				return ret;
			} catch (Exception e) {
				e.printStackTrace();
			}
			return args;
		}
	}
	
	/**TopicURLCountやURLExpandで作ったText-Int形式のSeqファイルをチェックしやすいTextファイルに変換するMap。<br>
	 * 単なるパイプ的Mapで、出力形式だけを変えるものなので、中身は実質無い。TextIntReduceを使う。
	 * @author YuMatsuzawa
	 *
	 */
	public static class SeqToTextMap extends MapReduceBase implements Mapper<Text, IntWritable, Text, IntWritable> {

		@Override
		public void map(Text key, IntWritable value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			output.collect(key, value);
		}
		
	}
	
	public static class TextIntReduce extends TweetCount.TextIntReduce {};
	public static class LongIntReduce extends TweetCount.LongIntReduce {};
	public static class IntIntReduce extends TweetCount.IntIntReduce {};
}

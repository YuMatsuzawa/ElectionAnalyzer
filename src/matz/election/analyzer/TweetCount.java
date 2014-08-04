package matz.election.analyzer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

/**選挙データ解析の端緒としてとりあえずMapReduceプログラミングを練習。<br>
 * →入力データはSeqFileに変換し直したので、[UserID]=[RawJSON]というk=vペアになっている。UserIDが読み取れなかったTweetはKeyに0が入っている。<br>
 * SequenceFileInputFormatは1つのK=VペアごとにMapperを呼び出すことに注意。TextInputFormatの場合は行ごと。<br>
 * 色々と異なるMapper/Reducerを実装して、ツイートstatusに含まれる色々なキーに対してカウントを行えるようにしてみる。
 * @author Matsuzawa
 *
 */
public class TweetCount {
	
	/**最も単純に総ツイート数を数えるためのMapper。1ペア読むごとに総数を加算する。<br>
	 * それだけではあまりに芸がないので、パース不正の起こっていたツイート数(keyが0のツイート数)も数える。<br>
	 * 従って最終的なK-Vペアは2組tweetNum=[総数],errorNum=[パース不正数]だけになる。
	 * @author Matsuzawa
	 *
	 */
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		private Text tweetNum = new Text("tweetNum");
		private Text errorNum = new Text("errorNum");
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			if (value.getLength()>1) output.collect(tweetNum, one);		//ValueはRawJSON．中身があることを一応確認してカウントアップする。
			if (key.get()==0) output.collect(errorNum, one);			//UserIDが0ならカウントアップ。
		}
	}
	
	/**ユーザごとにツイート数を数えるためのMapper。単にその行（Val）のツイートJSONを読み、ユーザidをkeyとして読み込み、valueを+1する。<br>
	 * Note:Interface Mapperの引数k1,v1,k2,v2のうちk1,v1の型と内容はInputFormatの分割形式に依存する。<br>
	 * 例えばTextInputFormatなら行単位(k1はLongWritableのポインタ、v1は現在行のText)であり、Mapperは1行ごとに呼ばれる。<br>
	 * これを前提としてMapperの作業内容を構築する。<br>
	 * SeqFileInputFormatならK=Vペア単位でMapperが呼ばれ、k1,v1はそれぞれ入力ファイルのkey形式、value形式。<br>
	 * 返り値に当たるのはk2,v2で、これの形式はMapper内で指定し、これをOutputCollectorに渡す。<br>
	 * @author Matsuzawa
	 *
	 */
	public static class UserTweetMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		private Text userid = new Text();
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			if (key.get()!=0) {
				userid.set(key.toString());
			} else {
				userid.set("unparsable_user");
			}
			output.collect(userid, one);
		}
		
	}
		
	/**ユーザ自体を数えるMapper。UserTweetCountによって出力された結果を使う。（行を数える）<br>
	 * 入力はTextInputFormatでLongWritable,Text。行ごとにMapperが呼ばれる。<br>
	 * せっかくなので度数分布に使えるようにツイート数に対する人数(XtweetしているのはY人)もカウントする。
	 * @author Matsuzawa
	 *
	 */
	public static class UserCountMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private static IntWritable one = new IntWritable(1);
		private Text userNum = new Text("userNum");
		private Text tweetNum = new Text();
		
		private String line = new String();
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			line = value.toString();
			String[] splitLine = line.split("\\s");
			output.collect(userNum, one);
			tweetNum.set(splitLine[1]);
			output.collect(tweetNum, one);
		}
	}
		
	/**秒単位で、時刻あたりのツイート数をカウントするMapper。<br>
	 * 例：XXXX/YY/ZZ-AA:BB:CCにV個のツイート。<br>
	 * ただ、ソートしたい関係上KeyはLongにする。後にデータを使うときに可読カレンダー型にパースせよ。<br>
	 * SeqFileをInputとする。
	 * @author Matsuzawa
	 *
	 */
	public static class TimeStampMap extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		private LongWritable dateLong = new LongWritable();
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, IntWritable> output, Reporter reporter)
				throws IOException {
			try {
				Status tweet = TwitterObjectFactory.createStatus(value.toString());
//				long timeStampInSecond = tweet.getCreatedAt().toString();
//				Date dateInSecond = new Date(timeStampInSecond*1000);
				dateLong.set(tweet.getCreatedAt().getTime());
				output.collect(dateLong, one);
			} catch (TwitterException e) {
				//do nothing
			} catch (Exception e) {
				//do nothing
			}
		}
	}
	
	/**Count arbitorary Retweets.
	 * @author Yu
	 *
	 */
	public static class RetweetMap extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, IntWritable> {
		private static IntWritable one = new IntWritable(1);
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, IntWritable> output,
				Reporter reporter) throws IOException {
			try {
				Status tweet = TwitterObjectFactory.createStatus(value.toString());
				Status retweet = tweet.getRetweetedStatus();
				if (retweet != null) {
					output.collect(new LongWritable(retweet.getId()), one);
				}
			} catch(TwitterException e) {
				//do nothing
			} catch(Exception e) {
				//do nothing
			}
		}
	}
	
	public static class RetweetFreqMap extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
				throws IOException {
			String[] splitText = value.toString().split("\\s");
			int retweetCount = Integer.parseInt(splitText[1]);
			output.collect(new IntWritable(retweetCount), one);
		}
		
	}
	
	/**Text, IntWritable形式のK,Vペアに対する単純な集計Reducer。<br>
	 * KeyがTextであるMapに対して使える。このとき、ソートは辞書順になることに注意する。(例：1,10,11,12,2,20,21,22,3,30,...）<br>
	 * 辞書的でなく、数値としてソートしたい場合はKeyの規模に応じてLongIntかIntIntReduceを使う。<br>
	 * @author Matsuzawa
	 *
	 */
	public static class TextIntReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			/* ここの処理は、Reducerに送られてくるkey=valペアの集合（厳密には、あるkeyに対応するval集合のイテレータ）に対して行われる。
			 * 単に、あるkeyに対応するvalの数を合計する、といった内容なら、その中身を精査する処理はいらない。
			 */
			int sum = 0;
			while (values.hasNext()) sum += values.next().get();
			output.collect(key, new IntWritable(sum));
		}
	}
	
	/**LongWritable, IntWritable形式のK,Vペアに対する集計Reducer。<br>
	 * KeyをLongで持つ時刻情報やUserID情報のMapに対して使える。
	 * @author Matsuzawa
	 *
	 */
	public static class LongIntReduce extends MapReduceBase implements Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
		
		@Override
		public void reduce(LongWritable key, Iterator<IntWritable> values,
				OutputCollector<LongWritable, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) sum += values.next().get();
			output.collect(key, new IntWritable(sum));
		}
		
	}
	
	/**IntWritable, IntWritable形式のK,Vペアに対する集計Reducer。<br>
	 * KeyをIntで持つ頻度・回数情報を扱うMapに対して使える。<br>
	 * @author Matsuzawa
	 *
	 */
	public static class IntIntReduce extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		
		@Override
		public void reduce(IntWritable key, Iterator<IntWritable> values,
				OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) sum += values.next().get();
			output.collect(key, new IntWritable(sum));
		}
		
	}
}

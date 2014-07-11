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
			if (value.getLength()>1) output.collect(tweetNum, one); //ValueはRawJSON．中身があることを一応確認してカウントアップする。
			if (key.get()==0) output.collect(errorNum, one); //UserIDが0ならカウントアップ。
		}
	}
	
	/**Text, IntWritable形式のK,Vペアに対する単純な集計Reducer。<br>
	 * Map, UserMap, UserCountMap全てに対して使える。
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
	
	/**ユーザごとにツイート数を数えるためのMapper。単にその行（Val）のツイートJSONを読み、ユーザidをkeyとして読み込み、valueを+1する。<br>
	 * Note:Interface Mapperの引数k1,v1,k2,v2のうちk1,v1の型と内容はInputFormatの分割形式に依存する。<br>
	 * 例えばTextInputFormatなら行単位(k1はLongWritableのポインタ、v1は現在行のText)であり、Mapperは1行ごとに呼ばれる。<br>
	 * これを前提としてMapperの作業内容を構築する。<br>
	 * SeqFileInputFormatならK=Vペア単位でMapperが呼ばれ、k1は
	 * 返り値に当たるのはk2,v2で、これの形式はMapper内で指定し、これをOutputCollectorに渡す。<br>
	 * @author Matsuzawa
	 *
	 */
	public static class UserMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		private Text userid = new Text();
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			if (key.get()!=0) {
				userid.set(key.toString());
			} else {
				userid.set("unparsable userID");
			}
			output.collect(userid, one);
		}
		
	}
	
	/**ユーザごとにツイート数を数えるためのReducer。同一Keyに対するValueを集計する。
	 * @author Matsuzawa
	 *
	 */
	public static class UserReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while(values.hasNext()) sum += values.next().get();
			output.collect(key, new IntWritable(sum));
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
	
	/**ユーザ自体を数えるReducer。だけど中身同じだからTextIntReduceで代用するのを試す。
	 * @author Matsuzawa
	 *
	 */
	public static class UserCountReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while(values.hasNext()) sum += values.next().get();
			output.collect(key, new IntWritable(sum));
		}
	}
	
	/**秒単位で、時刻あたりのツイート数をカウントするMapper。<br>
	 * 例：XXXX/YY/ZZ-AA:BB:CCにV個のツイート。<br>
	 * SeqFileをInputとする。
	 * @author Matsuzawa
	 *
	 */
	public static class TimeStampMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		private Text dateText = new Text();
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			try {
				Status tweet = TwitterObjectFactory.createStatus(value.toString());
//				long timeStampInSecond = tweet.getCreatedAt().toString();
//				Date dateInSecond = new Date(timeStampInSecond*1000);
				dateText.set(tweet.getCreatedAt().toString());
				output.collect(dateText, one);
			} catch (TwitterException e) {
				//do nothing
			} catch (Exception e) {
				//do nothing
			}
		}
		
	}
}

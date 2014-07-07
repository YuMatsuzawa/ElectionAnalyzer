package matz.election.analyzer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**選挙データ解析の端緒としてとりあえずMapReduceプログラミングを練習。<br>
 * 集めたデータのツイート数を数え上げる。ただし1行1ツイートになっているはずなので単に数えるだけならすぐ終わる。<br>
 * そこで色々とメソッドを実装して、ツイートstatusに含まれる色々なキーに対してカウントを行えるようにしてみる。
 * @author Matsuzawa
 *
 */
public class TweetCount {
	
	/**最も単純に総ツイート数を数えるためのMapper。1行読むごとに加算する。従って最終的なK-Vペアは一組だけになる。
	 * @author Matsuzawa
	 *
	 */
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		private Text tweetNum = new Text("tweetNum");
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			if (value.getLength()>1) output.collect(tweetNum, one); //単に1行読むごとに+1する。データ収集時のミスで改行が余計に入っているので、改行のみの行をスキップする。
		}
	}
	
	/**最も単純に総ツイート数を数えるためのReducer。<br>
	 * Reducerには同一のKeyに対応する全てのValueが収められたIteratorであるValuesが渡される。これを「集計」するのがReducerの作業。<br>
	 * 例えば上のMapでは単に"tweetNum"というKeyにひたすら"1"というValueが当てはめられたペアが大量に生成される。<br>
	 * これを集計するということは、要は全ての"1"を合計して"tweetNum"Keyに対するValueとして当て直すことになる。
	 * @author Matsuzawa
	 *
	 */
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) sum += values.next().get();
			output.collect(key, new IntWritable(sum));
		}
	}
	
	/**ユーザごとにツイート数を数えるためのMapper。単にその行のツイートJSONを読み、ユーザidをkeyとして読み込み、valueを+1する。<br>
	 * Note:Interface Mapperの引数k1,v1,k2,v2のうちk1,v1の型と内容はInputFormatの分割形式に依存する。<br>
	 * 例えばTextInputFormatなら行単位(k1はLongWritableのポインタ、v1は現在行のText)であり、Mapperは1行ごとに呼ばれる。<br>
	 * これを前提としてMapperの作業内容を構築する。<br>
	 * 返り値に当たるのはk2,v2で、これの形式はMapper内で指定し、これをOutputCollectorに渡す。<br>
	 * @author Matsuzawa
	 *
	 */
	public static class UserMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text user = new Text();
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO JSON parse
			
		}
		
	}
	
	public static class UserReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO 自動生成されたメソッド・スタブ
			
		}
		
	}

	
	public String usage() {
		String usage = TweetCount.class.getSimpleName();
		usage += "\t" + "Counts various values of keys in tweet status JSON.";
		return usage;
	}
}

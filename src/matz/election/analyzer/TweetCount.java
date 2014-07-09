package matz.election.analyzer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**選挙データ解析の端緒としてとりあえずMapReduceプログラミングを練習。<br>
 * →入力データはSeqFileに変換し直したので、[UserID]=[RawJSON]というk=vペアになっている。UserIDが読み取れなかったTweetはKeyに0が入っている。<br>
 * SequenceFileInputFormatは1つのK=VペアごとにMapperを呼び出すことに注意。TextInputFormatの場合は行ごと。
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
	
	/**最も単純に総ツイート数を数える(+エラー数を数える)ためのReducer。<br>
	 * @author Matsuzawa
	 *
	 */
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

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

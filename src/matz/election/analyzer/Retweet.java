/**
 * 
 */
package matz.election.analyzer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

/**Collecting/Clustering of retweet
 * @author Yu
 *
 */
public class Retweet {
	
	/**公式RTを収集するMap．出力はRT<s>Status</s>文面をKey、RTしたユーザをVal。文面はBase64エンコする。
	 * @author Yu
	 *
	 */
	public static class RetweetMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			try {
				Status tweet = TwitterObjectFactory.createStatus(value.toString());
				if (tweet.isRetweet()) {
					//retweet
					Status retweet = tweet.getRetweetedStatus();
//					String text = retweet.getText().replaceAll("[\t|\n]", "_");
					String text = retweet.getText();
					byte[] textByte = text.getBytes();
					byte[] encodedByte = Base64.encodeBase64(textByte);
					String encoded = new String(encodedByte);
					output.collect(new Text(encoded), new LongWritable(tweet.getUser().getId()));
				} else if (tweet.getRetweetCount() > 0) {
					//original
					String text = tweet.getText();
					byte[] textByte = text.getBytes();
					byte[] encodedByte = Base64.encodeBase64(textByte);
					String encoded = new String(encodedByte);
					output.collect(new Text(encoded), new LongWritable(tweet.getUser().getId()));
				}
			} catch (TwitterException e) {
				//do nothing
				e.printStackTrace();
			}
			
		}
	}
	
	/**Mapされたペアから、文面のKey、ユーザリストのcsvをつくるReducer。
	 * @author YuMatsuzawa
	 *
	 */
	public static class RetweetReduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, Text>, JobConfigurable {
		private static int threshold = 10;

		public void configure(JobConf job) {
			String extraArg = job.get("arg3");
			if (extraArg != null) {
				try {
					threshold = Integer.parseInt(extraArg);
				} catch (NumberFormatException e) {
					//do nothing. default value will be kept.
				}
			}
		};
		
		@Override
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Set<String> users = new HashSet<String>();
			while(values.hasNext()) users.add(values.next().toString());
			if (users.size() >= threshold) {
				String csv = "";
				for (String user : users) {
					if (!csv.isEmpty()) csv += ",";
					csv += user;
				}
				output.collect(key, new Text(csv));
			}
		}
	}
	
	/**RT-usrの全ての組合せペアを作り出すMap
	 * @author YuMatsuzawa
	 *
	 */
	public static class RTCrossJoinMap extends MapReduceBase implements Mapper<Text, Text, IntWritable, Text> {
		private static IntWritable one = new IntWritable(1);
		@Override
		public void map(Text key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			String concat = key.toString() + "," + value.toString();
			output.collect(one, new Text(concat));
		}	
	}
	
	/**既存のCrossJoinアルゴと同じだが、入力時点でCSVに加工済みなので多分こっちのほうが速い。
	 * @author YuMatsuzawa
	 *
	 */
	public static class RTCrossJoinReduce extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text> {

		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			ArrayList<String> total = new ArrayList<String>();
			while(values.hasNext()) total.add(values.next().toString());
			
			int i = 0;
			for (String pair : total) {
				int j = 0, cur = i;
				for (String innerPair : total) {
					if (j > cur) {
						output.collect(new Text(pair), new Text(innerPair));
					}
					j++;
				}
				i++;
			}
		}
		
	}
	
	/**ユーザの意見リスト（UOリスト）を参照データとするネットワークデータとの照合作業、その前段階として、
	 * テストデータとなるUFリスト（RT行動を行っているユーザと、そのRT頻度のリスト）を生成するMap。<br>
	 * このマップは、RetweetMap/Reduceで抽出した、(エンコ済みRT文面)=(RTしたユーザリスト)というデータを入力とする。Seqファイルになっているはず。<br>
	 * ユーザリストをまずカンマスプリットし、各ユーザIDについてIntの1をマップする。Reducerはこれを集計するので、出力は(ユーザID)=(RTしたツイートの数)となる。<br>
	 * UFリストやUOリストは最終的にDistributedCacheとして配布され、参照データとされる。これを考えると、出力はTextファイルで、単一Reducerが望ましい。<br>
	 * 出力Textは従ってTSVファイルとなる。これをDistributedCacheの利用法(Hacksの83P移行参照)に基づいて、別タスクから参照する。
	 * @author YuMatsuzawa
	 *
	 */
	public static class RTFreqMap extends MapReduceBase implements Mapper<Text, Text, LongWritable, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		private LongWritable userID = new LongWritable();
		
		@Override
		public void map(Text key, Text value,
				OutputCollector<LongWritable, IntWritable> output, Reporter reporter)
				throws IOException {
			String[] users = value.toString().split(",");
			for (String user : users) {
				try {
					userID.set(Long.valueOf(user));
					output.collect(userID, one);
				} catch(NumberFormatException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**詳細はRTFreqMap参照。単なる集計なので、<s>Combinerとして使用可能である。</s>閾値を設けてリストを縮小することにしたので、Combinerとしては使用できない。
	 * @author YuMatsuzawa
	 *
	 */
	public static class RTFreqReduce extends MapReduceBase implements Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
		private static final int threshold = 100;

		@Override
		public void reduce(LongWritable key, Iterator<IntWritable> values,
				OutputCollector<LongWritable, IntWritable> output, Reporter reporter)
				throws IOException {
			int count = 0;
			while(values.hasNext()) {
				count += values.next().get(); // if value=1 then 1, otherwise (means combined beforehand) discrete number more than 1.
			}
			if (count >= threshold) {
				output.collect(key, new IntWritable(count));
			}
		}
		
	}
	
	/**RetweetMapで作った、Base64RTKeyとユーザリストのValのSeqファイルを読んで、そこからユーザごとのBase64RTリストをつくる。<br>
	 * MapperではユーザをKey，Base64RTをValとして放射する。Reducer/CombinerではValをCSVとして結合する。
	 * @author YuMatsuzawa
	 *
	 */
	public static class UserRTListMap extends MapReduceBase implements Mapper<Text, Text, LongWritable, Text> {
		private LongWritable userid = new LongWritable();
		
		@Override
		public void map(Text key, Text value,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			String[] splits = value.toString().split(",");
			for (String split : splits) {
				Long userLong = Long.valueOf(split);
				userid.set(userLong);
				output.collect(userid, key);
			}
		}
	}
	
	/**MapperOutputはUserID(LongWritable)-Base64RT(Text)である。ReducerではBase64RTをCSVに結合する。Seqに出力すること。Combinerに使える。
	 * @author YuMatsuzawa
	 *
	 */
	public static class UserRTListReduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {

		@Override
		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			String redStr = values.next().toString(); // 1st one.
			while (values.hasNext()) {
				redStr += "," + values.next().toString();
			}
			output.collect(key, new Text(redStr));
		}
		
	}
	
	/**UOリストを作る。UserRTListで作った、ユーザごとのRTリストを基にする。<br>
	 * Cacheに入れたクラスタ分け済みRTのリストを使う。<br>
	 * ReducerはIdentityでよい。Text出力、SingleReduce.<br>
	 * 【備考】UserRTListの出力ファイルには，約10万ユーザ（3000人強x30ファイル）分のリストが含まれていた．(/user/matsuzawa/user-rt/)<br>
	 * 一方，このMapRによって生成されたUOリストには5万3千ユーザの2値化された意見が登録されている．(/user/matsuzawa/uolist/uo_100.tsv)<br>
	 * 即ち，このMapperのフィルタ処理によって，ヴォーカルユーザと見なされていたユーザの約半数が意見判定不可能（⇒見かけ上サイレント）とされたことになる．
	 * @author YuMatsuzawa
	 *
	 */
	public static class RTOpinionMap extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, IntWritable> {
		private static final String linkname = AnalyzerMain.DIST_LINKNAME;
		private static HashMap<String, Integer> RTOPList = new HashMap<String, Integer>();

		private static final IntWritable op0 = new IntWritable(0);
		private static final IntWritable op1 = new IntWritable(1);
		
		public void configure(JobConf job) {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new InputStreamReader(new FileInputStream(linkname)));
				String line = "";
				while((line=br.readLine())!=null) {
					String[] splits = line.split("\t");
					String rt = splits[0];
					Integer op = Integer.parseInt(splits[1]);
					System.out.println(rt);
					RTOPList.put(rt, op);
				}
			} catch(Exception e) {
				e.printStackTrace();
			} finally {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, IntWritable> output,
				Reporter reporter) throws IOException {
			String[] RTs = value.toString().split(",");
			int count0 = 0, count1 = 0;
			for (String RT : RTs) { // base64encoded.
				byte[] byteRT = Base64.decodeBase64(RT);
				String decodedRT = new String(byteRT);
				Integer OP = RTOPList.get(decodedRT); 
				if (OP != null && OP == 0) count0++;
				else if (OP != null && OP == 1) count1++;
			}
			
			if (count0 > 0 || count1 > 0) { //RT-opリストにマップ済みの意見をいずれか1つ以上RTしているヴォーカルユーザを，意見の2値化が可能なヴォーカルユーザとみなしている．
				if (count0 > count1) output.collect(key, op0);
				if (count0 < count1) output.collect(key, op1);
			}
		}
		
	}
	
	public static class RTOpinionReduce extends IdentityReducer<LongWritable, IntWritable> {};
}

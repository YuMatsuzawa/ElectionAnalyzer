/**
 * 
 */
package matz.election.analyzer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import twitter4j.TwitterObjectFactory;
import twitter4j.User;

/**ツイートに含まれるURLの群とその言及ユーザの群からなる2部グラフから、URLのクラスタリングを行うクラス。<br>
 * その他のネットワーク関連解析も取り扱う。
 * @author YuMatsuzawa
 *
 */
public class GraphAnalysis {

	/**PairedURL等を使ってJoinした、URL1,(comma-separated userid list)\tURL2,(comma-separated userid list)形式のText入力に対し、<br>
	 * URL1とURL2との間のJaccard係数を求めるMap.出力はカンマ区切りのエッジ（URL1,URL2）をKey、Jaccard係数をValueとする。<br>
	 * デフォルトではJaccard係数閾値を0とする。即ち全てのエッジを出力する。<br>
	 * JobConfigurableを実装するので、閾値を設けることもできる。閾値を設けた場合、Jaccard係数が閾値以上のURLペアのみをエッジとして出力する。
	 * @author YuMatsuzawa
	 *
	 */
	public static class JaccardLinkMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable>, JobConfigurable {
		private double threshold = 0.0;

		public void configure(JobConf job) {
			String extraArg = job.get("arg3");
			if (extraArg != null) {
				try {
					threshold = Double.parseDouble(extraArg);
				} catch (NumberFormatException e) {
					//do nothing. default value will be kept.
				}
			}
		}
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
						throws IOException {
			String[] inputs = value.toString().split("\\s");
			String[] urlUsers1 = inputs[0].split(",");
			String[] urlUsers2 = inputs[1].split(",");
			String user1 = urlUsers1[0], user2 = urlUsers2[0];
			HashSet<Long> intersection = new HashSet<Long>(), union = new HashSet<Long>(), userSet2 = new HashSet<Long>();
			for (int i=1; i<urlUsers1.length; i++) {
				intersection.add(Long.parseLong(urlUsers1[i]));
				union.add(Long.parseLong(urlUsers1[i]));
			}
			for (int j=1; j<urlUsers2.length; j++) userSet2.add(Long.parseLong(urlUsers2[j]));

			intersection.retainAll(userSet2);
			union.addAll(userSet2);

			double jaccard = (double) intersection.size() / (double) union.size();
			if (jaccard >= threshold) {
				output.collect(new Text(user1+","+user2), new DoubleWritable(jaccard));
			}
		}		
	}
	
	/**Base64デコードしてから結果を吐き出すJaccardLinkMap。
	 * @author YuMatsuzawa
	 *
	 */
	public static class JaccardLinkDecodeMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable>, JobConfigurable {
		private double threshold = 0.0;

		public void configure(JobConf job) {
			String extraArg = job.get("arg3");
			if (extraArg != null) {
				try {
					threshold = Double.parseDouble(extraArg);
				} catch (NumberFormatException e) {
					//do nothing. default value will be kept.
				}
			}
		}
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			try {
				String[] inputs = value.toString().split("\\s");
				String[] keyUsers1 = inputs[0].split(",");
				String[] keyUsers2 = inputs[1].split(",");
				String key1 = keyUsers1[0], key2 = keyUsers2[0];
				HashSet<Long> intersection = new HashSet<Long>(), union = new HashSet<Long>(), userSet2 = new HashSet<Long>();
				for (int i=1; i<keyUsers1.length; i++) {
					intersection.add(Long.parseLong(keyUsers1[i]));
					union.add(Long.parseLong(keyUsers1[i]));
				}
				for (int j=1; j<keyUsers2.length; j++) userSet2.add(Long.parseLong(keyUsers2[j]));
				
				intersection.retainAll(userSet2);
				union.addAll(userSet2);
				
				double jaccard = (double) intersection.size() / (double) union.size();
				if (jaccard >= threshold) {
					byte[] key1Byte = key1.getBytes(), key2Byte = key2.getBytes();
					byte[] decoded1Byte = Base64.decodeBase64(key1Byte), decoded2Byte = Base64.decodeBase64(key2Byte);
					String decoded1 = new String(decoded1Byte), decoded2 = new String(decoded2Byte);
					decoded1 = decoded1.replaceAll("\\s+", " ").replaceAll(",", "，");
					decoded2 = decoded2.replaceAll("\\s+", " ").replaceAll(",", "，");
					output.collect(new Text(decoded1+","+decoded2), new DoubleWritable(jaccard));
				}
			} catch (Exception e) {
				System.err.println(value.toString());
				e.printStackTrace();
			}
		}		
	}
	
	/**JaccardLinkMapが出力した閾値以上のJaccard係数を持つURLペアを集計する。特に計算はしないので無内容でいい。
	 * @author YuMatsuzawa
	 *
	 */
	public static class JaccardLinkReduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		public void reduce(Text key, Iterator<DoubleWritable> values,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			while(values.hasNext()) output.collect(key, values.next());
		}
		
	}
	
	/**フォロワーネットワークデータ(全世界)から、日本語ユーザ、及び政治関連ツイートをリツイートしたユーザ(Vocalユーザ)を抽出する。<br>
	 * 入力データはSeqファイルで、Keyにユーザプロファイル、Valueにネットワーク情報のCSVが入っている。<br>
	 * プロフィール内のlangを見てjaであれば無条件で通す。<br>
	 * あるいは、DistributedCache内のUO/UFリストに含まれているVocalユーザであっても通す。<br>
	 * そして、CSV内のユーザID全て(そのユーザ自身のIDも含めて)に、頻度値をスペース区切りで併記する。これを最初にやることで、後にDistributedCacheを参照する必要がなくなる。<br>
	 * このマージ処理が重いので、時間は結構掛かるはず。UO/UFリストはマップとして取り込んでおけば多分多少速い。ユーザIDのKeyに対応するValue(頻度値)をすぐ引けるからである。<br>
	 * @author YuMatsuzawa
	 *
	 */
	public static class FilterNetworkMap extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		private static final String linkname = AnalyzerMain.DIST_LINKNAME;
		private static final String langja = "ja";
		private static final int followLimit = 2000;
		private static HashMap<Long, Integer> uxlist = new HashMap<Long, Integer>();
		private Text csv = new Text();
		
		/**setupメソッドはMapperがインスタンス化された時に呼ばれる。ここでHashMapにuflistを取り込む。
		 * @param context
		 */
		@SuppressWarnings("rawtypes")
		public void setup(Context context) {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new InputStreamReader(new FileInputStream(linkname)));
				String line = "";
				while((line=br.readLine())!=null) {
					String[] splits = line.split("\t");
					Long userid = Long.parseLong(splits[0]);
					Integer freq = Integer.parseInt(splits[1]);
					uxlist.put(userid, freq);
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
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			try {
				User user = TwitterObjectFactory.createUser(key.toString());
				if ( user.getLang().equalsIgnoreCase(langja) || uxlist.containsKey(user.getId()) ) {
					String[] splits = value.toString().split(",");
					ArrayList<Long> splitLong = new ArrayList<Long>();
					for (String split : splits) {
						try {
							splitLong.add(Long.parseLong(split));
						} catch(Exception e) {
							e.printStackTrace();
						}
					}
					Long userid = splitLong.get(0), numFollowed = splitLong.get(1), numFollowing = splitLong.get(2);
					if (numFollowing > followLimit) return;
					
					String newCsv = "";
					newCsv += userid + " " + getFreqOf(userid) + "," + numFollowed + "," + numFollowing;
					
					
					int count = 3;
					while(count < splitLong.size()) {
						newCsv += "," + getFreqOf(splitLong.get(count));
						count++;
					}
					
					csv.set(newCsv);
					output.collect(key, csv);
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		private Integer getFreqOf(Long userid) {
			Integer freq = (uxlist.get(userid)!=null)? uxlist.get(userid) : 0;
			return freq;
		}
		
	}
	
	/**Mapperないで処理が完了するので、IdentityReducerで良い。
	 * @author YuMatsuzawa
	 *
	 */
	public static class FilterNetworkReduce extends IdentityReducer<Text, Text> {};
}

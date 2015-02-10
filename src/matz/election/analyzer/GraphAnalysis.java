/**
 * 
 */
package matz.election.analyzer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import twitter4j.TwitterObjectFactory;
import twitter4j.User;

/**ツイートに含まれるURLの群とその言及ユーザの群からなる2部グラフから、URLのクラスタリングを行うクラス。<br>
 * その他のネットワーク関連解析も取り扱う。
 * @author YuMatsuzawa
 *
 */
/**
 * @author Yu
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
	
	/**FilterNetworkよりもシンプルな条件指定の元で、フォロワーネットワークをフィルタリングするMapR。<br>
	 * UserProfileを読み込んで、その中の値で絞り込む。実験的なものなので、引数ではなくメソッド内で条件を指定する。変更の際は再ビルドする。
	 * @author Yu
	 *
	 */
	public static class SimpleFilterNetworkMap extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		private static final String langja = "ja";
		private static final int followLimit = 2000;

		@Override
		public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			try {
				User user = TwitterObjectFactory.createUser(key.toString());
				if (user.getLang().equalsIgnoreCase(langja) && user.getFriendsCount() < followLimit) { // filter condition;
					output.collect(key, value);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**Mapperでフィルタリングを行っているので、Identityでよい。
	 * @author Yu
	 *
	 */
	public static class SimpleFilterNetworkReduce extends IdentityReducer<Text, Text> {};
	
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
		
		/**configureメソッドはMapperがインスタンス化された時に呼ばれる。ここでHashMapにuflistを取り込む。
		 * @param job
		 */
		public void configure(JobConf job) {
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
		
		/* (非 Javadoc)ループを含む条件判定を1つだけ含むようにしたマッパ。
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
		 */
		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String keyStr = key.toString();
			//System.out.println(valStr.substring(0, (valStr.length() > 50)? 50 : valStr.length()));
			try {
				User user = TwitterObjectFactory.createUser(keyStr);
				if ( user.getFriendsCount() < followLimit && uxlist.containsKey(user.getId()) ) {
					output.collect(key, value);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		/**このMapメソッドの中ではかなり多くのループが回る。<br>
		 * まず最初のif文の中にあるcontainsKeyは実質HashMapの中から合致するKeyを探してくるのでループ。<br>
		 * Valueをカンマスプリットするために、String文字列を最初から最後まで走査するループ。<br>
		 * Listを用意して、カンマスプリットしたアレイから移し替えつつ、Longパースするループ。<br>
		 * その後出来上がったCSVを走査し、存在するユーザID一つ一つについて、UFリストを走査する二重ループ。<br>
		 * 恐らく、このように処理を単一マッパの中に詰め込んでしまうためにヒープ領域エラーが発生している。新マッパは処理内容を大幅に減らし、複数のMapRを多段階に組み合わせることでエラーを防ぐ。
		 * @param key
		 * @param value
		 * @param output
		 * @param reporter
		 * @throws IOException
		 */
		public void _map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			try {
				User user = TwitterObjectFactory.createUser(key.toString());
				if ( user.getFriendsCount() < followLimit && 
						(user.getLang().equalsIgnoreCase(langja) || uxlist.containsKey(user.getId())) ) {
					
					String[] splits = value.toString().split(",");			// culprit for timeout/OOM
					ArrayList<Long> splitLong = new ArrayList<Long>();		// another culprit
					for (String split : splits) {
						try {
							splitLong.add(Long.parseLong(split));
						} catch(Exception e) {
							e.printStackTrace();
						}
					}
					
					Long userid = splitLong.get(0), numFollowed = splitLong.get(1), numFollowing = splitLong.get(2);
					if (numFollowing > followLimit) return; // remove excessive following user
					
					String newCsv = "";
					newCsv += userid + " " + getFreqOf(userid) + "," + numFollowed + "," + numFollowing;
					
					if (numFollowing > 0) {
						int count = 3;
						while(count < splitLong.size()) {
							Long tarUser = splitLong.get(count);
							newCsv += "," + tarUser;
							if(count > 2 + numFollowed) newCsv += ";" + getFreqOf(tarUser);
							count++;
						}
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
	
	public static class SimplifyAllMap extends MapReduceBase implements Mapper<Text, Text, LongWritable, Text> {
		private static final int followLimit = 2000;
		private LongWritable userid = new LongWritable();
		private Text textCsv = new Text();
		
		@Override
		public void map(Text key, Text value,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			try {
				User user = TwitterObjectFactory.createUser(key.toString());
				if (user.getFriendsCount() < followLimit ) {
					String csv = value.toString(), newCsv = "";
					String[] splitCsv = csv.split(",");
					String numFollowed = splitCsv[1], numFollowing = splitCsv[2];
					int index = 3;
					if (!numFollowed.equals("-1") && !numFollowing.equals("-1")) {
						int intFollowed = Integer.valueOf(numFollowed), intFollowing = Integer.valueOf(numFollowing);
						index += intFollowed;
						for (int i = index; i < index + intFollowing; i++) {
							if (!newCsv.isEmpty()) newCsv += ",";
							newCsv += splitCsv[i];
						}
						textCsv.set(newCsv);
						userid.set(user.getId());
						output.collect(userid, textCsv);
					}
				}
			} catch (Exception e) {};

		}
	}
	
	public static class SimplifyAllReduce extends IdentityReducer<LongWritable, Text> {};

	/**フィルタリングしたネットワークデータのValueに入っているCSVのうち、Friends（Following）リストだけを残して他をドロップするマップ。<br>
	 * FollowingListはCSV末尾に当たる。
	 * @author YuMatsuzawa
	 *
	 */
	public static class ReduceCSVMap extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		private Text textCsv = new Text();
		
		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String csv = value.toString(), newCsv = "";
			String[] splitCsv = csv.split(",");
			String numFollowed = splitCsv[1], numFollowing = splitCsv[2];
			int index = 3;
			if (!numFollowed.equals("-1") && !numFollowing.equals("-1")) {
				int intFollowed = Integer.valueOf(numFollowed), intFollowing = Integer.valueOf(numFollowing);
				index += intFollowed;
				for (int i = index; i < index + intFollowing; i++) {
					if (!newCsv.isEmpty()) newCsv += ",";
					newCsv += splitCsv[i];
				}
				textCsv.set(newCsv);
				output.collect(key, textCsv);
			}
			
		}
	}
	
	public static class ReduceCSVReduce extends IdentityReducer<Text, Text> {};
	
	public static class SimplifyNetworkMap extends MapReduceBase implements Mapper<Text, Text, LongWritable, Text> {
		private static final int followLimit = 2000;
		private LongWritable userid = new LongWritable();

		@Override
		public void map(Text key, Text value,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			try {
				User user = TwitterObjectFactory.createUser(key.toString());
				if ( user.getFriendsCount() < followLimit ) {
					userid.set(user.getId());
					output.collect(userid, value);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class SimplifyNetworkReduce extends IdentityReducer<LongWritable, Text> {};
	
	public static class DistCacheTestMap extends MapReduceBase implements Mapper<Text, Text, LongWritable, IntWritable> {
		private static final String linkname = AnalyzerMain.DIST_LINKNAME;
		private static HashMap<Long, Integer> uxlist = new HashMap<Long, Integer>();
				
		/**configureメソッドはMapperがインスタンス化された時に呼ばれる。ここでHashMapにuflistを取り込む。
		 * @param job
		 */		
		public void configure(JobConf job) {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new InputStreamReader(new FileInputStream(linkname)));
				String line = "";
				while((line=br.readLine())!=null) {
					String[] splits = line.split("\t");
					Long userid = Long.parseLong(splits[0]);
					Integer freq = Integer.parseInt(splits[1]);
					System.out.println(userid);
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
				OutputCollector<LongWritable, IntWritable> output,
				Reporter reporter) throws IOException {
			for (Entry<Long, Integer> entry : uxlist.entrySet()) {
				output.collect(new LongWritable(entry.getKey()), new IntWritable(entry.getValue()));
				break;
			}
		}
	}
	
	public static class DistCacheTestReduce extends IdentityReducer<LongWritable, IntWritable> {};
	
	/**UFリストを参照データとし、Vocalユーザ群について、各ユーザのRT数と、そのユーザの周囲のVocalユーザ率をマップする。
	 * @author YuMatsuzawa
	 *
	 */
	public static class VocalFriendsAttitudeMap extends MapReduceBase implements Mapper<Text, Text, IntWritable, DoubleWritable> {
		private static final String linkname = AnalyzerMain.DIST_LINKNAME;
		private static HashMap<Long, Integer> uxlist = new HashMap<Long, Integer>();
		
		private IntWritable numRT = new IntWritable();
		private DoubleWritable rate = new DoubleWritable();
		
		/**configureメソッドはMapperがインスタンス化された時に呼ばれる。ここでHashMapにuflistを取り込む。
		 * @param job
		 */
		public void configure(JobConf job) {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new InputStreamReader(new FileInputStream(linkname)));
				String line = "";
				while((line=br.readLine())!=null) {
					String[] splits = line.split("\t");
					Long userid = Long.parseLong(splits[0]);
					Integer freq = Integer.parseInt(splits[1]);
					System.out.println(userid);
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
				OutputCollector<IntWritable, DoubleWritable> output,
				Reporter reporter) throws IOException {
			try {
				User user = TwitterObjectFactory.createUser(key.toString());
				Long userId = user.getId();
				if ( uxlist.containsKey(userId) ) { // which means the user is Vocal
					numRT.set(getFreqOf(userId));
					
					String[] followingList = value.toString().split(",");
					int numFollowing = followingList.length;
					double vocalRate = 0.0;
					for (String followingId : followingList) {
						try {
							Long followingIdByLong = Long.valueOf(followingId);
							if (uxlist.containsKey(followingIdByLong)) {  // which means this followee is Vocal
								vocalRate += 1.0;
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					vocalRate /= (double) numFollowing;
					rate.set(vocalRate);
					output.collect(numRT, rate);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		private Integer getFreqOf(Long userid) {
			Integer freq = (uxlist.get(userid)!=null)? uxlist.get(userid) : 0;
			return freq;
		}
		
	}
	
	public static class VocalFriendsAttitudeReduce extends IdentityReducer<IntWritable, DoubleWritable> {};

	public static class VocalFriendsAttitudeAverageReduce extends MapReduceBase implements Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		private DoubleWritable avgRatio = new DoubleWritable(0.0);
		@Override
		public void reduce(IntWritable key, Iterator<DoubleWritable> values,
				OutputCollector<IntWritable, DoubleWritable> output,
				Reporter reporter) throws IOException {
			double denom = 0.0, numer = 0.0;
			while (values.hasNext()) {
				denom += 1.0;
				numer += values.next().get();
			}
			avgRatio.set(numer/denom);
			output.collect(key, avgRatio);
		}
		
	}
	
	/**UFリストを参照データとし、Vocalユーザ群について、各ユーザのRT数と、そのユーザの周囲の平均RT数をマップする。
	 * @author YuMatsuzawa
	 *
	 */
	public static class VocalFriendsAverageMap extends MapReduceBase implements Mapper<Text, Text, IntWritable, Text> {
		private static final String linkname = AnalyzerMain.DIST_LINKNAME;
		private static HashMap<Long, Integer> uxlist = new HashMap<Long, Integer>();
		
		private IntWritable numRT = new IntWritable();
		private Text rates = new Text();
		
		/**configureメソッドはMapperがインスタンス化された時に呼ばれる。ここでHashMapにuflistを取り込む。
		 * @param job
		 */
		public void configure(JobConf job) {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new InputStreamReader(new FileInputStream(linkname)));
				String line = "";
				while((line=br.readLine())!=null) {
					String[] splits = line.split("\t");
					Long userid = Long.parseLong(splits[0]);
					Integer freq = Integer.parseInt(splits[1]);
					System.out.println(userid);
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
				OutputCollector<IntWritable, Text> output,
				Reporter reporter) throws IOException {
			try {
				User user = TwitterObjectFactory.createUser(key.toString());
				Long userId = user.getId();
				if ( uxlist.containsKey(userId) ) { // which means the user is Vocal
					numRT.set(getFreqOf(userId));
					
					String[] followingList = value.toString().split(",");
					int numFollowing = followingList.length;
					int numVocal = 0;
					double avgRT = 0.0;
					for (String followingId : followingList) {
						try {
							Long followingIdByLong = Long.valueOf(followingId);
							if (uxlist.containsKey(followingIdByLong)) {  // which means this followee is Vocal
								numVocal++;
								avgRT += (double) getFreqOf(followingIdByLong);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					double vocalAvgRT = 0.0, totalAvgRT = 0.0;
					if (numVocal > 0) {
						vocalAvgRT = avgRT / (double) numVocal;
					}
					totalAvgRT = avgRT / (double) numFollowing;
					rates.set(vocalAvgRT + "," + totalAvgRT);
					output.collect(numRT, rates);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		private Integer getFreqOf(Long userid) {
			Integer freq = (uxlist.get(userid)!=null)? uxlist.get(userid) : 0;
			return freq;
		}
		
	}
	
	public static class VocalFriendsAverageReduce extends IdentityReducer<IntWritable, Text> {};
	
	/**UOリストを読んで、各ユーザの意見と、周囲のヴォーカルユーザの意見の平均値を出力する。
	 * @author YuMatsuzawa
	 *
	 */
	public static class VocalFriendsOpinionMap extends MapReduceBase implements Mapper<Text, Text, IntWritable, DoubleWritable> {
		private static final String linkname = AnalyzerMain.DIST_LINKNAME;
		private static HashMap<Long, Integer> uxlist = new HashMap<Long, Integer>();
		
		private IntWritable op = new IntWritable();
		private DoubleWritable rates = new DoubleWritable();
		
		/**configureメソッドはMapperがインスタンス化された時に呼ばれる。ここでHashMapにuflistを取り込む。
		 * @param job
		 */
		public void configure(JobConf job) {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new InputStreamReader(new FileInputStream(linkname)));
				String line = "";
				while((line=br.readLine())!=null) {
					String[] splits = line.split("\t");
					Long userid = Long.parseLong(splits[0]);
					Integer freq = Integer.parseInt(splits[1]);
					System.out.println(userid);
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
				OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
				throws IOException {
			try {
				User user = TwitterObjectFactory.createUser(key.toString());
				Long userId = user.getId();
				if ( uxlist.containsKey(userId) ) { // which means the user is Vocal
					op.set(uxlist.get(userId));
					
					String[] followingList = value.toString().split(",");
//					int numFollowing = followingList.length;
					int numVocal = 0;
					double avgOP = 0.0;
					for (String followingId : followingList) {
						try {
							Long followingIdByLong = Long.valueOf(followingId);
							if (uxlist.containsKey(followingIdByLong)) {  // which means this followee is Vocal
								numVocal++;
								avgOP += uxlist.get(followingIdByLong);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					double vocalAvgOP = 0.0;
					if (numVocal > 0) {
						vocalAvgOP = avgOP / (double) numVocal;
					}
					rates.set(vocalAvgOP);
					output.collect(op, rates);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public static class VocalFriendsOpinionReduce extends IdentityReducer<IntWritable, DoubleWritable> {};
	
	/**ユーザの次数（入次数＝#followed）を、意見ごとにカウントするマップ。Silentユーザはop=-1としてこれも計上する。<br>
	 * political_noprofileネットワークを使うほうが多分いい．Text形式で，Profileはなく，KeyにユーザID，ValにCSVが入っている．
	 * @author YuMatsuzawa
	 *
	 */
	public static class VocalDegreeMap extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private static final String linkname = AnalyzerMain.DIST_LINKNAME;
		private static HashMap<Long, Integer> uxlist = new HashMap<Long, Integer>();
		
		private IntWritable op = new IntWritable();
		private IntWritable inDegree = new IntWritable();
		
		/**configureメソッドはMapperがインスタンス化された時に呼ばれる。ここでHashMapにuflistを取り込む。
		 * @param job
		 */
		public void configure(JobConf job) {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new InputStreamReader(new FileInputStream(linkname)));
				String line = "";
				while((line=br.readLine())!=null) {
					String[] splits = line.split("\t");
					Long userid = Long.parseLong(splits[0]);
					Integer freq = Integer.parseInt(splits[1]);
					System.out.println(userid);
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
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {
			try {
//				User user = TwitterObjectFactory.createUser(key.toString());
//				Long userid = user.getId();
				String[] split = value.toString().split("\t");
				Long userid = Long.valueOf(split[0]);
				Integer opInt = uxlist.get(userid);
				if (opInt == null) opInt = -1;
				
				op.set(opInt);
				String[] csvStr = split[1].toString().split(",");
				int numFollowed = Integer.valueOf(csvStr[1]);
				if(numFollowed >= 0) {
					inDegree.set(numFollowed);
					output.collect(op, inDegree);
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	/**結果をFreqに変えておく．Excelの頻度分布は低機能のため．
	 * @author YuMatsuzawa
	 *
	 */
	public static class VocalDegreeReduce extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, Text>{
		private TreeMap<Integer,Integer> freqMap = null;
		
		@Override
		public void reduce(IntWritable key, Iterator<IntWritable> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			freqMap = new TreeMap<Integer,Integer>();
			while (values.hasNext()) {
				Integer degreeKey = values.next().get();
				if (freqMap.containsKey(degreeKey)) {
					int freqCount = freqMap.get(degreeKey);
					freqMap.put(degreeKey, ++freqCount);
				} else {
					freqMap.put(degreeKey, 1);
				}
			}
			for (Entry<Integer,Integer> freqEntry : freqMap.entrySet()) {
				String outVal = freqEntry.getKey().toString() + "\t" + freqEntry.getValue().toString();
				output.collect(key, new Text(outVal));
			}
		}
		
	}
	
	/**ネットワーク全体での次数分布を表示するためのMapper
	 * @author YuMatsuzawa
	 *
	 */
	public static class TotalDegreeMap extends MapReduceBase implements Mapper<LongWritable,Text,IntWritable,IntWritable> {
		private IntWritable op = new IntWritable(1);
		private IntWritable inDegree = new IntWritable();
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {
			try {
				String[] split = value.toString().split("\t");
				String[] csvStr = split[1].toString().split(",");
				
				int numFollowed = Integer.valueOf(csvStr[1]);
				if(numFollowed >= 0) {
					inDegree.set(numFollowed);
					output.collect(op, inDegree);
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	/**ネットワーク全体での次数分布を表示するためのReducer
	 * @author YuMatsuzawa
	 *
	 */
	public static class TotalDegreeReduce extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, Text> {
		private static TreeMap<Integer,Integer> totalFreqMap = new TreeMap<Integer,Integer>();
		
		@Override
		public void reduce(IntWritable key, Iterator<IntWritable> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			while (values.hasNext()) {
				Integer degreeKey = values.next().get();
				if (totalFreqMap.containsKey(degreeKey)) {
					int freqCount = totalFreqMap.get(degreeKey);
					totalFreqMap.put(degreeKey, ++freqCount);
				} else {
					totalFreqMap.put(degreeKey, 1);
				}
			}
			for (Entry<Integer,Integer> freqEntry : totalFreqMap.entrySet()) {
				String outVal = freqEntry.getKey().toString() + "\t" + freqEntry.getValue().toString();
				output.collect(key, new Text(outVal));
			}
		}
		
	}
	
	/**シミュレーションに投入するためのサンプルネットワーク取得の元データを生成するMapR．出力はUserIDをKey，CSVをValueにもつ．ユーザプロファイルは使用しないので消すということになる．<br>
	 * Cacheにフィルタリストを読み込み，そのリストに載っているユーザのみ抽出する．
	 * @author YuMatsuzawa
	 *
	 */
	public static class DropProfileAndFilterMap extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
//		private final static int USERID_INDEX = 0, NUM_FOLLOWED_INDEX = 1, NUM_FOLLOWING_INDEX = 2;
		private static final String linkname = AnalyzerMain.DIST_LINKNAME;
		private static final int followLimit = 2000;
		private static HashMap<Long, Integer> uxlist = new HashMap<Long, Integer>();

		private Text userid = new Text();
		/**configureメソッドはMapperがインスタンス化された時に呼ばれる。ここでHashMapにcacheを取り込む。
		 * @param job
		 */
		public void configure(JobConf job) {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new InputStreamReader(new FileInputStream(linkname)));
				String line = "";
				while((line=br.readLine())!=null) {
					String[] splits = line.split("\t");
					Long userid = Long.parseLong(splits[0]);
					Integer freq = Integer.parseInt(splits[1]);
//					System.out.println(userid);
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
			User user = null;
			try {
				user = TwitterObjectFactory.createUser(key.toString());
				if (user.getFriendsCount() < followLimit && uxlist.containsKey(user.getId())) { //followLimitによるフィルタ
//					String[] csv = value.toString().split(",");
//					if (csv[NUM_FOLLOWED_INDEX].equals("-1") || csv[NUM_FOLLOWING_INDEX].equals("-1")) return; //鍵付きユーザ除外→あとでやる
					
					userid.set(String.valueOf(user.getId()));
					output.collect(userid, value);
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class DropProfileAndFilterReduce extends IdentityReducer<Text, Text> {};
}

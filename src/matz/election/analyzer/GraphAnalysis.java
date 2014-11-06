/**
 * 
 */
package matz.election.analyzer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

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
			String[] inputs = value.toString().split("\t");
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
}

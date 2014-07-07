package matz.election.analyzer;

import java.util.ArrayList;
import java.util.Arrays;

import javax.xml.soap.Text;

import matz.election.analyzer.TweetCount.Map;
import matz.election.analyzer.TweetCount.Reduce;
import matz.election.analyzer.TweetCount.UserMap;
import matz.election.analyzer.TweetCount.UserReduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.*;

/**本パッケージにおいて、解析のためのエントリポイントとなるクラス。<br>
 * HDFS上のデータをMapReduceプログラムで処理すること、及び、<br>
 * jarに固めてサーバ上で走らせることを念頭に置いて開発されたし。<br>
 * 基本的な思想としては、実際の解析内容ごとにクラスを複数実装しておき、<br>
 * 本クラス内で希望する解析のためのクラスをインスタンス化(ジョブ指定)して実行する。<br>
 * main関数内をその都度書き換えてビルドしなおしてもいいが、<br>
 * オプションと引数によってスイッチできるようにしておくのがベター。<br>
 * @author Matsuzawa
 *
 */
public class AnalyzerMain {
	protected final static String DEFAULT_INPUT = "/user/data/twitter2013july";
	protected final static String DEFAULT_OUTPUT = "/user/matsuzawa/electionAnalyzer";
	protected final static String[] COUNT_JOB_ARRAY = {
		"TweetCount",
		"UserTweetCount",
		"HogeHoge"
		};
	protected final static ArrayList<String> COUNT_JOB_LIST = (ArrayList<String>)Arrays.asList(COUNT_JOB_ARRAY);
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		JobConf job = null;
		if (args[0].isEmpty()) {
			System.err.println("Specify jobClass:");
			for(String jobName : COUNT_JOB_LIST) {
				System.err.println(jobName);
			}
			System.exit(1);
		} else {
			if (COUNT_JOB_LIST.contains(args[0])) {
				job = new JobConf(TweetCount.class);
				if (!args[1].isEmpty() && !args[2].isEmpty()) {
					FileInputFormat.setInputPaths(job, args[1]);
					FileOutputFormat.setOutputPath(job, new Path(args[2]));
				} else {
					FileInputFormat.setInputPaths(job, DEFAULT_INPUT);
					FileOutputFormat.setOutputPath(job, new Path(DEFAULT_OUTPUT));
				}
			}
		}
		if (args[0] == "TweetCount") {
			job.setJobName(args[0]);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
					
			job.setInputFormat(TextInputFormat.class);
			job.setOutputFormat(TextOutputFormat.class);
			
			job.setMapperClass(Map.class);
			job.setCombinerClass(Reduce.class);
			job.setReducerClass(Reduce.class);
		}
		else if (args[0] == "UserTweetCount") {
			job.setJobName(args[0]);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
					
			job.setInputFormat(TextInputFormat.class);
			job.setOutputFormat(TextOutputFormat.class);
			
			job.setMapperClass(UserMap.class);
			job.setCombinerClass(UserReduce.class);
			job.setReducerClass(UserReduce.class);
		}
		
	}

}

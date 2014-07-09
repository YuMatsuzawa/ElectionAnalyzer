package matz.election.analyzer;

import java.util.Arrays;
import java.util.List;

import matz.election.analyzer.TweetCount.Map;
import matz.election.analyzer.TweetCount.Reduce;
import matz.election.analyzer.TweetCount.UserMap;
import matz.election.analyzer.TweetCount.UserReduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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

	/* Reduceの数はmaxとニアイコールで与えて確実に即時実行されるようにするか、約2倍で与えておいて、<br>
	 * 初回割り当て分を早く終了する計算能力の高いノードが2回・3回とReduceを処理できるようにしてクラスタを有効利用するか、<br>
	 * という2通りの考え方がある。<br>
	 * また、Reducerの数とOutputの分割数はイコールになるので、出力ディレクトリ内にReducerの数だけファイルが出来る。<br>
	 * hadoop fs -getmerge /user/matsuzawa/<output path> /path/in/local/fs/output.txt<br>
	 * 上記コマンドでマージした集計結果ファイルをローカルファイルシステムに取得できる。基本的にこの操作はMapReduceの枠組み内では行ってくれない。
	 */
	private final static int NUM_NODES = 9;
	private final static int MAX_REDUCE_PER_NODE = 2;
	public final static int IMMEDIATE_REDUCE_NUM = (int)(0.95*NUM_NODES*MAX_REDUCE_PER_NODE);
	public final static int BALANCED_REDUCE_NUM = (int)(1.75*NUM_NODES*MAX_REDUCE_PER_NODE);
	
	/* 残念なことに0.20.2-cdh3u6はbzip2にまだネイティブ対応していなかった。
	 * LZOに圧縮し直すか、SeqFileに変換するなどの善後策を要する。gzipももちろん無理。
	 * →結局Block単位で圧縮したSeqFileに変換し直した。
	 */
	protected final static String DEFAULT_INPUT = "/user/data/twitter2013july";
	protected final static String DEFAULT_OUTPUT = "/user/matsuzawa/electionAnalyzer";
	protected final static String[] COUNT_JOB_ARRAY = {
		"TweetCount",
		"UserTweetCount",
		"HogeHoge"
		};
	protected static List<String> COUNT_JOB_LIST = Arrays.asList(COUNT_JOB_ARRAY);
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		JobConf job = null;
		if (args.length > 0 && COUNT_JOB_LIST.contains(args[0])) {
			job = new JobConf(TweetCount.class);
			if (args.length > 2 && !args[1].isEmpty() && !args[2].isEmpty()) {
				FileInputFormat.setInputPaths(job, args[1]);
				FileOutputFormat.setOutputPath(job, new Path(args[2]+System.currentTimeMillis()));
			} else {
				FileInputFormat.setInputPaths(job, DEFAULT_INPUT);
				FileOutputFormat.setOutputPath(job, new Path(DEFAULT_OUTPUT+System.currentTimeMillis()));
			}
		} else {
			System.err.println("Specify jobClass:");
			for(String jobName : COUNT_JOB_ARRAY) {
				System.err.println(jobName);
			}
			System.exit(1);
		}
		
		
		/* データのエンコードもutf8になっていなかった。HadoopのTextInputFormatはutf8以外無理なので、データを修正した方がいいかもしれない。
		 * →SeqFileに変換し直したので、入力はそれにならう。KeyはUserID(パース不正で読み込めなかった場合は0)、Valには元JSONが入っている。
		 */
//		job.setInputFormat(TextInputFormat.class);
		job.setInputFormat(SequenceFileInputFormat.class);
		
		if (args[0].equals("TweetCount")) {
			job.setJobName(args[0]);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
					
			job.setOutputFormat(TextOutputFormat.class);
			
			job.setMapperClass(Map.class);
			job.setCombinerClass(Reduce.class);
			job.setReducerClass(Reduce.class);
		}
		else if (args[0].equals("UserTweetCount")) {
			job.setJobName(args[0]);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			job.setOutputFormat(TextOutputFormat.class);
			
			job.setMapperClass(UserMap.class);
			job.setCombinerClass(UserReduce.class);
			job.setReducerClass(UserReduce.class);
		}
		
		job.setNumReduceTasks(BALANCED_REDUCE_NUM);
		
		JobClient.runJob(job);
		
	}

}

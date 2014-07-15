package matz.election.analyzer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
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
/**
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
	public final static String IMMEDIATE_REDUCE_NUM = String.valueOf((int)(0.95*NUM_NODES*MAX_REDUCE_PER_NODE));
	public final static String BALANCED_REDUCE_NUM = String.valueOf((int)(1.75*NUM_NODES*MAX_REDUCE_PER_NODE));
	public final static String SINGLE_REDUCE_NUM = "1";
	
	/* 残念なことに0.20.2-cdh3u6はbzip2にまだネイティブ対応していなかった。
	 * LZOに圧縮し直すか、SeqFileに変換するなどの善後策を要する。gzipももちろん無理。
	 * →結局Block単位で圧縮したSeqFileに変換し直した。
	 */

	protected final static String PROP_SEQ_INPUT = "SequenceFileInputFormat";
	protected final static String PROP_TEXT_INPUT = "TextInputFormat";
	protected final static String PROP_SEQ_OUTPUT = "SequenceFileOutputFormat";
	protected final static String PROP_TEXT_OUTPUT = "TextOutputFormat";
	protected final static String PROP_TEXT = "Text";
	protected final static String PROP_LONG = "LongWritable";
	protected final static String PROP_INT = "IntWritable";
	
	protected final static String INPUT_FORMAT_PACKAGE_SUFFIX = "org.apache.hadoop.mapred.";
	protected final static String WRITABLE_PACKAGE_SUFFIX = "org.apache.hadoop.io.";
	
	protected final static int PROP_INDEX_JOB_NAME = 0, PROP_INDEX_JOB_CLASS = 1, PROP_INDEX_MAP_CLASS = 2,
			PROP_INDEX_REDUCE_CLASS = 3, PROP_INDEX_USAGE = 4, PROP_INDEX_INPUT_FORMAT = 5, PROP_INDEX_OUTPUT_FORMAT = 6,
			PROP_INDEX_OUTPUT_KEY_CLASS = 7, PROP_INDEX_OUTPUT_VALUE_CLASS = 8, PROP_INDEX_REDUCE_NUM = 9;
	
	
	/**使用可能なジョブについての情報を保持する2次元配列。<br>
	 * ジョブは適当な名前をつけ、同一パッケージのクラス内に使用するMapper/Reducerをサブクラスとして定義する。<br>
	 * 本配列内にジョブ名、定義クラス、使用するMapper名、Reducer名、引数、入力ファイルフォーマット、出力ファイルフォーマット、出力Key形式、出力Value形式、Reducer数をStringで記述する。<br>
	 * main関数内で、本配列に登録された各種クラスを名前引きでロードし、jobインスタンスに投入、job実行する。
	 */
	protected final static String[][] JOB_PROP = {
		{"TweetCount","TweetCount","Map","TextIntReduce"," <input_seqFile_Path> <outputPath>",PROP_SEQ_INPUT,PROP_TEXT_OUTPUT,PROP_TEXT,PROP_INT,SINGLE_REDUCE_NUM},
		{"UserTweetCount","TweetCount","UserTweetMap","TextIntReduce"," <input_seqFile_Path> <outputPath>",PROP_SEQ_INPUT,PROP_TEXT_OUTPUT,PROP_TEXT,PROP_INT,BALANCED_REDUCE_NUM},
		{"UserCount","TweetCount","UserCountMap","TextIntReduce"," <input_textFile_Path> <outputPath>",PROP_TEXT_INPUT,PROP_TEXT_OUTPUT,PROP_TEXT,PROP_INT,SINGLE_REDUCE_NUM},
		{"TimeSeries","TweetCount","TimeStampMap","LongIntReduce"," <input_seqFile_Path> <outputPath>",PROP_SEQ_INPUT,PROP_TEXT_OUTPUT,PROP_LONG,PROP_INT,SINGLE_REDUCE_NUM},
		{"URLCount","URLTweet","URLCountMap","TextIntReduce"," <input_seqFile_Path> <outputPath>",PROP_SEQ_INPUT,PROP_TEXT_OUTPUT,PROP_TEXT,PROP_INT,BALANCED_REDUCE_NUM},
		{"URLFreq","URLTweet","URLFreqMap","IntIntReduce"," <input_textFile_Path> <outputPath>",PROP_TEXT_INPUT,PROP_TEXT_OUTPUT,PROP_INT,PROP_INT,SINGLE_REDUCE_NUM},
		{"BuzzExtract","URLTweet","BuzzExtractMap","BuzzExtractReduce"," <input_textFile_Path> <outputPath> [<buzzThreshold>]",PROP_TEXT_INPUT,PROP_TEXT_OUTPUT,PROP_INT,PROP_TEXT,SINGLE_REDUCE_NUM},
		{"BuzzURLExpand","URLTweet","BuzzURLExpandMap","TextIntReduce"," <input_textFile_Path> <outputPath>",PROP_TEXT_INPUT,PROP_TEXT_OUTPUT,PROP_TEXT,PROP_INT,BALANCED_REDUCE_NUM},
		{"PoliticalTweet","PoliticalTweet","PoliticalTweetMap","PoliticalTweetReduce"," <input_seqFile_Path> <outputPath>",PROP_SEQ_INPUT,PROP_SEQ_OUTPUT,PROP_LONG,PROP_TEXT,BALANCED_REDUCE_NUM},
	};
	
	/**引数が不正・不足の際に使用する、ジョブリストと使用方法を出力するメソッド。
	 * 
	 */
	private static void jobList() {
		for (String[] prop : JOB_PROP) {
			System.out.println(prop[PROP_INDEX_JOB_NAME] + "\t" + "Usage: " + prop[PROP_INDEX_JOB_NAME] + prop[PROP_INDEX_USAGE]);
		}
	}
	
	/**
	 * @param args
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		JobConf job = null;
		int jobIndex = 0;
		String curPackage = AnalyzerMain.class.getPackage().getName().concat(".");
		
//		if (args.length > 0 && COUNT_JOB_LIST.contains(args[0])) {
		if (args.length == 0) {
			System.err.println("Specify job:");
//			for(String jobName : COUNT_JOB_ARRAY) {
//				System.err.println(jobName);
//			}
			jobList();
			System.exit(1);
		} else {
			do {
				if (jobIndex >= JOB_PROP.length) {
					System.err.println("Available jobs:");
					jobList();
					System.exit(1);
				}
				if (args[0].equals(JOB_PROP[jobIndex][PROP_INDEX_JOB_NAME])) break;
				jobIndex++;
			} while(true);
			
//			job = new JobConf(TweetCount.class);
			job = new JobConf(Class.forName(curPackage + JOB_PROP[jobIndex][PROP_INDEX_JOB_CLASS]));
//			FileInputFormat.setInputPaths(job, DEFAULT_INPUT);
//			FileOutputFormat.setOutputPath(job, new Path(DEFAULT_OUTPUT+System.currentTimeMillis()));
			if (args.length > 2) {
				FileInputFormat.setInputPaths(job, args[1]);
				/* MapReduceのoutputはそのまま別のジョブのinputに使えるような形式になっている。
				 * そのような連鎖的利用を考えると、後に指定しやすいようoutputのpath名は簡単であった方がいい。
				 * 現在時刻を後置して重複を防ぐのは楽だが、あとでどのpathが最新（必要）なパスなのかがわかりにくくなって大変良くない。
				 * HDFSのマウント先(/hdfs)でパスを検索してカウンターをインクリメントする、とかやってもいいと思うが、正直微妙。
				 */
				FileOutputFormat.setOutputPath(job, new Path(args[2]));

				//残りのコマンドライン引数を全てjobConfのプロパティとして渡す。これはMapperやReducer内部からcontext経由で取得できる。
				for (int i = 3; i < args.length; i++) {
					job.set(String.format("arg%d",i), args[i]);
				}
			} else {
				System.err.println("Usage: " + JOB_PROP[jobIndex][PROP_INDEX_JOB_NAME] + JOB_PROP[jobIndex][PROP_INDEX_USAGE]);
				System.exit(1);
			}
		}
		
		
		/* データのエンコードもutf8になっていなかった。HadoopのTextInputFormatはutf8以外無理なので、データを修正した方がいいかもしれない。
		 * →SeqFileに変換し直したので、入力はそれにならう。KeyはUserID(パース不正で読み込めなかった場合は0)、Valには元JSONが入っている。
		 */	
		job.setJobName(JOB_PROP[jobIndex][PROP_INDEX_JOB_NAME]);
		job.setInputFormat((Class<? extends InputFormat<Writable,Writable>>) Class.forName(
				INPUT_FORMAT_PACKAGE_SUFFIX + JOB_PROP[jobIndex][PROP_INDEX_INPUT_FORMAT]));
		job.setOutputFormat((Class<? extends OutputFormat<Writable,Writable>>) Class.forName(
				INPUT_FORMAT_PACKAGE_SUFFIX + JOB_PROP[jobIndex][PROP_INDEX_OUTPUT_FORMAT]));
		job.setOutputKeyClass(Class.forName(
				WRITABLE_PACKAGE_SUFFIX + JOB_PROP[jobIndex][PROP_INDEX_OUTPUT_KEY_CLASS]));
		job.setOutputValueClass(Class.forName(
				WRITABLE_PACKAGE_SUFFIX + JOB_PROP[jobIndex][PROP_INDEX_OUTPUT_VALUE_CLASS]));
		job.setMapperClass((Class<? extends Mapper<Writable,Writable,Writable,Writable>>) Class.forName(
				curPackage + JOB_PROP[jobIndex][PROP_INDEX_JOB_CLASS] + "$" + JOB_PROP[jobIndex][PROP_INDEX_MAP_CLASS]));
		job.setCombinerClass((Class<? extends Reducer<Writable,Writable,Writable,Writable>>) Class.forName(
				curPackage + JOB_PROP[jobIndex][PROP_INDEX_JOB_CLASS] + "$" + JOB_PROP[jobIndex][PROP_INDEX_REDUCE_CLASS]));
		job.setReducerClass((Class<? extends Reducer<Writable,Writable,Writable,Writable>>) Class.forName(
				curPackage + JOB_PROP[jobIndex][PROP_INDEX_JOB_CLASS] + "$" + JOB_PROP[jobIndex][PROP_INDEX_REDUCE_CLASS]));
		
		try {
			job.setNumReduceTasks(Integer.parseInt(JOB_PROP[jobIndex][PROP_INDEX_REDUCE_NUM]));
		} catch (NumberFormatException e) {
			e.printStackTrace();
			System.exit(1);
		}
		JobClient.runJob(job);
		
	}

}

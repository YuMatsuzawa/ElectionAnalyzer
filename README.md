#ElectionAnalyzer

**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [ElectionAnalyzer](#ElectionAnalyzer)
	- [概要](#概要)
	- [使用方法](#使用方法)
	- [ビルド（Jarエクスポート）方法](#ビルド（Jarエクスポート）方法)
	- [クラスタへのアップロード・実行](#クラスタへのアップロード・実行)
	- [ジョブリスト](#ジョブリスト)
		- [基本的なツイート情報に関するジョブ](#基本的なツイート情報に関するジョブ)
			- [TweetCount](#tweetcount)
			- [UserTweetCount](#UserTweetCount)
			- [FilterUTCount](#FilterUTCount)
			- [UserCount](#UserCount)
			- [TimeSeries](#TimeSeries)
			- [TimeStamp](#TimeStamp)
			- [TimeFreq](#TimeFreq)
			- [OriginalTimeFreq](#OriginalTimeFreq)
			- [RetweetCount](#RetweetCount)
			- [RetweetFreq](#RetweetFreq)
		- [選挙関連ツイートに関するジョブ。](#選挙関連ツイートに関するジョブ。)
			- [PoliticalTweet](#PoliticalTweet)
			- [PartyBuzz](#PartyBuzz)
		- [URLによるツイートクラスタリング関連](#URLによるツイートクラスタリング関連)
			- [URLCount](#URLCount)
			- [URLFreq](#URLFreq)
			- [BuzzExtract](#BuzzExtract)
			- [BuzzURLExpand](#BuzzURLExpand)
			- [URLRefer](#URLRefer)
			- [URLReferList](#URLReferList)
			- [URLJoin](#URLJoin)
			- [話題ごとのURL関連](#話題ごとのURL関連)
			- [URLExpand](#URLExpand)
			- [SeqToText](#SeqToText)
		- [リツイートのクラスタリング関連](#リツイートのクラスタリング関連)
			- [Retweet](#Retweet)
			- [RTJoin](#RTJoin)
			- [RTFreq](#RTFreq)
			- [UserRTList](#UserRTList)
			- [RTOpinion](#RTOpinion)
		- [類似度ネットワーク分析のためのジョブ](#類似度ネットワーク分析のためのジョブ)
			- [JaccardLink](#JaccardLink)
			- [JaccardLinkDec](#JaccardLinkDec)
		- [フォローネットワークデータ分析のためのジョブ](#フォローネットワークデータ分析のためのジョブ)
			- [VFAttitude](#VFAttitude)
			- [VFAverage](#VFAverage)
			- [VFOpinion](#VFOpinion)
			- [VDegree](#VDegree)
			- [TotalVDegree](#TotalVDegree)
		- [フォローネットワークデータの整形関連ジョブ](#フォローネットワークデータの整形関連ジョブ)
			- [DropFilter](#DropFilter)

##概要

選挙時期の実データの分析のためのプロジェクト及びパッケージ．

HadoopのMapReduce v1フレームワークを使用．MapReduce v1については各自で勉強して下さい．

Eclipse Luna(4.4.0)で作業を行ったプロジェクトです．Eclipse上で自動ビルドしながら作業するためには，少なくともHadoop-0.20.2の主要なライブラリをインポートするか,
あるいはHadoop-0.20.2の主要なライブラリをインポートしたプロジェクトをビルドパスに含めて下さい．（多分この方式がベターです．他のHadoopプロジェクトを作るときにも使えるので．）
もし準備が面倒であれば，私が作成したCDH3準拠のHadoopプロジェクトがあるので，これをGithubからPullしてインポートしてもよいでしょう．ただし，CDH3はかなり古いことは注意して下さい．
  
[松澤のHadoopEclipseProject](https://github.com/YuMatsuzawa/HadoopEclipseProject)

もっと言うと，2014年度に稼働した研究室クラスタはCDH5で，  Githubに行くとCDH5用のEclipseプラグインがあるようです．
これをインポートし，かつクラスタの管理者（松澤卒業後は前田さん）の方と相談して，クラスタの設置してあるネットワークのルータを設定し，ポートを大橋・鳥海研ネットワーク向けに開放しておくことで，
本番環境と折衝しながらデバッグするような準備も一応できるようですが，私はやったことがなく，どの程度のことができるのかもちょっと不明です．
やってみる場合は「CDH Eclipse」等で検索して出てくるブログやガイドなどを読みながら設定して下さい．

なんにせよ必要なクラスを提供するライブラリがきちんとビルドパスに含まれていれば，Eclipse上でエラーチェックしながら作業はできます．

##使用方法

クラスタ上で実行するためには、jarファイルに固めてゲートウェイのホームフォルダにアップロードし、

``$ hadoop jar <path>/<jarfilename>.jar [argument]``

とします。argumentには`AnalyzerMain`クラス内で規定したMapReduceジョブの指定パターンを入力します。`JOB_PROP`テーブル内で有効なパターンが指定されています。

例えば、`UserTweetCount`というMapRジョブであれば、

``$ hadoop jar ~/lib/ElectionAnalyzer.jar UserTweetCount /user/data/Political2013july/ /user/matsuzawa/utc/1/``

以上のように入力し、実行します。ジョブ名のあとは入力データのパス名、出力先パス名です。
ジョブ名のあとにはいくつでもコマンドライン引数を追加可能ですが、最初の2つは必ず入力データパス、出力パスと解釈されます。
その後の引数は`arg3,arg4,...`と命名され、`JobConf`オブジェクト経由でMapper/Reducer内からアクセス可能です。

argumentを入力せずにコマンド実行することで、パターンリストが表示されます。
各ジョブの詳細な利用法は各Mapper/Reducerのソース内コメントあるいは以下のdocを参照して下さい。

新たなジョブのためのMapper/Reducerを作成した場合は、`JOB_PROP`テーブルにそのMapRを使用するためのジョブ情報を登録して下さい。
必要なジョブ情報は、

* ジョブ名
* Mapper/Reducerの定義クラス
* 使用するMapper名
* Reducer名
* 引数説明
* 入力ファイルフォーマット
* 出力ファイルフォーマット
* 出力Key形式
* 出力Value形式
* Reducer数

以上を基本とします。さらに加えて、

* Mapper出力Key形式
* Mapper出力Value形式
* "DistributedCache"

以上をオプションとして追加できます。Mapper出力Key・Value形式は、Mapper出力とReducer出力の変数形式が異なる場合に使用してください。

例えばMapperは`Text-IntWritable`のペアを放射するが、Reducerはそれを集計した上で`Text-Text`ペアで出力する、といった場合です。
MapRフレームワークの制約上、このようなケースではCombinerが使用不可能ですので、Combinerは定義されません。
逆に、上記オプションが指定されない場合、指定されたReducerがCombinerとしても定義されることに注意してください。
Mapper/Reducerの出力形式は同一だがCombinerを明示的に使用したくない場合も、このオプションを利用してください。
その場合は出力Key/Value形式と同一の内容をMapper出力Key/Value形式として登録します。

"DistributedCache"はDistributedCache機能を利用する場合のオプションです。`JOB_PROP`では`DIST_CACHE`と表記できます。
このオプションがある場合、コマンドライン引数の3つめ（本来`arg3`として`JobConf`に渡される引数）が、DistributedCacheに配置するファイルパスとなります。

これらJOB_PROP要素の書き方はソース内にも記述されています。

##ビルド（Jarエクスポート）方法

クラスタにアップロードするためのJarファイルの作り方を説明します。

まず、HadoopクラスタではそのクラスタのMapReduceサービスに関連するライブラリが自動的にクラスパスに含まれるので、
MapReduceに関連するクラスを格納しているライブラリを明示的にクラスパスに含める必要はありません。
（例：Mapperの基本となる抽象クラス`MapReduceBase`が含まれるライブラリファイルなど）

従って、特にその他の外部ライブラリを使用していないならば、Eclipseのエクスポート機能から、作成したソース等を選択してJarファイルにエクスポートするだけです。
このとき、通常のJar（実行可能Jar/Runneble Jarでない方）を選びます。実行可能JarとするためにはHadoop関連のクラスも全てコンパイルして梱包する必要がありますが、
上記の通りそちらはすでにクラスタに存在しているので、自分で作成したクラスだけコンパイルされていればいいからです。エントリポイントとして`AnalyzerMain`を指定しておきます。
現状のビルド情報はbuild.jardescファイルに保存されているので、これを利用するのが早いでしょう。

`Twitter4j`などの外部ライブラリを使用している場合、Hadoopクラスタが認識できる形でそのライブラリファイルを一緒にアップロードする必要があります。
いくつか方法がありますが、例えばプロジェクト内に`lib`ディレクトリを用意し、必要なライブラリのJarファイルをそこに収めておいて、
エクスポートの際に一緒にパッケージしてあげる（エクスポートするリソースの選択時に`lib`ディレクトリにチェックを入れる）という手段があります。
Hadoopクラスタは実行時に必要なクラスを見つけるため、jar内の`lib`サブディレクトリを自動で探索するからです。
松澤はこの方法を使っているので、本プロジェクトの`lib`ディレクトリにいくつか外部ライブラリのJarが入っています。

もしくは実行時、`$ hadoop jar -libjars`というオプションを付けて必要なライブラリファイルを列挙する方法があります。
ちょうど通常のJavaプログラム実行時に`$ java -cp`と付けて必要なファイルを列挙するのと同様のやり方です。
こうすると必要なライブラリファイルが実行時にDistributedCache経由でクラスタに配布されるという仕組みで、CDH4以降はこのやり方が推奨されているようです。
[参考](http://blog.cloudera.com/blog/2011/01/how-to-include-third-party-libraries-in-your-map-reduce-job/)
こちらの方法を取る場合はクラスタのホームディレクトリに必要なjarをアップロードして、指定します。

##クラスタへのアップロード・実行

詳しくは研究室内資料に残しておきます。簡単に言うと、

1. ゲートウェイの`/home/<user>/`ディレクトリ以下に上記Jarをアップロード。
2. マスターノードにログインし、`$ hadoop ...`コマンドを実行。

これだけです。ゲートウェイとマスターノードはNFSで`/home`を共有しているため、上記のような手順となります。
ゲートウェイ自体はHadoopクラスタには含まれていないので、`$ hadoop`コマンドを実行できないことに注意してください。

##ジョブリスト

本プロジェクトで実行可能なデータ分析ジョブについて、入力・出力等を中心に説明

###基本的なツイート情報に関するジョブ

データ全体について調べたり、クリーニング等を行うジョブ。

####TweetCount

``$ hadoop jar <jarname>.jar TweetCount <input_seqFile_Path> <outputPath>``

ツイートデータから総ツイート数を数えるジョブ。

* 入力:SequentialFile形式の選挙関連ツイートデータのディレクトリ。KeyにID（`LongWritable`）、ValueにRawJSON（`Text`）
* 出力:TextFile形式の集計結果。Keyは"tweetNum"（`Text`）、Valueはカウント（`IntWritable`）

####UserTweetCount

``$ hadoop jar <jarname>.jar UserTweetCount <input_seqFile_Path> <outputPath>``

ツイートデータからユーザごとにツイート数を数えるジョブ。

* 入力:SequentialFile形式の選挙関連ツイートデータのディレクトリ。KeyにID（`LongWritable`）、ValueにRawJSON（`Text`）
* 出力:TextFile形式の集計結果。KeyはUserID（`LongWritable`）、Valueはカウント（`IntWritable`）

####FilterUTCount

``$ hadoop jar <jarname>.jar FilterUTCount <input_textFile_Path> <outputPath>[ <th>]``

UserTweetCountの結果に閾値を当てはめるジョブ。オプションの第三引数に閾値（int数値）。

* 入力:TextFile形式のUserTweetCount出力のディレクトリ。KeyはUserID（`LongWritable`）、Valueはカウント（`IntWritable`）
* 出力:TextFile形式の集計結果。KeyはUserID（`LongWritable`）、Valueは閾値以上のツイート数を持つユーザのカウント（`IntWritable`）

####UserCount

``$ hadoop jar <jarname>.jar UserCount <input_textFile_Path> <outputPath>``

UserTweetCountの結果から、ユーザの総数を数え、かつツイート数ごとのユーザ数頻度を算出するジョブ。

* 入力:TextFile形式のUserTweetCount出力のディレクトリ。KeyはUserID（`LongWritable`）、Valueはカウント（`IntWritable`）
* 出力:TextFile形式の集計結果。Keyはツイート数あるいは"userNum"（`Text`）、Valueはカウント（`IntWritable`）

####TimeSeries

``$ hadoop jar <jarname>.jar TimeSeries <input_seqFile_Path> <outputPath>``

ツイートデータから、時刻ごとのツイート数を数えるジョブ。時刻はミリ秒。

* 入力:SequentialFile形式の選挙関連ツイートデータのディレクトリ。KeyにID（`LongWritable`）、ValueにRawJSON（`Text`）
* 出力:TextFile形式の集計結果。Keyはエポックミリ秒（`LongWritable`）、Valueはカウント（`IntWritable`）

####TimeStamp

``$ hadoop jar <jarname>.jar TimeStamp <input_seqFile_Path> <outputPath>``

ツイートデータから、時刻ごとのツイート数を数えるジョブ。時刻はミリ秒。出力に時刻の可読表現を加える。

* 入力:SequentialFile形式の選挙関連ツイートデータのディレクトリ。KeyにID（`LongWritable`）、ValueにRawJSON（`Text`）
* 出力:TextFile形式の集計結果。Keyはエポックミリ秒（`LongWritable`）、Valueは時刻の可読表現とカウント（`Text`）

####TimeFreq

``$ hadoop jar <jarname>.jar TimeFreq <input_seqFile_Path> <outputPath>``

ツイートデータから、2013/7/27からさかのぼって1日ごとのツイート数を数えるジョブ。時刻はミリ秒。出力に時刻の可読表現を加える。

* 入力:SequentialFile形式の選挙関連ツイートデータのディレクトリ。KeyにID（`LongWritable`）、ValueにRawJSON（`Text`）
* 出力:TextFile形式の集計結果。Keyはエポックミリ秒（`LongWritable`）、Valueは時刻の可読表現とカウント（`Text`）

####OriginalTimeFreq

``$ hadoop jar <jarname>.jar OriginalTimeFreq <input_seqFile_Path> <outputPath>``

ツイートデータから、2013/7/27からさかのぼって1日ごとのツイート数を数えるジョブ。リツイートを除く。時刻はミリ秒。出力に時刻の可読表現を加える。

* 入力:SequentialFile形式の選挙関連ツイートデータのディレクトリ。KeyにID（`LongWritable`）、ValueにRawJSON（`Text`）
* 出力:TextFile形式の集計結果。Keyはエポックミリ秒（`LongWritable`）、Valueは時刻の可読表現とカウント（`Text`）

####RetweetCount

``$ hadoop jar <jarname>.jar RetweetCount <input_seqFile_Path> <outputPath>``

ツイートデータから、任意のリツイートについて、それぞれのリツイート数を数えるジョブ。

* 入力:SequentialFile形式の選挙関連ツイートデータのディレクトリ。KeyにID（`LongWritable`）、ValueにRawJSON（`Text`）
* 出力:TextFile形式の集計結果。Keyはエポックミリ秒（`LongWritable`）、Valueは時刻の可読表現とカウント（`Text`）

####RetweetFreq

``$ hadoop jar <jarname>.jar RetweetFreq <input_textFile_Path> <outputPath>``

RetweetCountの結果から、リツイート数の頻度分布を作成するジョブ。

* 入力:TextFile形式のRetweetCountの集計結果。Keyはエポックミリ秒（`LongWritable`）、Valueは時刻の可読表現とカウント（`Text`）
* 出力:TextFile形式の集計結果。Keyはリツイート数（`IntWritable`）、Valueは頻度（`IntWritable`）

###選挙関連ツイートに関するジョブ。

####PoliticalTweet

``$ hadoop jar <jarname>.jar PoliticalTweet <input_seqFile_Path> <outputPath>``

ツイートデータから、政治・選挙関連単語による絞り込みを行い、同形式のツイートデータで再度出力するジョブ。
データ収集時、捕捉されたユーザについて遡及的なツイート取得を行ったために、無関係のツイートも多く含まれていたが、研究ではそれを使用しないことにしたので、再度絞り込んだ。

* 入力:SequentialFile形式の選挙関連ツイートデータのディレクトリ。KeyにID（`LongWritable`）、ValueにRawJSON（`Text`）
* 出力:SequentialFile形式の選挙関連ツイートデータ再抽出出力。KeyにID（`LongWritable`）、ValueにRawJSON（`Text`）

####PartyBuzz

``$ hadoop jar <jarname>.jar PartyBuzz <input_seqFile_Path> <outputPath>``

ツイートデータから、政党名ごとのツイート数を数えるジョブ。

対象政党名は、"自民党","民主党","日本維新の会","公明党","みんなの党","生活の党","共産党","社民党","新党改革","みどりの風"。

* 入力:SequentialFile形式の選挙関連ツイートデータのディレクトリ。KeyにID（`LongWritable`）、ValueにRawJSON（`Text`）
* 出力:TextFile形式の集計結果。Keyは政党名（`Text`）、Valueはカウント（`IntWritable`）


###URLによるツイートクラスタリング関連

論文2.4.1項のURLによる分析のためのデータ集計ジョブ。

####URLCount

``$ hadoop jar <jarname>.jar URLCount <input_seqFile_Path> <outputPath>``

ツイートデータから、言及されたURLと、URLごとの言及数を数えるジョブ。

* 入力:SequentialFile形式の選挙関連ツイートデータのディレクトリ。KeyにID（`LongWritable`）、ValueにRawJSON（`Text`）
* 出力:TextFile形式の集計結果。KeyはURL（`Text`）、Valueはカウント（`IntWritable`）

####URLFreq

``$ hadoop jar <jarname>.jar URLJoin <input_textFile_Path> <outputPath>``

URLCountの出力から、URL言及の頻度分布を出力するジョブ。

* 入力:TextFile形式のURLCountの集計結果。KeyはURL（`Text`）、Valueはカウント（`IntWritable`）
* 出力:TextFile形式の集計結果。Keyは言及数（`IntWritable`）、Valueは頻度（`IntWritable`）

####BuzzExtract

``$ hadoop jar <jarname>.jar BuzzExtract <input_textFile_Path> <outputPath> [<buzzThreshold>]``

URLCountの出力から、閾値以上の回数言及されたURLを、言及回数ごとにCSVでまとめるジョブ。オプション引数で閾値指定。

* 入力:TextFile形式のURLCountの集計結果。KeyはURL（`Text`）、Valueはカウント（`IntWritable`）
* 出力:TextFile形式の集計結果。Keyは言及数（`IntWritable`）、Valueは当該回数言及されたURLのCSVリスト（`Text`）

####BuzzURLExpand

``$ hadoop jar <jarname>.jar BuzzURLExpand <input_textFile_Path> <outputPath>``

BuzzExtractの出力を読み、ValueのCSVに含まれるURLそれぞれについてURL展開を行い、展開済URLをKey，言及数をValueとして出力するジョブ。
出力形式はURLCountと同一形式になるので、再度BuzzExtractに入力することでURL展開済みのBuzzExtractを行うことができる。

* 入力:TextFile形式のBuzzExtractの集計結果。Keyは言及数（`IntWritable`）、Valueは当該回数言及されたURLのCSVリスト（`Text`）
* 出力:TextFile形式の集計結果。Keyは言及数（`IntWritable`）、Valueは当該回数言及されたURLのCSVリスト（`Text`）

####URLRefer

``$ hadoop jar <jarname>.jar URLRefer <input_seqFile_Path> <outputPath>[ <th>]``

ツイートデータから、言及されたURLと、言及したユーザのペアを出力するジョブ。Reducer内で短縮URLの展開を行う。オプションで閾値を入力し、Reducer入力時点（非展開URL）での最小言及数を指定できる。
短縮URLの展開ではURLコネクションを大量に開くので注意する。

* 入力:SequentialFile形式の選挙関連ツイートデータのディレクトリ。KeyにID（`LongWritable`）、ValueにRawJSON（`Text`）
* 出力:TextFile形式の集計結果。Keyは展開済URL（`Text`）、ValueはユーザID（`LongWritable`）

####URLReferList

``$ hadoop jar <jarname>.jar URLReferList <input_textFile_Path> <outputPath>``

URLReferの結果から、URLごとに言及したユーザのCSVリストを出力するジョブ。

* 入力:TextFile形式のURLReferの集計結果。Keyは展開済URL（`Text`）、ValueはユーザID（`LongWritable`）
* 出力:TextFile形式の集計結果。Keyは展開済URL（`Text`）、ValueはユーザIDのCSVリスト（`Text`）

####URLJoin

``$ hadoop jar <jarname>.jar URLJoin <input_textFile_Path> <outputPath>``

URLReferListの出力から、任意の2つのURLに関するレコード（URLとCSVのペア）を連結したレコードを全て出力するジョブ。すなわちJOIN操作。

元のURLがN件ある場合、JOINの出力はN(N-1)/2件に増える。出力のサイズも相当量に増えるので注意する。

* 入力:TextFile形式のURLReferListの集計結果。Keyは展開済URL（`Text`）、ValueはユーザIDのCSVリスト（`Text`）
* 出力:TextFile形式の集計結果。KeyはURL,CSVからなるレコード1（`Text`）、ValueはURL,CSVからなるレコード2（`Text`）

####話題ごとのURL関連

元々選挙関連ツイートをさらに話題ごとに絞り込み、その中でURLについてクラスタリングしていたが、それよりもURL全てについてクラスタリングする方がいいと考え、方針転換した。
従って以下のジョブによる分析は研究後半では使用しなかった。一部、未完成のものもあるので使用すべきでない。

* TopicURLCount
* TopicURLUser
* TopicURLUserText
* TopicURLTitle
* URLExpand
* SeqToText
* FilterURL
* ThresholdURL
* PairedURL

話題を考慮しない、URL集合全体としてのクラスタリングは、上記URLRefer/URLReferList/URLJoin等のジョブでJoin済リストを作成した後、
後述のJaccard係数計算のためのジョブを利用することで可能なので、そちらを使うべきである。
特にそちらのコードのほうがあとから書き直したものであるため、ワークフローが簡潔になっており、容易に使用できる。

URLExpandとSeqToTextは別用途にも利用できる可能性があるので特記する。

####URLExpand

``$ hadoop jar <jarname>.jar URLExpand <input_seqFile_Path> <outputPath>``

Seq形式のText-Intファイルで、KeyにURLが入っているものに対し、そのURLの実際のリンク先（展開URL）を取得するジョブ。
Valueは元の値を保つ。URL展開の結果同一のURLとなったレコードについてはValueは合計される。

* 入力:SeqFile形式のText-IntWritableデータ。KeyはURL（`Text`）、Valueはカウント（`IntWritable`）
* 出力:SeqFile形式の集計結果。KeyはURL（`Text`）、Valueはカウント（`IntWritable`）

####SeqToText

``$ hadoop jar <jarname>.jar SeqToText <input_seqFile_Path> <outputPath>``

Seq形式のText-Intペアからなるデータを、Text形式に単に直すジョブ。

* 入力:SeqFile形式のText-IntWritableデータ。Keyは`Text`、Valueは`IntWritable`であれば内容不問。
* 出力:入力データをTextFile形式にしたもの。バイナリでないのでテキストエディタで可読。

###リツイートのクラスタリング関連

Base64エンコードを利用しています。テキストデータに含まれるタブやスペース、クオーテーションマーク、文字コード上問題を起こしやすい文字等も、Base64エンコードしてしまうことで確実に取り扱えます。

####Retweet

``$ hadoop jar <jarname>.jar Retweet <input_seqFile_Path> <outputPath>[ <th in Int>]``

ツイートデータから、リツイートと、リツイートしたユーザを抽出するジョブ。出力はKeyにBase64エンコードされた文面、ValueにユーザIDのCSVが入る。
最小限のリツイート数を閾値としてオプション引数で指定可能。

* 入力:SequentialFile形式の選挙関連ツイートデータのディレクトリ。KeyにID（`LongWritable`）、ValueにRawJSON（`Text`）
* 出力:SequentialFile形式の集計結果。Keyはエンコード済文面（`Text`）、ValueはユーザIDのCSV（`IntWritable`）

####RTJoin

``$ hadoop jar <jarname>.jar RTJoin <input_TextFile_Path> <outputPath>``

Retweetの結果から、任意の2レコードを連結したレコードを出力するジョブ。JOIN操作。

* 入力:SequentialFile形式のRetweetの集計結果。Keyはエンコード済文面（`Text`）、ValueはユーザIDのCSV（`IntWritable`）
* 出力:TextFile形式の集計結果。Keyはレコード1（`Text`）、Valueはレコード2（`Text`）

####RTFreq

``$ hadoop jar <jarname>.jar RTFreq <input_TextFile_Path> <outputPath>``

Retweetの結果から、ユーザごとのリツイート回数を数えるジョブ。いわゆるUFリスト（ミーティング資料参照）を作る。
RetweetFreq（リツイートごと）と違い、ユーザごとである点に注意。

* 入力:SequentialFile形式のRetweetの集計結果。Keyはエンコード済文面（`Text`）、ValueはユーザIDのCSV（`IntWritable`）
* 出力:TextFile形式の集計結果。KeyはユーザID（`LongWritable`）、Valueはカウント（`IntWritable`）

####UserRTList

``$ hadoop jar <jarname>.jar UserRTList <input_TextFile_Path> <outputPath>``

Retweetの結果から、ユーザごとのリツイート回数を数えるジョブ。いわゆるUFリスト（ミーティング資料参照）を作る。
RetweetFreq（リツイートごと）と違い、ユーザごとである点に注意。

* 入力:SequentialFile形式のRetweetの集計結果。Keyはエンコード済文面（`Text`）、ValueはユーザIDのCSV（`IntWritable`）
* 出力:SequentialFile形式の集計結果。KeyはユーザID（`LongWritable`）、ValueはBase64エンコード済リツイート文面のCSV（`Text`）

####RTOpinion

``$ hadoop jar <jarname>.jar RTOpinion <input_seqFile_Path> <outputPath> <rtoplist_path>``

UserRTListの結果と、リツイートと意見の対応リスト(rtoplist)から、ユーザとユーザの意見の対応リストをつくるジョブ。いわゆるUOリスト（ミーティング資料参照）を作る。
rtoplistは後述のJaccard係数による類似度計算を行った上で、Cytoscapeを用いてクラスタリングし、連結成分に属するノードリストを取得して作成する。
形式としては、Base64エンコードした文面がKey，意見値をValueとするタブ区切りペアを1行とするリストになっていればよい。
rtoplistはDistributedCacheに格納するので、先に作成してHDFSにアップロードしておく。

* 入力:SequentialFile形式のUserRTList集計結果。KeyはユーザID（`LongWritable`）、ValueはBase64エンコード済リツイート文面のCSV（`Text`）
* 出力:TextFile形式の集計結果。KeyはユーザID（`LongWritable`）、Valueは2値意見（`IntWritable`）
* Cache：rtoplist。単なるテキストファイルでよい。HDFSにアップロードしておく。


###類似度ネットワーク分析のためのジョブ

####JaccardLink

``$ hadoop jar <jarname>.jar JaccardLink <input_textFile_Path> <outputPath>[ <th in Double>]``

URLJoinあるいはRTJoinを用いてJoinしたデータから、全てのURLペアあるいはRTペアについてJaccard係数を算出するジョブ。
オプション引数で閾値を指定。出力はカンマとタブ区切りテキストで、Node1,Node2 JaccardCoeff.となっている。

* 入力:TextFile形式のURLJoinやRTJoinの集計結果。Keyはレコード1（`Text`）、Valueはレコード2（`Text`）
* 出力:TextFile形式の集計結果。Keyはカンマで区切ったレコード1のノードKeyとレコード2のノードKey（`Text`）、ValueはJaccard係数（`DoubleWritable`）

####JaccardLinkDec

``$ hadoop jar <jarname>.jar JaccardLinkDec <input_textFile_Path> <outputPath>[ <th in Double>]``

Base64エンコードされているJoin済データに対するJaccardLink。従ってRTJoinのデータで内容を確認したいならこちらを使う。
出力はBase64デコードされているので可読。

* 入力:TextFile形式のURLJoinやRTJoinの集計結果。Keyはレコード1（`Text`）、Valueはレコード2（`Text`）
* 出力:TextFile形式の集計結果。Keyはカンマで区切ったレコード1のノードKeyとレコード2のノードKey（`Text`）、ValueはJaccard係数（`DoubleWritable`）

###フォローネットワークデータ分析のためのジョブ

ここからはフォローネットワークデータも使います。フォローネットワークデータはKeyにユーザProfile、ValueにネットワークCSVの入ったSeqFileです。

####VFAttitude

``$ hadoop jar <jarname>.jar VFAttitude <input_seqFile_Path> <outputPath> <uxlist_Path>``

フォローネットワークデータから、UFリストに載っているユーザについて、リツイート数と、そのユーザの周囲のヴォーカルユーザ（UFリストに載っているユーザ）の比率を求めるジョブ。
DistCacheにUFリストを入れる。論文では採用しなかったが、ミーティング資料に結果あり（散布図）。

* 入力:SequentialFile形式のフォローネットワークデータ。KeyはUserProfile（`Text`）、ValueはネットワークCSV（`Text`）
* 出力:TextFile形式の集計結果。Keyはリツイート数（`IntWritable`）、Valueは周囲のヴォーカル率（`DoubleWritable`）
* Cache：UFリスト。

####VFAverage

``$ hadoop jar <jarname>.jar VFAverage <input_seqFile_Path> <outputPath> <uxlist_Path>``

フォローネットワークデータから、UFリストに載っているユーザについて、リツイート数と、そのユーザの周囲のヴォーカルユーザ（UFリストに載っているユーザ）の平均RT数を求めるジョブ。
DistCacheにUFリストを入れる。論文では採用しなかったが、ミーティング資料に結果あり（散布図）。

* 入力:SequentialFile形式のフォローネットワークデータ。KeyはUserProfile（`Text`）、ValueはネットワークCSV（`Text`）
* 出力:TextFile形式の集計結果。Keyはリツイート数（`IntWritable`）、Valueは周囲の平均RT数（`DoubleWritable`）
* Cache：UFリスト。

####VFOpinion

``$ hadoop jar <jarname>.jar VFOpinion <input_seqFile_Path> <outputPath> <uxlist_Path>``

フォローネットワークデータから、UOリストに載っているユーザについて、2値意見と、そのユーザの周囲のヴォーカルユーザ（UOリストに載っているユーザ）の平均意見を求めるジョブ。
DistCacheにUOリストを入れる。論文のデータ分析章の最後の図。

* 入力:SequentialFile形式のフォローネットワークデータ。KeyはUserProfile（`Text`）、ValueはネットワークCSV（`Text`）
* 出力:TextFile形式の集計結果。Keyは2値意見（`IntWritable`）、Valueは周囲の平均意見（`DoubleWritable`）
* Cache：UOリスト。

####VDegree

``$ hadoop jar <jarname>.jar VDegree <input_textFile_Path> <outputPath> <uxlist_Path>``

DropFilterで絞り込んだテキスト形式のフォローネットワークデータから、UOリストに載っているユーザについて、意見ごとに入次数を調べるジョブ。

* 入力:TextFile形式のフォローネットワークデータ。KeyはUserId（`Text`）、ValueはネットワークCSV（`Text`）
* 出力:TextFile形式の集計結果。Keyは意見（`IntWritable`）、Valueは入次数（`DoubleWritable`）
* Cache：UOリスト。

####TotalVDegree

``$ hadoop jar <jarname>.jar TotalVDegree <input_textFile_Path> <outputPath>``

DropFilterで絞り込んだテキスト形式のフォローネットワークデータから、任意のユーザについて、入次数を調べるジョブ。比較用。

* 入力:TextFile形式のフォローネットワークデータ。KeyはUserID（`Text`）、ValueはネットワークCSV（`Text`）
* 出力:TextFile形式の集計結果。Keyは意見（`IntWritable`）、Valueは入次数（`DoubleWritable`）

###フォローネットワークデータの整形関連ジョブ

フォローネットワークはかなり大きなデータであり、また一部のデータはKeyあるいはValueのサイズが大きすぎるためHeap Spaceエラーとなる。
ワークアラウンドとしていらないデータを削った小さなネットワークデータに作り変えるなどの作業が必要だった。

しかし、クラスタのチューニングを行った結果比較的大きなデータもある程度そのまま扱えるようになったので、大部分不要になった。
ただし、元データの一部にエラーがあったので、そのブロックのみ除外した。

以下のDropFilterのみ、実際に使用した。

####DropFilter

``$ hadoop jar <jarname>.jar VFOpinion <input_seqFile_Path> <outputPath> <uxlist_Path>``

フォローネットワークデータから、UOリストに載っているユーザのデータのみ抽出し、ダウンサイズしたネットワークデータを作るジョブ。
シミュレーションのための実ネットワークサンプルとしても使用できる。

* 入力:SequentialFile形式のフォローネットワークデータ。KeyはUserProfile（`Text`）、ValueはネットワークCSV（`Text`）
* 出力:TextFile形式のフォローネットワークデータ。KeyはUserID（`Text`）、ValueはネットワークCSV（`Text`）
* Cache：UOリスト。
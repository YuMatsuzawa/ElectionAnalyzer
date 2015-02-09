# ElectionAnalyzer

選挙時期の実データの分析のためのプロジェクト及びクラス．

基本的にほとんどのクラスがHadoopのMapReduce v1フレームワークを使用．MapReduce v1については各自で勉強して下さい．

Eclipse Luna(4.4.0)で作業を行ったプロジェクトです．Eclipse上で自動ビルドしながら作業するためには，少なくともHadoop-0.20.2の主要なライブラリをインポートするか,
あるいはHadoop-0.20.2の主要なライブラリをインポートしたプロジェクトをビルドパスに含めて下さい．（多分この方式がベターです．他のHadoopプロジェクトを作るときにも使えるので．）
もし準備が面倒であれば，私が作成したCDH3準拠のHadoopプロジェクトがあるので，これをGithubからPullしてインポートしてもよいでしょう．ただし，CDH3はかなり古いことは注意して下さい．
  
[松澤のHadoopEclipseProject](https://github.com/YuMatsuzawa/HadoopEclipseProject)

もっと言うと，2014年度に稼働した研究室クラスタはCDH5で，  Githubに行くとCDH5用のEclipseプラグインがあるようです．
これをインポートし，かつクラスタの管理者（松澤卒業後は前田さん）の方と相談して，クラスタの設置してあるネットワークのルータを設定し，ポートを大橋・鳥海研ネットワーク向けに開放しておくことで，
本番環境と折衝しながらデバッグするような準備も一応できるようですが，私はやったことがなく，どの程度のことができるのかもちょっと不明です．
やってみる場合は「CDH Eclipse」等で検索して出てくるブログやガイドなどを読みながら設定して下さい．

なんにせよ必要なクラスを提供するライブラリがきちんとビルドパスに含まれていれば，Eclipse上でビルドしながら作業はできます．

## 使用方法

クラスタ上で実行するためには、jarファイルに固めてゲートウェイのホームフォルダにアップロードし、

``$ hadoop jar <path>/<jarfilename>.jar [argument]``

とします。argumentにはAnalyzerMainクラス内で規定したMapReduceジョブの指定パターンを入力します。JOB_PROPテーブル内で有効なパターンが指定されています。

例えば、UserTweetCountというMapRジョブであれば、

``$ hadoop jar ~/lib/ElectionAnalyzer.jar UserTweetCount /user/data/Political2013july/ /user/matsuzawa/utc/1/``

以上のように入力し、実行します。ジョブ名のあとは入力データのパス名、出力先パス名です。
ジョブ名のあとにはいくつでもコマンドライン引数を追加可能ですが、最初の2つは必ず入力データパス、出力パスと解釈されます。
その後の引数はarg3,arg4,...と命名され、JobConfオブジェクト経由でMapper/Reducer内からアクセス可能です。

argumentを入力せずにコマンド実行することで、パターンリストが表示されます。
詳細な利用法は各Mapper/Reducerのソース内コメントあるいは以下のdocを参照して下さい。

新たなジョブのためのMapper/Reducerを作成した場合は、JOB_PROPテーブルにそのMapRを使用するためのジョブ情報を登録して下さい。
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
例えばMapperはText-IntWritableのペアを放射するが、Reducerはそれを集計した上でText-Textペアで出力する、といった場合です。
MapRフレームワークの制約上、このようなケースではCombinerが使用不可能ですので、Combinerは定義されません。
逆に、上記オプションが指定されない場合、指定されたReducerがCombinerとしても定義されることに注意してください。
Mapper/Reducerの出力形式は同一だがCombinerを明示的に使用したくない場合も、このオプションを利用してください。
その場合は出力Key/Value形式と同一の内容をMapper出力Key/Value形式として登録します。

"DistributedCache"はDistributedCache機能を利用する場合のオプションです。JOB___PROPでは｀`DIST_CACHE``と表記できます。
このオプションがある場合、コマンドライン引数の3つめ（本来arg3としてJobConfに渡される引数）が、DistributedCacheに配置するファイルパスとなります。

これらJOB_PROP要素の書き方はdoc内にも記述されています。


## AnalyzerMain

ドライバークラス（Hadoopに対するジョブの投入を行うクラス）です．main関数を含み，ElectionAnalyzerクラス全体としてのエントリポイントです．
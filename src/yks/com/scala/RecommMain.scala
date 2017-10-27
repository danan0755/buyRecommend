package yks.com.scala

import java.util.Properties

import scala.collection.mutable
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import breeze.linalg.SparseVector
import breeze.linalg.norm
import yks.com.java.SkuDescFilterEn

/**
  * Created by cgt on 17-10-24.
  */
object RecommMain {
  val conf = new SparkConf().setAppName("RecommSys").setMaster("local[*]")
  val sc = new SparkContext(conf)
  //创建case类
  case class Person(skuId: Int, sallId: Int, cosim: Double)
  //创建可变Set,保存sku数据
  val dataSet = mutable.Set.empty[String]
  //热销品推荐主方法
  def main(args: Array[String]): Unit = {
    val sqlContext = new SQLContext(sc)
    //创建Properties存储数据库相关属性
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    //val scB = sc.broadcast(sc)
    //逐个取公司在售sku数据与平台在售商品数据匹配计算相似度
    for_loop( sqlContext, prop)
    sc.stop()
  }
  //逐个取公司在售sku数据与平台在售商品数据匹配计算相似度
  def for_loop( sqlContext: SQLContext, prop: Properties) {
    //创建连接公司在售sku数据库的柄
    val skuDF: DataFrame = select_mysql(sqlContext, prop, "forcast.ebay_sku_onsale_out")
    //创建连接各个平台在售商品数据库的柄
    val ebayDF: DataFrame = select_mysql(sqlContext, prop, "forcast.ebay_store_listing_bigdata_1026")
    val ebaskuB = sc.broadcast(ebayDF)
    //println("skuDF=--------------="+skuDF.count()+"----ebaskuB----"+ebaskuB.value.count())
    val sqlContextB = sc.broadcast(sqlContext)
    val propB = sc.broadcast(prop)
    //逐个取公司在售sku数据与平台在售商品数据匹配计算相似度
    skuDF.foreach(skuRow => {
      val skuId = skuRow.getInt(0);
      //println("skuId-----" + skuId);
      var skuDesc = skuRow.getString(1);
      skuDesc = SkuDescFilterEn.evaluate(skuDesc);
      //println("skuDesc-----" + skuDesc);
      //ebaskuB.value.foreach(sallRow =>println("sallRow-----"+sallRow));
      ebaskuB.value.foreach(sallRow => {
        //println("sallRow-----" + sallRow);
        val sallerId = sallRow.getInt(0);
        //println("sallerId-----" + sallerId)
        var sallerDesc = sallRow.getString(1);
        sallerDesc = SkuDescFilterEn.evaluate(sallerDesc);
        //println("sallerDesc-----" + sallerDesc)
        val cosim: Double = data_deal(sqlContextB.value, skuId, skuDesc, sallerId, sallerDesc)
        val str: String = skuId.toString() + "#" + sallerId.toString() + "#" + cosim.toString()
       // println("str-----" + str)
        dataSet += str
        //dataSet.foreach(x => println("-----" + x))
      });
      //write_myql(sqlContextB.value,propB.value, "test.result",dataSet.toArray);
      write_hive(dataSet.toArray);
      dataSet.clear();
    })
  }
  //数据处理步骤
  def data_deal(sqlContext: SQLContext, skuId: Int, skuDesc: String, sallId: Int, sallerDesc: String): Double = {
    //分词处理，正则过滤掉，字符，数值
    val remoDF = tokenizer_deal(sqlContext, skuId, skuDesc, sallId, sallerDesc)
    //计算商品描述词频，得到两个商品描述的词频向量
    val tfVecDF = tfIdf_deal(remoDF)
    //计算这两个商品的相似度
    val coSim: Double = cosin_dis(tfVecDF)
    coSim
  }

  //查询mysql中数据
  def select_mysql(sqlContext: SQLContext, prop: Properties, dbtable: String): DataFrame = {
    //拆分数据库字符串
    val datatable = dbtable.split("\\.")
    //创建连接数据库的柄
    var skuDF = sqlContext.read.format("jdbc").options(
      Map("url" -> ("jdbc:mysql://dev.forcastdbm.kokoerp.com:3306/"+datatable(0)), "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> datatable(1), "user" -> "forcast", "password" -> "V6PfT7Y1mE2sF2M2")).load()
    //    sqlContext.udf.register("concat2", (a: String, b: String) => {
    //      a + " " + b
    //    })
    skuDF.registerTempTable("tmp")
    val skuDFilter = sqlContext.sql("select id, bannertext from tmp where bannertext_langid='en' ")
    println("--------完成mysql表数据查询----------")
    skuDFilter
  }
  //写相似度数据到mysql表
  def write_myql(sqlContext: SQLContext, prop: Properties, dbtable: String, arrEbay: Array[String]) {
    //拆分数据库字符串
    val datatable = dbtable.split("\\.")
    //println("-----arrEbay(0)------" + arrEbay(0))
    //通过并行化创建RDD
    val personRDD = sc.parallelize(arrEbay).map(_.split("#"))
    //通过StructType直接指定每个字段的schema
    val schema = StructType(
      List(
        StructField("skuId", IntegerType, true),
        StructField("sallId", IntegerType, true),
        StructField("cosim", DoubleType, true)))
    //将RDD映射到rowRDD
    val rowRDD = personRDD.filter(str => !str(2).equals("NaN"))
      .map(p => Row(p(0).toInt, p(1).toInt, p(2).toDouble))
    //将schema信息应用到rowRDD上
    val personDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    //将数据追加到数据库
    personDataFrame.write.mode("append").jdbc("jdbc:mysql://192.168.201.74:3306/" + datatable(0), dbtable, prop)
    println("--------完成数据写入mysql表----------")
  }
  //分词处理，正则过滤掉，字符，数值
  def tokenizer_deal(sqlContext: SQLContext, skuId: Int, skuDesc: String, sallId: Int, sallerDesc: String): DataFrame = {
    val sentenceDataFrame = sqlContext.createDataFrame(Seq(
      (skuId, skuDesc),
      (sallId, sallerDesc))).toDF("id", "itemDesc")
    //过滤非法字符，数值
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("itemDesc")
      .setOutputCol("words")
      .setPattern("\\W")
    val tokenized = regexTokenizer.transform(sentenceDataFrame)
    //去除停用词
    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtWords")
    val remoDF = remover.transform(tokenized)
    //println("----1-----")
    remoDF.show()
    //以dataframe的形式，返回包含两个商品描述的词向量
    remoDF
  }
  //计算商品描述词频，得到向量
  def tfIdf_deal(remoDF: DataFrame): DataFrame = {
    //tf模型训练，设置词向量维度为20
    val hashingTF = new HashingTF().setInputCol("filtWords").setOutputCol("wordsFeatu")
    val featurizedData = hashingTF.transform(remoDF)
   // println("----2-----")
    featurizedData.select("id", "wordsFeatu").show()
    //featurizedData.foreach(println)
    //使用idf模型计算Inverse Document Frequency,这里不适合运用该模型，文档仅仅2个容易为0,导致Nan
    //    val idf = new IDF().setInputCol("wordTF").setOutputCol("wordsFeatu")
    //    //拟合idf模型
    //    val idfModel = idf.fit(featurizedData)
    //    val tfIdfDF = idfModel.transform(featurizedData)
    //以dataframe的形式，返回包含两个商品描述的bag of words向量
    featurizedData
  }
  //商品相似度
  def cosin_dis(tfIdfDF: DataFrame): Double = {
    println("----3-----")
    tfIdfDF.select("id", "wordsFeatu").take(2).foreach(println)
    val filtDF = tfIdfDF.select("id", "wordsFeatu").rdd.take(2)
    //在售sku的id,词频
    val skuItem = filtDF(0)
    //在售sall的id,词频
    val sallItem = filtDF(1)
    //println("skuItem=" + skuItem + "--sallItem=" + sallItem)
    val sv1 = skuItem(1).asInstanceOf[SV];
    val sv2 = sallItem(1).asInstanceOf[SV];
    //构建稀疏向量1
    val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
    //构建稀疏向量2
    val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
    //计算两向量点乘除以两向量范数得到向量余弦值
    val cosSim: Double = bsv1.dot(bsv2).asInstanceOf[Double] / (norm(bsv1) * norm(bsv2))
    //println("bsv1---" + bsv1 + "---bsv2---" + bsv2 + "---id1--" + skuItem(0) + "--id2---" + sallItem(0) + "--cosSim--" + cosSim)
    cosSim
  }

  //写相似度数据到hive表
  def write_hive(arrEbay: Array[String]): Unit = {
    val personRDD = sc.parallelize(arrEbay).map(_.split("#"))
    val hiveContext = new HiveContext(sc)
    //hiveContext.setConf("yarn.timeline-service.enabled","false")
    import hiveContext.implicits._
    hiveContext.sql("use recomm")
    //将RDD映射到rowRDD
    val recolRdd = personRDD.map(p => Person(p(0).trim().toInt, p(1).trim().toInt, p(2).trim().toDouble))
    //使用下面这条语句，case class必须是object/class成员变量
    recolRdd.toDF().write.mode("append").saveAsTable("similar")
    println("--------完成数据写入hive表----------")
  }
  //排序，取出topn
  def sort_top(): Unit = {}
}

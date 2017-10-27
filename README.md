python代码
1.langdetect_classify.py，该代码用于检测商品描述使用的语言(只取english)
2.在售商品表、各大平台(ebay,wish,亚马逊抓取的商品数据)进行检测，之后运行src下的scala、java代码
src下代码
1.使用idea编辑器，建立普通scala项目
2.从hdp2.4.3的安装目录下复制spark依赖包，如：/usr/hdp/2.4.3.0-227/spark/lib下所有依赖包，导入该依赖
3.如果不需要保存到hive表，可以注释掉--write_hive(dataSet.toArray)，然后在local模式本地运行
4.提交到集群运行，请参考recomHiveRun.sh
sql代码
1.recomHiveSql.sql项目中创建表的脚本



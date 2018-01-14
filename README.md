**BUILD DOCKER IMAGE FOR SPARK 2.1.0**

docker build -t="dockerfile/myntelligence_spark" .

**SUBMIT SERVICE ON SPARK MASTER**

spark-submit --class com.myntelligence.text.tranformation.ScrapperService --master spark://spark-master:7077 --deploy-mode cluster /usr/local/transformation/target/scala-2.11/scrapper-service.jar

spark-submit --class com.myntelligence.text.tranformation.ScrapperService --master spark://Macintosh.local:7077 /Users/admin/workspace/classification-service/myntelligence-text-transformation/target/scala-2.11/scrapper-service.jar# restsparkserver

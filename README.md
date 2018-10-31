 
 Запуск задания №5 через DF
        
        spark-submit --master yarn-cluster --conf spark.yarn.executor.memoryOverhead=0 --conf spark.yarn.driver.memoryOverhead=0 --class "com.griddynamics.aborgatin.finalproject.df.Ex5" --verbose --jars /home/aborgatin/mysql-connector-java-5.1.6.jar --conf spark.eventLog.dir=hdfs:///user/aborgatin/applicationHistory  spark-final-project_2.10-0.2.jar 
 
  Запуск задания №5 через RDD
  
          spark-submit --master yarn-cluster --conf spark.yarn.executor.memoryOverhead=0 --conf spark.yarn.driver.memoryOverhead=0 --class "com.griddynamics.aborgatin.finalproject.rdd.Ex5" --verbose --jars /home/aborgatin/mysql-connector-java-5.1.6.jar --conf spark.eventLog.dir=hdfs:///user/aborgatin/applicationHistory  spark-final-project_2.10-0.2.jar 

  Запуск задания №5 через SQL
  
          spark-submit --master yarn-cluster --conf spark.yarn.executor.memoryOverhead=0 --conf spark.yarn.driver.memoryOverhead=0 --class "com.griddynamics.aborgatin.finalproject.sql.Ex5" --verbose --jars /home/aborgatin/mysql-connector-java-5.1.6.jar --conf spark.eventLog.dir=hdfs:///user/aborgatin/applicationHistory  spark-final-project_2.10-0.2.jar 

 Запуск задания №6 через DF

        spark-submit --master yarn-cluster --conf spark.yarn.executor.memoryOverhead=0 --conf spark.yarn.driver.memoryOverhead=0 --class "com.griddynamics.aborgatin.finalproject.df.Ex6" --verbose --jars /home/aborgatin/mysql-connector-java-5.1.6.jar --conf spark.eventLog.dir=hdfs:///user/aborgatin/applicationHistory  spark-final-project_2.10-0.2.jar 

 

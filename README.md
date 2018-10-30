 
 Запуск задания №5 через DF
 
 spark-submit --master yarn-cluster --class "com.griddynamics.aborgatin.finalproject.df.Ex5" --verbose  spark-final-project_2.10-0.2.jar 
 spark-submit --master yarn-cluster --executor-memory 641m --conf spark.executor-memoryOverhead=383m --class "com.griddynamics.aborgatin.finalproject.df.Ex5" --verbose spark-final-project_2.10-0.2.jar 
 
--conf spark.executor-memoryOverhead=383m
--conf spark.master=yarn-cluster

 
  Запуск задания №6 через DF
 spark-submit  --class "com.griddynamics.aborgatin.finalproject.df.Ex6" --verbose spark-final-project_2.10-0.2.jar 
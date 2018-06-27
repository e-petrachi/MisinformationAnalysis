# MISINFORMATION ANALYSIS

### LAUNCH

```$SPARK_HOME/bin/spark-submit --class spark.analysis.Polarity ~/Development/JAVA/MisinformationAnalysis/build/libs/Polarity-all-1.0.0.jar```

```$SPARK_HOME/bin/spark-submit --class spark.analysis.Fonts ~/Development/JAVA/MisinformationAnalysis/build/libs/Fonts-all-1.0.0.jar```

```$SPARK_HOME/bin/spark-submit --class spark.analysis.SocialBot ~/Development/JAVA/MisinformationAnalysis/build/libs/SocialBot-all-1.0.0.jar```

```$SPARK_HOME/bin/spark-submit --class spark.analysis.HashtagsGroup ~/Development/JAVA/MisinformationAnalysis/build/libs/HashtagsGroup-all-1.0.0.jar```
  
```$SPARK_HOME/bin/spark-submit --class spark.analysis.MentionsGroup ~/Development/JAVA/MisinformationAnalysis/build/libs/MentionsGroup-all-1.0.0.jar```

```$SPARK_HOME/bin/spark-submit --class spark.analysis.post_analysis.HashtagsCommunity ~/Development/JAVA/MisinformationAnalysis/build/libs/HashtagsCommunity-all-1.0.0.jar```
   
```$SPARK_HOME/bin/spark-submit --class spark.analysis.post_analysis.MentionsCommunity ~/Development/JAVA/MisinformationAnalysis/build/libs/MentionsCommunity-all-1.0.0.jar```

```$SPARK_HOME/bin/spark-submit --class spark.analysis.post_analysis.Communities ~/Development/JAVA/MisinformationAnalysis/build/libs/Communities-all-1.0.0.jar```


### EXPORT to CSV

```mongoexport --host localhost --db bigdata --collection communitiesHashtag --query '{ $or : [ { 'polarity_value' : {$gte: 75 } },  {'polarity_value' : { $lte : -75} }] }' --type=csv --out ~/Desktop/communitiesHashtag.csv --fields hashtags,polarity,polarity_value```
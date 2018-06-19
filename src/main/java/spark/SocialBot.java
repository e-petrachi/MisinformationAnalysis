package spark;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import scala.Tuple2;
import scala.Tuple3;
import spark.model.Constant;
import spark.model.QueryResult;
import spark.model.Tweet;
import spark.model.User;
import spark.temp.MongoRDDLoader;

import java.util.ArrayList;

public class SocialBot {

    private static final Logger LOG = Logger.getLogger(SocialBot.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static void main(String[] args){
        MongoRDDLoader ml = new MongoRDDLoader();

        Tuple2<JavaRDD<QueryResult>, JavaSparkContext> rdd2jsc =  ml.openloader(doc -> {
            return new QueryResult(doc, Constant.socialbot);
        });

        JavaSparkContext jsc = rdd2jsc._2();
        JavaRDD<QueryResult> rdd = rdd2jsc._1();

        JavaPairRDD<Long,Tuple2<String,Double>> r = rdd
                .flatMapToPair(a -> {
                    ArrayList<Tuple2<Tuple2<Long,String>,Double>> l = new ArrayList<>();
                    Tweet tweet = a.getTweet();
                    ArrayList<String> hash = tweet.getHashtagEntities();
                    for (String h: hash){
                        l.add(new Tuple2<>(new Tuple2<>(tweet.getUser().getId(),h),1.0));
                    }
                    return l.iterator();
                }).reduceByKey( (a,b) -> a + b )
                .mapToPair(a -> new Tuple2<>(a._1()._1(),new Tuple2<>(a._1()._2(),a._2())));

        JavaPairRDD<Long,Double> s = rdd
                .mapToPair( a -> new Tuple2<>(a.getTweet().getUser().getId(),1.0))
                .reduceByKey( (a,b) -> a + b );

        JavaPairRDD<Long,Tuple3<String,Double,Double>> rs = r
                .join(s)
                .mapToPair( a -> new Tuple2<>(a._1(),new Tuple3<>(a._2()._1()._1(), a._2()._1()._2() / a._2()._2(), a._2()._2())));

        JavaPairRDD<Tuple2<Long,Double>,Iterable<String>> t = rs
                .mapToPair( a -> {
                    double p = Math.round(a._2()._2()*10000.0)/100.0;
                    return new Tuple2<>(new Tuple2<>(a._1(),a._2()._3()),
                        "{'hashtag': '" + a._2()._1() + "','percentage': '" + p + "'}");
                })
                .groupByKey();

        JavaRDD<Document> mongordd = t
                .map(a -> Document.parse(
                        "{ 'id_user': " + a._1()._1() +
                        ", 'tweets_count': " + a._1()._2() +
                        ", 'hashtags': " + a._2() +
                        "}"));

        MongoSpark.save(mongordd, WriteConfig.create(jsc).withOption("collection","socialbot"));
        
        jsc.close();

    }
}
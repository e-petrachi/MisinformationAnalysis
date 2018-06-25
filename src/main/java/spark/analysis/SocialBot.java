package spark.analysis;

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

import java.util.ArrayList;

public class SocialBot {

    private static final Logger LOG = Logger.getLogger(SocialBot.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static Tuple2<JavaRDD<QueryResult>, JavaSparkContext> loadDocument() {
        MongoRDDLoader ml = new MongoRDDLoader();
        return ml.openloader(doc -> {
            return new QueryResult(doc, Constant.fonts);
        });
    }

    public static void execute(JavaRDD<QueryResult> rdd, JavaSparkContext jsc) {
        // id_user + <mention,counter>
        JavaPairRDD<Long,Tuple2<Long,Double>> m = rdd
                .flatMapToPair(a -> {
                    ArrayList<Tuple2<Tuple2<Long,Long>,Double>> l = new ArrayList<>();
                    Tweet tweet = a.getTweet();
                    ArrayList<User> mention = tweet.getUserMentionEntities();
                    for (User u: mention){
                        l.add(new Tuple2<>(new Tuple2<>(tweet.getUser().getId(),u.getId()),1.0));
                    }
                    return l.iterator();
                }).reduceByKey( (a,b) -> a + b )
                .mapToPair(a -> new Tuple2<>(a._1()._1(),new Tuple2<>(a._1()._2(),a._2())));

        // id_user + <hashtag,counter>
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

        // id_user + tweets_counter
        JavaPairRDD<Long,Double> s = rdd
                .mapToPair( a -> new Tuple2<>(a.getTweet().getUser().getId(),1.0))
                .reduceByKey( (a,b) -> a + b );

        // id_user + <hashtag,counter,tweets_counter>
        JavaPairRDD<Long,Tuple3<String,Double,Double>> rs = r
                .join(s)
                .mapToPair( a -> new Tuple2<>(a._1(),new Tuple3<>(a._2()._1()._1(), a._2()._1()._2() / a._2()._2(), a._2()._2())));

        // id_user + <mention,counter,tweets_counter>
        JavaPairRDD<Long,Tuple3<Long,Double,Double>> ms = m
                .join(s)
                .mapToPair( a -> new Tuple2<>(a._1(),new Tuple3<>(a._2()._1()._1(), a._2()._1()._2() / a._2()._2(), a._2()._2())));



        // <id_user,tweets_counter>,[<hashtag,percentage>,<hashtag,percentage>,..]
        JavaPairRDD<Tuple2<Long,Double>,Iterable<String>> t1 = rs
                .mapToPair( a -> {
                    double p = Math.round(a._2()._2()*10000.0)/100.0;
                    return new Tuple2<>(new Tuple2<>(a._1(),a._2()._3()),
                            "{'hashtag': '" + a._2()._1() + "','percentage': '" + p + "'}");
                })
                .groupByKey();

        // <id_user,tweets_counter>,[<mention,percentage>,<mention,percentage>,..]
        JavaPairRDD<Tuple2<Long,Double>,Iterable<String>> t2 = ms
                .mapToPair( a -> {
                    double p = Math.round(a._2()._2()*10000.0)/100.0;
                    return new Tuple2<>(new Tuple2<>(a._1(),a._2()._3()),
                            "{'mention': '" + a._2()._1() + "','percentage': '" + p + "'}");
                })
                .groupByKey();

        // <id_user,tweets_counter> + <[<hashtag,counter>],[<mention,counter>]>
        JavaPairRDD<Tuple2<Long,Double>,Tuple2<Iterable<String>,Iterable<String>>> rms = t1
                .join(t2);


        JavaRDD<Document> mongordd = rms
                .map(a -> Document.parse(
                        "{ 'id_user': " + a._1()._1() +
                                ", 'tweets_count': " + a._1()._2() +
                                ", 'hashtags': " + a._2()._1() +
                                ", 'mentions': " + a._2()._2() +
                                "}"));

        MongoSpark.save(mongordd, WriteConfig.create(jsc).withOption("collection","socialbot"));
    }

    public static void main(String[] args){
        Tuple2<JavaRDD<QueryResult>, JavaSparkContext> rdd2jsc = loadDocument();

        JavaSparkContext jsc = rdd2jsc._2();
        JavaRDD<QueryResult> rdd = rdd2jsc._1();

        execute(rdd, jsc);
        
        jsc.close();

    }
}

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
import spark.model.Constant;
import spark.model.QueryResult;
import spark.model.Tweet;
import spark.model.User;
import spark.temp.MongoRDDLoader;

import java.util.ArrayList;

public class MentionsGroup {

    private static final Logger LOG = Logger.getLogger(MentionsGroup.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static void main(String[] args){
        MongoRDDLoader ml = new MongoRDDLoader();

        Tuple2<JavaRDD<QueryResult>, JavaSparkContext> rdd2jsc =  ml.openloader(doc -> {
            return new QueryResult(doc, Constant.mentionsgroup);
        });

        JavaSparkContext jsc = rdd2jsc._2();
        JavaRDD<QueryResult> rdd = rdd2jsc._1();

        JavaPairRDD<Long,Long> r = rdd
                .flatMapToPair(a -> {
                    ArrayList<Tuple2<Long,Long>> l = new ArrayList<>();
                    Tweet tweet = a.getTweet();
                    ArrayList<User> mention = tweet.getUserMentionEntities();
                    for (User u: mention){
                        l.add(new Tuple2<>(u.getId(),tweet.getUser().getId()));
                    }
                    return l.iterator();
                });

        JavaPairRDD<Long, Iterable<Long>> s = r
                .groupByKey();

        JavaRDD<Document> mongordd = s
                .map(a -> Document.parse("{'mention': '" + a._1() +
                        "', 'users': " + a._2() +
                        "}"));

        MongoSpark.save(mongordd, WriteConfig.create(jsc).withOption("collection","mentionsgroup"));

        jsc.close();

    }
}

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
import spark.model.Constant;
import spark.model.QueryResult;
import spark.model.Tweet;
import spark.utilities.MongoRDDLoader;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Gruppi di utenti che hanno utilizzato gli stessi hashtag
 * (utili ad identificare comunità di diffusione automatica di misinformation)
 *
 * Filtro gruppi di cui ho almeno 6 hashtag
 */
public class HashtagsGroup {

    private static final Logger LOG = Logger.getLogger(HashtagsGroup.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static Tuple2<JavaRDD<QueryResult>, JavaSparkContext> loadDocument() {
        MongoRDDLoader ml = new MongoRDDLoader("fakenewsnetwork","matteodb","bigdata","hashtagsgroup");
        return ml.openloader(doc -> {
            return new QueryResult(doc, Constant.hashtagsgroup);
        });
    }

    public static void execute(JavaRDD<QueryResult> rdd, JavaSparkContext jsc) {
        JavaPairRDD<String,String> r = rdd
                .flatMapToPair(a -> {
                    ArrayList<Tuple2<String,String>> l = new ArrayList<>();
                    Tweet tweet = a.getTweet();
                    ArrayList<String> hash = tweet.getHashtagEntities();
                    for (String h: hash){
                        l.add(new Tuple2<>(h,"'" + tweet.getUser().getScreenName().replaceAll("[^a-zA-Z0-9]","") + "'"));
                    }
                    return l.iterator();
                }).distinct();

        JavaPairRDD<String, Iterable<String>> s = r
                .groupByKey();

        JavaPairRDD<Integer, Tuple2<String,Iterable<String>>> c = s
                .mapToPair(a -> new Tuple2<>(((Collection<String>) a._2()).size(),new Tuple2<>(a._1(),a._2())))
                .sortByKey(false)
                .filter( a -> a._1() > 5);

        JavaRDD<Document> mongordd = c
                .map(a -> Document.parse("{'hashtag': '" + a._2()._1() +
                        "', 'size': " + a._1() +
                        " , 'users': " + a._2()._2() +
                        "}"));

        MongoSpark.save(mongordd, WriteConfig.create(jsc));
    }

    public static void main(String[] args){
        Tuple2<JavaRDD<QueryResult>, JavaSparkContext> rdd2jsc = loadDocument();

        JavaSparkContext jsc = rdd2jsc._2();

        execute(rdd2jsc._1(), jsc);

        jsc.close();

    }
}

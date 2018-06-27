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
import spark.utilities.MongoRDDLoader;

import java.util.ArrayList;

/**
 * Polarità degli utenti con le relative percentuali e quantità di tweet per ognuno di essi +
 * Identificazione di utenti fonte di misinformation in funzione del rapporto friends/followers
 * (valori bassi rispetto alla media indicano probabili fonti)
 */

public class Polarity {

    private static final Logger LOG = Logger.getLogger(Polarity.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static Tuple2<JavaRDD<QueryResult>, JavaSparkContext> loadDocument() {
        MongoRDDLoader ml = new MongoRDDLoader("fakenewsnetwork","matteodb","bigdata","polarity");
        return ml.openloader(doc -> {
            return new QueryResult(doc, Constant.polarity);
        });
    }

    public static void execute(JavaRDD<QueryResult> rdd, JavaSparkContext jsc) {
        // < user, type , friends/followers > + counter
        JavaPairRDD<Tuple3<String,String,Double>,Integer> r = rdd
                .mapToPair(a -> new Tuple2<>(
                        new Tuple3<>(
                                "'" + a.getTweet().getUser().getScreenName().replaceAll("[^a-zA-Z0-9]","") + "'",
                                a.getType_page(),
                                Math.round((double)a.getTweet().getUser().getFriendsCount()/(double)a.getTweet().getUser().getFollowersCount()*100.0)/100.0
                        ),1))
                .reduceByKey( (a,b) -> a + b)
                .mapToPair(a -> new Tuple2<>(a._2(),a._1()))
                .sortByKey()
                .mapToPair(a -> new Tuple2<>(a._2(),a._1()));

        // < user, friends/followers > + <[type , counter]>
        JavaPairRDD<Tuple2<String,Double>, Iterable<Tuple2<String,Integer>>> s = r
                .mapToPair(a -> new Tuple2<>(new Tuple2<>(a._1()._1(),a._1()._3()),new Tuple2<>(a._1()._2(),a._2())))
                .groupByKey();

        // < user, friends/followers > + <type , percentage, counter>
        JavaPairRDD<Tuple2<String,Double>, Tuple3<String,Double,Integer>> t = s
                .mapToPair(a -> {
                            ArrayList<Tuple2<String, Integer>> array = new ArrayList<>();
                            a._2().forEach(b -> array.add(new Tuple2<>(b._1(), b._2())));

                            if (array.size() == 1) {
                                double percent = array.get(0)._1().equalsIgnoreCase("misinformation") ? 100.0 : 0.0;
                                return new Tuple2<>(a._1(), new Tuple3<>("misinformation", percent, array.get(0)._2()));

                            } else {
                                double mis_count = 0.0;
                                double inf_count = 0.0;

                                if (array.get(0)._1().equalsIgnoreCase("misinformation")) {
                                    mis_count = (double) array.get(0)._2();
                                    inf_count = (double) array.get(1)._2();
                                } else {
                                    mis_count = (double) array.get(1)._2();
                                    inf_count = (double) array.get(0)._2();
                                }

                                int total = (int) (mis_count + inf_count);
                                double result = Math.round((mis_count / total) * 10000.0);
                                return new Tuple2<>(a._1(), new Tuple3<>("misinformation", result / 100.0, total));
                            }
                        });
                //}).filter( a -> a._2()._3() > 5);

        JavaRDD<Document> mongordd = t
                .map(a -> Document.parse("{'user': " + a._1()._1() +
                        ", 'misinformation': " + a._2()._2() +
                        ", 'information':" + (100.0 - a._2()._2()) +
                        ", 'tweets':" + a._2()._3() +
                        ", 'friends/followers':" + a._1()._2() +
                        "}"));

        MongoSpark.save(mongordd, WriteConfig.create(jsc));
    }

    public static void main(String[] args){
        Tuple2<JavaRDD<QueryResult>, JavaSparkContext> rdd2jsc = loadDocument();

        JavaSparkContext jsc = rdd2jsc._2();
        JavaRDD<QueryResult> rdd = rdd2jsc._1();

        execute(rdd, jsc);

        jsc.close();

    }
}

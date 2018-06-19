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
import spark.temp.MongoRDDLoader;

import java.util.ArrayList;

public class Polarity {

    private static final Logger LOG = Logger.getLogger(Polarity.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static void main(String[] args){
        MongoRDDLoader ml = new MongoRDDLoader();

        Tuple2<JavaRDD<QueryResult>, JavaSparkContext> rdd2jsc =  ml.openloader(doc -> {
            return new QueryResult(doc, Constant.polarity);
        });

        JavaSparkContext jsc = rdd2jsc._2();
        JavaRDD<QueryResult> rdd = rdd2jsc._1();

        JavaPairRDD<Tuple2<Long,String>,Integer> r = rdd
                .mapToPair(a -> new Tuple2<>(new Tuple2<>(a.getTweet().getUser().getId(),a.getType_page()),1))
                .reduceByKey( (a,b) -> a + b)
                .mapToPair(a -> new Tuple2<>(a._2(),a._1()))
                .sortByKey()
                .mapToPair(a -> new Tuple2<>(a._2(),a._1()));

        //JavaRDD<Document> mongordd = r.map(a -> Document.parse("{'id_user': " + a._1()._1() + ", 'type': '" + a._1()._2().toString().toLowerCase() + "', 'counter': " + a._2() + "}"));


        JavaPairRDD<Long, Iterable<Tuple2<String,Integer>>> s = r
                .mapToPair(a -> new Tuple2<>(a._1()._1(),new Tuple2<>(a._1()._2(),a._2())))
                .groupByKey();

        JavaPairRDD<Long, Tuple3<String,Double,Integer>> t = s
                .mapToPair(a -> {
                    ArrayList<Tuple2<String,Integer>> array = new ArrayList<>();
                    a._2().forEach( b -> array.add(new Tuple2<>(b._1(),b._2())));

                    if (array.size() == 1){
                        double percent = array.get(0)._1().equalsIgnoreCase("misinformation") ? 100.0 : 0.0;
                        return new Tuple2<>(a._1(),new Tuple3<>("misinformation", percent, array.get(0)._2() ));

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
                        double result = Math.round((mis_count/total)*10000.0);
                        return new Tuple2<>(a._1(),new Tuple3<>("misinformation", result/100.0, total));
                    }

                });

        JavaRDD<Document> mongordd = t
                .map(a -> Document.parse("{'id_user': " + a._1() +
                        ", 'misinformation': " + a._2()._2() +
                        ", 'information':" + (100.0 - a._2()._2()) +
                        ", 'tweets':" + a._2()._3() +
                        "}"));

        MongoSpark.save(mongordd, WriteConfig.create(jsc).withOption("collection","polarity"));
        //LOG.debug(r.first().toString());

        jsc.close();

    }
}

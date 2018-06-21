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

public class Fonts {

    private static final Logger LOG = Logger.getLogger(Fonts.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static void main(String[] args){
        MongoRDDLoader ml = new MongoRDDLoader();

        Tuple2<JavaRDD<QueryResult>, JavaSparkContext> rdd2jsc =  ml.openloader(doc -> {
            return new QueryResult(doc, Constant.fonts);
        });

        JavaSparkContext jsc = rdd2jsc._2();
        JavaRDD<QueryResult> rdd = rdd2jsc._1();

        JavaPairRDD<String,Long> r = rdd
                .mapToPair(a -> new Tuple2<>(a.getType_page(),a.getTweet().getUser().getId()))
                .distinct();

        JavaPairRDD<String, Iterable<Long>> s = r
                .groupByKey();

        JavaRDD<Document> mongordd = s
                .map(a -> Document.parse("{'font': '" + (a._1().equalsIgnoreCase("misinformation") ? a._1() : "information") +
                        "', 'users': " + a._2() +
                        "}"));

        MongoSpark.save(mongordd, WriteConfig.create(jsc).withOption("collection","fonts"));

        jsc.close();

    }
}

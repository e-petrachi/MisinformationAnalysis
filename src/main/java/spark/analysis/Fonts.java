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
import spark.utilities.MongoRDDLoader;

public class Fonts {

    private static final Logger LOG = Logger.getLogger(Fonts.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static Tuple2<JavaRDD<QueryResult>, JavaSparkContext> loadDocument() {
        MongoRDDLoader ml = new MongoRDDLoader("fakenewsnetwork","matteodb","bigdata","fonts");
        return ml.openloader(doc -> {
            return new QueryResult(doc, Constant.fonts);
        });
    }

    public static void execute(JavaRDD<QueryResult> rdd, JavaSparkContext jsc) {
        JavaPairRDD<String,Long> r = rdd
                .mapToPair(a -> new Tuple2<>(a.getType_page(),a.getTweet().getUser().getId()))
                .distinct();

        JavaPairRDD<String, Iterable<Long>> s = r
                .groupByKey();

        JavaRDD<Document> mongordd = s
                .map(a -> Document.parse("{'font': '" + (a._1().equalsIgnoreCase("misinformation") ? a._1() : "information") +
                        "', 'users': " + a._2() +
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

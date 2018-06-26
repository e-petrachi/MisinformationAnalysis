package spark.post_analysis;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import scala.Tuple2;
import scala.Tuple3;
import spark.model.post_model.MentionsGroup;
import spark.model.post_model.HashtagsGroup;
import spark.utilities.MongoRDDLoader;

public class Communities {

    private static final Logger LOG = Logger.getLogger(Communities.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static Tuple3<JavaMongoRDD<Document>, JavaMongoRDD<Document>, JavaSparkContext> loadDocument() {
        MongoRDDLoader ml = new MongoRDDLoader("bigdata","hashtagsgroup","bigdata","communities");

        JavaSparkContext jsc = ml.getSparkContext();

        JavaMongoRDD<Document> uno = ml.getMongoRDD(jsc);
        JavaMongoRDD<Document> due = ml.getMongoRDDOverride(jsc, "bigdata", "mentionsgroup");

        return new Tuple3<>(uno,due,jsc);
    }

    public static void execute(JavaRDD<HashtagsGroup> rdd_1, JavaRDD<MentionsGroup> rdd_2, JavaSparkContext jsc) {

        JavaPairRDD<String,Integer> r = rdd_1
                .mapToPair( a -> new Tuple2<>(a.getHashtag(),a.getSize()));

        JavaPairRDD<String,Integer> s = rdd_2
                .mapToPair( a -> new Tuple2<>(a.getMention(),a.getSize()));

        JavaRDD<Document> mongordd_1 = r
                .map(a -> Document.parse("{'hashtag': '" + a._1() +
                        "', 'size': " + a._2() +
                        "}"));

        MongoSpark.save(mongordd_1, WriteConfig.create(jsc));

        JavaRDD<Document> mongordd_2 = s
                .map(a -> Document.parse("{'mention': '" + a._1() +
                        "', 'size': " + a._2() +
                        "}"));

        MongoSpark.save(mongordd_2, WriteConfig.create(jsc));
    }

    public static void main(String[] args){
        Tuple3<JavaMongoRDD<Document>,JavaMongoRDD<Document>, JavaSparkContext> rdd_rdd2jsc = loadDocument();

        JavaSparkContext jsc = rdd_rdd2jsc._3();
        JavaRDD<HashtagsGroup> rdd_1 = rdd_rdd2jsc._1().map(doc -> new HashtagsGroup(doc));
        JavaRDD<MentionsGroup> rdd_2 = rdd_rdd2jsc._2().map(doc -> new MentionsGroup(doc));

        execute(rdd_1, rdd_2, jsc);

        jsc.close();

    }
}

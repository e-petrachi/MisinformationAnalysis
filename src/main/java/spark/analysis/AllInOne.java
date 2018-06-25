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

public class AllInOne {

    private static final Logger LOG = Logger.getLogger(Fonts.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static Tuple2<JavaRDD<QueryResult>, JavaSparkContext> loadDocument() {
        MongoRDDLoader ml = new MongoRDDLoader();
        return ml.openloader(doc -> {
            return new QueryResult(doc, Constant.fonts);
        });
    }

    public static void main(String[] args){
        Tuple2<JavaRDD<QueryResult>, JavaSparkContext> rdd2jsc = loadDocument();

        JavaSparkContext jsc = rdd2jsc._2();
        JavaRDD<QueryResult> rdd = rdd2jsc._1();

        Polarity.execute(rdd, jsc);
        Fonts.execute(rdd, jsc);
        SocialBot.execute(rdd, jsc);
        HashtagsGroup.execute(rdd, jsc);
        MentionsGroup.execute(rdd, jsc);

        jsc.close();
    }
}

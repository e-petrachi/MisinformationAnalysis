package spark.analysis;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
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

    public static Tuple2<JavaMongoRDD<Document>, JavaSparkContext> loadDocument() {
        MongoRDDLoader ml = new MongoRDDLoader();
        return ml.openloaderDocument();
    }

    public static void main(String[] args){
        Tuple2<JavaMongoRDD<Document>, JavaSparkContext> rdd2jsc = loadDocument();

        JavaSparkContext jsc = rdd2jsc._2();
        JavaMongoRDD<Document> rdd = rdd2jsc._1();

        JavaRDD<QueryResult> rdd_qr_p = rdd.map(doc -> { return new QueryResult(doc, Constant.polarity); });
        JavaRDD<QueryResult> rdd_qr_f = rdd.map(doc -> { return new QueryResult(doc, Constant.fonts); });
        JavaRDD<QueryResult> rdd_qr_sb = rdd.map(doc -> { return new QueryResult(doc, Constant.socialbot); });
        JavaRDD<QueryResult> rdd_qr_hg = rdd.map(doc -> { return new QueryResult(doc, Constant.hashtagsgroup); });
        JavaRDD<QueryResult> rdd_qr_mg = rdd.map(doc -> { return new QueryResult(doc, Constant.mentionsgroup); });

        Polarity.execute(rdd_qr_p, jsc);
        Fonts.execute(rdd_qr_f, jsc);
        SocialBot.execute(rdd_qr_sb, jsc);
        HashtagsGroup.execute(rdd_qr_hg, jsc);
        MentionsGroup.execute(rdd_qr_mg, jsc);

        jsc.close();
    }
}

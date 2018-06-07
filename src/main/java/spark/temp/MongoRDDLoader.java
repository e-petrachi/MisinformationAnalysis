package spark.temp;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import spark.model.QueryResult;

public class MongoRDDLoader {

    private static final Logger LOG = Logger.getLogger(MongoRDDLoader.class);
    static { LOG.setLevel(Level.DEBUG);}

    public JavaRDD<QueryResult> loader(){
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MisinformationAnalysis")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/fakenewsnetwork.matteodb")
                //.config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/fakenewsnetwork.temp")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

        JavaRDD<QueryResult> rdd_qr = rdd.map(doc -> {
            return new QueryResult(doc);
        });

        LOG.debug(rdd_qr.first().toString());

        jsc.close();

        return rdd_qr;
    }
}

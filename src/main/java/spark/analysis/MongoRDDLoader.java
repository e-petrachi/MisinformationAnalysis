package spark.analysis;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import scala.Tuple2;
import spark.model.QueryResult;

import org.apache.spark.api.java.function.Function;

public class MongoRDDLoader {

    private static final Logger LOG = Logger.getLogger(MongoRDDLoader.class);
    static { LOG.setLevel(Level.DEBUG);}

    ///*
    private static final String dbname_i = "bigdata";
    //private static final String coll_input = "mini";
    private static final String coll_input = "dataset";
    //*/
    ///*
    //private static final String dbname_i = "fakenewsnetwork";
    //private static final String coll_input = "matteodb";
    //*/

    private static final String dbname_o = "bigdata";
    private static final String coll_output = "temp";

    public Tuple2<JavaRDD<QueryResult>, JavaSparkContext> openloader(Function<Document, QueryResult> function){
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MisinformationAnalysis")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/" + dbname_i + "." + coll_input)
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/" + dbname_o + "." + coll_output)
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

        JavaRDD<QueryResult> rdd_qr = rdd.map(function);

        return new Tuple2<>(rdd_qr,jsc);
    }

    public Tuple2<JavaMongoRDD<Document>, JavaSparkContext> openloaderDocument(){
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MisinformationAnalysis")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/" + dbname_i + "." + coll_input)
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/" + dbname_o + "." + coll_output)
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

        return new Tuple2<>(rdd, jsc);
    }
}

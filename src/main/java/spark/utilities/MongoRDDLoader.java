package spark.utilities;

import com.mongodb.spark.config.ReadConfig;
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
import spark.model.post_model.HashtagsGroup;

public class MongoRDDLoader {

    private static final Logger LOG = Logger.getLogger(MongoRDDLoader.class);
    static { LOG.setLevel(Level.DEBUG);}


    private String dbname_i = "bigdata";
    private String dbname_o = "bigdata";
    private String coll_input = "dataset";
    private String coll_output = "temp";

    public MongoRDDLoader(){ }

    public MongoRDDLoader(String dbname_i, String coll_input, String dbname_o, String coll_output) {
        this.dbname_i = dbname_i;
        this.dbname_o = dbname_o;
        this.coll_input = coll_input;
        this.coll_output = coll_output;
    }

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

    public JavaSparkContext getSparkContext(){
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MisinformationAnalysis")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/" + dbname_i + "." + coll_input)
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/" + dbname_o + "." + coll_output)
                .getOrCreate();

        return new JavaSparkContext(spark.sparkContext());
    }

    public JavaMongoRDD<Document> getMongoRDDOverride(JavaSparkContext jsc, String db_input, String collection_input){
        ReadConfig rc = ReadConfig.create(jsc).withOption("database", db_input).withOption("collection", collection_input);

        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc, rc);

        return rdd;
    }
    public JavaMongoRDD<Document> getMongoRDD(JavaSparkContext jsc){
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

        return rdd;
    }
}

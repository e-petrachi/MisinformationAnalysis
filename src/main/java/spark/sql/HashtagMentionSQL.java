package spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import scala.Tuple2;
import spark.utilities.MongoRDDLoader;


public class HashtagMentionSQL {

    private static final Logger LOG = Logger.getLogger(HashtagMentionSQL.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static Tuple2<Dataset<Row>, SQLContext> loadDocument() {
        MongoRDDLoader ml = new MongoRDDLoader("fakenewsnetwork","matteodb","","");
        return ml.openloaderSQL();
    }

    public static void main(String[] args){

        Tuple2<Dataset<Row>, SQLContext> rdd2jsc = loadDocument();

        execute(rdd2jsc._1(), rdd2jsc._2());

        rdd2jsc._2().sparkSession().close();
    }

    public static void execute(Dataset<Row> dataset, SQLContext sc) {

        Dataset<Row> ds_1 = dataset
                .select(functions.explode(dataset.col("Tweet.hashtagEntities.text")))
                .distinct()
                .orderBy("col");

        ds_1.show();

        Dataset<Row> ds_2 = ds_1
                .groupBy()
                .count();
        ds_2.show();

        Dataset<Row> ds_3 = dataset
                .select(functions.explode(dataset.col("Tweet.userMentionEntities.screenName")))
                .distinct()
                .orderBy("col");

        ds_3.show();

        Dataset<Row> ds_4 = ds_3
                .groupBy()
                .count();

        ds_4.show();

    }
}

package spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import scala.Tuple2;
import spark.utilities.MongoRDDLoader;


public class CommunitiesSQL {

    private static final Logger LOG = Logger.getLogger(CommunitiesSQL.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static Tuple2<Dataset<Row>, SQLContext> loadDocument() {
        MongoRDDLoader ml = new MongoRDDLoader("bigdata","communities","","");
        return ml.openloaderSQL();
    }

    public static void main(String[] args){

        Tuple2<Dataset<Row>, SQLContext> rdd2jsc = loadDocument();

        execute(rdd2jsc._1(), rdd2jsc._2());

        rdd2jsc._2().sparkSession().close();
    }

    public static void execute(Dataset<Row> dataset, SQLContext sc) {

        Dataset<Row> ds_1 = dataset
                .selectExpr("size", "hashtags_mentions", "polarity")
                .orderBy(org.apache.spark.sql.functions.desc("size"))
                .distinct();

        ds_1.show(20, false);

    }
}

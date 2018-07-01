package spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import scala.Tuple2;
import spark.utilities.MongoRDDLoader;

/**
 * Quanti utenti hanno condiviso contenuti provenienti da fonti mainstream o di misinformation
 */

public class FontsSQL {

    private static final Logger LOG = Logger.getLogger(FontsSQL.class);
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

        dataset.createOrReplaceTempView("fonts");

        Dataset<Row> ds_1 = sc.sql("SELECT type_page AS polarity, COUNT(type_page) AS tweets FROM fonts GROUP BY type_page");
        ds_1.show();

        Dataset<Row> ds_2 = dataset.select("Tweet.User.id","type_page").distinct().groupBy("type_page").count();
        ds_2.show();

    }
}

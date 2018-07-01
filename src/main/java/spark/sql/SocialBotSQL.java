package spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import scala.Tuple2;
import spark.utilities.MongoRDDLoader;

/**
 * Riuso degli stessi hashtag/mention per utente
 * (utili per identificare eventuali social bot)
 *
 * Filtro utenti di cui ho almeno 6 tweet
 */


public class SocialBotSQL {

    private static final Logger LOG = Logger.getLogger(SocialBotSQL.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static Tuple2<Dataset<Row>, SQLContext> loadDocument() {
        MongoRDDLoader ml = new MongoRDDLoader("bigdata","socialbot","","");
        return ml.openloaderSQL();
    }

    public static void main(String[] args){

        Tuple2<Dataset<Row>, SQLContext> rdd2jsc = loadDocument();

        execute(rdd2jsc._1(), rdd2jsc._2());

        rdd2jsc._2().sparkSession().close();
    }

    public static void execute(Dataset<Row> dataset, SQLContext sc) {

        Dataset<Row> ds_1 = dataset
                .selectExpr("user", "EXPLODE(hashtags.percentage) AS percentage")
                .where("percentage >= 50")
                .drop("percentage")
                .distinct();

        ds_1.show();

        Dataset<Row> ds_2 = dataset
                .selectExpr("user", "EXPLODE(mentions.percentage) AS percentage")
                .where("percentage >= 50")
                .drop("percentage")
                .distinct();

        ds_2.show();

        Dataset<Row> ds = ds_1.union(ds_2)
                .distinct()
                .groupBy()
                .count();

        long all = ds.first().getLong(0);

        LOG.info("UTENTI CON RIUSO >= 50% TROVATI: " + all);

        Dataset<Row> ds_11 = dataset
                .selectExpr("user", "EXPLODE(hashtags.percentage) AS percentage")
                .where("percentage >= 75")
                .drop("percentage")
                .distinct();

        Dataset<Row> ds_22 = dataset
                .selectExpr("user", "EXPLODE(mentions.percentage) AS percentage")
                .where("percentage >= 75")
                .drop("percentage")
                .distinct();

        Dataset<Row> dss = ds_11.union(ds_22)
                .distinct()
                .groupBy()
                .count();

        all = dss.first().getLong(0);

        LOG.info("UTENTI CON RIUSO >= 75% TROVATI: " + all);

    }
}

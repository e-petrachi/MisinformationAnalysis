package spark.model;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.json.JSONObject;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;


public class QueryResult implements Serializable {
    private long id_tweet;
    private Date postDate;
    private String type_page;
    private Tweet Tweet;

    private static final Logger LOG = Logger.getLogger(QueryResult.class);
    static { LOG.setLevel(Level.DEBUG);}

    public QueryResult(Document doc) {
        JSONObject json = new JSONObject(doc.toJson());
        setId_tweet(json.getJSONObject(Constant.id_tweet));
        setPostDate(json.optJSONObject(Constant.postDate));
        setType_page(json.getString(Constant.type_page));
        //setTweet(json.getJSONObject(Constant.Tweet));

    }

    public long getId_tweet() {
        return id_tweet;
    }

    public void setId_tweet(JSONObject id_tweet) {
        this.id_tweet = id_tweet.getLong(Constant.numberLong);
    }

    public Date getPostDate() {
        return postDate;
    }

    public void setPostDate(JSONObject postDate) {
        Date date = null;
        try {
            date = new SimpleDateFormat("MMM dd, yyyy HH:mm:ss a", Locale.ENGLISH).parse(postDate.get(Constant.date).toString());
            LOG.info("8=================================================> ." + date);
        } catch (Exception e1) {
            //LOG.error("DATE_ERROR" + this.id_tweet + "" + e1.getMessage());
            try {
                date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.ENGLISH).parse(postDate.get(Constant.date).toString());
                LOG.info("8==========================================================> . ." + date);
            } catch (Exception e2){
                //LOG.error("DATE_ERROR" + this.id_tweet + "" + e2.getMessage());
                try {
                    date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.ENGLISH).parse(postDate.get(Constant.date).toString());
                    LOG.info("8===============================================================> . . ." + date);
                } catch (Exception e3){
                    LOG.error("DATE_ERROR" + this.id_tweet + "" + e3.getMessage());
                }
            }
        }
        this.postDate = date;
    }

    public String getType_page() {
        return type_page;
    }

    public void setType_page(String type_page) {
        this.type_page = type_page;
    }

    public spark.model.Tweet getTweet() {
        return Tweet;
    }

    public void setTweet(JSONObject tweet) {
        //Tweet = new Tweet(tweet);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryResult that = (QueryResult) o;

        return id_tweet == that.id_tweet;
    }

    @Override
    public String toString() {
        return "QueryResult{" +
                "id_tweet=" + id_tweet +
                ", postDate=" + postDate +
                ", type_page='" + type_page + '\'' +
                '}';
    }
}

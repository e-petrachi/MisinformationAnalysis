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
    private String url_tweet;

    private static final Logger LOG = Logger.getLogger(QueryResult.class);
    static { LOG.setLevel(Level.INFO);}

    public QueryResult(Document doc) {
        JSONObject json = new JSONObject(doc.toJson());
        setId_tweet(json.getJSONObject(Constant.id_tweet));
        setPostDate(json.optJSONObject(Constant.postDate),json.optJSONObject(Constant.tweetDate));
        setType_page(json.getString(Constant.type_page));
        setTweet(json.getJSONObject(Constant.Tweet));
        setUrl_tweet(json.optString(Constant.url_tweet));

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

    public void setPostDate(JSONObject postDate, JSONObject tweetDate) {
        Date date = null;
        try {
            date = new Date(postDate.optLong(Constant.date));
            LOG.debug("\t\t" + date);
        } catch (Exception e) {
            date = new Date(tweetDate.optLong(Constant.date));
            LOG.debug("\t\t" + date);
        }
        if (date != null)
            this.postDate = date;
        else
            this.postDate = new Date(0L);
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
        Tweet = new Tweet(tweet);
    }

    public String getUrl_tweet() {
        return url_tweet;
    }

    public void setUrl_tweet(String url_tweet) {
        this.url_tweet = url_tweet;
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
                ", Tweet=" + Tweet +
                '}';
    }
}

package spark.model;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import spark.SocialBot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;

public class Tweet implements Serializable {
    private String text;
    private String source;
    private boolean isFavorited;
    private boolean isRetweeted;
    private int favouriteCount;
    private int retweetCount;
    private ArrayList<User> userMentionEntities;
    private ArrayList<String> hashtagEntities;
    private User user;

    private static final Logger LOG = Logger.getLogger(Tweet.class);
    static { LOG.setLevel(Level.DEBUG);}

    public Tweet(JSONObject tweet) {
        this.setText(tweet.getString(Constant.text));
        this.setSource(tweet.optString(Constant.source));
        this.setFavorited(tweet.optBoolean(Constant.isFavorited));
        this.setRetweeted(tweet.optBoolean(Constant.isRetweeted));
        this.setFavouriteCount(tweet.optInt(Constant.favouriteCount));
        this.setRetweetCount(tweet.optInt(Constant.retweetCount));
        this.setUserMentionEntities(tweet.optJSONArray(Constant.userMentionEntities));
        this.setHashtagEntities(tweet.optJSONArray(Constant.hashtagEntities));
        this.setUser(tweet.optJSONObject(Constant.user));
    }

    public Tweet(JSONObject tweet, String analysis) {
        switch (analysis) {
            case Constant.polarity:
            case Constant.fonts: {
                this.setUser(tweet.optJSONObject(Constant.user),analysis);
                break;
            }
            case Constant.socialbot: {
                this.setUser(tweet.optJSONObject(Constant.user),analysis);
                this.setHashtagEntities(tweet.optJSONArray(Constant.hashtagEntities));
                this.setUserMentionEntities(tweet.optJSONArray(Constant.userMentionEntities));
                break;
            }
            case Constant.hashtagsgroup: {
                this.setUser(tweet.optJSONObject(Constant.user),analysis);
                this.setHashtagEntities(tweet.optJSONArray(Constant.hashtagEntities));
                break;
            }
            case Constant.mentionsgroup: {
                this.setUser(tweet.optJSONObject(Constant.user),analysis);
                this.setUserMentionEntities(tweet.optJSONArray(Constant.userMentionEntities));
                break;
            }
            default: {
                new Tweet(tweet);
                break;
            }
        }
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public boolean isFavorited() {
        return isFavorited;
    }

    public void setFavorited(boolean favorited) {
        isFavorited = favorited;
    }

    public boolean isRetweeted() {
        return isRetweeted;
    }

    public void setRetweeted(boolean retweeted) {
        isRetweeted = retweeted;
    }

    public int getFavouriteCount() {
        return favouriteCount;
    }

    public void setFavouriteCount(int favouriteCount) {
        this.favouriteCount = favouriteCount;
    }

    public int getRetweetCount() {
        return retweetCount;
    }

    public void setRetweetCount(int retweetCount) {
        this.retweetCount = retweetCount;
    }

    public ArrayList<User> getUserMentionEntities() {
        return userMentionEntities;
    }

    public void setUserMentionEntities(JSONArray userMentionEntities) {
        this.userMentionEntities = new ArrayList<>();
        for (int i=0;i<userMentionEntities.length();i++){
            this.userMentionEntities.add(new User(userMentionEntities.getJSONObject(i),false));
        }
    }

    public ArrayList<String> getHashtagEntities() {
        return hashtagEntities;
    }

    public void setHashtagEntities(JSONArray hashtagEntities) {
        this.hashtagEntities = new ArrayList<>();
        for (int i=0;i<hashtagEntities.length();i++){
            this.hashtagEntities.add(hashtagEntities.getJSONObject(i).getString("text").toLowerCase());

        }
    }

    public User getUser() {
        return user;
    }

    public void setUser(JSONObject user) {
        this.user = new User(user,true);
    }

    public void setUser(JSONObject user, String analysis) {
        this.user = new User(user,true, analysis);
    }

    @Override
    public String toString() {
        return "Tweet{" +
                "text='" + text + '\'' +
                ", source='" + source + '\'' +
                ", isFavorited=" + isFavorited +
                ", isRetweeted=" + isRetweeted +
                ", favouriteCount=" + favouriteCount +
                ", retweetCount=" + retweetCount +
                ", userMentionEntities=" + userMentionEntities +
                ", hashtagEntities=" + hashtagEntities +
                ", user=" + user +
                '}';
    }
}

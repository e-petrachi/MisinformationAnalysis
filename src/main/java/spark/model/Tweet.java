package spark.model;

import org.json.JSONArray;
import org.json.JSONObject;

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
            this.hashtagEntities.add(hashtagEntities.getString(i));
        }
    }

    public User getUser() {
        return user;
    }

    public void setUser(JSONObject user) {
        this.user = new User(user,true);
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

package spark.model;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.Date;

public class User implements Serializable {
    private long id;
    private String name;
    private String screenName;
    private String location;
    private int followersCount;
    private int friendsCount;
    private int favouritesCount;
    private int statusesCount;
    private int listedCount;

    private static final Logger LOG = Logger.getLogger(User.class);
    static { LOG.setLevel(Level.DEBUG);}

    public User(JSONObject o, boolean complete) {

        try {
            this.setId(o.optLong(Constant.id));
        } catch (Exception e){
            this.setId(o.optJSONObject(Constant.id).getLong(Constant.numberLong));
        }

        this.setName(o.getString(Constant.name));
        this.setScreenName(o.getString(Constant.screenName));
        if (complete){
            this.setLocation(o.getString(Constant.location));
            this.setFollowersCount(o.getInt(Constant.followersCount));
            this.setFriendsCount(o.getInt(Constant.friendsCount));
            this.setFavouritesCount(o.getInt(Constant.favouritesCount));
            this.setStatusesCount(o.getInt(Constant.statusesCount));
            this.setListedCount(o.getInt(Constant.listedCount));
        }
    }

    public User(JSONObject o, boolean complete, String analysis) {
        switch (analysis) {
            case Constant.polarity:
            case Constant.fonts:
            case Constant.socialbot: {
                try {
                    this.setId(o.getLong(Constant.id));
                    if (this.getId() == 0L) {
                        this.setId((long) o.getInt(Constant.id));
                    }
                } catch (Exception e){
                    this.setId(o.optJSONObject(Constant.id).getLong(Constant.numberLong));
                }

                this.setScreenName(o.getString(Constant.screenName));
                break;
            }
            default: {
                new User(o,complete);
                break;
            }
        }
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getScreenName() {
        return screenName;
    }

    public void setScreenName(String screenName) {
        this.screenName = screenName;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public int getFollowersCount() {
        return followersCount;
    }

    public void setFollowersCount(int followersCount) {
        this.followersCount = followersCount;
    }

    public int getFriendsCount() {
        return friendsCount;
    }

    public void setFriendsCount(int friendsCount) {
        this.friendsCount = friendsCount;
    }

    public int getFavouritesCount() {
        return favouritesCount;
    }

    public void setFavouritesCount(int favouritesCount) {
        this.favouritesCount = favouritesCount;
    }

    public int getStatusesCount() {
        return statusesCount;
    }

    public void setStatusesCount(int statusesCount) {
        this.statusesCount = statusesCount;
    }

    public int getListedCount() {
        return listedCount;
    }

    public void setListedCount(int listedCount) {
        this.listedCount = listedCount;
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", screenName='" + screenName + '\'' +
                ", location='" + location + '\'' +
                ", followersCount=" + followersCount +
                ", friendsCount=" + friendsCount +
                ", favouritesCount=" + favouritesCount +
                ", statusesCount=" + statusesCount +
                ", listedCount=" + listedCount +
                '}';
    }
}

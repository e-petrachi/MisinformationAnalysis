package spark.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;

public class Tweet implements Serializable {
    private Date createdAt;
    private long id;
    private String text;
    private String source;
    private boolean isFavorited;
    private boolean isRetweeted;
    private int favouriteCount;
    private int retweetCount;
    private ArrayList<User> userMentionEntities;
    private ArrayList<Url> urlEntities;
    private ArrayList<Hashtag> hashtagEntities;
    private User user;
    private Tweet retweetedStatus;
}

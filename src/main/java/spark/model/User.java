package spark.model;

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
    private Date createdAt;
    private String timeZone;
    private int statusesCount;
    private int listedCount;
}

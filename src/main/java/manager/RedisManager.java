package manager;

import redis.clients.jedis.Jedis;

import java.util.Properties;

public class RedisManager {

    // init the Java Redis client
    private static Jedis jedis;
    static Properties prop;
    static{
        try{
            prop = new Properties();
            prop.load(RedisManager.class.getClassLoader().getResourceAsStream("redis.properties"));
            String host = prop.getProperty("redis.host");
            int port = Integer.parseInt(prop.getProperty("redis.port"));

            jedis = new Jedis(host,port);
        } catch(Exception e){
            System.err.println("Failed to connect to Redis: " + e.getMessage());
             throw new IllegalStateException(e);
        }
    }

    public boolean createGroup(String groupName, String ServerID){
        if (groupExists(groupName)) return false;

        jedis.set("group:"+groupName+":serverid",ServerID); // Create an empty group
        return true;
    }

    public boolean joinGroup(String groupName, String username, String ServerID){
        if (!groupExists(groupName)) return false;

        jedis.sadd("group:"+groupName+":members",username+":"+ServerID);
        return true;
    }

    public boolean leaveGroup(String groupName, String username){
        if (!groupExists(groupName)) return false;

        jedis.srem("group:"+groupName+":members",username);
        return true;
    }

    public String getGroupServerID(String groupName){
        if (!groupExists(groupName)) return "";

        return jedis.get("group:"+groupName+":serverid");
    }

    public boolean groupExists(String groupName){
        return jedis.exists("group:"+groupName+":serverid");
    }

    public boolean isGroupCrossServer(String groupName, String ServerID){
        if (!groupExists(groupName)) return false;
        return jedis.smembers("group:"+groupName+":members").stream().
                anyMatch(member -> !member.endsWith(":"+ServerID));
    }
}

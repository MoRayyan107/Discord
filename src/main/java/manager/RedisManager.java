package manager;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Arrays;
import java.util.Properties;

public class RedisManager {

    private static JedisPool jedisPool;

    // init the Java Redis client
    static Properties prop;

    static {
        try{
            prop = new Properties();
            prop.load(RedisManager.class.getClassLoader().getResourceAsStream("redis.properties"));
            String host = prop.getProperty("redis.host");
            int port = Integer.parseInt(prop.getProperty("redis.port"));

            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(Integer.parseInt(prop.getProperty("redis.maxTotal")));

            jedisPool = new JedisPool(config, host, port);
        } catch(Exception e){
            System.err.println("Failed to connect to Redis: " + e.getMessage());
            throw new IllegalStateException(e);
        }
    }



    // ------------------------------------------ GROUP FUNCTIONS ----------------------------------------------
    public boolean createGroup(String groupName, String ServerID){
        try (Jedis jedis = jedisPool.getResource()) {

            if (groupExists(groupName)) return false;
            jedis.set("group:"+groupName+":serverid",ServerID); // Create an empty group

            return true;
        }
    }

    public boolean joinGroup(String groupName, String username, String ServerID){
        try(Jedis jedis = jedisPool.getResource()) {

            if (!groupExists(groupName)) return false;

            jedis.sadd("group:"+groupName+":members",username+":"+ServerID);

            return true;
        }
    }

    public boolean leaveGroup(String groupName, String username, String ServerID){
        try(Jedis jedis = jedisPool.getResource()) {

            if (!groupExists(groupName)) return false;

            jedis.srem("group:"+groupName+":members",username+":"+ServerID);
            return true;
        }
    }

    public String getGroupServerID(String groupName){
        try(Jedis jedis = jedisPool.getResource()) {

            if (!groupExists(groupName)) return "";

            return jedis.get("group:"+groupName+":serverid");
        }
    }

    public boolean deleteGroup(String groupName, String ServerID){
        try(Jedis jedis = jedisPool.getResource()) {

            if (!groupExists(groupName)) return false;

            jedis.del("group:"+groupName+":members"); // kick all the members
            jedis.del("group:"+groupName+":serverid"); // delete the group
            return true;
        }
    }

    public boolean groupExists(String groupName){
        try (Jedis jedis = jedisPool.getResource()) {

            return jedis.exists("group:"+groupName+":serverid");
        }
    }

    public boolean isGroupCrossServer(String groupName, String ServerID){
        try(Jedis jedis = jedisPool.getResource()) {

            if (groupName.isEmpty() || !groupExists(groupName)) return false;

            return jedis.smembers("group:"+groupName+":members").stream().
                    anyMatch(member -> !member.endsWith(":"+ServerID));
        }
    }

    // ------------------------------------------- USER-SERVER TRACKING -----------------------------------------------
    public void setUserServer(String username, String serverID) {
        try(Jedis jedis = jedisPool.getResource()) {
            jedis.set("user:" + username + ":server", serverID);
        }
    }

    public String getUserServer(String username) {
        try(Jedis jedis = jedisPool.getResource()) {
            return jedis.get("user:" + username + ":server");
        }
    }

    public void removeUserServer(String username) {
        try(Jedis jedis = jedisPool.getResource()) {
            jedis.del("user:" + username + ":server");
        }
    }

    // ------------------------------------------- DM FUNCTIONS -----------------------------------------------
    // sort the usernames so that we keep one key pair for everyones DM rather than creating duplicate keys for one DM
    // DM key is stored in redis -> dm:userA:userB (sorted by username)

    public static boolean createDirectMessage(String senderUsername, String receiverUsername){
        try(Jedis jedis = jedisPool.getResource()){

            String[] userss = {senderUsername,receiverUsername};
            Arrays.sort(userss);

            jedis.set("dm:"+userss[0]+":"+userss[1], "DM_exists");
            return true;
        }
    }

    public static boolean directMessageExists(String senderUsername, String receiverUsername){
        try(Jedis jedis = jedisPool.getResource()){

            String[] userss = {senderUsername,receiverUsername};
            Arrays.sort(userss);

            return jedis.exists("dm:"+userss[0]+":"+userss[1]);
        }
    }

    public static boolean deleteDirectMessage(String senderUsername, String receiverUsername){
        try(Jedis jedis = jedisPool.getResource()){

            String[] userss = {senderUsername,receiverUsername};
            Arrays.sort(userss);

            jedis.del("dm:"+userss[0]+":"+userss[1]);
            return true;
        }
    }

}

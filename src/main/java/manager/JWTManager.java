package manager;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;

import java.util.Properties;

public class JWTManager {

    private static String SECRET_KEY;
    private static String ISSUER;
    private static Algorithm ALGORITHM;
    private static long EXPIRATION_TIME; // in ms

    static Properties props;
    static{
        try{
            props = new Properties();
            props.load(JWTManager.class.getClassLoader().getResourceAsStream("jwt.properties"));
            SECRET_KEY = props.getProperty("discord.secretKey");
            ISSUER = props.getProperty("discord.issuer");
            String AlgorithmConfig = props.getProperty("discord.algorithm");
            EXPIRATION_TIME = Integer.parseInt(props.getProperty("discord.expirationTime"));

            if (ISSUER == null || ISSUER.isEmpty())
                throw new IllegalArgumentException("Property discord.Issuer is missing");

            if (AlgorithmConfig == null ||  AlgorithmConfig.isEmpty())
                throw new IllegalArgumentException("property discord.algorithm is missing");

            switch (AlgorithmConfig){
                case "HMAC256" -> ALGORITHM = Algorithm.HMAC256(SECRET_KEY);
                case "HMAC512" -> ALGORITHM = Algorithm.HMAC512(SECRET_KEY);
                default -> throw new IllegalArgumentException("Unsupported algorithm: " + AlgorithmConfig);
            }

        } catch(Exception e){
            throw new IllegalArgumentException("Failed to initialize JWTManager: " + e.getMessage(), e);
        }
    }

    private static final JWTVerifier VERIFIER = JWT.require(ALGORITHM)
            .withIssuer(ISSUER)
            .build();

    public static String generateToken(String username){
        return JWT.create()
                .withSubject(username)
                .withIssuer(ISSUER)
                .withExpiresAt(new java.util.Date(System.currentTimeMillis() + EXPIRATION_TIME))
                .sign(ALGORITHM);
    }

    public static String verifyTokenAndFetchUsername(String token){
        try{
            return VERIFIER.verify(token).getSubject();
        } catch (Exception e){
            System.err.println("Failed to verify token: " + e.getMessage());
            return null;
        }
    }
}

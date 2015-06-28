package services.database;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.mongodb.morphia.Morphia;
import play.Play;

import java.net.UnknownHostException;

/**
 * Created by Kris on 2015-05-07.
 */
public class DBServicesProvider {
    static DBVisitDAO dbVisitDAO = null;
    static MongoClientURI mongoClientURI;
    static MongoClient mongoClient;
    static Morphia morphia;
    static String uriString;
    static String dbName;

    static {
        uriString = Play.application().configuration().getString("mongo.uri");
        dbName = Play.application().configuration().getString("mongo.db");
        try {
            mongoClientURI = new MongoClientURI(uriString);
            mongoClient = new MongoClient(mongoClientURI);
            morphia = new Morphia();
            morphia.getMapper().getOptions().setStoreEmpties(true);
            morphia.getMapper().getOptions().setStoreNulls(true);
        } catch (UnknownHostException ex) {
            ex.printStackTrace();
        }
    }

    public static DBVisitDAO getDbVisitDAO() {
        if (dbVisitDAO == null) {
            dbVisitDAO = new DBVisitDAO(mongoClient, morphia, dbName);
        }
        return dbVisitDAO;
    }

}

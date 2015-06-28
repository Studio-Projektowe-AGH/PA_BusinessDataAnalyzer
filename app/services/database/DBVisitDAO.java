package services.database;

import com.mongodb.MongoClient;
import models.FinishedVisit;
import org.bson.types.ObjectId;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.dao.BasicDAO;

public class DBVisitDAO extends BasicDAO<FinishedVisit, ObjectId>{

    protected DBVisitDAO(MongoClient mongoClient, Morphia morphia, String dbName) {
        super(mongoClient, morphia, dbName);
    }

}

package models;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

/**
 * Created by Pine on 27/06/15.
 * {
 "_id": {
 "$oid": "55885f16e4b04eaf19cff720"
 },
 "user_id": "557565f8e4b0e1b46827377a",
 "item_id": "554f0f69e4b0d915eeee3bc2",
 "visit_start": 1435000490000,
 "visit_end": 1435014890000,
 "qr_scanned": 5,
 "preference": 4
 }
 */
@Entity(value = "finishedVisits", noClassnameStored=true)
public class FinishedVisit {

    @Id
    private ObjectId id;

    public String user_id;
    public String item_id;
    public long visit_start;
    public long visit_end;
    public int qr_scanned;
    public int preference;


    public ObjectId getId() {
        return id;
    }

    public String getUser_id() {
        return user_id;
    }

    public String getClub_id() {
        return item_id;
    }

    public long getVisit_start() {
        return visit_start;
    }

    public long getVisit_end() {
        return visit_end;
    }

    public int getQr_scanned() {
        return qr_scanned;
    }

    public int getRating() {
        return preference;
    }

    @Override
    public String toString() {
        return "FinishedVisit{" +
                "id=" + id +
                ", user_id='" + user_id + '\'' +
                ", item_id='" + item_id + '\'' +
                ", visit_start=" + visit_start +
                ", visit_end=" + visit_end +
                ", qr_scanned=" + qr_scanned +
                ", preference=" + preference +
                '}';
    }
}

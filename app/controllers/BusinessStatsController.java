package controllers;

import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import models.FinishedVisit;
import org.joda.time.DateTime;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.format.DateTimeFormat;
import org.json.JSONArray;
import org.json.JSONObject;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.query.Query;
import play.libs.Json;
import play.mvc.BodyParser;
import play.mvc.Controller;
import play.mvc.Result;
import services.database.DBServicesProvider;
import services.database.DBVisitDAO;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Pine on 27/06/15.
 */
public class BusinessStatsController extends Controller {
    static final int MAX_REQUEST_COUNT = 200;
    static DBVisitDAO dbVisitDAO = DBServicesProvider.getDbVisitDAO();
    static Morphia morphia = new Morphia();

    static class Tuple {

        List val;
        List time;

        public Tuple(List val, List time) {
            this.val = val;
            this.time = time;
        }
    }

    static {
        morphia.map(FinishedVisit.class).getMapper().getOptions().setStoreNulls(true);
    }

    public static Result getAll(String userId) {

        List<FinishedVisit> visits = dbVisitDAO.getDatastore().find(FinishedVisit.class).field("club_id").equal(userId).asList();

        if (!visits.isEmpty()) {
            String s = "";
            for (FinishedVisit v : visits) {
                s += Json.toJson(v);
            }
            return ok(s);

        }
        return notFound("no club with this id");
    }

    private static DateTime getDate(String s, String club_id) throws Exception {

        if (s.equals("big_bang")) {

            Query q = dbVisitDAO.getDatastore().createQuery(FinishedVisit.class);
            q.field("club_id").equal(club_id);
            q.order("visit_start");
            q.limit(1);
            FinishedVisit f = (FinishedVisit) q.get();
            if (f == null)
                throw new Exception(Json.newObject().put("status", "error").put("reason", "there are no stats for this club in specified time range, sorry.").toString());
            return new DateTime(((FinishedVisit) q.get()).getVisit_start());
        }
        if (s.length() == 10)
            s += " 00:00";
        return DateTime.parse(s, DateTimeFormat.forPattern("YYYY-MM-dd HH:mm"));
    }

    private static DateTime incrementDateBy(DateTime date, String order) {
        int i = 1;
        switch (order) {
            case "hour":
                return date.plusHours(i);
            case "day":
                return date.plusDays(i);
            case "month":
                return date.plusMonths(i);
            case "week":
                return date.plusWeeks(i);
            case "year":
                return date.plusYears(i);
            case "all_time":
                return date;
            default:
                return date.minusDays(1);
        }
    }

    private static void filterRequest(JsonNode jsonBody, String club_id) throws Exception {

        DateTime fromGlobal;
        DateTime toGlobal;
        try {
            fromGlobal = getDate(jsonBody.get("date_from").asText(), club_id);
            System.out.println("date from is " + fromGlobal);
            toGlobal = getDate(jsonBody.get("date_to").asText(), club_id);
        } catch (Exception ex) {
            throw new Exception(Json.newObject().put("status", "error").put("reason", "bad date format").toString());
        }

        if (fromGlobal.isAfter(toGlobal)) {
            throw new Exception(Json.newObject().put("status", "error").put("reason", "date_from cannot be greater than date_to").toString());
        }

        String aggregate;
        try {
            aggregate = jsonBody.get("aggregate").asText();
        } catch (Exception ex) {
            throw new Exception(Json.newObject().put("status", "error").put("reason", "missing aggregate parameter").toString());
        }
        if (incrementDateBy(fromGlobal, aggregate).isBefore(fromGlobal)) {
            throw new Exception(Json.newObject().put("status", "error").put("reason", "wrong aggregate parameter").toString());
        }
        if (!aggregate.equals("all_time") && Hours.hoursBetween(fromGlobal, toGlobal).getHours() /
                Hours.hoursBetween(fromGlobal, incrementDateBy(fromGlobal, aggregate)).getHours() > MAX_REQUEST_COUNT) {
            throw new Exception(Json.newObject().put("status", "error").put("reason", "cannot split to so many chunks").toString());
        }
    }

    @BodyParser.Of(BodyParser.Json.class)
    public static Result getValue(String club_id) {


        JsonNode jsonBody = request().body().asJson();
        DateTime fromGlobal;
        DateTime toGlobal;
        try {
            filterRequest(jsonBody, club_id);
            fromGlobal = getDate(jsonBody.get("date_from").asText(), club_id);
            toGlobal = getDate(jsonBody.get("date_to").asText(), club_id);
        } catch (Exception ex) {
            return badRequest(ex.getMessage());
        }
        String aggregate = jsonBody.get("aggregate").asText();
        String searchedValue;
        try {
            searchedValue = jsonBody.get("value").asText();
        } catch (Exception ex) {
            return badRequest(Json.newObject().put("status", "error").put("reason", "did you specify value?").toString());
        }
        try {
            Tuple tuptup = getStatListForValue(searchedValue, fromGlobal, toGlobal, aggregate, club_id);
            return tuptup == null ? badRequest(Json.newObject().put("status", "error").put("reason", "unsupported value type")) : wrapJSON(tuptup.val, tuptup.time);
        }catch(NoSuchFieldException ex){
            return notFound(Json.newObject().put("status", "error").put("reason", "there are no stats for this club in specified time range, sorry.").toString());
        }
    }
    @BodyParser.Of(BodyParser.Json.class)
    public static Result getRatio(String club_id) {
        JsonNode jsonBody = request().body().asJson();
        DateTime fromGlobal;
        DateTime toGlobal;
        try {
            filterRequest(jsonBody, club_id);
            fromGlobal = getDate(jsonBody.get("date_from").asText(), club_id);
            toGlobal = getDate(jsonBody.get("date_to").asText(), club_id);
        } catch (Exception ex) {
            return badRequest(ex.getMessage());
        }
        String numerator, denominator;
        try {
            numerator = jsonBody.get("value_numerator").asText();
            denominator = jsonBody.get("value_denominator").asText();
        } catch (Exception ex) {
            return badRequest(Json.newObject().put("status", "error").put("reason", "did you specify value_numerator and value_denominator?").toString());
        }
        String aggregate = jsonBody.get("aggregate").asText();

        try {
            Tuple topTup = getStatListForValue(numerator, fromGlobal, toGlobal, aggregate, club_id);
            if (topTup.val == null) {
                return badRequest(Json.newObject().put("status", "error").put("reason", "unsupported numerator type"));
            }
            Tuple botTup = getStatListForValue(denominator, fromGlobal, toGlobal, aggregate, club_id);
            if (botTup.val == null) {
                return badRequest(Json.newObject().put("status", "error").put("reason", "unsupported numerator type"));
            }
            if (topTup.val.size() != botTup.val.size())
                return internalServerError(Json.newObject().put("status", "error").put("reason", "shit happens, sorry"));
            List<Float> ratios = new LinkedList<>();
            JSONArray aa = new JSONArray(topTup.val);
            JSONArray bb = new JSONArray(botTup.val);

            for (int i = 0; i < aa.length(); i++) {
                ratios.add(bb.getDouble(i) == 0 ? 0f : roundTwoDecimals((aa.getDouble(i)) / bb.getDouble(i)));
            }
            return wrapJSON(ratios, topTup.time);

        }
        catch (NoSuchFieldException ex){
            return notFound(Json.newObject().put("status", "error").put("reason", "there are no stats for this club, sorry.").toString());
        }
    }

    private static float roundTwoDecimals(double d) {
        DecimalFormat twoDForm = new DecimalFormat("#.####");
        return Float.valueOf(twoDForm.format(d));
    }

    private static Tuple getStatListForValue(String value, DateTime fromGlobal, DateTime toGlobal, String aggregate, String club_id) throws NoSuchFieldException {

        boolean all_time = aggregate.equals("all_time");

        DateTime from = fromGlobal;
        DateTime to = incrementDateBy(fromGlobal, aggregate);

        switch (value) {

            case "total_visits": {

                List<Float> l = new ArrayList<>();
                List<Long> t = new ArrayList<>();
                if (all_time) {
                    l.add(calculateTotalVisits(fromGlobal, toGlobal, club_id));
                    t.add(fromGlobal.getMillis());
                } else {

                    while (to.isBefore(incrementDateBy(toGlobal, aggregate))) {

                        l.add(calculateTotalVisits(from, to, club_id));
                        t.add(from.getMillis());

                        from = to;
                        to = incrementDateBy(to, aggregate);
                    }
                }
                return new Tuple(l, t);
            }
            case "unique_visits": {

                List<Float> l = new ArrayList<>();
                List<Long> t = new ArrayList<>();

                if (all_time) {
                    l.add(calculateUniqueVisits(fromGlobal, toGlobal, club_id));
                    t.add(fromGlobal.getMillis());
                } else {

                    while (to.isBefore(incrementDateBy(toGlobal, aggregate))) {

                        l.add(calculateUniqueVisits(from, to, club_id));
                        t.add(from.getMillis());
                        from = to;
                        to = incrementDateBy(to, aggregate);
                    }
                }
                return new Tuple(l, t);

            }
            case "qr_scanned": {

                List<Float> l = new ArrayList<>();
                List<Long> t = new ArrayList<>();

                if (all_time) {
                    int sum = 0;
                    for (FinishedVisit o : getVisitsList(fromGlobal, toGlobal, club_id)) {
                        sum += o.getQr_scanned();
                    }
                    l.add((float) sum);
                    t.add(fromGlobal.getMillis());

                } else {

                    while (to.isBefore(incrementDateBy(toGlobal, aggregate))) {

                        int sum = 0;
                        for (FinishedVisit o : getVisitsList(from, to, club_id)) {
                            sum += o.getQr_scanned();
                        }
                        l.add((float) sum);
                        t.add(from.getMillis());
                        from = to;
                        to = incrementDateBy(to, aggregate);
                    }
                }
                return new Tuple(l, t);
            }
            case "avg_rating": {

                List<Long> t = new ArrayList<>();
                List<Float> l = new ArrayList<>();


                if (all_time) {
                    l = calculateAvgRating(fromGlobal, toGlobal, club_id);
                    t.add(fromGlobal.getMillis());
                    return new Tuple(l, t);

                } else {

                    while (to.isBefore(incrementDateBy(toGlobal, aggregate))) {

                        l.addAll(calculateAvgRating(from, to, club_id));
                        t.add(from.getMillis());
                        from = to;
                        to = incrementDateBy(to, aggregate);
                    }
                    return new Tuple(l, t);
                }
            }
            case "avg_visit_length": {

                List<Long> t = new ArrayList<>();
                List<Float> l = new ArrayList<>();

                if (all_time) {
                    l = calculateAvgVisitLength(fromGlobal, toGlobal, club_id);
                    t.add(fromGlobal.getMillis());
                    return new Tuple(l, t);
                } else {

                    while (to.isBefore(incrementDateBy(toGlobal, aggregate))) {

                        l.addAll(calculateAvgVisitLength(from, to, club_id));
                        t.add(from.getMillis());
                        from = to;
                        to = incrementDateBy(to, aggregate);
                    }
                    return new Tuple(l, t);
                }
            }
            default:
                return null;
        }
    }


    private static List<FinishedVisit> getVisitsList(DateTime from, DateTime to, String clubId) throws NoSuchFieldException {

//        System.out.println("Millis: " + from.getMillis()/1000);

        return dbVisitDAO.getDatastore()
                .find(FinishedVisit.class)
                .field("club_id").equal(clubId)
//                .filter("visit_start >=", from.getMillis())
                .field("visit_start").greaterThanOrEq(from.getMillis()/1000)
                .field("visit_start").lessThanOrEq(to.getMillis()/1000)
                .asList();
    }

    private static float calculateTotalVisits(DateTime from, DateTime to, String clubId) throws NoSuchFieldException {

        List l = getVisitsList(from, to, clubId);
        return (l.size());
    }

    private static float calculateUniqueVisits(DateTime from, DateTime to, String clubId) throws NoSuchFieldException {

        DBObject query = QueryBuilder.start("visit_start")
                .greaterThanEquals(from.getMillis() / 1000)
                .lessThanEquals(to.getMillis() / 1000)
                .and(new BasicDBObject("club_id", clubId))
                .get();

        DBCollection myCol = dbVisitDAO.getDatastore().getCollection(FinishedVisit.class);
        return (myCol.distinct("user_id", query).size());
    }

    private static List calculateAvgRating(DateTime from, DateTime to, String clubId) throws NoSuchFieldException {


        List l = new LinkedList<>();

        List<FinishedVisit> cursor = getVisitsList(from, to, clubId);
        int sum = 0;
        for (FinishedVisit o : cursor) {
            sum += o.getRating();
        }
        if (cursor.isEmpty()) {
            l.add(0f);
        } else
            l.add(sum * 1f / cursor.size());

        return l;
    }

    private static List calculateAvgVisitLength(DateTime from, DateTime to, String clubId) throws NoSuchFieldException {


        List l = new LinkedList<>();

        List<FinishedVisit> cursor = getVisitsList(from, to, clubId);
        int sum = 0;
        for (FinishedVisit o : cursor) {
            sum += Minutes.minutesBetween(new DateTime(o.getVisit_start()), new DateTime(o.getVisit_end())).getMinutes();
        }
        if (cursor.isEmpty()) {
            l.add(0);
        } else
            l.add(sum / cursor.size());

        return l;
    }

    private static Result wrapJSON(Object value, Object time) {

        return ok(new JSONObject().put("status", "ok").put("stat", value).put("timestamp", time).toString());
    }
}



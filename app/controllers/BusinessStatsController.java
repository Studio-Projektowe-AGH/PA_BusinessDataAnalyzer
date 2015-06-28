package controllers;

import com.fasterxml.jackson.databind.JsonNode;
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

    static {
        morphia.map(FinishedVisit.class).getMapper().getOptions().setStoreNulls(true);
    }

    @BodyParser.Of(BodyParser.Json.class)
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

    private static DateTime getDate(String s) throws Exception {

        if(s.equals("big_bang")){
            Query q = dbVisitDAO.getDatastore().createQuery(FinishedVisit.class);
            q.order("visit_start");
            q.limit(1);
            return new DateTime(((FinishedVisit)q.get()).getVisit_start());
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

    private static void filterRequest(JsonNode jsonBody) throws Exception {

        DateTime fromGlobal;
        DateTime toGlobal;
        try {
            fromGlobal = getDate(jsonBody.get("date_from").asText());
            System.out.println("date from is " + fromGlobal);
            toGlobal = getDate(jsonBody.get("date_to").asText());
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
            filterRequest(jsonBody);
            fromGlobal = getDate(jsonBody.get("date_from").asText());
            toGlobal = getDate(jsonBody.get("date_to").asText());
        } catch (Exception ex) {
            return badRequest(ex.getMessage());
        }
        String aggregate = jsonBody.get("aggregate").asText();
        String searchedValue = jsonBody.get("value").asText();
        List l = getStatListForValue(searchedValue, fromGlobal, toGlobal, aggregate);
        return l == null ? badRequest(Json.newObject().put("status", "error").put("reason", "unsupported value type")) : wrapJSON(l);
    }

    @BodyParser.Of(BodyParser.Json.class)
    public static Result getRatio(String club_id) {
        JsonNode jsonBody = request().body().asJson();
        DateTime fromGlobal;
        DateTime toGlobal;
        try {
            filterRequest(jsonBody);
            fromGlobal = getDate(jsonBody.get("date_from").asText());
            toGlobal = getDate(jsonBody.get("date_to").asText());
        } catch (Exception ex) {
            return badRequest(ex.getMessage());
        }
        String numerator = jsonBody.get("value_numerator").asText();
        String denominator = jsonBody.get("value_denominator").asText();
        String aggregate = jsonBody.get("aggregate").asText();

        List top = getStatListForValue(numerator, fromGlobal, toGlobal, aggregate);
        if (top == null) {
            return badRequest(Json.newObject().put("status", "error").put("reason", "unsupported numerator type"));
        }
        List bot = getStatListForValue(denominator, fromGlobal, toGlobal, aggregate);
        if (bot == null) {
            return badRequest(Json.newObject().put("status", "error").put("reason", "unsupported numerator type"));
        }
        if (top.size() != bot.size())
            return internalServerError(Json.newObject().put("status", "error").put("reason", "shit happens, sorry"));
        List<Float> ratios = new LinkedList<>();
        JSONArray aa = new JSONArray(top);
        JSONArray bb = new JSONArray(bot);

        for (int i = 0; i < aa.length(); i++) {
            ratios.add(bb.getDouble(i) == 0 ? 0f : roundTwoDecimals((aa.getDouble(i)) / bb.getDouble(i)));
        }
        return wrapJSON(ratios);
    }
    private static float roundTwoDecimals(double d) {
        DecimalFormat twoDForm = new DecimalFormat("#.####");
        return Float.valueOf(twoDForm.format(d));
    }
    private static List getStatListForValue(String value, DateTime fromGlobal, DateTime toGlobal, String aggregate) {

        boolean all_time = aggregate.equals("all_time");

        DateTime from = fromGlobal;
        DateTime to = incrementDateBy(fromGlobal, aggregate);

        switch (value) {

            case "total_visits": {

                List<Float> l = new ArrayList<>();
                if (all_time) {
                    l.add(calculateTotalVisits(fromGlobal, toGlobal));
                } else {

                    while (to.isBefore(incrementDateBy(toGlobal, aggregate))) {

                        l.add(calculateTotalVisits(from, to));
                        from = to;
                        to = incrementDateBy(to, aggregate);
                    }
                }
                return l;
            }
            case "unique_visits": {

                List<Float> l = new ArrayList<>();
                if (all_time) {
                    l.add(calculateUniqueVisits(fromGlobal, toGlobal));
                } else {

                    while (to.isBefore(incrementDateBy(toGlobal, aggregate))) {

                        l.add(calculateUniqueVisits(from, to));
                        from = to;
                        to = incrementDateBy(to, aggregate);
                    }
                }
                return l;

            }
            case "qr_scanned": {

                List<Float> l = new ArrayList<>();
                if (all_time) {
                    int sum = 0;
                    for (FinishedVisit o : getVisitsList(fromGlobal, toGlobal)) {
                        sum += o.getQr_scanned();
                    }
                    l.add((float) sum);

                } else {

                    while (to.isBefore(incrementDateBy(toGlobal, aggregate))) {

                        int sum = 0;
                        for (FinishedVisit o : getVisitsList(from, to)) {
                            sum += o.getQr_scanned();
                        }
                        l.add((float) sum);
                        from = to;
                        to = incrementDateBy(to, aggregate);
                    }
                }
                return l;
            }
            case "avg_rating": {

                if (all_time) {
                    return calculateAvgRating(fromGlobal, toGlobal);

                } else {

                    List<Float> l = new ArrayList<>();
                    while (to.isBefore(incrementDateBy(toGlobal, aggregate))) {

                        l.addAll(calculateAvgRating(from, to));
                        from = to;
                        to = incrementDateBy(to, aggregate);
                    }
                    return l;
                }
            }
            case "avg_visit_length": {


                if (all_time) {
                    return calculateAvgVisitLength(fromGlobal, toGlobal);
                } else {

                    List<Float> l = new ArrayList<>();
                    while (to.isBefore(incrementDateBy(toGlobal, aggregate))) {

                        l.addAll(calculateAvgVisitLength(from, to));
                        from = to;
                        to = incrementDateBy(to, aggregate);
                    }
                    return l;
                }
            }
            default:
                return null;
        }
    }


    private static List<FinishedVisit> getVisitsList(DateTime from, DateTime to) {

        Query q = dbVisitDAO.getDatastore().createQuery(FinishedVisit.class);
        q.field("visit_start").greaterThan(from.toDate().getTime());
        q.field("visit_start").lessThan(to.toDate().getTime());
        return q.asList();
    }

    private static float calculateTotalVisits(DateTime from, DateTime to) {

        DBObject query = QueryBuilder.start("visit_start")
                .greaterThanEquals(from.toDate().getTime())
                .lessThanEquals(to.toDate().getTime())
                .get();

        DBCollection myCol = dbVisitDAO.getDatastore().getCollection(FinishedVisit.class);
        return (myCol.count(query));
    }

    private static float calculateUniqueVisits(DateTime from, DateTime to) {

        DBObject query = QueryBuilder.start("visit_start")
                .greaterThanEquals(from.toDate().getTime())
                .lessThanEquals(to.toDate().getTime())
                .get();

        DBCollection myCol = dbVisitDAO.getDatastore().getCollection(FinishedVisit.class);
        return (myCol.distinct("user_id", query).size());
    }

    private static List calculateAvgRating(DateTime from, DateTime to) {


        List l = new LinkedList<>();

        List<FinishedVisit> cursor = getVisitsList(from, to);
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

    private static List calculateAvgVisitLength(DateTime from, DateTime to) {


        List l = new LinkedList<>();

        List<FinishedVisit> cursor = getVisitsList(from, to);
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

    private static Result wrapJSON(Object value) {

        return ok(new JSONObject().put("status", "ok").put("stat", value).toString());
    }
}



package controllers;

import play.mvc.Controller;
import play.mvc.Result;

public class Application extends Controller {

    public static Result index() {

        return ok("<html><body><h3>Witaj na GoParty: Statistics Service</h3></body></html>").as("text/html");
    }

}

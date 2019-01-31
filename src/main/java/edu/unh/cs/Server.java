package edu.unh.cs;

import spark.Spark;
import spark.utils.IOUtils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static spark.Spark.*;

public class Server {

    public static void main(String[] args) {
        get("/", (req, res) -> "Chatbot App");

        get("/ask", (req, res) -> {
            String query = req.queryParams("query");

            return "something";
        });
    }

}

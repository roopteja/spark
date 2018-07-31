/*
 * This code is launch spark using a restful api.
 * The restAPI takes a json text and gives a file.
 */
package com.wis.rest.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.spark.launcher.SparkLauncher;
import org.json.JSONObject;

/**
 * REST Web Service
 *
 * @author RoopTeja
 */
@Path("service")
public class Service {

    /**
     * PUT method for updating or creating an instance of Service
     *
     * @param json
     * @return 
     */
    @POST
    @Path("getResult")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces("image/jpeg")
    public Response getResult(String json) {
        int exitCode = launchSpark(json);
        if (exitCode == 0) {
            String jsonFile = "/home/roopteja/json.txt";
            StringBuilder text = new StringBuilder("");
            try {
                String line;
                BufferedReader br = new BufferedReader(new FileReader(jsonFile));
                while((line = br.readLine()) != null){
                    text.append(line);
                }
            } catch (IOException ex) {
                Logger.getLogger(Service.class.getName()).log(Level.SEVERE, null, ex);
            }
            File medianimg = new File("/home/roopteja/median.jpg");
            Response.ResponseBuilder response = Response.ok((Object) medianimg);
            response.header("medianData", text);
            response.header("Access-Control-Expose-Headers", "medianData");
            return response.build();
        }
        return Response.serverError().build();
    }

    private int launchSpark(String jsonText) {
        JSONObject obj = new JSONObject(jsonText);
        String location = obj.getString("location");
        String starttime = obj.getString("starttime");
        String endtime = obj.getString("endtime");
        SparkLauncher sparkLauncher = new SparkLauncher();
        sparkLauncher.setAppResource("/home/roopteja/server.py");
        sparkLauncher.setSparkHome("/home/roopteja/spark-1.6.1");
        sparkLauncher.setMaster("spark://ipaddress:7077");
        sparkLauncher.setConf(SparkLauncher.DRIVER_MEMORY, "2g");
        sparkLauncher.addAppArgs(location, starttime, endtime);
        Process spark;
        try {
            spark = sparkLauncher.launch();
            int exitCode = spark.waitFor();
            return exitCode;
        } catch (InterruptedException ex) {
            Logger.getLogger(Service.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Service.class.getName()).log(Level.SEVERE, null, ex);
        }
        return -1;
    }

}

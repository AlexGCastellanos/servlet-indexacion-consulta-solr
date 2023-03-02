/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package services;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 *
 * @author bgarzon
 */
public class LoggerService {

    public static Logger configLogger(String className) {
        
        Logger logger  = Logger.getLogger(className);
        String pathLog = "/opt/data/IFindIt/admin_dashboards/admin_serviceDash/logs/api_servlet.log";
        try {

            FileHandler fhandler = new FileHandler(pathLog, true);
            logger.addHandler(fhandler);
            SimpleFormatter formatter = new SimpleFormatter();
            fhandler.setFormatter(formatter);

        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return logger;
    }

}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package services;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 *
 * @author bgarzon
 */
public class PropertiesService {
    
    private String pathFileModule;
    
    static Logger logger = LoggerService.configLogger(PropertiesService.class.getName());
    
    /*
     * Funcion que carga los valores iniciales del archivo de configuracion configuration.properties
     */
    
    public boolean loadProperties() {
        
        String absoluteDiskPath = "/opt/data/IFindIt/admin_dashboards/admin_serviceDash/temp/configuration.properties";
        Properties props = new Properties();
        FileInputStream is;
        try {
            is = new FileInputStream(absoluteDiskPath);
            props.load(is);
        } catch (IOException e1) {
            logger.info("Error al abrir configfile: " + e1.getMessage());
        }
        
        pathFileModule = props.getProperty("pathFileModule");
                
        return true;
    }

    public String getPathFileModule() {
        return pathFileModule;
    }

    public void setPathFileModule(String pathFileModule) {
        this.pathFileModule = pathFileModule;
    }  
    
}

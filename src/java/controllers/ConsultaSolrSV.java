package controllers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.URL;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.annotation.MultipartConfig;
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.logging.*;
import services.LoggerService;
import services.PropertiesService;

@MultipartConfig(
        fileSizeThreshold = 1024 * 1024 * 10, // 10MB
        maxFileSize = 1024 * 1024 * 10, // 10MB
        maxRequestSize = 1024 * 1024 * 50 // 50MB
)
public class ConsultaSolrSV extends HttpServlet {

    final static Logger logger = LoggerService.configLogger(ConsultaSolrSV.class.getName());

    private String[] idsArray(String ids) {

        if (ids.isEmpty() || ids.equals(" ")) {
            return null;
        }

        //Genero un arreglo a partir de la cadena limpia
        String[] idsArray = ids.split("\\|\\|");

        for (int i = 0; i < idsArray.length; i++) {
            //Limpio la cadena de texto de los ids
            idsArray[i] = idsArray[i].trim();
        }

        return idsArray;

    }

    private void indexar(URL url, URL urlSchemaDestino, JSONArray jsonDocsOrigen, JSONArray fieldTypesJsonOrigen, JSONArray fieldTypesJsonDestino, JSONArray fieldsJsonOrigen,
            JSONArray fieldsJsonDestino, JSONArray jsonCopyFieldsOrigen, JSONArray jsonCopyFieldsDestino) throws IOException {

        HttpURLConnection connDestino = (HttpURLConnection) url.openConnection();
        connDestino.setRequestMethod("POST");
        connDestino.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
        connDestino.setDoOutput(true);

        OutputStream os = connDestino.getOutputStream();

        //Añado los fieldTypes extraidos del archivo json al schema de la coleccion destino
        añadirConfigSchema(urlSchemaDestino, fieldTypesJsonOrigen, fieldTypesJsonDestino, "name", "add-field-type");

        //Añado los fields extraidos del archivo json al schema de la coleccion destino
        añadirConfigSchema(urlSchemaDestino, fieldsJsonOrigen, fieldsJsonDestino, "name", "add-field");

        //Añado los copyFields extraidos del archivo json al schema de la coleccion destino
        añadirConfigSchema(urlSchemaDestino, jsonCopyFieldsOrigen, jsonCopyFieldsDestino, "source", "add-copy-field");

        for (int i = 0; i < jsonDocsOrigen.length(); i++) {
            JSONObject doc = jsonDocsOrigen.getJSONObject(i);
            if (doc.has("_version_")) {
                doc.remove("_version_");
            }
            List<String> nombresCampos = new ArrayList<>(doc.keySet());
            if (!jsonCopyFieldsOrigen.isEmpty()) {
                for (int j = 0; j < nombresCampos.size(); j++) {
                    for (int k = 0; k < jsonCopyFieldsOrigen.length(); k++) {
                        JSONObject docCopyField = jsonCopyFieldsOrigen.getJSONObject(k);
                        if (docCopyField.getString("dest").equals(nombresCampos.get(j))) {
                            doc.remove(nombresCampos.get(j));
                        }
                    }
                }
            }
            String documento = "[" + doc.toString() + "]";
            os.write(documento.getBytes());
            os.flush();
        }

        int statusUpdate = connDestino.getResponseCode();
        System.out.println("Response Code de la Indexacion: " + statusUpdate);
        logger.info("Response Code de la Indexacion: " + statusUpdate);

        connDestino.disconnect();

    }

    private String consultarSchema(String ip, String puerto, String coleccion) {

        try {
            //Obteniendo url para consultar schema Origen
            String schema = "http://" + ip + ":" + puerto + "/solr/" + coleccion + "/schema";
            URL urlSchema = new URL(schema);

            logger.info("URL de consulta a schema quedó como: " + schema);

            HttpURLConnection connOrigen = (HttpURLConnection) urlSchema.openConnection();
            connOrigen.setRequestMethod("GET");
            connOrigen.setConnectTimeout(3000);

            int responseCode = connOrigen.getResponseCode();
            logger.info("El responseCode de la consulta a schema es: " + responseCode);

            StringBuilder resultado = new StringBuilder();

            BufferedReader rd = new BufferedReader(new InputStreamReader(connOrigen.getInputStream()));

            String linea;

            while ((linea = rd.readLine()) != null) {
                resultado.append(linea);
            }

            connOrigen.disconnect();
            return resultado.toString();

        } catch (IOException ex) {

            logger.info("La respuesta error de la consulta a schema es: " + ex.getMessage());
            return "error";

        }

    }

    private String consultarTamañoColeccion(String ip, String puerto, String origen) throws IOException {

        //Formando la url de consulta de status
        String urlSolr = "http://" + ip + ":" + puerto + "/solr/admin/cores?action=STATUS&core=" + origen;

        logger.info("La URL de consulta de status quedó como: " + urlSolr);
        URL solrOrigen = new URL(urlSolr);

        HttpURLConnection connOrigen = (HttpURLConnection) solrOrigen.openConnection();
        connOrigen.setRequestMethod("GET");

        BufferedReader rd = new BufferedReader(new InputStreamReader(connOrigen.getInputStream()));

        StringBuilder resultado = new StringBuilder();
        String linea;

        while ((linea = rd.readLine()) != null) {
            resultado.append(linea);
        }

        connOrigen.disconnect();

        return resultado.toString();

    }

    private String consultarColeccionPorBatch(String ip, String puerto, String origen, Integer start, Integer rows) throws IOException {

        //Formando la url de origen de consulta
        StringBuilder urlSolr = new StringBuilder().append("http://").append(ip).append(":").append(puerto).append("/solr/").append(origen)
                .append("/select?q=*:*&wt=json&start=").append(start).append("&rows=").append(rows);

        logger.info("La url de indexacion por batch quedó como: " + urlSolr.toString());
        URL solrOrigen = new URL(urlSolr.toString());

        HttpURLConnection connOrigen = (HttpURLConnection) solrOrigen.openConnection();
        connOrigen.setRequestMethod("GET");

        BufferedReader rd = new BufferedReader(new InputStreamReader(connOrigen.getInputStream()));

        StringBuilder resultado = new StringBuilder();
        String linea;

        while ((linea = rd.readLine()) != null) {
            resultado.append(linea);
        }

        connOrigen.disconnect();

        return resultado.toString();
    }

    private String consultarColeccion(String ip, String puerto, String origen, String ids, Integer numDocs) {

        //Formando la url de origen de consulta
        String urlSolr = "http://" + ip + ":" + puerto + "/solr/" + origen + "/select?";

        StringBuilder queryParam = new StringBuilder();

        if (ids.equals(" ") || ids.isEmpty()) {
            queryParam.append("");
            urlSolr = urlSolr + "rows=" + numDocs + "&";
        } else {
            //Genero un arreglo a partir de la cadena limpia
            String[] idsArray = idsArray(ids);

            //Doy formato URL por cada id en el arreglo
            for (int i = 0; i < idsArray.length; i++) {
                if (i == (idsArray.length - 1)) {
                    queryParam.append("id:").append("\""+idsArray[i]+"\"");
                } else {
                    queryParam.append("id:").append("\""+idsArray[i]+"\"").append(" OR ");
                }
            }
        }
        
        logger.info("Los parametros id quedaron como: " + queryParam.toString());

        try {

            URL solrOrigen = new URL(urlSolr + "fq=" + URLEncoder.encode(queryParam.toString(), StandardCharsets.UTF_8.toString()) + "&q=*:*&wt=json");

            HttpURLConnection connOrigen = (HttpURLConnection) solrOrigen.openConnection();
            connOrigen.setRequestMethod("GET");
            connOrigen.setConnectTimeout(3000);

            StringBuilder resultado = new StringBuilder();

            int responseCode = connOrigen.getResponseCode();
            logger.info("El response code de la consulta a la coleccion es: " + responseCode);

            BufferedReader rd = new BufferedReader(new InputStreamReader(connOrigen.getInputStream()));

            String linea;

            while ((linea = rd.readLine()) != null) {
                resultado.append(linea);
            }

            connOrigen.disconnect();
            return resultado.toString();

        } catch (IOException ex) {
            logger.info("La respuesta error de la consulta a coleccion es: " + ex.getMessage());
            return "error";
        }

    }

    private void añadirConfigSchema(URL url, JSONArray arrOrigen, JSONArray arrDestino, String nameKey, String comando) throws IOException {

        URL urlSchemaDestino = url;

        HttpURLConnection connUpdateDestino = (HttpURLConnection) urlSchemaDestino.openConnection();
        connUpdateDestino.setRequestMethod("POST");
        connUpdateDestino.setRequestProperty("Content-Type", "application/json");
        connUpdateDestino.setDoOutput(true);

        JSONArray jsonArrayAdd = new JSONArray();

        //Valido si existe el field de origen en el de destino sino lo agrego al destino
        for (int i = 0; i < arrOrigen.length(); i++) {

            JSONObject fieldOrigen = arrOrigen.getJSONObject(i);
            String nameFieldOrigen = fieldOrigen.getString(nameKey);

            boolean existe = false;

            for (int j = 0; j < arrDestino.length(); j++) {
                JSONObject fieldDestino = arrDestino.getJSONObject(j);
                String nameFieldDestino = fieldDestino.getString(nameKey);
                if (nameFieldOrigen.equals(nameFieldDestino)) {
                    existe = true;
                    break;
                }
            }

            if (!existe) {
                jsonArrayAdd.put(fieldOrigen);
            }

        }

        OutputStream upDestino = connUpdateDestino.getOutputStream();

        JSONObject addJson = new JSONObject();
        addJson.put(comando, jsonArrayAdd);
        logger.info("El comando para añadir quedó como: " + addJson.toString());

        upDestino.write(addJson.toString().getBytes());
        upDestino.flush();
        upDestino.close();

        BufferedReader rd = new BufferedReader(new InputStreamReader(connUpdateDestino.getInputStream()));

        String line;
        StringBuilder respuesta = new StringBuilder();

        while ((line = rd.readLine()) != null) {
            respuesta.append(line);
        }

        rd.close();

        int statusUpdate = connUpdateDestino.getResponseCode();
        logger.info("Response Code : " + statusUpdate);

    }

    private void guardar(JSONArray json, JSONArray jsonCopyFieldsOrigen, JSONObject fields, JSONObject copyFields, JSONObject fieldTypes, String[] idsArray, String coleccion) throws IOException {

        PropertiesService pf = new PropertiesService();
        pf.loadProperties();

        //Creo un JSONArray para guardar mis docs limpios
        JSONArray jsonDocsOrigen = new JSONArray();

        //Quito los copyFields de los docs que voy a guardar
        for (int i = 0; i < json.length(); i++) {

            JSONObject doc = json.getJSONObject(i);
            doc.remove("_version_");
            List<String> nombresCampos = new ArrayList<>(doc.keySet());
            if (!jsonCopyFieldsOrigen.isEmpty()) {
                for (int j = 0; j < nombresCampos.size(); j++) {
                    for (int k = 0; k < jsonCopyFieldsOrigen.length(); k++) {
                        JSONObject docCopyField = jsonCopyFieldsOrigen.getJSONObject(k);
                        if (docCopyField.getString("dest").equals(nombresCampos.get(j))) {
                            doc.remove(nombresCampos.get(j));
                        }
                    }
                }
            }

            jsonDocsOrigen.put(doc);

        }

        JSONObject docs = new JSONObject();
        docs.put("docs", jsonDocsOrigen);

        //Doy formato al nombre que tendrá el archivo.json
        StringBuilder elementosNombre = new StringBuilder();

        LocalDate date = LocalDate.now();
        LocalTime time = LocalTime.now();
        DateTimeFormatter formatoFecha = DateTimeFormatter.ofPattern("_yyyy-MM-dd");
        DateTimeFormatter formatoHora = DateTimeFormatter.ofPattern("hh_mm_ss");
        String fecha = date.format(formatoFecha);
        String hora = time.format(formatoHora);

        if (idsArray == null) {
            elementosNombre.append("coleccion_completa").append(fecha).append("T").append(hora).append("Z");
        } else {

            for (int i = 0; i < idsArray.length; i++) {
                if (i == (idsArray.length - 1)) {
                    elementosNombre.append("id_").append(idsArray[i]).append(fecha).append("T").append(hora).append("Z");
                } else {
                    elementosNombre.append("id_").append(idsArray[i]).append("_");
                }
            }
        }

        //Escribo docs, fields, fieldTypes y copyfields en un archivo.json
        String path = pf.getPathFileModule() + "/copyCollection/" + coleccion;
        File file = new File(path);

        if (file.exists()) {

            file = new File(path + "/" + elementosNombre.toString() + ".json");

        } else {

            if (file.mkdir()) {

                file = new File(path + "/" + elementosNombre.toString() + ".json");

            } else {
                logger.info("No se pudo crear el directorio: " + file.getAbsolutePath());
            }
        }

        file.setExecutable(true);
        file.setReadable(true);
        file.setWritable(true);

        JSONArray textoArchivoJson = new JSONArray();
        textoArchivoJson.put(docs);
        textoArchivoJson.put(fieldTypes);
        textoArchivoJson.put(fields);
        textoArchivoJson.put(copyFields);

        String absolutePath = file.getAbsolutePath();

        logger.info("La ruta dónde se escribirá el archivo es: " + absolutePath);

        try (FileWriter writer = new FileWriter(file, true)) {

            String jsonTexto = textoArchivoJson.toString();

            writer.append(jsonTexto);

            writer.flush();
        }
    }

    private void guardarPorBatch(JSONArray json, JSONArray jsonCopyFieldsOrigen, JSONObject fields, JSONObject copyFields, JSONObject fieldTypes, Integer numBatch, String coleccion) throws IOException {

        PropertiesService pf = new PropertiesService();
        pf.loadProperties();

        //Creo un JSONArray para guardar mis docs limpios
        JSONArray jsonDocsOrigen = new JSONArray();

        //Quito los copyFields de los docs que voy a guardar
        for (int i = 0; i < json.length(); i++) {

            JSONObject doc = json.getJSONObject(i);
            doc.remove("_version_");
            List<String> nombresCampos = new ArrayList<>(doc.keySet());
            if (!jsonCopyFieldsOrigen.isEmpty()) {
                for (int j = 0; j < nombresCampos.size(); j++) {
                    for (int k = 0; k < jsonCopyFieldsOrigen.length(); k++) {
                        JSONObject docCopyField = jsonCopyFieldsOrigen.getJSONObject(k);
                        if (docCopyField.getString("dest").equals(nombresCampos.get(j))) {
                            doc.remove(nombresCampos.get(j));
                        }
                    }
                }
            }

            jsonDocsOrigen.put(doc);

        }

        JSONObject docs = new JSONObject();
        docs.put("docs", jsonDocsOrigen);

        //Doy formato al nombre que tendrá el archivo.json
        StringBuilder elementosNombre = new StringBuilder();

        LocalDate date = LocalDate.now();
        LocalTime time = LocalTime.now();
        DateTimeFormatter formatoFecha = DateTimeFormatter.ofPattern("_yyyy-MM-dd");
        DateTimeFormatter formatoHora = DateTimeFormatter.ofPattern("hh_mm_ss");
        String fecha = date.format(formatoFecha);
        String hora = time.format(formatoHora);

        elementosNombre.append("coleccion_completa").append("_").append(numBatch).append(fecha).append("T").append(hora).append("Z");

        //Escribo docs, fields, fieldTypes y copyfields en un archivo.json
        String path = pf.getPathFileModule() + "/copyCollection/" + coleccion;
        File file = new File(path);

        if (file.exists()) {

            file = new File(path + "/" + elementosNombre.toString() + ".json");

        } else {

            if (file.mkdir()) {
                file = new File(path + "/" + elementosNombre.toString() + ".json");
            } else {
                logger.info("No se pudo crear el directorio: " + file.getAbsolutePath());
            }
        }

        file.setExecutable(true);
        file.setReadable(true);
        file.setWritable(true);

        JSONArray textoArchivoJson = new JSONArray();
        textoArchivoJson.put(docs);
        textoArchivoJson.put(fieldTypes);
        textoArchivoJson.put(fields);
        textoArchivoJson.put(copyFields);

        logger.info("La ruta dónde se escribirà el archivo es: " + file.getAbsolutePath());

        try (FileWriter writer = new FileWriter(file, true)) {

            String jsonTexto = textoArchivoJson.toString();

            writer.append(jsonTexto);

            writer.flush();
        }
    }

    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        BufferedReader reader = request.getReader();
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            stringBuilder.append(line);
        }
        String requestBody = stringBuilder.toString();

        JSONObject datosFormulario = new JSONObject(requestBody);

        logger.info("El json de la solicitud es: " + datosFormulario.toString());

        //Se realizan validaciones solicitadas desde play
        if (datosFormulario.has("queryCollection")) {

            String ip = datosFormulario.getJSONObject("queryCollection").getString("ip");
            String port = datosFormulario.getJSONObject("queryCollection").getString("port");
            String collection = datosFormulario.getJSONObject("queryCollection").getString("collection");
            String ids = datosFormulario.getJSONObject("queryCollection").getString("ids");

            String queryResponse = consultarColeccion(ip, port, collection, ids, 0);
            logger.info("El valor de queryResponse es: " + queryResponse.toString());

            try (PrintWriter out = response.getWriter()) {

                if (!queryResponse.equals("error")) {

                    JSONObject jsonQuery = new JSONObject(queryResponse);
                    logger.info("Se realiza con exito la consulta a la coleccion");
                    response.setContentType("application/json;charset=UTF-8");
                    out.println(jsonQuery.toString());

                } else {

                    logger.info("queryResponse en error");
                    response.setContentType("application/json;charset=UTF-8");
                    out.println(queryResponse.toString());

                }

            } catch (IOException ex) {
                logger.info("Error al escribir respuesta de la consulta a coleccion: " + ex.getMessage());
            }

        } else if (datosFormulario.has("querySchema")) {

            String ip = datosFormulario.getJSONObject("querySchema").getString("ip");
            String port = datosFormulario.getJSONObject("querySchema").getString("port");
            String collection = datosFormulario.getJSONObject("querySchema").getString("collection");

            String queryResponse = consultarSchema(ip, port, collection);
            logger.info("El valor de queryResponse es: " + queryResponse.toString());

            try (PrintWriter out = response.getWriter()) {

                if (!queryResponse.equals("error")) {
                    JSONObject responseHeader = new JSONObject(queryResponse).getJSONObject("responseHeader");
                    JSONObject resp = new JSONObject();
                    resp.put("responseHeader", responseHeader);
                    logger.info("Se realiza con exito la consulta al schema");
                    response.setContentType("application/json;charset=UTF-8");
                    out.println(resp.toString());

                } else {
                    response.setContentType("application/json;charset=UTF-8");
                    out.println(queryResponse.toString());
                }

            } catch (IOException ex) {
                logger.info("Error al escribir respuesta de la consulta a schema: " + ex.getMessage());
            }

        }

        //Obteniendo datos origen
        String ip = datosFormulario.getString("ip");
        String puerto = datosFormulario.getString("puerto");
        String origen = datosFormulario.getString("origen");
        String ids = datosFormulario.getString("id");

        //Capturando datos destino
        String ipDestino = datosFormulario.getString("ipD");
        String puertoDestino = datosFormulario.getString("puertoD");
        String destino = datosFormulario.getString("destino");

        String operacion = datosFormulario.getString("operacion");

        //Obtengo texto del archivo json
        String textoJsonLeido = datosFormulario.getString("archivoJson");

        JSONObject jsonSchemaOrigen = new JSONObject();
        JSONArray jsonFieldTypesOrigen = new JSONArray();
        JSONArray jsonFieldsOrigen = new JSONArray();
        JSONArray jsonCopyFieldsOrigen = new JSONArray();

        JSONObject fieldTypes = new JSONObject();
        JSONObject fields = new JSONObject();
        JSONObject copyFields = new JSONObject();

        Integer numDocs = 0;
        Integer sizeInBytes = 0;

        if (!operacion.equals("Indexar Archivo JSON")) {
            //Obtengo el schema Origen en formato Json
            jsonSchemaOrigen = new JSONObject(consultarSchema(ip, puerto, origen)).getJSONObject("schema");

            //Obtengo los fieldTypes de la colección de origen
            jsonFieldTypesOrigen = jsonSchemaOrigen.getJSONArray("fieldTypes");
            fieldTypes.put("fieldTypes", jsonFieldTypesOrigen);

            //Obtengo los fields de la colección de origen
            jsonFieldsOrigen = jsonSchemaOrigen.getJSONArray("fields");
            fields.put("fields", jsonFieldsOrigen);

            //Obtengo los copyfields Origen
            jsonCopyFieldsOrigen = jsonSchemaOrigen.getJSONArray("copyFields");
            copyFields.put("copyFields", jsonCopyFieldsOrigen);

            //Obtengo status de la coleccion de origen
            JSONObject statusCore = new JSONObject(consultarTamañoColeccion(ip, puerto, origen));
            numDocs = statusCore.getJSONObject("status").getJSONObject(origen).getJSONObject("index").getInt("numDocs");
            sizeInBytes = statusCore.getJSONObject("status").getJSONObject(origen).getJSONObject("index").getInt("sizeInBytes");
        }

        JSONObject jsonSchemaDestino = new JSONObject();
        JSONArray jsonCopyFieldsDestino = new JSONArray();
        JSONArray jsonFieldsDestino = new JSONArray();
        JSONArray jsonFieldTypesDestino = new JSONArray();

        if (!operacion.equals("Guardar")) {
            //Obtengo el schema Destino en formato Json
            jsonSchemaDestino = new JSONObject(consultarSchema(ipDestino, puertoDestino, destino)).getJSONObject("schema");

            //Obtengo los copyfields Destino
            jsonCopyFieldsDestino = jsonSchemaDestino.getJSONArray("copyFields");

            //Obtengo los fields de la colección de Destino
            jsonFieldsDestino = jsonSchemaDestino.getJSONArray("fields");

            //Obtengo los fieldTypes de la colección de destino
            jsonFieldTypesDestino = jsonSchemaDestino.getJSONArray("fieldTypes");
        }

        // Realizo la operación especificada por el usuario 
        try (PrintWriter out = response.getWriter()) {

            //Preparo array para almacenar documentos de origen
            JSONArray jsonDocsOrigen = new JSONArray();

            //Formo la URL necesaria para indexar en la coleccion destino
            String urlSolrIndexar = "http://" + ipDestino + ":" + puertoDestino + "/solr/" + destino + "/update?commit=true";
            URL urlDestino = new URL(urlSolrIndexar);

            //Formo la URL necesaria para modificar el schema destino
            String schema = "http://" + ipDestino + ":" + puertoDestino + "/solr/" + destino + "/schema";
            URL urlSchemaDestino = new URL(schema);

            System.out.println("La operacion seleccionada por el usuario es: " + operacion);
            logger.info("La operacion seleccionada por el usuario es: " + operacion);

            switch (operacion) {

                case "Indexar":

                    if (((ids.equals(" ") || ids.isEmpty()) && sizeInBytes <= 20000000) || !(ids.equals(" ") || ids.isEmpty())) {

                        jsonDocsOrigen = new JSONObject(consultarColeccion(ip, puerto, origen, ids, numDocs)).getJSONObject("response").getJSONArray("docs");
                        indexar(urlDestino, urlSchemaDestino, jsonDocsOrigen, jsonFieldTypesOrigen, jsonFieldTypesDestino, jsonFieldsOrigen, jsonFieldsDestino, jsonCopyFieldsOrigen, jsonCopyFieldsDestino);

                    } else {

                        Integer promDocsBatch = 10000000 / (sizeInBytes / numDocs);
                        logger.info("El número de documentos por Batch es: " + promDocsBatch);

                        for (int i = 0; i <= promDocsBatch; i += promDocsBatch) {

                            //Obtengo el resultado de la consulta en formato Json
                            jsonDocsOrigen = new JSONObject(consultarColeccionPorBatch(ip, puerto, origen, i + 1, promDocsBatch)).getJSONObject("response").getJSONArray("docs");
                            indexar(urlDestino, urlSchemaDestino, jsonDocsOrigen, jsonFieldTypesOrigen, jsonFieldTypesDestino, jsonFieldsOrigen, jsonFieldsDestino, jsonCopyFieldsOrigen, jsonCopyFieldsDestino);

                        }

                    }

                    response.setContentType("application/json;charset=UTF-8");
                    out.println("La indexacion ha sido exitosa");
                    break;

                case "Guardar":

                    if (((ids.equals(" ") || ids.isEmpty()) && sizeInBytes <= 20000000) || !(ids.equals(" ") || ids.isEmpty())) {

                        jsonDocsOrigen = new JSONObject(consultarColeccion(ip, puerto, origen, ids, numDocs)).getJSONObject("response").getJSONArray("docs");
                        guardar(jsonDocsOrigen, jsonCopyFieldsOrigen, fields, copyFields, fieldTypes, idsArray(ids), origen);

                    } else {

                        Integer promDocsBatch = 10000000 / (sizeInBytes / numDocs);
                        logger.info("El número de documentos por Batch es: " + promDocsBatch);
                        int numBatch = 1;

                        for (int i = 0; i <= promDocsBatch; i += promDocsBatch) {

                            //Obtengo el resultado de la consulta en formato Json
                            jsonDocsOrigen = new JSONObject(consultarColeccionPorBatch(ip, puerto, origen, i, promDocsBatch)).getJSONObject("response").getJSONArray("docs");
                            guardarPorBatch(jsonDocsOrigen, jsonCopyFieldsOrigen, fields, copyFields, fieldTypes, numBatch++, origen);

                        }

                    }

                    response.setContentType("application/json;charset=UTF-8");
                    out.println("La consulta se ha guardado con exito");
                    break;

                case "Guardar e Indexar":

                    if (((ids.equals(" ") || ids.isEmpty()) && sizeInBytes <= 20000000) || !(ids.equals(" ") || ids.isEmpty())) {

                        jsonDocsOrigen = new JSONObject(consultarColeccion(ip, puerto, origen, ids, numDocs)).getJSONObject("response").getJSONArray("docs");
                        guardar(jsonDocsOrigen, jsonCopyFieldsOrigen, fields, copyFields, fieldTypes, idsArray(ids), origen);
                        indexar(urlDestino, urlSchemaDestino, jsonDocsOrigen, jsonFieldTypesOrigen, jsonFieldTypesDestino, jsonFieldsOrigen, jsonFieldsDestino, jsonCopyFieldsOrigen, jsonCopyFieldsDestino);

                    } else {

                        Integer promDocsBatch = 10000000 / (sizeInBytes / numDocs);
                        logger.info("El número de documentos por Batch es: " + promDocsBatch);
                        int numBatch = 1;

                        for (int i = 0; i <= promDocsBatch; i += promDocsBatch) {

                            //Obtengo el resultado de la consulta en formato Json
                            jsonDocsOrigen = new JSONObject(consultarColeccionPorBatch(ip, puerto, origen, i + 1, promDocsBatch)).getJSONObject("response").getJSONArray("docs");
                            guardarPorBatch(jsonDocsOrigen, jsonCopyFieldsOrigen, fields, copyFields, fieldTypes, numBatch++, origen);
                            indexar(urlDestino, urlSchemaDestino, jsonDocsOrigen, jsonFieldTypesOrigen, jsonFieldTypesDestino, jsonFieldsOrigen, jsonFieldsDestino, jsonCopyFieldsOrigen, jsonCopyFieldsDestino);

                        }

                    }

                    response.setContentType("application/json;charset=UTF-8");
                    out.println("La consulta se ha guardado e indexado con exito");
                    break;

                case "Indexar Archivo JSON":

                    JSONArray arrayJsonLeído = new JSONArray(textoJsonLeido);

                    //Extraigo los docs, fields, fieldTypes y copyfields desde el archivo json leido
                    JSONArray docsJsonLeido = new JSONArray();
                    JSONArray fieldsJsonLeido = new JSONArray();
                    JSONArray copyFieldsJsonLeido = new JSONArray();
                    JSONArray fieldTypesJsonLeido = new JSONArray();

                    for (int i = 0; i < arrayJsonLeído.length(); i++) {
                        JSONObject obj = arrayJsonLeído.getJSONObject(i);
                        System.out.println("El obj " + i + " del json leido es: " + obj.toString());
                        logger.info("El obj " + i + " del json leido es: " + obj.toString());
                        if (obj.has("docs")) {
                            docsJsonLeido = obj.getJSONArray("docs");
                        } else if (obj.has("fields")) {
                            fieldsJsonLeido = obj.getJSONArray("fields");
                        } else if (obj.has("copyFields")) {
                            copyFieldsJsonLeido = obj.getJSONArray("copyFields");
                        } else if (obj.has("fieldTypes")) {
                            fieldTypesJsonLeido = obj.getJSONArray("fieldTypes");
                        }
                    }

                    indexar(urlDestino, urlSchemaDestino, docsJsonLeido, fieldTypesJsonLeido, jsonFieldTypesDestino, fieldsJsonLeido, jsonFieldsDestino, copyFieldsJsonLeido, jsonCopyFieldsDestino);

                    response.setContentType("application/json;charset=UTF-8");
                    out.println("El archivo json se ha indexado con exito");
                    break;

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

}

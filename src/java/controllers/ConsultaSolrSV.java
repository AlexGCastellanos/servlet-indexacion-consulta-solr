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

        //A??ado los fieldTypes extraidos del archivo json al schema de la coleccion destino
        logger.info("Los fieldTypes que voy a a??adir son: " + fieldTypesJsonOrigen);
        System.out.println("Los fieldTypes que voy a a??adir son: " + fieldTypesJsonOrigen);
        a??adirConfigSchema(urlSchemaDestino, fieldTypesJsonOrigen, fieldTypesJsonDestino, "name", "add-field-type");

        //A??ado los fields extraidos del archivo json al schema de la coleccion destino
        logger.info("Los fields que voy a a??adir son: " + fieldsJsonOrigen);
        System.out.println("Los fields que voy a a??adir son: " + fieldsJsonOrigen);
        a??adirConfigSchema(urlSchemaDestino, fieldsJsonOrigen, fieldsJsonDestino, "name", "add-field");

        //A??ado los copyFields extraidos del archivo json al schema de la coleccion destino
        logger.info("Los copyFields que voy a a??adir son: " + jsonCopyFieldsOrigen);
        System.out.println("Los copyFields que voy a a??adir son: " + jsonCopyFieldsOrigen);
        a??adirConfigSchema(urlSchemaDestino, jsonCopyFieldsOrigen, jsonCopyFieldsDestino, "source", "add-copy-field");

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
            logger.info("El documento " + i + " formateado es: " + documento);
            System.out.println("El documento " + i + " formateado es: " + documento);
            os.write(documento.getBytes());
            os.flush();
        }

        int statusUpdate = connDestino.getResponseCode();
        System.out.println("Response Code de la Indexacion: " + statusUpdate);
        logger.info("Response Code de la Indexacion: " + statusUpdate);

        connDestino.disconnect();

    }

    private String consultarSchema(String ip, String puerto, String coleccion) {

        logger.info("Ingres?? al m??todo consultarSchema: ");
        try {
            //Obteniendo url para consultar schema Origen
            String schema = "http://" + ip + ":" + puerto + "/solr/" + coleccion + "/schema";
            URL urlSchema = new URL(schema);

            logger.info("URL de consulta a schema qued?? como: " + schema);

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

            logger.info("La respuesta de la consulta a schema es: " + resultado.toString());
            connOrigen.disconnect();
            return resultado.toString();

        } catch (IOException ex) {

            logger.info("La respuesta error de la consulta a schema es: " + ex.getMessage());
            return "error";

        }

    }

    private String consultarTama??oColeccion(String ip, String puerto, String origen) throws IOException {

        //Formando la url de consulta de status
        String urlSolr = "http://" + ip + ":" + puerto + "/solr/admin/cores?action=STATUS&core=" + origen;

        logger.info("La URL de consulta de status qued?? como: " + urlSolr);
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

        logger.info("La url de indexacion por batch qued?? como: " + urlSolr.toString());
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

        logger.info("Ingres?? al m??todo consultarColeccion: ");

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
                    queryParam.append("id:").append(idsArray[i]);
                } else {
                    queryParam.append("id:").append(idsArray[i]).append("%20OR%20");
                }
            }
        }

        logger.info("Los parametros id quedaron como: " + queryParam.toString());
        System.out.println("Los parametros id quedaron como: " + queryParam.toString());

        try {

            URL solrOrigen = new URL(urlSolr + "fq=" + queryParam.toString() + "&q=*:*&wt=json");

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

            logger.info("La respuesta de la consulta a coleccion es: " + resultado.toString());
            connOrigen.disconnect();
            return resultado.toString();

        } catch (IOException ex) {
            logger.info("La respuesta error de la consulta a coleccion es: " + ex.getMessage());
            return "error";
        }

    }

    private void a??adirConfigSchema(URL url, JSONArray arrOrigen, JSONArray arrDestino, String nameKey, String comando) throws IOException {

        URL urlSchemaDestino = url;

        HttpURLConnection connUpdateDestino = (HttpURLConnection) urlSchemaDestino.openConnection();
        connUpdateDestino.setRequestMethod("POST");
        connUpdateDestino.setRequestProperty("Content-Type", "application/json");
        connUpdateDestino.setDoOutput(true);

        JSONArray jsonArrayAdd = new JSONArray();

        //Valido si existe el field de origen en el de destino sino lo agrego al destino
        for (int i = 0; i < arrOrigen.length(); i++) {

            System.out.println("El elemento " + i + " del JSONArray es: " + arrOrigen.getJSONObject(i).toString());
            JSONObject fieldOrigen = arrOrigen.getJSONObject(i);
            String nameFieldOrigen = fieldOrigen.getString(nameKey);
            System.out.println("nombre campo origen: " + nameFieldOrigen);

            boolean existe = false;

            for (int j = 0; j < arrDestino.length(); j++) {
                JSONObject fieldDestino = arrDestino.getJSONObject(j);
                String nameFieldDestino = fieldDestino.getString(nameKey);
                System.out.println("nombre campo destino: " + nameFieldDestino);
                if (nameFieldOrigen.equals(nameFieldDestino)) {
                    existe = true;
                    break;
                }
            }

            if (!existe) {
                logger.info("field origen en el if de existencia es: " + fieldOrigen.toString());
                System.out.println("field origen en el if de existencia es: " + fieldOrigen.toString());
                jsonArrayAdd.put(fieldOrigen);
            }

        }

        logger.info("Longitud del arreglo: " + jsonArrayAdd.length());
        System.out.println("Longitud del arreglo: " + jsonArrayAdd.length());

        OutputStream upDestino = connUpdateDestino.getOutputStream();

        JSONObject addJson = new JSONObject();
        addJson.put(comando, jsonArrayAdd);
        logger.info("El comando para a??adir qued?? como: " + addJson.toString());
        System.out.println("El comando para a??adir qued?? como: " + addJson.toString());

        upDestino.write(addJson.toString().getBytes());
        upDestino.flush();
        upDestino.close();

        BufferedReader rd = new BufferedReader(new InputStreamReader(connUpdateDestino.getInputStream()));

        String line;
        StringBuilder respuesta = new StringBuilder();

        while ((line = rd.readLine()) != null) {
            respuesta.append(line);
        }

        // Imprime la respuesta del servidor
        logger.info("La respuesta es: " + respuesta);
        System.out.println("La respuesta es: " + respuesta);
        rd.close();

        int statusUpdate = connUpdateDestino.getResponseCode();
        System.out.println("Response Code : " + statusUpdate);
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

            String documento = doc.toString();
            logger.info("El documento " + i + " a escribir, con copyFields limpios es: " + documento);
            System.out.println("El documento " + i + " a escribir, con copyFields limpios es: " + documento);
            jsonDocsOrigen.put(doc);

        }

        JSONObject docs = new JSONObject();
        docs.put("docs", jsonDocsOrigen);

        //Doy formato al nombre que tendr?? el archivo.json
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
        logger.info("El array que se escribir?? en el archivo JSON es el siguiente: " + textoArchivoJson.toString());
        System.out.println("El array que se escribir?? en el archivo JSON es el siguiente: " + textoArchivoJson.toString());

        logger.info("La ruta d??nde se escribir?? el archivo es: " + absolutePath);
        System.out.println("La ruta d??nde se escribir?? el archivo es: " + absolutePath);

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

            String documento = doc.toString();
            logger.info("El documento " + i + " a escribir, con copyFields limpios es: " + documento);
            jsonDocsOrigen.put(doc);

        }

        JSONObject docs = new JSONObject();
        docs.put("docs", jsonDocsOrigen);

        //Doy formato al nombre que tendr?? el archivo.json
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

        logger.log(Level.INFO, "El array que se escribir\u00e1 en el archivo JSON es el siguiente: {0}", textoArchivoJson.toString());
        //System.out.println("El array que se escribir?? en el archivo JSON es el siguiente: " + textoArchivoJson.toString());

        logger.info("La ruta d??nde se escribir?? el archivo es: " + file.getAbsolutePath());
        //System.out.println("La ruta d??nde se escribir?? el archivo es: " + pf.getPathFileModule() + "/copyCollection/" + elementosNombre.toString() + ".json");

        try (FileWriter writer = new FileWriter(file, true)) {

            String jsonTexto = textoArchivoJson.toString();

            writer.append(jsonTexto);

            writer.flush();
        }
    }

    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        logger.info("Probando funcionamiento del logger");

        BufferedReader reader = request.getReader();
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            stringBuilder.append(line);
        }
        String requestBody = stringBuilder.toString();

        JSONObject datosFormulario = new JSONObject(requestBody);

        System.out.println("El json de la solicitud es: " + datosFormulario.toString());
        logger.info("El json de la solicitud es: " + datosFormulario.toString());

        //Se realizan validaciones solicitadas desde play
        if (datosFormulario.has("queryCollection")) {

            logger.info("Ingres?? al if de validaci??n de la coleccion");

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

                    logger.info("Entr?? al else de error del queryResponse");
                    response.setContentType("application/json;charset=UTF-8");
                    out.println(queryResponse.toString());

                }

            } catch (IOException ex) {
                logger.info("Error al escribir respuesta de la consulta a coleccion: " + ex.getMessage());
            }

        } else if (datosFormulario.has("querySchema")) {

            logger.info("Ingres?? al if de validaci??n del schema");

            String ip = datosFormulario.getJSONObject("querySchema").getString("ip");
            String port = datosFormulario.getJSONObject("querySchema").getString("port");
            String collection = datosFormulario.getJSONObject("querySchema").getString("collection");

            String queryResponse = consultarSchema(ip, port, collection);
            logger.info("El valor de queryResponse es: " + queryResponse.toString());

            try (PrintWriter out = response.getWriter()) {

                if (!queryResponse.equals("error")) {
                    logger.info("Entr?? al if del queryResponse");
                    JSONObject responseHeader = new JSONObject(queryResponse).getJSONObject("responseHeader");
                    JSONObject resp = new JSONObject();
                    resp.put("responseHeader", responseHeader);
                    logger.info("Se realiza con exito la consulta al schema");
                    logger.info("El responseHeader del schema es: " + resp.toString());
                    response.setContentType("application/json;charset=UTF-8");
                    out.println(resp.toString());

                } else {
                    logger.info("Entr?? al else de error del queryResponse");
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

        logger.info("Los valores de origen para la URL son: ip: " + ip + ", puerto: " + puerto + ", origen: " + origen + ", ids: " + ids);
        System.out.println("Los valores de origen para la URL son: ip: " + ip + ", puerto: " + puerto + ", origen: " + origen + ", ids: " + ids);

        logger.info("Los valores de destino para la URL son: ip: " + ipDestino + ", puerto: " + puertoDestino + ", destino: " + destino);
        System.out.println("Los valores de destino para la URL son: ip: " + ipDestino + ", puerto: " + puertoDestino + ", destino: " + destino);

        logger.info("La operacion es: " + operacion);
        System.out.println("La operacion es: " + operacion);

        logger.info("El array del json leido al final de la validacion es: " + textoJsonLeido);
        System.out.println("El array del json leido al final de la validacion es: " + textoJsonLeido);

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

            //Obtengo los fieldTypes de la colecci??n de origen
            jsonFieldTypesOrigen = jsonSchemaOrigen.getJSONArray("fieldTypes");
            fieldTypes.put("fieldTypes", jsonFieldTypesOrigen);

            //Obtengo los fields de la colecci??n de origen
            jsonFieldsOrigen = jsonSchemaOrigen.getJSONArray("fields");
            fields.put("fields", jsonFieldsOrigen);

            //Obtengo los copyfields Origen
            jsonCopyFieldsOrigen = jsonSchemaOrigen.getJSONArray("copyFields");
            copyFields.put("copyFields", jsonCopyFieldsOrigen);

            //Obtengo status de la coleccion de origen
            JSONObject statusCore = new JSONObject(consultarTama??oColeccion(ip, puerto, origen));
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

            //Obtengo los fields de la colecci??n de Destino
            jsonFieldsDestino = jsonSchemaDestino.getJSONArray("fields");

            //Obtengo los fieldTypes de la colecci??n de destino
            jsonFieldTypesDestino = jsonSchemaDestino.getJSONArray("fieldTypes");
        }

        System.out.println("Este es el schema original: " + jsonSchemaOrigen.toString() + "\n");
        logger.info("Este es el schema original: " + jsonSchemaOrigen.toString() + "\n");

        System.out.println("Este es el schema destino: " + jsonSchemaDestino.toString() + "\n");
        logger.info("Este es el schema destino: " + jsonSchemaDestino.toString() + "\n");

        System.out.println("Este es el copyfields original: " + jsonCopyFieldsOrigen.toString() + "\n");
        logger.info("Este es el copyfields original: " + jsonCopyFieldsOrigen.toString() + "\n");

        System.out.println("Este es el copyfields destino: " + jsonCopyFieldsDestino.toString() + "\n");
        logger.info("Este es el copyfields destino: " + jsonCopyFieldsDestino.toString() + "\n");

        System.out.println("Este es el fields original de origen: " + jsonFieldsOrigen.toString() + "\n");
        logger.info("Este es el fields original de origen: " + jsonFieldsOrigen.toString() + "\n");

        System.out.println("Este es el fields original de destino: " + jsonFieldsDestino.toString() + "\n");
        logger.info("Este es el fields original de destino: " + jsonFieldsDestino.toString() + "\n");

        System.out.println("Este es el fieldTypes original de origen: " + jsonFieldTypesOrigen.toString() + "\n");
        logger.info("Este es el fieldTypes original de origen: " + jsonFieldTypesOrigen.toString() + "\n");

        System.out.println("Este es el fieldTypes original de destino: " + jsonFieldTypesDestino.toString() + "\n");
        logger.info("Este es el fieldTypes original de destino: " + jsonFieldTypesDestino.toString() + "\n");

        // Realizo la operaci??n especificada por el usuario 
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
                        logger.info("El n??mero de documentos por Batch es: " + promDocsBatch);

                        for (int i = 0; i <= promDocsBatch; i += promDocsBatch) {

                            //Obtengo el resultado de la consulta en formato Json
                            jsonDocsOrigen = new JSONObject(consultarColeccionPorBatch(ip, puerto, origen, i + 1, promDocsBatch)).getJSONObject("response").getJSONArray("docs");
                            indexar(urlDestino, urlSchemaDestino, jsonDocsOrigen, jsonFieldTypesOrigen, jsonFieldTypesDestino, jsonFieldsOrigen, jsonFieldsDestino, jsonCopyFieldsOrigen, jsonCopyFieldsDestino);

                        }

                    }

                    response.setContentType("application/json;charset=UTF-8");
                    out.println("La indexacion ha sido exitosa");
                    //out.println(jsonDocsOrigen.toString());
                    break;

                case "Guardar":

                    if (((ids.equals(" ") || ids.isEmpty()) && sizeInBytes <= 20000000) || !(ids.equals(" ") || ids.isEmpty())) {

                        jsonDocsOrigen = new JSONObject(consultarColeccion(ip, puerto, origen, ids, numDocs)).getJSONObject("response").getJSONArray("docs");
                        guardar(jsonDocsOrigen, jsonCopyFieldsOrigen, fields, copyFields, fieldTypes, idsArray(ids), origen);

                    } else {

                        Integer promDocsBatch = 10000000 / (sizeInBytes / numDocs);
                        logger.info("El n??mero de documentos por Batch es: " + promDocsBatch);
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
                        logger.info("El n??mero de documentos por Batch es: " + promDocsBatch);
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
                    //out.println(jsonDocsOrigen.toString());
                    break;

                case "Indexar Archivo JSON":

                    //System.out.println("El array del json leido es: " + textoJsonLeido);
                    logger.info("El array del json leido es: " + textoJsonLeido);

                    JSONArray arrayJsonLe??do = new JSONArray(textoJsonLeido);

                    //Extraigo los docs, fields, fieldTypes y copyfields desde el archivo json leido
                    JSONArray docsJsonLeido = new JSONArray();
                    JSONArray fieldsJsonLeido = new JSONArray();
                    JSONArray copyFieldsJsonLeido = new JSONArray();
                    JSONArray fieldTypesJsonLeido = new JSONArray();

                    for (int i = 0; i < arrayJsonLe??do.length(); i++) {
                        JSONObject obj = arrayJsonLe??do.getJSONObject(i);
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
                    //out.println(docsJsonLeido.toString());
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

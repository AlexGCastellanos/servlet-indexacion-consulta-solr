package controllers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.annotation.MultipartConfig;
import org.json.JSONArray;
import org.json.JSONObject;

@MultipartConfig(
        fileSizeThreshold = 1024 * 1024 * 10, // 10MB
        maxFileSize = 1024 * 1024 * 10, // 10MB
        maxRequestSize = 1024 * 1024 * 50 // 50MB
)
public class ConsultaSolrSV extends HttpServlet {

    private String[] idsArray(String ids) {
        
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
        System.out.println("Los fieldTypes que voy a añadir son: " + fieldTypesJsonOrigen);
        añadirConfigSchema(urlSchemaDestino, fieldTypesJsonOrigen, fieldTypesJsonDestino, "name", "add-field-type");

        //Añado los fields extraidos del archivo json al schema de la coleccion destino
        System.out.println("Los fields que voy a añadir son: " + fieldsJsonOrigen);
        añadirConfigSchema(urlSchemaDestino, fieldsJsonOrigen, fieldsJsonDestino, "name", "add-field");

        //Añado los copyFields extraidos del archivo json al schema de la coleccion destino
        System.out.println("Los copyFields que voy a añadir son: " + jsonCopyFieldsOrigen);
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
            System.out.println("El documento " + i + " formateado es: " + documento);
            os.write(documento.getBytes());
            os.flush();
        }

        int statusUpdate = connDestino.getResponseCode();
        System.out.println("Response Code de la Indexacion: " + statusUpdate);

        connDestino.disconnect();

    }

    private String consultarSchema(String ip, String puerto, String coleccion) throws IOException {

        //Obteniendo url para consultar schema Origen
        String schema = "http://" + ip + ":" + puerto + "/solr/" + coleccion + "/schema";
        URL urlSchema = new URL(schema);

        HttpURLConnection connOrigen = (HttpURLConnection) urlSchema.openConnection();
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

    private String consultarColeccion(String ip, String puerto, String origen, String ids) throws IOException {

        //Formando la url de origen de consulta
        String urlSolr = "http://" + ip + ":" + puerto + "/solr/" + origen + "/select?";

        //Genero un arreglo a partir de la cadena limpia
        String[] idsArray = idsArray(ids);

        StringBuilder queryParam = new StringBuilder();

        //Doy formato URL por cada id en el arreglo
        for (int i = 0; i < idsArray.length; i++) {
            if (i == (idsArray.length - 1)) {
                queryParam.append("id:").append(idsArray[i]);
            } else {
                queryParam.append("id:").append(idsArray[i]).append("%20OR%20");
            }
        }

        System.out.println("Los parametros id quedaron como: " + queryParam.toString());
        URL solrOrigen = new URL(urlSolr + "fq=" + queryParam.toString() + "&q=*:*&wt=json");

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

    private void añadirConfigSchema(URL url, JSONArray arrOrigen, JSONArray arrDestino, String nameKey, String comando) throws IOException {

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
                System.out.println("field origen en el if de existencia es: " + fieldOrigen.toString());
                jsonArrayAdd.put(fieldOrigen);
            }

        }

        System.out.println("Longitud del arreglo: " + jsonArrayAdd.length());

        OutputStream upDestino = connUpdateDestino.getOutputStream();

        JSONObject addJson = new JSONObject();
        addJson.put(comando, jsonArrayAdd);
        System.out.println("El comando para añadir quedó como: " + addJson.toString());

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
        System.out.println("La respuesta es: " + respuesta);
        rd.close();

        int statusUpdate = connUpdateDestino.getResponseCode();
        System.out.println("Response Code : " + statusUpdate);

    }

    private void guardar(JSONArray json, JSONArray jsonCopyFieldsOrigen, JSONObject fields, JSONObject copyFields, JSONObject fieldTypes, String[] idsArray, String ubicacion) throws IOException {

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
            System.out.println("El documento " + i + " a escribir, con copyFields limpios es: " + documento);
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

        for (int i = 0; i < idsArray.length; i++) {
            if (i == (idsArray.length - 1)) {
                elementosNombre.append("id_").append(idsArray[i]).append(fecha).append("T").append(hora).append("Z");
            } else {
                elementosNombre.append("id_").append(idsArray[i]).append("_");
            }
        }

        //Escribo docs, fields, fieldTypes y copyfields en un archivo.json
        File archivo = new File(ubicacion + "\\" + elementosNombre.toString() + ".json");
        JSONArray textoArchivoJson = new JSONArray();
        textoArchivoJson.put(docs);
        textoArchivoJson.put(fieldTypes);
        textoArchivoJson.put(fields);
        textoArchivoJson.put(copyFields);
        System.out.println("El array que se escribirá en el archivo JSON es el siguiente: " + textoArchivoJson.toString());

        System.out.println("La ruta dónde se escribirà el archivo es: " + ubicacion + "\\" + elementosNombre.toString() + ".json");

        try (FileWriter writer = new FileWriter(archivo)) {

            String jsonTexto = textoArchivoJson.toString();

            writer.append(jsonTexto);

            writer.flush();
        }
    }

    private Map<String, String> validarCampos(String ip, String puerto, String origen, String ids, String ipDestino, String puertoDestino, String destino, String operacion, String ubicacion, String textoJsonLeido) {

        Map<String, String> errores = new HashMap<>();

        String regexIp = "^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$";
        String regexPuerto = "^(6553[0-5]|655[0-2][0-9]|65[0-4][0-9]{2}|6[0-4][0-9]{3}|[0-5]?([0-9]){0,3}[0-9])$";

        if (operacion.equals("Indexar Archivo JSON")) {

            System.out.println("El archivo JSON dentro de la validacion es: " + textoJsonLeido);

            if (textoJsonLeido == null || textoJsonLeido.isEmpty()) {
                errores.put("archivo", "Por favor seleccione un archivo json valido para indexar");
            }

        } else {

            if (ip.isEmpty()) {
                errores.put("ip", "El campo ip origen no puede ser vacío");
            } else if (!ip.matches(regexIp)) {
                errores.put("ip", "La ip no tiene el formato correcto xxx.xxx.xxx.xxx");
            }

            if (puerto.isEmpty()) {
                errores.put("puerto", "El campo puerto origen no puede ser vacío");
            } else if (!puerto.matches(regexPuerto)) {
                errores.put("puerto", "El puerto no tiene el formato correcto, debe ser un numero entre 0 y 65535");
            }

            if (origen.isEmpty()) {
                errores.put("origen", "El nombre de la colección no puede ser vacío");
            } else {
                if (!(errores.containsKey("ip") || errores.containsKey("puerto"))) {
                    try {
                        JSONObject consulta = new JSONObject(consultarSchema(ip, puerto, origen));
                        Integer status = consulta.getJSONObject("responseHeader").getInt("status");
                        if (status != 0) {
                            errores.put("origen", "Esta colección no existe, ingresa una colección valida");
                        }
                    } catch (IOException ex) {
                        errores.put("origen", "Error consultando esta colección, verifica si existe o si está escrita correctamente");
                    }
                } 
            }
        }

        if (operacion.equals("Indexar") || operacion.equals("Guardar") || operacion.equals("Guardar e Indexar")) {
            if (ids.isEmpty()) {
                errores.put("id", "El campo id no puede ser vacío");
            } else {
                try {
                    JSONObject jsonConsulta = new JSONObject(consultarColeccion(ip, puerto, origen, ids));
                    JSONObject responseHeader = jsonConsulta.getJSONObject("responseHeader");
                    Integer status = responseHeader.getInt("status");
                    JSONObject respuesta = jsonConsulta.getJSONObject("response");
                    Integer numFound = respuesta.getInt("numFound");
                    if (status != 0 || numFound == 0) {
                        errores.put("id", "El id ingresado no existe en la colección de origen");
                    }
                } catch (IOException ex) {
                    errores.put("id", "Error en la consulta, verifica que los datos de id, ip, puerto y coleccion de origen sean correctos");
                }
            }
        }

        if (operacion.equals("Guardar")) {
            if (ubicacion.isEmpty()) {
                errores.put("ubicacion", "La ubicación no puede ser vacía");
            }
        } else {
            if (ipDestino.isEmpty()) {
                errores.put("ipD", "El campo ip destino no puede ser vacío");
            } else if (!ipDestino.matches(regexIp)) {
                errores.put("ipD", "La IP no tiene el formato correcto xxx.xxx.xxx.xxx");
            }

            if (puertoDestino.isEmpty()) {
                errores.put("puertoD", "El campo puerto destino no puede ser vacío");
            } else if (!puertoDestino.matches(regexPuerto)) {
                errores.put("puertoD", "El puerto no tiene el formato correcto, debe ser un numero entre 0 y 65535");
            }

            if (destino.isEmpty()) {
                errores.put("destino", "El nombre de la colección no puede ser vacío");
            } else {
                if (!(errores.containsKey("ipD") || errores.containsKey("puertoD"))) {
                    try {
                        JSONObject consulta = new JSONObject(consultarSchema(ipDestino, puertoDestino, destino));
                        Integer status = consulta.getJSONObject("responseHeader").getInt("status");
                        if (status != 0) {
                            errores.put("destino", "Esta colección no existe, ingresa una colección valida");
                        }
                    } catch (IOException ex) {
                        errores.put("destino", "Error consultando esta colección, verifica si existe o si está escrita correctamente");
                    }
                }
            }
        }

        if (operacion.equals("Guardar e Indexar")) {
            if (ubicacion.isEmpty()) {
                errores.put("ubicacion", "La ubicación no puede ser vacía");
            }
        }
        
        return errores;
    }

    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        InputStream datos = request.getInputStream();
        StringBuilder datosLeidos = new StringBuilder();
        
        try(BufferedReader rd = new BufferedReader(new InputStreamReader(datos))){
            String linea;
            if((linea = rd.readLine()) != null){
                datosLeidos.append(linea);
            }            
        }       
        
        JSONObject datosFormulario = new JSONObject(datosLeidos);
        
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

        String ubicacion = datosFormulario.getString("ubicacion");
        
        //Obtengo texto del archivo json
        String textoJsonLeido = datosFormulario.getString("archivoJson");
        
        System.out.println("Los valores de origen para la URL son: ip: " + ip + ", puerto: " + puerto + ", origen: " + origen + ", ids: " + ids);
        System.out.println("Los valores de destino para la URL son: ip: " + ipDestino + ", puerto: " + puertoDestino + ", destino: " + destino);
        System.out.println("La ubicacion es: " + ubicacion);
        System.out.println("La operacion es: " + operacion);

        System.out.println("El array del json leido al final de la validacion es: " + textoJsonLeido);

        Map<String, String> errores = validarCampos(ip, puerto, origen, ids, ipDestino, puertoDestino, destino, operacion, ubicacion, textoJsonLeido);

        if (!errores.isEmpty()) {
            
            try (PrintWriter out = response.getWriter()){
                out.println("Se encontraron los siguientes errores durante la validación de campos");
                out.println(errores);
            }           

        } else {

            JSONObject jsonSchemaOrigen = new JSONObject();
            JSONArray jsonFieldTypesOrigen = new JSONArray();
            JSONArray jsonFieldsOrigen = new JSONArray();
            JSONArray jsonCopyFieldsOrigen = new JSONArray();

            JSONObject fieldTypes = new JSONObject();
            JSONObject fields = new JSONObject();
            JSONObject copyFields = new JSONObject();

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

            System.out.println("Este es el schema original: " + jsonSchemaOrigen.toString() + "\n");
            System.out.println("Este es el schema destino: " + jsonSchemaDestino.toString() + "\n");
            System.out.println("Este es el copyfields original: " + jsonCopyFieldsOrigen.toString() + "\n");
            System.out.println("Este es el copyfields destino: " + jsonCopyFieldsDestino.toString() + "\n");
            System.out.println("Este es el fields original de origen: " + jsonFieldsOrigen.toString() + "\n");
            System.out.println("Este es el fields original de destino: " + jsonFieldsDestino.toString() + "\n");
            System.out.println("Este es el fieldTypes original de origen: " + jsonFieldTypesOrigen.toString() + "\n");
            System.out.println("Este es el fieldTypes original de destino: " + jsonFieldTypesDestino.toString() + "\n");

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

                switch (operacion) {

                    case "Indexar":

                        //Obtengo el resultado de la consulta en formato Json
                        jsonDocsOrigen = new JSONObject(consultarColeccion(ip, puerto, origen, ids)).getJSONObject("response").getJSONArray("docs");

                        indexar(urlDestino, urlSchemaDestino, jsonDocsOrigen, jsonFieldTypesOrigen, jsonFieldTypesDestino, jsonFieldsOrigen, jsonFieldsDestino, jsonCopyFieldsOrigen, jsonCopyFieldsDestino);

                        response.setContentType("application/json;charset=UTF-8");
                        out.println("Se ha indexado con exito el siguiente json: ");
                        out.println(jsonDocsOrigen.toString());
                        break;

                    case "Guardar":

                        //Obtengo el resultado de la consulta en formato Json
                        jsonDocsOrigen = new JSONObject(consultarColeccion(ip, puerto, origen, ids)).getJSONObject("response").getJSONArray("docs");

                        guardar(jsonDocsOrigen, jsonCopyFieldsOrigen, fields, copyFields, fieldTypes, idsArray(ids), ubicacion);
                        response.setContentType("application/json;charset=UTF-8");
                        out.println("Se ha guardado con exito el siguiente json: ");
                        out.println(jsonDocsOrigen.toString());
                        break;

                    case "Guardar e Indexar":

                        //Obtengo el resultado de la consulta en formato Json
                        jsonDocsOrigen = new JSONObject(consultarColeccion(ip, puerto, origen, ids)).getJSONObject("response").getJSONArray("docs");

                        guardar(jsonDocsOrigen, jsonCopyFieldsOrigen, fields, copyFields, fieldTypes, idsArray(ids), ubicacion);
                        indexar(urlDestino, urlSchemaDestino, jsonDocsOrigen, jsonFieldTypesOrigen, jsonFieldTypesDestino, jsonFieldsOrigen, jsonFieldsDestino, jsonCopyFieldsOrigen, jsonCopyFieldsDestino);

                        response.setContentType("application/json;charset=UTF-8");
                        out.println("Se ha guardado e indexado con exito el siguiente json: ");
                        out.println(jsonDocsOrigen.toString());
                        break;

                    case "Indexar Archivo JSON":

                        System.out.println("El array del json leido es: " + textoJsonLeido);
                        JSONArray arrayJsonLeído = new JSONArray(textoJsonLeido);

                        //Extraigo los docs, fields, fieldTypes y copyfields desde el archivo json leido
                        JSONArray docsJsonLeido = new JSONArray();
                        JSONArray fieldsJsonLeido = new JSONArray();
                        JSONArray copyFieldsJsonLeido = new JSONArray();
                        JSONArray fieldTypesJsonLeido = new JSONArray();

                        for (int i = 0; i < arrayJsonLeído.length(); i++) {
                            JSONObject obj = arrayJsonLeído.getJSONObject(i);
                            System.out.println("El obj " + i + " del json leido es: " + obj.toString());
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
                        out.println("El archivo.json se ha indexado con exito, con la siguiente informacion: ");
                        out.println(docsJsonLeido.toString());
                        break;
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
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

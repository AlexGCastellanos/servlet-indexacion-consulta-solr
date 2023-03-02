<%-- 
    Document   : consulta
    Created on : Feb 13, 2023, 9:14:49 AM
    Author     : bgarzon
--%>

<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@page import="java.util.Map"%>
<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>Indexar app</title>
    </head>

    <%
        Map<String, String> errores = (Map<String, String>) request.getAttribute("errores");
        String ip = (String) request.getAttribute("ip");
        String puerto = (String) request.getAttribute("puerto");
        String origen = (String) request.getAttribute("origen");
        String id = (String) request.getAttribute("id");
        String ipD = (String) request.getAttribute("ipDestino");
        String puertoD = (String) request.getAttribute("puertoDestino");
        String destino = (String) request.getAttribute("destino");
        String operacion = (String) request.getAttribute("operacion");
        String ubicacion = (String) request.getAttribute("ubicacion");
        
        if(ip == null){
            ip = "";
        }
        
        if(puerto == null){
            puerto = "";
        }
        
        if(origen == null){
            origen = "";
        }
        
        if(id == null){
            id = "";
        }
        
        if(ipD == null){
            ipD = "";
        }
        
        if(puertoD == null){
            puertoD = "";
        }
        
        if(destino == null){
            destino = "";
        }
        
        if(operacion == null || operacion.isEmpty()){
            operacion = "1";
        }
        
        if(ubicacion == null){
            ubicacion = "";
        }        

    %>

    <body>

        <h1>Ingresa los parametros para la indexacion</h1>
        
        <form action="/api-servlet-solr/consulta" method="post" enctype="multipart/form-data">
            <div id="ip_div" style="margin: 1em;">
                <label>Ip Origen ${operacion}</label>
                <div>
                    <input type="text" name="ip" value="<%=ip%>" />
                </div>
                <c:if test="${errores != null && errores.containsKey('ip')}">
                    <div style="color: red;">${errores.ip}</div>
                </c:if> 
            </div>
            <div id="puerto_div" style="margin: 1em;">
                <label>Puerto Origen</label>
                <div>
                    <input type="text" name="puerto" value="<%=puerto%>" />
                </div>
                <c:if test="${errores != null && errores.containsKey('puerto')}">
                    <div style="color: red;">${errores.puerto}</div>
                </c:if>
            </div>
            <div id="origen_div" style="margin: 1em;">
                <label>Nombre de la Colección Origen</label>
                <div>
                    <input type="text" name="origen" value="<%=origen%>"/>
                </div>
                <c:if test="${errores != null && errores.containsKey('origen')}">
                    <div style="color: red;">${errores.origen}</div>
                </c:if>
            </div>
            <div id="ids_div" style="margin: 1em;">
                <label>Ids a consultar (usa || para separarlos)</label>
                <div>
                    <input id="input_ids" style="width : 500px;" type="text" name="id" placeholder="id1 || id2" value="<%=id%>"/>
                </div>
                <c:if test="${errores != null && errores.containsKey('id')}">
                    <div style="color: red;">${errores.id}</div>
                </c:if>
            </div>
            <div id="ipD_div" style="margin: 1em;">
                <label>Ip Destino</label>
                <div>
                    <input type="text" name="ipD" value="<%=ipD%>"/>
                </div>
                <c:if test="${errores != null && errores.containsKey('ipD')}">
                    <div style="color: red;">${errores.ipD}</div>
                </c:if>
            </div>
            <div id="puertoD_div" style="margin: 1em;">
                <label>Puerto Destino</label>
                <div>
                    <input type="text" name="puertoD" value="<%=puertoD%>"/>
                </div>
                <c:if test="${errores != null && errores.containsKey('puertoD')}">
                    <div style="color: red;">${errores.puertoD}</div>
                </c:if>
            </div>
            <div id="destino_div" style="margin: 1em;">
                <label>Nombre de la Colección Destino</label>
                <div>
                    <input type="text" name="destino" value="<%=destino%>"/>
                </div>
                <c:if test="${errores != null && errores.containsKey('destino')}">
                    <div style="color: red;">${errores.destino}</div>
                </c:if>
            </div>
            <div style="margin: 1em">
                <label>Elige la operación que quieres realizar</label>
                <div>
                    <select id="operacion" name="operacion">
                        <option value="0" 
                                <c:choose>
                                    <c:when test="${operacion.equals('0')}">
                                        <c:out value="selected"/>
                                    </c:when>
                                    <c:otherwise>
                                        <c:out value=""/>
                                    </c:otherwise>
                                </c:choose> > 
                            selecciona
                        </option> 
                        <option value="1" selected > 
                            Indexar
                        </option>
                        <option value="2"
                                <c:choose>
                                    <c:when test="${operacion.equals('2')}">
                                        <c:out value="selected"/>
                                    </c:when>
                                    <c:otherwise>
                                        <c:out value=""/>
                                    </c:otherwise>
                                </c:choose> > 
                            Guardar
                        </option>
                        <option value="3" 
                              <c:choose>
                                    <c:when test="${operacion.equals('3')}">
                                        <c:out value="selected"/>
                                    </c:when>
                                    <c:otherwise>
                                        <c:out value=""/>
                                    </c:otherwise>
                                </c:choose> > 
                            Indexar y Guardar
                        </option>
                        <option value="4" 
                                <c:choose>
                                    <c:when test="${operacion.equals('4')}">
                                        <c:out value="selected"/>
                                    </c:when>
                                    <c:otherwise>
                                        <c:out value=""/>
                                    </c:otherwise>
                                </c:choose> > 
                            Indexar Archivo JSON
                        </option>
                    </select>
                </div>
                <c:if test="${errores != null && errores.containsKey('operacion')}">
                    <div style="color: red;">${errores.operacion}</div>
                </c:if>
            </div>
            <div id="guardar-ubicacion" style="margin: 1em;">
                <label>Ubicación: </label>
                <input style="width : 500px;" type=text name="ubicacion" placeholder="coloca la ubicación dónde quieres guardar el archivo" value="<%=ubicacion%>"/>
                <c:if test="${errores != null && errores.containsKey('ubicacion')}" >
                    <div style="color: red;">${errores.ubicacion}</div>
                </c:if>
            </div>
            <div id="cargar-ubicacion" style="margin: 1em; display: none">
                <label>Elige el archivo json a indexar</label>
                <input style="width : 500px;" type="file" name="archivo"/>
                <c:if test="${errores != null && errores.containsKey('archivo')}">
                    <div style="color: red;">${errores.archivo}</div>
                </c:if>
            </div>  
            <div style="margin-left: 1em">
                <input type="submit" value="Ejecutar"/>
            </div>            
        </form> 
    </body>

    <script>

        var select = document.getElementById("operacion");
        var ip = document.getElementById("ip_div");
        var puerto = document.getElementById("puerto_div");
        var origen = document.getElementById("origen_div");
        var guardarUbicacion = document.getElementById("guardar-ubicacion");
        var cargarUbicacion = document.getElementById("cargar-ubicacion");
        var inputIds = document.getElementById("input_ids");
        var idsDiv = document.getElementById("ids_div");
        var ipDestino = document.getElementById("ipD_div");
        var puertoDestino = document.getElementById("puertoD_div");
        var destino = document.getElementById("destino_div");

        select.addEventListener("change", function () {
            if (select.value === "0" || select.value === "1") {

                guardarUbicacion.style.display = "none";
                cargarUbicacion.style.display = "none";
                idsDiv.style.display = "block";
                ip.style.display = "block";
                puerto.style.display = "block";
                origen.style.display = "block";
                ipDestino.style.display = "block";
                puertoDestino.style.display = "block";
                destino.style.display = "block";

            } else if (select.value === "2") {
                
                guardarUbicacion.style.display = "block";
                cargarUbicacion.style.display = "none";
                ip.style.display = "block";
                puerto.style.display = "block";
                origen.style.display = "block";
                ipDestino.style.display = "none";
                puertoDestino.style.display = "none";
                destino.style.display = "none";

            } else if (select.value === "3") {

                guardarUbicacion.style.display = "block";
                cargarUbicacion.style.display = "none";
                idsDiv.style.display = "block";
                ip.style.display = "block";
                puerto.style.display = "block";
                origen.style.display = "block";
                ipDestino.style.display = "block";
                puertoDestino.style.display = "block";
                destino.style.display = "block";

            } else if (select.value === "4") {

                guardarUbicacion.style.display = "none";
                cargarUbicacion.style.display = "block";
                inputIds.required = false;
                idsDiv.style.display = "none";
                ip.style.display = "none";
                puerto.style.display = "none";
                origen.style.display = "none";
                ipDestino.style.display = "block";
                puertoDestino.style.display = "block";
                destino.style.display = "block";

            }
        });
    </script>

</html>


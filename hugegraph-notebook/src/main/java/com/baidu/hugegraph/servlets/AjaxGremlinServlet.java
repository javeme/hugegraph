package com.baidu.hugegraph.servlets;

import com.google.common.io.ByteStreams;

import java.io.*;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;



/**
 * Created by jishilei on 2017/3/1.
 */
public class AjaxGremlinServlet extends HttpServlet{
    private static final long serialVersionUID = 1L;


    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {

        //TODO: query from gremlin server
        try{
            InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("sample.json");
            OutputStream output = response.getOutputStream();

            ByteStreams.copy(inputStream, output);

            output.flush();


        }catch (Exception ex){

        }

    }
}

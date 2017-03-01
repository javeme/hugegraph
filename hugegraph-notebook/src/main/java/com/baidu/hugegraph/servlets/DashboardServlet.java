package com.baidu.hugegraph.servlets;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


/**
 * Created by jishilei on 2017/2/28.
 */
public class DashboardServlet extends HttpServlet{
    private static final long serialVersionUID = 1L;

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {


        PrintWriter writer = response.getWriter();
        com.baidu.hugegraph.tmpl.DashboardTmpl t = new com.baidu.hugegraph.tmpl.DashboardTmpl();
        t.render(writer);
        writer.flush();
    }
}

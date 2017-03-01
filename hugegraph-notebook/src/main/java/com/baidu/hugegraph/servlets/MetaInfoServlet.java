package com.baidu.hugegraph.servlets;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by jishilei on 2017/3/1.
 */
public class MetaInfoServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {


        PrintWriter writer = response.getWriter();
        com.baidu.hugegraph.tmpl.MetaInfoTmpl t = new com.baidu.hugegraph.tmpl.MetaInfoTmpl();
        t.render(writer);
        writer.flush();
    }
}

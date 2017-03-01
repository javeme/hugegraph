package com.baidu.hugegraph;

import com.baidu.hugegraph.http.*;
import com.baidu.hugegraph.servlets.*;
import org.eclipse.jetty.servlet.ServletHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jishilei on 2017/2/28.
 */
public class Notebook {

    private static final Logger logger = LoggerFactory.getLogger(Notebook.class);

    public static void main(String[] args) throws Exception {

        //TODO : get port from config
        int port = 8080;
        HttpServer server = new HttpServer(8080);

        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(DashboardServlet.class, "/cluster");
        handler.addServletWithMapping(QueryServlet.class, "/query");
        handler.addServletWithMapping(MetaInfoServlet.class, "/meta");
        handler.addServletWithMapping(AddClusterServlet.class, "/add");
        handler.addServletWithMapping(AjaxGremlinServlet.class, "/ajax/gremlin");
        server.addHandler(handler);

        logger.info("HugeGraph Notebook started on port " + port);
        server.start();
        server.join();


    }
}

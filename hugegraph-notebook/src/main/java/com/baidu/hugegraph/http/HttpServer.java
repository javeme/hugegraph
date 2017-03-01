package com.baidu.hugegraph.http;

import java.io.IOException;
import java.net.URL;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * Created by jishilei on 2017/2/28.
 */
public class HttpServer {
    protected HandlerCollection handlers;
    protected final Server server;
    protected final int port;

    protected ServletContextHandler handler;

    public HttpServer(int port) {
        this.server = new Server();
        this.port = port;

        initializeWebServer();
    }


    private void initializeWebServer() {
        handlers = new HandlerCollection();
        handlers.addHandler(addResourceHandlers());
    }

    protected ResourceHandler addResourceHandlers() {

        ResourceHandler handler = new ResourceHandler();
        handler.setResourceBase(getResource("webapps").toString());
        handler.setStylesheet(getResource("webapps/static/assets/css").toString());
        handler.setDirAllowed(false);
        return handler;

    }

    public URL getResource(String resource) {
        return Thread.currentThread().getContextClassLoader().getResource(resource);
    }

    public void start() throws Exception {

        server.addConnector(createConnector());
        server.setHandler(handlers);
        server.setStopAtShutdown(true);
        server.start();

    }

    private ServerConnector createConnector() {
        ServerConnector http = new ServerConnector(server);
        http.setHost("0.0.0.0");
        http.setPort(this.port);
        http.setIdleTimeout(30000);
        return http;
    }

    public void join() throws InterruptedException {
        server.join();
    }

    public void stop() throws Exception {
        server.stop();
    }

    public void addHandler(AbstractHandler handler) {
        handlers.addHandler(handler);
    }



}

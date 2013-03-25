package org.apache.oozie.client;

import org.apache.ivory.security.AuthenticationInitializationService;
import org.apache.ivory.util.RuntimeProperties;
import org.apache.ivory.util.StartupProperties;
import org.apache.oozie.client.rest.RestConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CustomOozieClient extends AuthOozieClient {

    private static final Map<String, String> none = new HashMap<String, String>();

    public CustomOozieClient(String oozieUrl) {
        this(oozieUrl, StartupProperties.get().getProperty
                (AuthenticationInitializationService.AUTHENTICATION_TYPE,
                        AuthType.SIMPLE.name()));
    }

    public CustomOozieClient(String oozieUrl, String authOption) {
        super(oozieUrl, authOption);
    }

    public Properties getConfiguration() throws OozieClientException {
        return (new OozieConfiguration(
                RestConstants.ADMIN_CONFIG_RESOURCE)).call();
    }

    public Properties getProperties() throws OozieClientException {
        return (new OozieConfiguration(
                RestConstants.ADMIN_JAVA_SYS_PROPS_RESOURCE)).call();
    }

    @Override
    protected HttpURLConnection createConnection(URL url, String method)
            throws IOException, OozieClientException {
        HttpURLConnection conn = super.createConnection(url, method);
        
        int connectTimeout = Integer.valueOf(RuntimeProperties.get().
                getProperty("oozie.connect.timeout", "1000"));
        conn.setConnectTimeout(connectTimeout);

        int readTimeout = Integer.valueOf(RuntimeProperties.get().
                getProperty("oozie.read.timeout", "45000"));
        conn.setReadTimeout(readTimeout);
        
        return conn;
    }
    
    private class OozieConfiguration extends ClientCallable<Properties> {

        public OozieConfiguration(String resource) {
            super("GET", RestConstants.ADMIN, resource, none);
        }

        @Override
        protected Properties call(HttpURLConnection conn)
                throws IOException, OozieClientException {
            conn.setRequestProperty("content-type",
                    RestConstants.XML_CONTENT_TYPE);
            if ((conn.getResponseCode() == HttpURLConnection.HTTP_OK)) {
                Reader reader = new InputStreamReader(conn.getInputStream());
                JSONObject json = (JSONObject) JSONValue.parse(reader);
                Properties props = new Properties();
                for (Object key : json.keySet()) {
                    props.put(key, json.get(key));
                }
                return props;
            }
            else {
                handleError(conn);
                return null;
            }
        }
    }
}

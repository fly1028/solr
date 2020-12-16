package org.apache.solr.servlet;

import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

public class StopJetty {
  private static final int EXIT_USAGE = 1;
  private static final int ERR_LOGGING = -1;
  private static final int ERR_INVOKE_MAIN = -2;
  private static final int ERR_NOT_STOPPED = -4;
  private static final int ERR_UNKNOWN = -5;

  public static void main(String[] args) {
    int port = Integer.getInteger("STOP.PORT",-1);
    String key = System.getProperty("STOP.KEY",null);
    int timeout =  Integer.getInteger("STOP.WAIT",0);
    stop(port,key,timeout);
  }

  public static void stop(int port, String key, int timeout)
  {
    int _port = port;
    String _key = key;
    try
    {
      if (_port <= 0)
      {
        System.err.println("STOP.PORT system property must be specified");
      }
      if (_key == null)
      {
        _key = "";
        System.err.println("STOP.KEY system property must be specified");
        System.err.println("Using empty key");
      }
      Socket s = new Socket(InetAddress.getByName("127.0.0.1"),_port);
      if (timeout > 0)
        s.setSoTimeout(timeout * 1000);
      try
      {
        OutputStream out = s.getOutputStream();
        out.write((_key + "\r\nstopexit\r\n").getBytes());
        out.flush();
        if (timeout > 0)
        {
          System.err.printf("Waiting %,d seconds for jetty to stop%n",timeout);
          LineNumberReader lin = new LineNumberReader(new InputStreamReader(s.getInputStream()));
          String response;
          while ((response = lin.readLine()) != null)
          {
           // Config.debug("Received \"" + response + "\"");
            if ("Stopped".equals(response))
              System.err.println("Server reports itself as Stopped");
          }
        }
      }
      finally
      {
        s.close();
      }
    }
    catch (SocketTimeoutException e)
    {
      System.err.println("Timed out waiting for stop confirmation");
      System.exit(ERR_UNKNOWN);
    }
    catch (ConnectException e)
    {
      e.printStackTrace(System.err);
    }
    catch (Exception e)
    {
      e.printStackTrace(System.err);
    }
  }

}
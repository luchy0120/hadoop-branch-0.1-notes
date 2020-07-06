/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ipc;

import java.net.Socket;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;

import java.io.IOException;
import java.io.EOFException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;

import java.rmi.RemoteException;

import java.util.Hashtable;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.apache.hadoop.util.LogFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.UTF8;

/** A client for an IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @author Doug Cutting
 * @see Server
 */
public class Client {
  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.hadoop.ipc.Client");

  private Hashtable connections = new Hashtable();

  private Class valueClass;                       // class of call values
  private int timeout ;// timeout for calls
  private int counter;                            // counter for call ids
  private boolean running = true;                 // true while client runs
  private Configuration conf;

  // 一次调用，调用id，调用的invocation，调用的返回值，调用的error，调用被处理的i/o时刻，调用是否结束
  /** A call waiting for a value. */
  private class Call {
    int id;                                       // call id
    Writable param;                               // parameter
    Writable value;                               // value, null if error
    String error;                                 // error, null if value
    long lastActivity;                            // time of last i/o
    boolean done;                                 // true when call is done

    protected Call(Writable param) {
      this.param = param;
      synchronized (Client.this) {
        this.id = counter++;
      }
      touch();
    }

    /** Called by the connection thread when the call is complete and the
     * value or error string are available.  Notifies by default.  */
    public synchronized void callComplete() {
        notify();                                 // notify caller
    }
    // 每次读取或者写出和这个Call相关的网络数据时，记录时间
    /** Update lastActivity with the current time. */
    public synchronized void touch() {
      lastActivity = System.currentTimeMillis();
    }

    // 设置这个Call获得的结果
    public synchronized void setResult(Writable value, String error) {
      this.value = value;
      this.error = error;
      this.done = true;
    }
    
  }
  // 建立socket连接，获得输入输出流，并开启单独线程监听
  /** Thread that reads responses and notifies callers.  Each connection owns a
   * socket connected to a remote address.  Calls are multiplexed through this
   * socket: responses may be delivered out of order. */
  private class Connection extends Thread {
    private InetSocketAddress address;            // address of server
    private Socket socket;                        // connected socket
    private DataInputStream in;                   
    private DataOutputStream out;
    private Hashtable calls = new Hashtable();    // currently active calls  // 当前线程正在处理的call
    private Call readingCall;
    private Call writingCall;

    public Connection(InetSocketAddress address) throws IOException {
      this.address = address;
      this.socket = new Socket(address.getAddress(), address.getPort());
      socket.setSoTimeout(timeout);
      this.in = new DataInputStream
        (new BufferedInputStream
         (new FilterInputStream(socket.getInputStream()) {
             public int read(byte[] buf, int off, int len) throws IOException {
               int value = super.read(buf, off, len);
               if (readingCall != null) {
                 readingCall.touch();         // 当读到一些数据时，设置这个Call的activity时间
               }
               return value;
             }
          }));
      this.out = new DataOutputStream
        (new BufferedOutputStream
         (new FilterOutputStream(socket.getOutputStream()) {
             public void write(byte[] buf, int o, int len) throws IOException {
               out.write(buf, o, len);
               if (writingCall != null) {
                 writingCall.touch();         // 当向外写一些数据时，设置这个Call的activity时间
               }
             }
           }));
      this.setDaemon(true);
      this.setName("Client connection to "
                   + address.getAddress().getHostAddress()
                   + ":" + address.getPort());
    }

    public void run() {
      LOG.info(getName() + ": starting");          // 线程开始了，伙计
      try {
        while (running) {
          int id;
          try {
            id = in.readInt();                    // try to read an id  ，从namenode取回一个int，是call的id
          } catch (SocketTimeoutException e) {
            continue;
          }

          if (LOG.isLoggable(Level.FINE))
            LOG.fine(getName() + " got value #" + id);

          Call call = (Call)calls.remove(new Integer(id));           // 根据Call id，获取那个Call
          boolean isError = in.readBoolean();     // read if error
          if (isError) {
            UTF8 utf8 = new UTF8();
            utf8.readFields(in);                  // read error string
            call.setResult(null, utf8.toString());       // 设置错误信息
          } else {
            Writable value = makeValue();
            try {
              readingCall = call;
              if(value instanceof Configurable) {
                ((Configurable) value).setConf(conf);
              }
              value.readFields(in);                 // read value
            } finally {
              readingCall = null;
            }
            call.setResult(value, null);
          }
          call.callComplete();                   // deliver result to caller ， 唤醒在这个Call上挂起的调用者
        }
      } catch (EOFException eof) {
          // This is what happens when the remote side goes down
      } catch (Exception e) {
        LOG.log(Level.INFO, getName() + " caught: " + e, e);
      } finally {
        close();
      }
    }

    /** Initiates a call by sending the parameter to the remote server.
     * Note: this is not called from the Connection thread, but by other
     * threads.
     */
    public void sendParam(Call call) throws IOException {
      boolean error = true;
      try {
        calls.put(new Integer(call.id), call);      // 这个call开始处理了
        synchronized (out) {
          if (LOG.isLoggable(Level.FINE))
            LOG.fine(getName() + " sending #" + call.id);
          try {
            writingCall = call;
            out.writeInt(call.id);     // 发送Call id
            call.param.write(out);     // 发送 Invocation
            out.flush();
          } finally {
            writingCall = null;
          }
        }
        error = false;
      } finally {
        if (error)
          close();                                // close on error
      }
    }

    /** Close the connection and remove it from the pool. */
    public void close() {
      LOG.info(getName() + ": closing");
      synchronized (connections) {
        connections.remove(address);              // remove connection
      }
      try {
        socket.close();                           // close socket
      } catch (IOException e) {}
    }

  }
   // 更高级的Call，一次发n个调用过去，获得n个结果
  /** Call implementation used for parallel calls. */
  private class ParallelCall extends Call {
    private ParallelResults results;     // 持有多个result的句柄
    private int index;                   // 当前Call的下标
    
    public ParallelCall(Writable param, ParallelResults results, int index) {
      super(param);
      this.results = results;
      this.index = index;
    }

    /** Deliver result to result collector. */
    public void callComplete() {
      results.callComplete(this);
    }
  }

  /** Result collector for parallel calls. */
  private static class ParallelResults {
    private Writable[] values;
    private int size;
    private int count;

    public ParallelResults(int size) {
      this.values = new Writable[size];
      this.size = size;
    }
    // 一个Call获得一个结果，获得所有结果后，对挂起的线程解挂
    /** Collect a result. */
    public synchronized void callComplete(ParallelCall call) {
      values[call.index] = call.value;            // store the value
      count++;                                    // count it
      if (count == size)                          // if all values are in
        notify();                                 // then notify waiting caller
    }
  }

  /** Construct an IPC client whose values are of the given {@link Writable}
   * class. */
  public Client(Class valueClass, Configuration conf) {
    this.valueClass = valueClass;
    this.timeout = conf.getInt("ipc.client.timeout",10000);    // client 发送一个Call后等多长时间算timeout
    this.conf = conf;
  }

  /** Stop all threads related to this client.  No further calls may be made
   * using this client. */
  public void stop() {
    LOG.info("Stopping client");
    try {
      Thread.sleep(timeout);                        // let all calls complete
    } catch (InterruptedException e) {}
    running = false;
  }

  /** Sets the timeout used for network i/o. */
  public void setTimeout(int timeout) { this.timeout = timeout; }

  // 传入一个 Invocation，以及一个远程地址
  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code>, returning the value.  Throws exceptions if there are
   * network problems or if the remote code threw an exception. */
  public Writable call(Writable param, InetSocketAddress address)
    throws IOException {
    Connection connection = getConnection(address);     // 开线程，建立socket连接，启动线程
    Call call = new Call(param);                        // 一次调用的Call
    synchronized (call) {
      connection.sendParam(call);                 // send the parameter ，从RPC的Proxy方法拦截后交给Client，Client调用Call
      long wait = timeout;
      do {
        try {
          call.wait(wait);                       // wait for the result，call被发出去以后，就挂起当前线程进行等待
        } catch (InterruptedException e) {}
        wait = timeout - (System.currentTimeMillis() - call.lastActivity);   // 距离timeout的时刻还要等多久，还没timeout就再等等
      } while (!call.done && wait > 0);

      if (call.error != null) {
        throw new RemoteException(call.error);
      } else if (!call.done) {
        throw new IOException("timed out waiting for response");
      } else {
        return call.value;   // 获得调用的值
      }
    }
  }
   // 同时发多个调用给多个address，一个调用对应一个address
  /** Makes a set of calls in parallel.  Each parameter is sent to the
   * corresponding address.  When all values are available, or have timed out
   * or errored, the collected results are returned in an array.  The array
   * contains nulls for calls that timed out or errored.  */
  public Writable[] call(Writable[] params, InetSocketAddress[] addresses)
    throws IOException {
    if (addresses.length == 0) return new Writable[0];

    ParallelResults results = new ParallelResults(params.length);
    synchronized (results) {
      for (int i = 0; i < params.length; i++) {
        ParallelCall call = new ParallelCall(params[i], results, i);      // 构造第i个Call
        try {
          Connection connection = getConnection(addresses[i]);            // 拿一下连接
          connection.sendParam(call);             // send each parameter
        } catch (IOException e) {
          LOG.info("Calling "+addresses[i]+" caught: " + e); // log errors
          results.size--;                         //  wait for one fewer result
        }
      }
      try {
        results.wait(timeout);                    // wait for all results
      } catch (InterruptedException e) {}

      if (results.count == 0) {
        throw new IOException("no responses");
      } else {
        return results.values;                       // 获得所有结果
      }
    }
  }
  //每一个远程网络地址，就创建一个单独线程去连接
  /** Get a connection from the pool, or create a new one and add it to the
   * pool.  Connections to a given host/port are reused. */
  private Connection getConnection(InetSocketAddress address)
    throws IOException {
    Connection connection;
    synchronized (connections) {
      connection = (Connection)connections.get(address);
      if (connection == null) {
        connection = new Connection(address);
        connections.put(address, connection);
        connection.start();
      }
    }
    return connection;
  }
  // 根据valueClass创建一个默认对象
  private Writable makeValue() {
    Writable value;                             // construct value
    try {
      value = (Writable)valueClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e.toString());
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e.toString());
    }
    return value;
  }

}

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

package org.apache.hadoop.conf;

import java.util.*;
import java.net.URL;
import java.io.*;
import java.util.logging.Logger;

import javax.xml.parsers.*;

import org.w3c.dom.*;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.util.LogFormatter;

/** Provides access to configuration parameters.  Configurations are specified
 * by resources.  A resource contains a set of name/value pairs.
 *
 * <p>Each resources is named by either a String or by a File.  If named by a
 * String, then the classpath is examined for a file with that name.  If a
 * File, then the filesystem is examined directly, without referring to the
 * CLASSPATH.
 *
 * <p>Configuration resources are of two types: default and
 * final.  Default values are loaded first and final values are loaded last, and
 * thus override default values.
 *
 * <p>Hadoop's default resource is the String "hadoop-default.xml" and its
 * final resource is the String "hadoop-site.xml".  Other tools built on Hadoop
 * may specify additional resources.
 */
public class Configuration {
  private static final Logger LOG =
    LogFormatter.getLogger("org.apache.hadoop.conf.Configuration");
  // 默认资源是哪些
  private ArrayList defaultResources = new ArrayList();
  // 非默认资源是哪些
  private ArrayList finalResources = new ArrayList();

  private Properties properties;
  private ClassLoader classLoader = 
    Thread.currentThread().getContextClassLoader();

  /** A new configuration. */
  public Configuration() {
    defaultResources.add("hadoop-default.xml");
    finalResources.add("hadoop-site.xml");
  }

  /** A new configuration with the same settings cloned from another. */
  public Configuration(Configuration other) {
    this.defaultResources = (ArrayList)other.defaultResources.clone();
    this.finalResources = (ArrayList)other.finalResources.clone();
    if (other.properties != null)
      this.properties = (Properties)other.properties.clone();
  }

  /** Add a default resource. */
  public void addDefaultResource(String name) {
    addResource(defaultResources, name);
  }

  /** Add a default resource. */
  public void addDefaultResource(File file) {
    addResource(defaultResources, file);
  }

  /** Add a final resource. */
  public void addFinalResource(String name) {
    addResource(finalResources, name);
  }

  /** Add a final resource. */
  public void addFinalResource(File file) {
    addResource(finalResources, file);
  }

  // 将一个String添加进resource中，并将properties清空，使它重新装载
  private synchronized void addResource(ArrayList resources, Object resource) {
    resources.add(resource);                      // add to resources
    properties = null;                            // trigger reload
  }
  
  /**
   * Returns the value of the <code>name</code> property, or null if no such
   * property exists.
   */
  // 从Properties中取出name对应的value，value是任意Object
  public Object getObject(String name) { return getProps().get(name);}

  /** Sets the value of the <code>name</code> property. */
  // 设置Propperties中的name和value吗，value是任意Object
  public void setObject(String name, Object value) {
    getProps().put(name, value);
  }

  /** Returns the value of the <code>name</code> property.  If no such property
   * exists, then <code>defaultValue</code> is returned.
   */
  public Object get(String name, Object defaultValue) {
    Object res = getObject(name);
    if (res != null) return res;
    else return defaultValue;
  }
  
  /** Returns the value of the <code>name</code> property, or null if no
   * such property exists. */
  public String get(String name) { return getProps().getProperty(name);}

  /** Sets the value of the <code>name</code> property. */
  // 将value转换成String，然后将name，value放入Properties
  public void set(String name, Object value) {
    getProps().setProperty(name, value.toString());
  }
  
  /** Returns the value of the <code>name</code> property.  If no such property
   * exists, then <code>defaultValue</code> is returned.
   */
  public String get(String name, String defaultValue) {
     return getProps().getProperty(name, defaultValue);
  }
  
  /** Returns the value of the <code>name</code> property as an integer.  If no
   * such property is specified, or if the specified value is not a valid
   * integer, then <code>defaultValue</code> is returned.
   */
  public int getInt(String name, int defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      return Integer.parseInt(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /** Sets the value of the <code>name</code> property to an integer. */
  public void setInt(String name, int value) {
    set(name, Integer.toString(value));
  }


  /** Returns the value of the <code>name</code> property as a long.  If no
   * such property is specified, or if the specified value is not a valid
   * long, then <code>defaultValue</code> is returned.
   */
  public long getLong(String name, long defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      return Long.parseLong(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /** Sets the value of the <code>name</code> property to a long. */
  public void setLong(String name, long value) {
    set(name, Long.toString(value));
  }

  /** Returns the value of the <code>name</code> property as a float.  If no
   * such property is specified, or if the specified value is not a valid
   * float, then <code>defaultValue</code> is returned.
   */
  public float getFloat(String name, float defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      return Float.parseFloat(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /** Returns the value of the <code>name</code> property as an boolean.  If no
   * such property is specified, or if the specified value is not a valid
   * boolean, then <code>defaultValue</code> is returned.  Valid boolean values
   * are "true" and "false".
   */
  public boolean getBoolean(String name, boolean defaultValue) {
    String valueString = get(name);
    if ("true".equals(valueString))
      return true;
    else if ("false".equals(valueString))
      return false;
    else return defaultValue;
  }

  /** Sets the value of the <code>name</code> property to an integer. */
  public void setBoolean(String name, boolean value) {
    set(name, Boolean.toString(value));
  }

  /** Returns the value of the <code>name</code> property as an array of
   * strings.  If no such property is specified, then <code>null</code>
   * is returned.  Values are whitespace or comma delimted.
   */
  // 根据name拿到由逗号分隔的字符串
  public String[] getStrings(String name) {
    String valueString = get(name);
    if (valueString == null)
      return null;
    // 使用空白或者逗号分割的字符串
    StringTokenizer tokenizer = new StringTokenizer (valueString,", \t\n\r\f");
    List values = new ArrayList();
    while (tokenizer.hasMoreTokens()) {
      values.add(tokenizer.nextToken());
    }
    // 分成一个array
    return (String[])values.toArray(new String[values.size()]);
  }

  /** Returns the value of the <code>name</code> property as a Class.  If no
   * such property is specified, then <code>defaultValue</code> is returned.
   */
  // 根据name拿到class的名字，再用forName去寻找相应的class
  public Class getClass(String name, Class defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      return Class.forName(valueString);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /** Returns the value of the <code>name</code> property as a Class.  If no
   * such property is specified, then <code>defaultValue</code> is returned.
   * An error is thrown if the returned class does not implement the named
   * interface. 
   */
  // 如果theclass没有实现接口，就报错
  public Class getClass(String propertyName, Class defaultValue,Class xface) {
    try {
      Class theClass = getClass(propertyName, defaultValue);
      if (theClass != null && !xface.isAssignableFrom(theClass))
        throw new RuntimeException(theClass+" not "+xface.getName());
      return theClass;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Sets the value of the <code>name</code> property to the name of a class.
   * First checks that the class implements the named interface. 
   */
  // 如果theclass实现了xface这个inteface，那么就存入name和class的name
  public void setClass(String propertyName, Class theClass, Class xface) {
    if (!xface.isAssignableFrom(theClass))
      throw new RuntimeException(theClass+" not "+xface.getName());
    set(propertyName, theClass.getName());
  }

  /** Returns a file name under a directory named in <i>dirsProp</i> with the
   * given <i>path</i>.  If <i>dirsProp</i> contains multiple directories, then
   * one is chosen based on <i>path</i>'s hash code.  If the selected directory
   * does not exist, an attempt is made to create it.
   */
  // 返回任一个File
  public File getFile(String dirsProp, String path) throws IOException {
    // 拿到所有的目录
    String[] dirs = getStrings(dirsProp);
    // path的hash值
    int hashCode = path.hashCode();
    for (int i = 0; i < dirs.length; i++) {  // try each local dir
      // 根据hash值去pick一个dir
      int index = (hashCode+i & Integer.MAX_VALUE) % dirs.length;
      //  “dir/path” 检查这个长路径，是不是存在
      File file = new File(dirs[index], path).getAbsoluteFile();
      File dir = file.getParentFile();
      if (dir.exists() || dir.mkdirs()) {
        return file;    // 如果父路径存在，就返回这个文件
      }
    }
    throw new IOException("No valid local directories in property: "+dirsProp);
  }


  /** Returns the URL for the named resource. */
  public URL getResource(String name) {
    return classLoader.getResource(name);
  }
  /** Returns an input stream attached to the configuration resource with the
   * given <code>name</code>.
   */
  // 获取name指定的配置资源，并打开一个流来读取
  public InputStream getConfResourceAsInputStream(String name) {
    try {
      URL url= getResource(name);

      if (url == null) {
        LOG.info(name + " not found");
        return null;
      } else {
        LOG.info("found resource " + name + " at " + url);
      }

      return url.openStream();
    } catch (Exception e) {
      return null;
    }
  }

  /** Returns a reader attached to the configuration resource with the
   * given <code>name</code>.
   */
  // 获取name指定的配置资源，并打开一个reader来读取
  public Reader getConfResourceAsReader(String name) {
    try {
      URL url= getResource(name);

      if (url == null) {
        LOG.info(name + " not found");
        return null;
      } else {
        LOG.info("found resource " + name + " at " + url);
      }

      return new InputStreamReader(url.openStream());
    } catch (Exception e) {
      return null;
    }
  }
  // 如果properties是空的，那么会新建Properties，并将default和final的资源都装载一遍
  private synchronized Properties getProps() {
    if (properties == null) {
      Properties newProps = new Properties();
      // 默认的资源，根据顺序往properties里面添加，因为他们不会存在覆盖情况。（会打印log出来）
      loadResources(newProps, defaultResources, false, false);
      // 非默认资源，根据相反的顺序往properties里面添加，顺序在前的资源覆盖顺序在后的资源
      loadResources(newProps, finalResources, true, true);
      properties = newProps;
    }
    return properties;
  }
  // reverse，装载先后顺序，是不是反向的
  // 遍历resouce资源，一个一个装载
  private void loadResources(Properties props,
                             ArrayList resources,
                             boolean reverse, boolean quiet) {
    ListIterator i = resources.listIterator(reverse ? resources.size() : 0);
    while (reverse ? i.hasPrevious() : i.hasNext()) {
      loadResource(props, reverse ? i.previous() : i.next(), quiet);
    }
  }
  // 装载一个resource，如果name是文件，就从文件装载xml，如果是string，使用classLoader去获取资源
  private void loadResource(Properties properties, Object name, boolean quiet) {
    try {
      DocumentBuilder builder =
        DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = null;

      if (name instanceof String) {               // a CLASSPATH resource
        URL url = getResource((String)name);
        if (url != null) {
          LOG.info("parsing " + url);
          doc = builder.parse(url.toString());
        }
      } else if (name instanceof File) {          // a file resource
        File file = (File)name;
        if (file.exists()) {
          LOG.info("parsing " + file);
          doc = builder.parse(file);
        }
      }

      if (doc == null) {
        if (quiet)
          return;
        throw new RuntimeException(name + " not found");
      }

      Element root = doc.getDocumentElement();
      if (!"configuration".equals(root.getTagName()))
        LOG.severe("bad conf file: top-level element not <configuration>");
      NodeList props = root.getChildNodes();
      for (int i = 0; i < props.getLength(); i++) {
        Node propNode = props.item(i);
        if (!(propNode instanceof Element))
          continue;
        Element prop = (Element)propNode;
        if (!"property".equals(prop.getTagName()))
          LOG.warning("bad conf file: element not <property>");
        NodeList fields = prop.getChildNodes();
        String attr = null;
        String value = null;
        for (int j = 0; j < fields.getLength(); j++) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element))
            continue;
          Element field = (Element)fieldNode;
          if ("name".equals(field.getTagName()))
            attr = ((Text)field.getFirstChild()).getData();
          if ("value".equals(field.getTagName()) && field.hasChildNodes())
            value = ((Text)field.getFirstChild()).getData();
        }
        if (attr != null && value != null)
          properties.setProperty(attr, value);
      }
        
    } catch (Exception e) {
      LOG.severe("error parsing conf file: " + e);
      throw new RuntimeException(e);
    }
    
  }

  /** Writes non-default properties in this configuration.*/
  // 将非默认的属性通过xml输出来
  //  <configuration>
  //
  //      <property>
  //           <name> </name>
  //           <value> </value>
  //      </property>
  //
  // </configuration>
  public void write(OutputStream out) throws IOException {
    Properties properties = getProps();
    try {
      Document doc =
        DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
      Element conf = doc.createElement("configuration");
      doc.appendChild(conf);
      conf.appendChild(doc.createTextNode("\n"));
      for (Enumeration e = properties.keys(); e.hasMoreElements();) {
        String name = (String)e.nextElement();
        Object object = properties.get(name);
        String value = null;
        if(object instanceof String) {
          value = (String) object;
        }else {
          continue;
        }
        Element propNode = doc.createElement("property");
        conf.appendChild(propNode);
      
        Element nameNode = doc.createElement("name");
        nameNode.appendChild(doc.createTextNode(name));
        propNode.appendChild(nameNode);
      
        Element valueNode = doc.createElement("value");
        valueNode.appendChild(doc.createTextNode(value));
        propNode.appendChild(valueNode);

        conf.appendChild(doc.createTextNode("\n"));
      }
    
      DOMSource source = new DOMSource(doc);
      StreamResult result = new StreamResult(out);
      TransformerFactory transFactory = TransformerFactory.newInstance();
      Transformer transformer = transFactory.newTransformer();
      transformer.transform(source, result);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("Configuration: ");
    sb.append("defaults: ");
    toString(defaultResources, sb);
    sb.append("final: ");
    toString(finalResources, sb);
    return sb.toString();
  }

  private void toString(ArrayList resources, StringBuffer sb) {
    ListIterator i = resources.listIterator();
    while (i.hasNext()) {
      if (i.nextIndex() != 0) {
        sb.append(" , ");
      }
      Object obj = i.next();
      if (obj instanceof File) {
        sb.append((File)obj);
      } else {
        sb.append((String)obj);
      }
    }
  }

  /** For debugging.  List non-default properties to the terminal and exit. */
  public static void main(String[] args) throws Exception {
    new Configuration().write(System.out);
  }

}

package com.solace.connector.kafka.connect.source;

public class VersionUtil {

  /**
   * Returns the projects version number for the connector.
   * @return Version Number
   */
  public static String getVersion() {
    return "${version}";
  }

}

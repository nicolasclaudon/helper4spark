package com.capgemini.id.spark.tools

object PlatformUtils {
  private val OS_NAME = System.getProperty("os.name").toLowerCase()
  def isLinux: Boolean = OS_NAME.startsWith("linux")
  def isMac: Boolean = OS_NAME.startsWith("mac")
  def isWindows: Boolean = OS_NAME.startsWith("windows")
}

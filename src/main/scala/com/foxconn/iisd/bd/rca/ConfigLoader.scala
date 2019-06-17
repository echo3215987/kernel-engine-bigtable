package com.foxconn.iisd.bd.rca

import java.io.File
import java.util

import org.apache.spark.sql.SparkSession
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.io.BufferedSource
import scala.reflect.ClassTag

@SerialVersionUID(100L)
class ConfigLoader() extends Serializable {

    var defaultConfigPath: String = null
    var yaml: Yaml = null
    var defaultYamlMap: util.LinkedHashMap[String, AnyRef] = null
    var yamlLoaderMap: util.LinkedHashMap[String, AnyRef] = null
    val DEFAULT_CONFIG_NAME: String = """default.yaml"""
    var sparkSession: SparkSession = null

    def loadConfig[T:ClassTag](source: BufferedSource): T = {
        yaml = new Yaml(new Constructor(implicitly[ClassTag[T]].runtimeClass))
        yaml.load(source.getLines().mkString("\n")).asInstanceOf[T]
    }

    def loadConfig[T:ClassTag](configFile: File): T = {
        if (configFile.exists) {
            loadConfig(scala.io.Source.fromFile(configFile))
        } else {
            throw new Exception("Configuration file not found: " + configFile.getCanonicalPath)
        }
    }

    def loadConfig[T:ClassTag](configFileName: String): T = {
        loadConfig(new File(configFileName))
    }

    def getString(section: String, name: String): String = {
           getSectionByName(section).get(name)
    }

    def getSectionByName(name: String): util.LinkedHashMap[String, String] = {
        if(sparkSession == null) {
            try {
                getSection(DEFAULT_CONFIG_NAME).get(name).asInstanceOf[util.LinkedHashMap[String, String]]
            } catch {
                case ex: Exception => {
                    getSection(defaultConfigPath).get(name).asInstanceOf[util.LinkedHashMap[String, String]]
                }
            }
        } else {
            getSection(DEFAULT_CONFIG_NAME).get(name).asInstanceOf[util.LinkedHashMap[String, String]]
        }
    }

    def getSection(filePath: String): util.LinkedHashMap[String, String] = {

        if(defaultYamlMap == null) {

            defaultYamlMap = loadConfig[util.LinkedHashMap[String, AnyRef]](filePath)

            yamlLoaderMap = loadConfig[util.LinkedHashMap[String, AnyRef]](
                defaultYamlMap.get(defaultYamlMap.get("mode").toString + "_config_path").toString)

            val pattern = """\{\{([^\{\{]*)\}\}""".r

            yamlLoaderMap.keySet().forEach{
                section => {
                    val sectionMap =
                        yamlLoaderMap.get(section).asInstanceOf[util.LinkedHashMap[String, String]]
                    sectionMap.keySet().forEach {
                        key => {
                            pattern.findAllIn(sectionMap.get(key)).matchData foreach {
                                m => {
                                    val matchStr = m.group(1)
                                    val matchStrVal = sectionMap.get(matchStr)
                                    val modifiedVal = sectionMap.get(key).replaceAll("""\{\{""" + matchStr + """\}\}""", matchStrVal)
                                    sectionMap.put(key, modifiedVal)
                                }
                            }
                        }
                    }
                }
            }
        } else {

        }

        yamlLoaderMap.asInstanceOf[util.LinkedHashMap[String, String]]
    }

    def setConfig2SparkAddFile(spark: SparkSession): Unit = {
        spark.sparkContext.addFile(defaultConfigPath)
        this.sparkSession = spark
    }

    def setDefaultConfigPath(defaultConfigPath: String): Unit = {
        this.defaultConfigPath = defaultConfigPath
    }
}

package com.foxconn.iisd.bd.test.config

import java.io.File
import java.util

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.io.BufferedSource
import scala.reflect.ClassTag

@SerialVersionUID(100L)
class ConfigLoaderTest(defaultConfigPath: String) extends Serializable {

    var yaml: Yaml = null
    var defaultYamlMap: util.LinkedHashMap[String, AnyRef] = null
    var yamlLoaderMap: util.LinkedHashMap[String, AnyRef] = null

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
        getSection(section).get(name)
    }

    def getSection(name: String): util.LinkedHashMap[String, String] = {
        //        println("section name : " + name)
        if(defaultYamlMap == null) {
            //            println("defaultYamlMap is null")
            defaultYamlMap = loadConfig[util.LinkedHashMap[String, AnyRef]](defaultConfigPath)


            //            println("mode string : " + defaultYamlMap.get("mode").toString)
            yamlLoaderMap = loadConfig[util.LinkedHashMap[String, AnyRef]](
                defaultYamlMap.get(defaultYamlMap.get("mode").toString + "_config_path").toString)

            val pattern = """\{\{([^\{\{]*)\}\}""".r

            //            import collection.JavaConversions._
            //            for(sectionKeySet:String <- yamlLoaderMap.keySet()) {
            //                val sectionMap = yamlLoaderMap.get(sectionKeySet).asInstanceOf[util.LinkedHashMap[String, String]]
            //                for(keySet:String <- sectionMap.keySet()) {
            //                    pattern.findAllIn(sectionMap.get(keySet)).matchData foreach {
            //                        m => {
            //                            val matchStr = m.group(1)
            //                            val matchStrVal = sectionMap.get(matchStr)
            //                            val modifiedVal = sectionMap.get(keySet).replaceAll("""\{\{""" + matchStr + """\}\}""", matchStrVal)
            //                            sectionMap.put(keySet, modifiedVal)
            //                        }
            //                    }
            //                }
            //            }

            yamlLoaderMap.keySet().forEach{
                //                var i = 0
                section => {
                    //                    println("keySet[" + i + "] : " + section)
                    val sectionMap =
                        yamlLoaderMap.get(section).asInstanceOf[util.LinkedHashMap[String, String]]
                    sectionMap.keySet().forEach {
                        //                        var j = 0
                        key => {
                            //                            println("key[" + j + "] : " + key)
                            pattern.findAllIn(sectionMap.get(key)).matchData foreach {
                                //                                var k = 0
                                m => {
                                    //                                    println("m[" + k + "] : " + m)
                                    val matchStr = m.group(1)
                                    val matchStrVal = sectionMap.get(matchStr)
                                    val modifiedVal = sectionMap.get(key).replaceAll("""\{\{""" + matchStr + """\}\}""", matchStrVal)
                                    sectionMap.put(key, modifiedVal)
                                }
                                //                                k = k+1
                            }
                        }
                        //                        j = j+1
                    }
                }
                //                i = i+1
            }
        } else {
            //            println("defaultYamlMap is not null")
        }

        yamlLoaderMap.get(name).asInstanceOf[util.LinkedHashMap[String, String]]
    }
}

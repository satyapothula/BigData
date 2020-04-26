package ca.mcit.bigdata.bixipro
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

  trait  Main {

    val conf = new Configuration()

    conf.addResource(new Path("/home/bd-user/opt/hadoop/etc/cloudera/core-site.xml"))
    conf.addResource(new Path("/home/bd-user/opt/hadoop/etc/cloudera/hdfs-site.xml"))

    val fs = FileSystem.get(conf)

    val stationFiile = fs.copyFromLocalFile(new Path("/home/bd-user/Downloads/Bixi/Autodownload/station_information.json"), new Path("/user/cloudera/summer2019/satya_bixifeed/station_information"))
    val systemFiile = fs.copyFromLocalFile(new Path("/home/bd-user/Downloads/Bixi/Autodownload/system_information.json"), new Path("/user/cloudera/summer2019/satya_bixifeed/system_information"))
  }


package ca.mcit.bigdata.bixipro

import java.sql.{Connection, DriverManager}
import java.util.Calendar

object HiveCli extends Main with App {

  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)
  val connection: Connection = DriverManager.getConnection("jdbc:hive2://172.16.129.58:10000", "cloudera", "cloudera")
  val stmt = connection.createStatement()
  println("JSON Source files auto downloaded and uploaded to HDFS from LFS")
  stmt.execute("SET hive.exec.dynamic.partition.mode=nonstrict")
  stmt.execute(" SET hive.auto.convert.join=true")
  stmt.execute(" DROP TABLE IF EXISTS s19909_bixi_feed_satyap.Raw_station_information ")
  stmt.execute("CREATE EXTERNAL TABLE s19909_bixi_feed_satyap.Raw_station_information (textcol string) STORED AS TEXTFILE  LOCATION '/user/cloudera/summer2019/satya_bixifeed/station_information' ")
  stmt.execute(" DROP TABLE IF EXISTS s19909_bixi_feed_satyap.fj_station_information ")
  stmt.execute(" CREATE EXTERNAL TABLE s19909_bixi_feed_satyap.fj_station_information (json_body string) STORED AS TEXTFILE ")
  stmt.execute(" INSERT OVERWRITE TABLE s19909_bixi_feed_satyap.fj_station_information SELECT CONCAT_WS(' ',COLLECT_LIST(textcol)) AS singlelineJSON " +
    "FROM (SELECT INPUT__FILE__NAME, BLOCK__OFFSET__INSIDE__FILE, textcol FROM s19909_bixi_feed_satyap.raw_station_information DISTRIBUTE BY INPUT__FILE__NAME SORT BY BLOCK__OFFSET__INSIDE__FILE) x " +
    "GROUP BY INPUT__FILE__NAME")
  stmt.execute(" DROP TABLE IF EXISTS s19909_bixi_feed_satyap.gjo_station_information")
  stmt.execute(" CREATE TABLE s19909_bixi_feed_satyap.gjo_station_information AS SELECT " +
    "split(get_json_object(c.json_body,'$.data.stations.station_id'),\",\") as id,  " +
    "split(get_json_object(c.json_body,'$.data.stations.external_id'),\",\") as exid,  " +
    "split(get_json_object(c.json_body,'$.data.stations.name'),\",\") as name,  " +
    "split(get_json_object(c.json_body,'$.data.stations.short_name'),\",\") as short_name, " +
    "split(get_json_object(c.json_body,'$.data.stations.lat'),\",\") as lat, " +
    "split(get_json_object(c.json_body,'$.data.stations.lon'),\",\") as lon, " +
    "split(get_json_object(c.json_body,'$.data.stations.rental_methods'),\",\") as rental_methods, " +
    "split(get_json_object(c.json_body,'$.data.stations.capacity'),\",\") as capacity, " +
    "split(get_json_object(c.json_body,'$.data.stations.electric_bike_surcharge_waiver'),\",\") as ebsw, " +
    "split(get_json_object(c.json_body,'$.data.stations.eightd_has_key_dispenser'),\",\") as ehkd, " +
    "split(get_json_object(c.json_body,'$.data.stations.has_kiosk'),\",\") as kiosk " +
    "FROM s19909_bixi_feed_satyap.fj_station_information c")
  stmt.execute("DROP TABLE IF EXISTS s19909_bixi_feed_satyap.sid_name")
  stmt.execute("CREATE table s19909_bixi_feed_satyap.sid_name AS " +
    "SELECT posi,split(sid,'\"')[1] as sid,split(sname,'\"')[1] as sname " +
    "FROM s19909_bixi_feed_satyap.gjo_station_information k " +
    "LATERAL VIEW posexplode(id) k AS posi, sid " +
    "LATERAL VIEW posexplode(name) K AS posn, sname " +
    "where posi = posn")
  stmt.execute("DROP TABLE IF EXISTS s19909_bixi_feed_satyap.slat_lon")
  stmt.execute("CREATE table s19909_bixi_feed_satyap.slat_lon AS  " +
    "SELECT posl,split(slat,'\"')[0] as latitude,split(slon,'\"')[0] as longtitude  " +
    "FROM s19909_bixi_feed_satyap.gjo_station_information k  " +
    "LATERAL VIEW posexplode(lat) k AS posl, slat  " +
    "LATERAL VIEW posexplode(lon) K AS posln, slon  " +
    "where posl = posln")
  stmt.execute("DROP TABLE IF EXISTS s19909_bixi_feed_satyap.sname_cap")
  stmt.execute(" CREATE table s19909_bixi_feed_satyap.sname_cap AS " +
    "SELECT possn,split(ssname,'\"')[1] as ShortName,split(cap,'\"')[0] as Capacity " +
    "FROM s19909_bixi_feed_satyap.gjo_station_information k " +
    "LATERAL VIEW posexplode(short_name) k AS possn, ssname " +
    "LATERAL VIEW posexplode(capacity) k AS posc, cap " +
    "where possn = posc")
  stmt.execute("DROP TABLE IF EXISTS s19909_bixi_feed_satyap.waiver_keydisp")
  stmt.execute("CREATE table s19909_bixi_feed_satyap.waiver_keydisp AS " +
    "SELECT posb,split(sw,'\"')[0] as surcharge_waiver,split(kd,'\"')[0] as key_dispenser " +
    "FROM s19909_bixi_feed_satyap.gjo_station_information k " +
    "LATERAL VIEW posexplode(ebsw) k AS posb, sw " +
    "LATERAL VIEW posexplode(ehkd) k AS posc, kd " +
    "where posb = posc ")
  stmt.execute("DROP TABLE IF EXISTS s19909_bixi_feed_satyap.rm1")
  stmt.execute("create table s19909_bixi_feed_satyap.rm1 as " +
    "SELECT row_number() over() as rownum ,split(rm,'\"')[1] as rental1 " +
    "FROM s19909_bixi_feed_satyap.gjo_station_information k  " +
    "LATERAL VIEW posexplode(rental_methods) k AS posb, rm  " +
    "WHERE posb%2 != 0")
  stmt.execute("DROP TABLE IF EXISTS s19909_bixi_feed_satyap.rm2")
  stmt.execute("create table s19909_bixi_feed_satyap.rm2 as " +
    "SELECT row_number() over() as rownu ,split(rm,'\"')[1] as rental2 " +
    "FROM s19909_bixi_feed_satyap.gjo_station_information k  " +
    "LATERAL VIEW posexplode(rental_methods) k AS posb, rm  " +
    "WHERE posb%2 = 0 ")
  stmt.execute("DROP TABLE IF EXISTS s19909_bixi_feed_satyap.kiosk_eid")
  stmt.execute("CREATE table s19909_bixi_feed_satyap.kiosk_eid AS " +
    "SELECT posk,split(ki,'\"')[0] as kiosk,split(xi,'\"')[1] as extid " +
    "FROM s19909_bixi_feed_satyap.gjo_station_information k  " +
    "LATERAL VIEW posexplode(kiosk) k AS posk, ki LATERAL VIEW posexplode(exid) k AS pose, xi " +
    "where posk = pose")
  stmt.execute("DROP TABLE IF EXISTS s19909_bixi_feed_satyap.station_information")
  stmt.execute("CREATE table s19909_bixi_feed_satyap.station_information as SELECT cast(a.sid as INT) as station_Id,a.sname as sName,b.shortname as short_name,b.capacity as Capacity,c.latitude as lat,c.longtitude as lon, " +
    "d.surcharge_waiver as surcharge_waiver,d.key_dispenser as key_dispenser, e.kiosk as Kiosk,e.extid as Extid,f.rental1 as rental_Method_a,g.rental2 as rental_method_b " +
    "FROM s19909_bixi_feed_satyap.sid_name a  " +
    "JOIN s19909_bixi_feed_satyap.sname_cap b on a.posi =b.possn  " +
    "JOIN s19909_bixi_feed_satyap.slat_lon c on c.posl= b.possn " +
    "JOIN s19909_bixi_feed_satyap.waiver_keydisp d on d.posb = c.posl " +
    "JOIN s19909_bixi_feed_satyap.kiosk_eid e on e.posk = d.posb " +
    "JOIN s19909_bixi_feed_satyap.rm1 f on f.rownum = e.posk " +
    "JOIN s19909_bixi_feed_satyap.rm2 g on g.rownu = f.rownum ")

  println(Calendar.getInstance().getTime)

  stmt.execute("DROP TABLE IF EXISTS s19909_bixi_feed_satyap.Raw_system_information")
  stmt.execute(" CREATE EXTERNAL TABLE s19909_bixi_feed_satyap.Raw_system_information (textcol string) " +
    "STORED AS TEXTFILE " +
    "LOCATION '/user/cloudera/summer2019/satya_bixifeed/system_information'")
  stmt.execute("DROP TABLE IF EXISTS s19909_bixi_feed_satyap.fj_system_information")
  stmt.execute("CREATE EXTERNAL TABLE s19909_bixi_feed_satyap.fj_system_information (json_body string) STORED AS TEXTFILE")
  stmt.execute(" INSERT OVERWRITE TABLE s19909_bixi_feed_satyap.fj_system_information SELECT CONCAT_WS(' ',COLLECT_LIST(textcol)) AS singlelineJSON " +
    "FROM (SELECT INPUT__FILE__NAME, BLOCK__OFFSET__INSIDE__FILE, textcol FROM s19909_bixi_feed_satyap.Raw_system_information DISTRIBUTE BY INPUT__FILE__NAME SORT BY BLOCK__OFFSET__INSIDE__FILE) x " +
    "GROUP BY INPUT__FILE__NAME")
  stmt.execute("DROP TABLE IF EXISTS s19909_bixi_feed_satyap.system_information")
  stmt.execute("create table s19909_bixi_feed_satyap.system_information as select " +
    "get_json_object(c.json_body, '$.data.system_id') as system_id," +
    "get_json_object(c.json_body, '$.data.language') as language, " +
    "get_json_object(c.json_body, '$.data.url') as url," +
    "get_json_object(c.json_body, '$.data.start_date') as start_date, " +
    "get_json_object(c.json_body, '$.data.timezone') as timezone " +
    "from s19909_bixi_feed_satyap.fj_system_information c")

  println(Calendar.getInstance().getTime)

  stmt.execute("INSERT OVERWRITE DIRECTORY '/user/cloudera/summer2019/satya_bixifeed/Enricher' " +
    "ROW FORMAT DELIMITED " +
    "FIELDS TERMINATED BY ',' " +
    "SELECT * from s19909_bixi_feed_satyap.system_information join s19909_bixi_feed_satyap.station_information ")
  println(Calendar.getInstance().getTime)
  println("Enriched file in HDFS")
  stmt.execute("DROP TABLE IF EXISTS s19909_bixi_feed_satya_enricher.station_information")
  stmt.execute("CREATE external TABLE s19909_bixi_feed_satya_enricher.station_information ( " +
    "system_Id STRING, " +
    "language STRING, " +
    "url STRING, " +
    "system_startDate DATE, " +
    "timezone STRING, " +
    "station_Id INT, " +
    "station_name STRING, " +
    "short_name INT, " +
    "capacity Int, " +
    "latitude FLOAT, " +
    "longitude FLOAT, " +
    "surcharge_waiver BOOLEAN, " +
    "key_dispenser BOOLEAN, " +
    "kiosk BOOLEAN, " +
    "extr_id string, " +
    "rental_method_a STRING, " +
    "rental_method_b STRING ) " +
    "ROW FORMAT DELIMITED " +
    "FIELDS TERMINATED BY ',' " +
    "STORED AS TEXTFILE " +
    "LOCATION '/user/cloudera/summer2019/satya_bixifeed/Enricher'")
  println("External table for enriched station_information created in HIVE")

  stmt.close()
  connection.close()
}

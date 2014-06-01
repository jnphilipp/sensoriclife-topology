package org.sensoriclife.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.generator.electricity.ElectricityGenerator;
import org.sensoriclife.generator.heating.HeatingGenerator;
import org.sensoriclife.generator.water.WaterGenerator;
import org.sensoriclife.generator.world.WorldGenerator;
import org.sensoriclife.storm.bolts.AccumuloBolt;
import org.sensoriclife.storm.bolts.ColdWaterBolt;
import org.sensoriclife.storm.bolts.ElectricityBolt;
import org.sensoriclife.storm.bolts.HeatingBolt;
import org.sensoriclife.storm.bolts.HotWaterBolt;
import org.sensoriclife.storm.bolts.WorldBolt;

/**
 *
 * @author jnphilipp
 * @version 0.1.0
 */
public class App {
	public static void main(String args[]) {
		Logger.getInstance();

		boolean world = false, createTables = false;
		String confFile = "";
		if ( args.length != 0 ) {
			List<String> l = Arrays.asList(args);
			Iterator<String> it = l.iterator();

			while ( it.hasNext() ) {
				switch ( it.next() ) {
					case "--world":
						world = true;
						break;
					case "--create-tables":
						createTables = true;
						break;
					case "-conf":
						confFile = it.next();
						break;
				}
			}
		}

		Map<String, String> defaults = new LinkedHashMap<>();
		defaults.put("generator.realtime", "true");
		defaults.put("generator.timefactor", "1");
		defaults.put("generator.table_name", "sensoriclife_generator");
		defaults.put("accumulo.table_name", "sensoriclife_consumptioun");
		defaults.put("storm.debug", "false");
		defaults.put("strom.num_workers", "1");
		org.sensoriclife.Config.getInstance();

		try {
			if ( !confFile.isEmpty() )
				org.sensoriclife.Config.load(confFile);
			else
				org.sensoriclife.Config.load();
		}
		catch ( IOException e ) {
			Logger.error("Cannot load config file.", e.toString());
		}
		org.sensoriclife.Config.getInstance().setDefaults(defaults);


		try {
			if ( org.sensoriclife.Config.getProperty("accumulo.name").isEmpty() && org.sensoriclife.Config.getProperty("accumulo.zooServers").isEmpty() && org.sensoriclife.Config.getProperty("accumulo.user").isEmpty() && org.sensoriclife.Config.getProperty("accumulo.password").isEmpty() ){
				Accumulo.getInstance().connect();
				Accumulo.getInstance().createTable(org.sensoriclife.Config.getProperty("generator.table_name"), false);
				Accumulo.getInstance().createTable(org.sensoriclife.Config.getProperty("accumulo.table_name"), false);
			}
			else {
				Accumulo.getInstance().connect(org.sensoriclife.Config.getProperty("accumulo.name"), org.sensoriclife.Config.getProperty("accumulo.zooServers"), org.sensoriclife.Config.getProperty("accumulo.user"), org.sensoriclife.Config.getProperty("accumulo.password"));

				if ( createTables ) {
					Accumulo.getInstance().createTable(org.sensoriclife.Config.getProperty("generator.table_name"), false);
					Accumulo.getInstance().createTable(org.sensoriclife.Config.getProperty("accumulo.table_name"), false);
				}
			}
		}
		catch ( AccumuloException | AccumuloSecurityException e ) {
			Logger.error("Error while connecting to accumulo.", e.toString());
		}
		catch (TableExistsException e) {
			Logger.error("Error while creating table.", e.toString());
		}

		TopologyBuilder builder = new TopologyBuilder();

		if ( world ) {
			try {
				builder.setSpout("worldgenerator", new WorldGenerator(org.sensoriclife.Config.toMap()), 1);
				builder.setBolt("worldbolt", new WorldBolt(Accumulo.getInstance(), org.sensoriclife.Config.getProperty("accumulo.table_name"))).shuffleGrouping("worldgenerator");
				builder.setBolt("accumulobolt", new AccumuloBolt(org.sensoriclife.Config.toMap()), 20).shuffleGrouping("worldbolt");
			}
			catch ( TableNotFoundException e ) {
				Logger.error(App.class, e.toString());
			}
		}
		else {
			builder.setSpout("electricitygenerator", new ElectricityGenerator(org.sensoriclife.Config.toMap()), 1);
			builder.setSpout("watergenerator", new WaterGenerator(org.sensoriclife.Config.toMap()), 1);
			builder.setSpout("heatinggenerator", new HeatingGenerator(org.sensoriclife.Config.toMap()), 1);

			builder.setBolt("electricitybolt", new ElectricityBolt(), 5).shuffleGrouping("electricitygenerator");
			builder.setBolt("hotwaterbolt", new HotWaterBolt(), 5).shuffleGrouping("watergenerator","hotwater");
			builder.setBolt("coldwaterbolt", new ColdWaterBolt(), 5).shuffleGrouping("watergenerator","coldwater");
			builder.setBolt("heatingbolt", new HeatingBolt(), 5).shuffleGrouping("heatinggenerator");

			builder.setBolt("accumulobolt", new AccumuloBolt(org.sensoriclife.Config.toMap()), 20).shuffleGrouping("electricitybolt").shuffleGrouping("hotwaterbolt").shuffleGrouping("coldwaterbolt").shuffleGrouping("heatingbolt");
		}

		//for test
		Config conf = new Config();
		conf.setDebug(org.sensoriclife.Config.getBooleanProperty("storm.debug"));
		conf.setNumWorkers(org.sensoriclife.Config.getIntegerProperty("strom.num_workers"));

		if ( org.sensoriclife.Config.getBooleanProperty("storm.debug") ) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
		else {
			try {
				conf.setNumWorkers(org.sensoriclife.Config.getIntegerProperty("storm.num_workers"));
				StormSubmitter.submitTopology(org.sensoriclife.Config.getProperty("storm.name"), conf, builder.createTopology());
			}
			catch ( AlreadyAliveException | InvalidTopologyException e ) {
				Logger.error(App.class, "Error while submitting storm topology.", e.toString());
			}
		}
	}
}
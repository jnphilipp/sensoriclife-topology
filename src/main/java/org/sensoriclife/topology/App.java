package org.sensoriclife.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
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
 * @version 0.0.1
 */
public class App {
	public static void main(String args[]) {
		Logger.getInstance();

		boolean world = false;
		if ( args.length != 0 ) {
			List<String> l = Arrays.asList(args);
			Iterator<String> it = l.iterator();

			while ( it.hasNext() ) {
				switch ( it.next() ) {
					case "world":
						world = true;
						break;
				}
			}
		}

		Map<String, String> defaults = new LinkedHashMap<>();
		defaults.put("realtime", "true");
		defaults.put("storm.debug", "false");
		defaults.put("storm.name", "test");
		org.sensoriclife.Config.getInstance().setDefaults(defaults);


		try {
			if ( org.sensoriclife.Config.getProperty("accumulo.name").isEmpty() && org.sensoriclife.Config.getProperty("accumulo.zooServers").isEmpty() && org.sensoriclife.Config.getProperty("accumulo.user").isEmpty() && org.sensoriclife.Config.getProperty("accumulo.password").isEmpty() ){
				Accumulo.getInstance().connect();
				Accumulo.getInstance().createTable("generator_helper_table");
				Accumulo.getInstance().createTable("consumption");
			}
			else
				Accumulo.getInstance().connect(org.sensoriclife.Config.getProperty("accumulo.name"), org.sensoriclife.Config.getProperty("accumulo.zooServers"), org.sensoriclife.Config.getProperty("accumulo.user"), org.sensoriclife.Config.getProperty("accumulo.password"));
		}
		catch ( AccumuloException | AccumuloSecurityException e ) {
			Logger.error("Error while connecting to accumulo.", e.toString());
		} 
		catch (TableExistsException e) {
			Logger.error("Error while creating table.", e.toString());
		}

		TopologyBuilder builder = new TopologyBuilder();

		if ( world ) {
			builder.setSpout("world_generator", new WorldGenerator(), 1);
			builder.setBolt("worldbold", new WorldBolt()).shuffleGrouping("world_generator");
		}

		builder.setSpout("electricity_generator", new ElectricityGenerator(), 1);
		builder.setSpout("water_generator", new WaterGenerator(), 1);
		builder.setSpout("heating_generator", new HeatingGenerator(), 1);

		builder.setBolt("electricity_bolt", new ElectricityBolt()).shuffleGrouping("electricity_generator");
		builder.setBolt("hotwater_bolt", new HotWaterBolt()).shuffleGrouping("water_generator","hotwater");
		builder.setBolt("coldwater_bolt", new ColdWaterBolt()).shuffleGrouping("water_generator","coldwater");
		builder.setBolt("heating_bolt", new HeatingBolt()).shuffleGrouping("heating_generator");
		builder.setBolt("accumulo_bolt", new AccumuloBolt());

		//for test
		Config conf = new Config();
		conf.setDebug(org.sensoriclife.Config.getBooleanProperty("storm.debug"));

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
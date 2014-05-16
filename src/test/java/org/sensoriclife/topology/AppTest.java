package org.sensoriclife.topology;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;
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
public class AppTest {
	/**
	 * Test of main method, of class App.
	 */
	@Test
	public void testTopology() {
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.realtime", "true");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.timefactor", "1");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("storm.debug", "true");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.cities", "1");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.districts", "1");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.streets", "1");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.buildings", "10");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.residentialUnits", "10");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.users", "99");


		try {
			Accumulo.getInstance().connect();
			Accumulo.getInstance().createTable("generator_helper_table");
			Accumulo.getInstance().createTable("sensoriclife");
		}
		catch ( AccumuloException | AccumuloSecurityException e ) {
			Logger.error(AppTest.class, "Error while connecting to accumulo.", e.toString());
		} 
		catch (TableExistsException e) {
			Logger.error(AppTest.class, "Error while creating table.", e.toString());
		}

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("worldgenerator", new WorldGenerator(), 1);
		builder.setBolt("worldbolt", new WorldBolt()).shuffleGrouping("worldgenerator");

		builder.setSpout("electricitygenerator", new ElectricityGenerator(), 1);
		builder.setSpout("watergenerator", new WaterGenerator(), 1);
		builder.setSpout("heatinggenerator", new HeatingGenerator(), 1);

		builder.setBolt("electricitybolt", new ElectricityBolt()).shuffleGrouping("electricitygenerator");
		builder.setBolt("hotwaterbolt", new HotWaterBolt()).shuffleGrouping("watergenerator","hotwater");
		builder.setBolt("coldwaterbolt", new ColdWaterBolt()).shuffleGrouping("watergenerator","coldwater");
		builder.setBolt("heatingbolt", new HeatingBolt()).shuffleGrouping("heatinggenerator");
		builder.setBolt("accumulobolt", new AccumuloBolt(), 4).shuffleGrouping("worldbolt").shuffleGrouping("electricitybolt").shuffleGrouping("hotwaterbolt").shuffleGrouping("coldwaterbolt").shuffleGrouping("heatingbolt");

		//for test
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(2);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(120000);
		cluster.killTopology("test");
		cluster.shutdown();

		assertNotEquals(0, WorldBolt.getCount());

		try {
			Iterator<Entry<Key, Value>> iterator = Accumulo.getInstance().scanAll("sensoriclife");
			int i = 0;
			for ( ; iterator.hasNext(); ++i ) {iterator.next();}

			assertTrue(i > WorldBolt.getCount());
		}
		catch ( TableNotFoundException e ) {
			Logger.error(AppTest.class, "Table dows not exist.", e.toString());
		}
	}
}
package org.sensoriclife.topology;

import static org.junit.Assert.assertTrue;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.BeforeClass;
import org.junit.Test;
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
 * @version 0.0.2
 */
public class AppTest {
	@BeforeClass
	public static void setUp() {
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.cities", "1");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.districts", "1");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.streets", "1");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.buildings", "10");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.residentialUnits", "10");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.users", "99");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.table_name", "sensoriclife_generator");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.realtime", "true");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("generator.timefactor", "1");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("accumulo.table_name", "sensoriclife");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("storm.debug", "true");
		org.sensoriclife.Config.getInstance().getProperties().setProperty("accumulo.batch_writer.max_memory", "10000000");
	}

	@Test
	public void testTopology() throws TableNotFoundException, AccumuloException, AccumuloSecurityException, TableExistsException, IOException, InterruptedException {
		Accumulo.getInstance().connect();
		Accumulo.getInstance().createTable(org.sensoriclife.Config.getProperty("generator.table_name"), false);
		Accumulo.getInstance().createTable(org.sensoriclife.Config.getProperty("accumulo.table_name"), false);

		TopologyBuilder builder = new TopologyBuilder();

		new WorldBolt();
		assertTrue(0 == WorldBolt.getCount());

		builder.setSpout("worldgenerator", new WorldGenerator(false), 1);
		builder.setBolt("worldbolt", new WorldBolt()).shuffleGrouping("worldgenerator");
		builder.setBolt("accumulobolt", new AccumuloBolt(), 4).shuffleGrouping("worldbolt");

		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(8);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(60000);
		cluster.killTopology("test");
		cluster.shutdown();

		builder = new TopologyBuilder();

		builder.setSpout("electricitygenerator", new ElectricityGenerator(), 1);
		builder.setSpout("watergenerator", new WaterGenerator(), 1);
		builder.setSpout("heatinggenerator", new HeatingGenerator(), 1);

		builder.setBolt("electricitybolt", new ElectricityBolt(), 1).shuffleGrouping("electricitygenerator");
		builder.setBolt("hotwaterbolt", new HotWaterBolt(), 1).shuffleGrouping("watergenerator", "hotwater");
		builder.setBolt("coldwaterbolt", new ColdWaterBolt(), 1).shuffleGrouping("watergenerator", "coldwater");
		builder.setBolt("heatingbolt", new HeatingBolt(), 1).shuffleGrouping("heatinggenerator");
		builder.setBolt("accumulobolt", new AccumuloBolt(), 4).shuffleGrouping("electricitybolt").shuffleGrouping("hotwaterbolt").shuffleGrouping("coldwaterbolt").shuffleGrouping("heatingbolt");

		cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(60000);
		cluster.killTopology("test");
		cluster.shutdown();

		Accumulo.getInstance().closeBashWriter(org.sensoriclife.Config.getProperty("generator.table_name"));
		Accumulo.getInstance().closeBashWriter(org.sensoriclife.Config.getProperty("accumulo.table_name"));

		Iterator<Entry<Key, Value>> iterator = Accumulo.getInstance().scanAll(org.sensoriclife.Config.getProperty("accumulo.table_name"));
		int i = 0;
		for ( ; iterator.hasNext(); ++i ) {iterator.next();}

		new WorldBolt();
		assertTrue(i > WorldBolt.getCount());

		Accumulo.getInstance().deleteTable(org.sensoriclife.Config.getProperty("generator.table_name"));
		Accumulo.getInstance().deleteTable(org.sensoriclife.Config.getProperty("accumulo.table_name"));
		Accumulo.getInstance().disconnect();
	}
}
package common.configuration;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Properties;

public final class RmConfiguration {

	private final long period;
	private final int numPartitions;
	private final int maxNumRoutingEntries;
	private final long seed;
	private final boolean omniscent;
	private final int probes;

	public RmConfiguration(long seed, boolean omniscent, int probes) {
		this(2 * 1000, 10, 20, seed, omniscent, probes);
	}

	public RmConfiguration(long period, int numPartitions,
			int maxNumRoutingEntries, long seed, boolean omniscent, int probes) {
		this.period = period;
		this.numPartitions = numPartitions;
		this.maxNumRoutingEntries = maxNumRoutingEntries;
		this.seed = seed;
		this.omniscent = omniscent;
		this.probes = probes;
	}

	public long getPeriod() {
		return this.period;
	}

	public int getNumPartitions() {
		return numPartitions;
	}

	public int getMaxNumRoutingEntries() {
		return maxNumRoutingEntries;
	}

	public long getSeed() {
		return seed;
	}

	public void store(String file) throws IOException {
		Properties p = new Properties();
		p.setProperty("period", "" + period);
		p.setProperty("numPartitions", "" + numPartitions);
		p.setProperty("maxNumRoutingEntries", "" + maxNumRoutingEntries);
		p.setProperty("seed", "" + seed);
		p.setProperty("omniscent", Boolean.toString(this.isOmniscent()));
		p.setProperty("probes", "" + probes);

		Writer writer = new FileWriter(file);
		p.store(writer, "se.sics.kompics.p2p.overlay.application");
	}

	public static RmConfiguration load(String file) throws IOException {
		Properties p = new Properties();
		Reader reader = new FileReader(file);
		p.load(reader);

		long period = Long.parseLong(p.getProperty("period"));
		int numPartitions = Integer.parseInt(p.getProperty("numPartitions"));
		int maxNumRoutingEntries = Integer.parseInt(p
				.getProperty("maxNumRoutingEntries"));
		long seed = Long.parseLong(p.getProperty("seed"));
		boolean omniscent = Boolean.parseBoolean(p.getProperty("omniscent"));
		int probes = Integer.parseInt(p.getProperty("probes"));

		return new RmConfiguration(period, numPartitions, maxNumRoutingEntries,
				seed, omniscent, probes);
	}

	public boolean isOmniscent() {
		return omniscent;
	}

	public int getProbes() {
		return probes;
	}
}

package tman.system.peer.tman;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;

import common.configuration.TManConfiguration;
import common.peer.AvailableResources;
import common.peer.PeerCap;

import cyclon.system.peer.cyclon.CyclonSample;
import cyclon.system.peer.cyclon.CyclonSamplePort;
import cyclon.system.peer.cyclon.PeerDescriptor;

public final class TMan extends ComponentDefinition {

	private static final Logger log = LoggerFactory.getLogger(TMan.class);

	Negative<TManSamplePort> tmanPort = negative(TManSamplePort.class);
	Positive<CyclonSamplePort> cyclonSamplePort = positive(CyclonSamplePort.class);
	Positive<Network> networkPort = positive(Network.class);
	Positive<Timer> timerPort = positive(Timer.class);
	private long period;
	private Address self;
	private ArrayList<PeerDescriptor> tmanPartners;
	protected ArrayList<PeerDescriptor> cyclonPartners;
	private TManConfiguration tmanConfiguration;
	private Random r;
	private AvailableResources availableResources;

	private int psi = 5;
	private int m = 10;

	public class TManSchedule extends Timeout {

		public TManSchedule(SchedulePeriodicTimeout request) {
			super(request);
		}

		public TManSchedule(ScheduleTimeout request) {
			super(request);
		}
	}

	public TMan() {
		tmanPartners = new ArrayList<PeerDescriptor>();

		subscribe(handleInit, control);
		subscribe(handleRound, timerPort);
		subscribe(handleCyclonSample, cyclonSamplePort);
		subscribe(handleTManPartnersResponse, networkPort);
		subscribe(handleTManPartnersRequest, networkPort);
	}

	Handler<TManInit> handleInit = new Handler<TManInit>() {
		@Override
		public void handle(TManInit init) {
			self = init.getSelf();
			tmanConfiguration = init.getConfiguration();
			period = tmanConfiguration.getPeriod();
			r = new Random(tmanConfiguration.getSeed() * self.getId());
			availableResources = init.getAvailableResources();

			tmanPartners = new ArrayList<PeerDescriptor>();
			cyclonPartners = new ArrayList<PeerDescriptor>();

			SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(period,
					period);
			rst.setTimeoutEvent(new TManSchedule(rst));
			trigger(rst, timerPort);

		}
	};

	Handler<TManSchedule> handleRound = new Handler<TManSchedule>() {
		@Override
		public void handle(TManSchedule event) {
			PeerCap myself = generateMyPeerCap();
			if (tmanPartners.isEmpty()) {
				if (cyclonPartners.isEmpty()) {
					// nothing, wait for cyclon samples
					return;
				}
				Set<PeerDescriptor> cyclonSet = new HashSet<PeerDescriptor>(
						cyclonPartners);
				cyclonSet.remove(new PeerDescriptor(myself));
				tmanPartners = selectView(myself, cyclonSet, m);
			}

			PeerCap p = selectPeer();

			Set<PeerDescriptor> buf = new HashSet<PeerDescriptor>();
			buf.add(new PeerDescriptor(myself));
			buf.addAll(cyclonPartners);
			buf.addAll(tmanPartners);

			incrementAll(buf);
			ArrayList<PeerDescriptor> selectView = selectView(p, buf, m);

			ExchangeMsg.Request tmanViewRequest = new ExchangeMsg.Request(self,
					p.getAddress(), selectView, generateMyPeerCap());
			trigger(tmanViewRequest, networkPort);
			buf.clear();

			// Publish sample to connected components
			ArrayList<PeerCap> toReturn = new ArrayList<PeerCap>();
			// ArrayList<PeerDescriptor> selectView2 = selectView(
			// generateMyPeerCap(), new HashSet<PeerDescriptor>(
			// tmanPartners), m);
			for (PeerDescriptor peerDescriptor : tmanPartners) {
				toReturn.add(peerDescriptor.getPeerCap());
			}
			trigger(new TManSample(toReturn), tmanPort);
		}

	};

	private void incrementAll(Set<PeerDescriptor> buf) {
		for (PeerDescriptor peerDescriptor : buf) {
			peerDescriptor.incrementAndGetAge();
		}

	}

	Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
		@Override
		public void handle(CyclonSample event) {
			// event.getSample();
			cyclonPartners.clear();
			for (PeerCap pcap : event.getSample()) {
				cyclonPartners.add(new PeerDescriptor(pcap));
			}

		}
	};

	Handler<ExchangeMsg.Request> handleTManPartnersRequest = new Handler<ExchangeMsg.Request>() {
		@Override
		public void handle(ExchangeMsg.Request event) {

			Set<PeerDescriptor> buf = new HashSet<PeerDescriptor>();
			buf.add(new PeerDescriptor(generateMyPeerCap()));
			// buf.addAll(cyclonPartners);
			buf.addAll(tmanPartners);
			ArrayList<PeerDescriptor> rankedView = selectView(
					event.getSourcePeerCap(), buf, m);

			ExchangeMsg.Response tmanViewResponse = new ExchangeMsg.Response(
					self, event.getSource(), rankedView);
			buf.clear();
			trigger(tmanViewResponse, networkPort);

			buf.addAll(event.getRandomBuffer());
			// buf.addAll(cyclonPartners);
			buf.addAll(selectNewest(tmanPartners));

			// tmanPartners = selectView(generateMyPeerCap(), buf, m);
			tmanPartners = new ArrayList<PeerDescriptor>(buf);

			buf.clear();
		}
	};

	Handler<ExchangeMsg.Response> handleTManPartnersResponse = new Handler<ExchangeMsg.Response>() {
		@Override
		public void handle(ExchangeMsg.Response event) {

			Set<PeerDescriptor> buf = new HashSet<PeerDescriptor>();
			buf.addAll(event.getSelectedBuffer());
			// buf.addAll(cyclonPartners);
			buf.addAll(selectNewest(tmanPartners));

			tmanPartners = new ArrayList<PeerDescriptor>(buf);
		}
	};

	private ArrayList<PeerDescriptor> selectView(PeerCap ref,
			Set<PeerDescriptor> buf, int limit) {
		ArrayList<PeerDescriptor> rankedView = rank(ref, buf);

		if (rankedView.size() > limit)
			rankedView = new ArrayList<PeerDescriptor>(rankedView.subList(0,
					limit));

		return rankedView;
	}

	protected ArrayList<PeerDescriptor> selectNewest(
			ArrayList<PeerDescriptor> buf) {
		TreeSet<PeerDescriptor> ordered = new TreeSet<PeerDescriptor>(buf);
		ArrayList<PeerDescriptor> list = new ArrayList<PeerDescriptor>(ordered);

		return new ArrayList<PeerDescriptor>(list.subList(0,
				Math.min(m, list.size())));

	}

	protected PeerCap generateMyPeerCap() {
		return new PeerCap(self, availableResources.getTotalCpus(),
				availableResources.getTotalMemory(),
				availableResources.getNumFreeCpus(),
				availableResources.getFreeMemInMbs(),
				availableResources.getQueueLength());
	}

	private ArrayList<PeerDescriptor> rank(PeerCap ref, Set<PeerDescriptor> buf) {
		ArrayList<PeerDescriptor> list = new ArrayList<PeerDescriptor>(buf);
		Collections.sort(list, new GradientResourceComparator(ref, list, r));

		return list;
	}

	// TODO - if you call this method with a list of entries, it will
	// return a single node, weighted towards the 'best' node (as defined by
	// ComparatorById) with the temperature controlling the weighting.
	// A temperature of '1.0' will be greedy and always return the best node.
	// A temperature of '0.000001' will return a random node.
	// A temperature of '0.0' will throw a divide by zero exception :)
	// Reference:
	// http://webdocs.cs.ualberta.ca/~sutton/book/2/node4.html
	public Address getSoftMaxAddress(List<Address> entries) {
		Collections.sort(entries, new ComparatorById(self));

		double rnd = r.nextDouble();
		double total = 0.0d;
		double[] values = new double[entries.size()];
		int j = entries.size() + 1;
		for (int i = 0; i < entries.size(); i++) {
			// get inverse of values - lowest have highest value.
			double val = j;
			j--;
			values[i] = Math.exp(val / tmanConfiguration.getTemperature());
			total += values[i];
		}

		for (int i = 0; i < values.length; i++) {
			if (i != 0) {
				values[i] += values[i - 1];
			}
			// normalise the probability for this entry
			double normalisedUtility = values[i] / total;
			if (normalisedUtility >= rnd) {
				return entries.get(i);
			}
		}
		return entries.get(entries.size() - 1);
	}

	private PeerCap selectPeer() {
		if (tmanPartners.isEmpty()) {
			return null;
		} else if (tmanPartners.size() == 1) {
			return tmanPartners.get(0).getPeerCap();
		} else {

			ArrayList<PeerDescriptor> rankedView = selectView(
					generateMyPeerCap(), new HashSet<PeerDescriptor>(
							tmanPartners), psi);

			int q = r.nextInt(rankedView.size());

			PeerCap peerSelected = rankedView.get(q).getPeerCap();

			return peerSelected;
		}
	}

}

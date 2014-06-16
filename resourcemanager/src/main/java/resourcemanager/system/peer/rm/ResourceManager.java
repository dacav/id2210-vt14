package resourcemanager.system.peer.rm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import resourcemanager.system.peer.rm.Probing.Completed;
import resourcemanager.system.peer.rm.task.AvailableResourcesImpl;
import resourcemanager.system.peer.rm.task.RmWorker;
import resourcemanager.system.peer.rm.task.Task;
import resourcemanager.system.peer.rm.task.WorkerInit;
import resourcemanager.system.peer.rm.task.WorkerPort;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;
import system.peer.RmPort;
import tman.system.peer.tman.TManSample;
import tman.system.peer.tman.TManSamplePort;

import common.configuration.RmConfiguration;
import common.peer.PeerCap;
import common.simulation.ClientRequestResource;
import common.utils.FuncTools;

import cyclon.system.peer.cyclon.CyclonSample;
import cyclon.system.peer.cyclon.CyclonSamplePort;
import cyclon.system.peer.cyclon.PeerDescriptor;
import fdet.system.evts.FdetPort;

/**
 * Should have some comments here.
 * 
 * @author jdowling
 */
public final class ResourceManager extends ComponentDefinition {

	private static final Logger log = LoggerFactory
			.getLogger(ResourceManager.class);

	private final Positive<RmPort> indexPort = positive(RmPort.class);
	private final Positive<Network> networkPort = positive(Network.class);
	private final Positive<Timer> timerPort = positive(Timer.class);
	private final Positive<CyclonSamplePort> cyclonSamplePort = positive(CyclonSamplePort.class);
	private final Positive<TManSamplePort> tmanPort = positive(TManSamplePort.class);
	private final Positive<FdetPort> fdetPort = positive(FdetPort.class);

	private final ArrayList<PeerCap> neighbours = new ArrayList<PeerCap>();

	private Address self;
	private RmConfiguration configuration;
	private Random random;
	private Component worker;
	private AvailableResourcesImpl res;

	/* Components for probing */
	private final Queue<Task> waiting = new LinkedList<Task>();
	private final Map<Long, Address> assigned = new HashMap<Long, Address>();
	private final Map<Address, TreeMap<Long, Task>> outstanding = new TreeMap<Address, TreeMap<Long, Task>>();

	// private AvailableResources availableResources;
	// private TaskQueue queue;
	// When you partition the index you need to find new nodes
	// This is a routing table maintaining a list of pairs in each partition.
	private Map<Integer, List<PeerDescriptor>> routingTable;
	Comparator<PeerDescriptor> peerAgeComparator = new Comparator<PeerDescriptor>() {
		@Override
		public int compare(PeerDescriptor t, PeerDescriptor t1) {
			if (t.getAge() > t1.getAge()) {
				return 1;
			} else {
				return -1;
			}
		}
	};

	public ResourceManager() {

		subscribe(handleInit, control);
		subscribe(handleCyclonSample, cyclonSamplePort);
		subscribe(handleRequestResource, indexPort);
		subscribe(handleUpdateTimeout, timerPort);
		subscribe(handleProbingRequest, networkPort);
		subscribe(handleProbingResponse, networkPort);
		subscribe(handleProbingAllocate, networkPort);
		subscribe(handleProbingCancel, networkPort);
		subscribe(handleProbingTerminate, networkPort);
		subscribe(handleTManSample, tmanPort);
		subscribe(handleFailure, fdetPort);
		subscribe(handleRestore, fdetPort);

		worker = create(RmWorker.class);
		connect(timerPort, worker.getNegative(Timer.class));
		subscribe(handleConfirmRequest, worker.getNegative(WorkerPort.class));
		subscribe(handleCompleted, worker.getNegative(WorkerPort.class));
	}

	private String getId() {
		return "[" + res.getNumFreeCpus() + " " + self.getIp() + "]";
	}

	private static FuncTools.Proposition<PeerCap> buildFilterProposition (final Task t) {
		return new FuncTools.Proposition<PeerCap>() {
			@Override
			public boolean eval(PeerCap param) {
				return param.maxMemory >= t.required.memoryInMbs
						&& param.maxCpu >= t.required.numCpus;
			}
		};
	}

	Handler<RmInit> handleInit = new Handler<RmInit>() {

		@Override
		public void handle(RmInit init) {
			self = init.getSelf();
			configuration = init.getConfiguration();
			random = new Random(init.getConfiguration().getSeed()
					* self.getId());
			long period = configuration.getPeriod();

			SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(period,
					period);
			rst.setTimeoutEvent(new UpdateTimeout(rst));
			trigger(rst, timerPort);

			res = (AvailableResourcesImpl) init.getAvailableResources();
			trigger(new WorkerInit(self, res), worker.getControl());

		}
	};

	Handler<UpdateTimeout> handleUpdateTimeout = new Handler<UpdateTimeout>() {
		@Override
		public void handle(UpdateTimeout event) {

			// pick a random neighbour to ask for index updates from.
			// You can change this policy if you want to.
			// Maybe a gradient neighbour who is closer to the leader?
			if (neighbours.isEmpty()) {
				return;
			}
			Address dest = neighbours.get(random.nextInt(neighbours.size()))
					.getAddress();
		}
	};
	
	Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
		@Override
		public void handle(CyclonSample event) {
			// System.out.println(self.getIp() + " - received samples: "
			// + event.getSample().size());

			neighbours.clear();
			neighbours.addAll(event.getSample());
		}
	};

	Handler<FdetPort.Dead> handleFailure = new Handler<FdetPort.Dead>() {
		@Override
		public void handle(FdetPort.Dead event) {
			trigger(new FdetPort.Unsubscribe(event.ref), fdetPort);
			List<Task> orphans = clearOutstandingOf(event.ref);

			if (orphans == null) {
				log.info(getId() + ": Dead neighbor {}, no outstanding probes", event.ref);
			} else {
				log.info(getId() + ": Dead neighbor {}, retrying with {} outstanding probes",
					event.ref, orphans.size()
				);
				for (Task o : orphans) {
					List<PeerCap> candidates = FuncTools.filter(buildFilterProposition(o), neighbours);
					if (candidates.isEmpty()) {
						log.warn(getId() + ": Aborting execution for {}, no candidates.", o.id);
					} else {
						Address target = candidates.get(random.nextInt(candidates.size())).address;
						log.debug(getId() + " SENDING ANOTHER PROBE FOR " + o.id + " TO " + target);
						addToOustanding(target, o);
						trigger(new Probing.Request(self, target, o.required, o.id), networkPort);
						trigger(new FdetPort.Subscribe(target), fdetPort);
					}
				}
			}
		}
	};

	Handler<FdetPort.Undead> handleRestore = new Handler<FdetPort.Undead>() {
		@Override
		public void handle(FdetPort.Undead event) {
			log.debug(getId() + ": IGNORING ZOMBIE NEIGHBOR " + event.ref);
		}
	};

	public void probe(final Task t) {
		assert !waiting.contains(t);
		waiting.add(t);

		FuncTools.Proposition<PeerCap> p = buildFilterProposition(t);

		/* Select only among nodes which can satisfy this task */
		List<PeerCap> sel = FuncTools.filter(p, neighbours);
		Collections.shuffle(sel, random);

		List<PeerCap> chosen = new ArrayList<PeerCap>();
		int put = configuration.getProbes(); // × t.njobs
		for (PeerCap cap : sel) {
			if (put-- <= 0)
				break;
			chosen.add(cap);
			addToOustanding(cap.address, t);
			trigger(new Probing.Request(self, cap.address, t.required, t.id), networkPort);
			trigger(new FdetPort.Subscribe(cap.address), fdetPort);
		}

		log.debug(getId() + " SENDING PROBE FOR " + t.id + " TO " + chosen);

		// if (put > 0) {
		// log.warn("I don't know enough peers: need {} more", put);
		// }
	}

	private void serve(Address peer, Task reserved) {
		Iterator<Task> i = waiting.iterator();
		boolean isUsed = false;
		while (i.hasNext() && !isUsed) {
			Task t = i.next();
			if (reserved.required.numCpus >= t.required.numCpus
					&& reserved.required.memoryInMbs >= t.required.memoryInMbs) {
				i.remove();
				isUsed = true;
				assigned.put(t.id, peer);
				log.info(getId()
						+ " ASSIGNING "
						+ t.id
						+ " TO "
						+ peer.getIp()
						+ (t.id != reserved.id ? " (OLD:"
								+ reserved.id + ")" : ""));
				trigger(new Probing.Allocate(self, peer, reserved.id, t),
						networkPort);
			}
		}

		// if we couldn't assign anything we cancel
		// but only if that peer hasn't been already assigned for
		// that same task on a previous iteration
		if (!isUsed) {
			log.info(getId() + " SENDING CANCEL FOR " + reserved.id
					+ " TO " + peer.getIp());
			trigger(new Probing.Cancel(self, peer, reserved.id),
					networkPort);
			trigger(new FdetPort.Unsubscribe(peer), fdetPort);
		}
	}

	private final Handler<Probing.Response> handleProbingResponse = new Handler<Probing.Response>() {
		@Override
		public void handle(Probing.Response resp) {
			Task t = getOutstanding(resp.getSource(), resp.referenceId);
			if (t != null) {
				log.debug(getId() + " GOT RESPONSE FROM " + resp.getSource().getIp() + " FOR " + t.id);
				serve(resp.getSource(), t);
			} else {
				log.debug(getId() + " GOT LATE RESPONSE FROM " + resp.getSource().getIp());
			}
		}

	};

	private void addToOustanding(Address address, Task t) {
		if (!outstanding.containsKey(address))
			outstanding.put(address, new TreeMap<Long, Task>());

		outstanding.get(address).put(t.id, t);
	}

	private List<Task> clearOutstandingOf(Address faulty) {
		Map<Long, Task> remove = outstanding.remove(faulty);
		return remove == null ? null : new ArrayList<Task>(remove.values());
	}

	private Task getOutstanding(Address source, long id) {
		Task toReturn = null;
		Map<Long, Task> addressToIds = outstanding.get(source);
		if (addressToIds != null)
			toReturn = addressToIds.remove(id);

		if (addressToIds.isEmpty())
			outstanding.remove(source);
		return toReturn;
	}

	private final Handler<Probing.Request> handleProbingRequest = new Handler<Probing.Request>() {
		@Override
		public void handle(Probing.Request event) {
			log.debug(getId() + " GOT PROBE FOR " + event.taskId);
			trigger(new Resources.Reserve(event.getSource(), event.taskId, event.required),
					worker.getNegative(WorkerPort.class));
		}
	};

	private final Handler<Probing.Allocate> handleProbingAllocate = new Handler<Probing.Allocate>() {
		@Override
		public void handle(Probing.Allocate event) {

			log.debug(getId() + " GOT ALLOCATE FOR " + event.task.id);
			trigger(new Resources.Allocate(event.getSource(), event.referenceId, event.task),
					worker.getNegative(WorkerPort.class));
		}
	};

	private final Handler<Probing.Cancel> handleProbingCancel = new Handler<Probing.Cancel>() {
		@Override
		public void handle(Probing.Cancel event) {

			log.debug(getId() + " GOT cancel FOR " + event.referenceId);

			trigger(new Resources.Cancel(event.referenceId),
					worker.getNegative(WorkerPort.class));
		}
	};

	private final Handler<Resources.Confirm> handleConfirmRequest = new Handler<Resources.Confirm>() {
		@Override
		public void handle(Resources.Confirm event) {
			log.debug(getId() + " REQUESTING CONFIRMATION");
			trigger(new Probing.Response(self, event.tph.taskMaster, event.tph.taskId), networkPort);
		}
	};
	
	private final Handler<Resources.Completed> handleCompleted = new Handler<Resources.Completed>() {
		@Override
		public void handle(Resources.Completed event) {
			log.debug(getId() + " SIGNALING TERMINATION TO " + event.taskMaster);
			trigger(new Probing.Completed(self, event.taskMaster, event.task.id), networkPort);
		}
	};

	private final Handler<Probing.Completed> handleProbingTerminate = new Handler<Probing.Completed>() {
		@Override
		public void handle(Completed event) {
			log.debug(getId() + " TASK TERMINATED BY " + event.getSource());
			trigger(new FdetPort.Unsubscribe(event.getSource()), fdetPort);
		}
	};

	Handler<ClientRequestResource> handleRequestResource = new Handler<ClientRequestResource>() {
		@Override
		public void handle(ClientRequestResource event) {

			log.debug(getId() + " allocating " + event.taskId + " resources: "
					+ event.required.numCpus + " + " + event.required.memoryInMbs
					+ " (" + event.timeToHoldResource + ")");

			/*
			 * // Direct allocation trigger(new AllocateResources(event.getId(),
			 * event.getNumCpus(), event.getMemoryInMbs(),
			 * event.getTimeToHoldResource()),
			 * worker.getNegative(WorkerPort.class));
			 */
			Task t = new Task(event.taskId, event.required, event.timeToHoldResource);
			t.queue();

			if (configuration.isOmniscent()) {
				trigger(new Resources.AllocateDirectly(self, t),
						worker.getNegative(WorkerPort.class));
			} else {
				probe(t);
			}
		}
	};

	Handler<TManSample> handleTManSample = new Handler<TManSample>() {
		@Override
		public void handle(TManSample event) {
			// TODO:
		}
	};

}

package fdet.system;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fdet.system.evts.DetectorInit;
import fdet.system.evts.DetectorTimeout;
import fdet.system.evts.Ping;
import fdet.system.evts.FdetPort;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timer;

public class Detector extends ComponentDefinition {

	private static final Logger log = LoggerFactory.getLogger(Detector.class);
	
	private final Positive<Network> networkPort = positive(Network.class);
	private final Positive<Timer> timerPort = positive(Timer.class); 
	private final Negative<FdetPort> servicePort = negative(FdetPort.class);
	
	public Detector() {
		subscribe(handleInit, control);
		subscribe(handleSubscribe, servicePort);
		subscribe(handleUnsubscribe, servicePort);
		subscribe(handleTimeout, timerPort);
		subscribe(handlePing, networkPort);
	}
	
	private class Info {
		int refcount;
		UUID timeoutId;
		boolean dead;
		boolean alive;

		Info() {
			this.refcount = 1;
			this.dead = false;
			this.alive = false;
			this.timeoutId = null;
		}

		public void reset(UUID timeoutId) {
			this.alive = false;
			this.timeoutId = timeoutId;
		}
	}

	private DetectorInit conf = null;
	private Map<Address, Detector.Info> tracked = new HashMap<Address, Detector.Info>();
	private String selfName;
	
	private UUID setTimer (Address target, long delay) {
		ScheduleTimeout st = new ScheduleTimeout(delay);
		DetectorTimeout tout = new DetectorTimeout(st, target);
		st.setTimeoutEvent(tout);
		trigger(st, timerPort);
		return tout.getTimeoutId();	
	}

	private void sendPing(Address target, boolean ansEx) {
		trigger(new Ping(conf.self, target, ansEx), networkPort);
	}
	
	private Handler<DetectorInit> handleInit = new Handler<DetectorInit>() {
		@Override
		public void handle(DetectorInit event) {
			conf = event;
			selfName = conf.self.getIp().toString();
		}
	};
	
	private void startRound(Address target, Info info) {
		sendPing(target, true);
		info.reset(setTimer(target, conf.pingTimeout));
	}

	private Handler<FdetPort.Subscribe> handleSubscribe = new Handler<FdetPort.Subscribe>() {
		@Override
		public void handle(FdetPort.Subscribe event) {
			if (event.ref.equals(conf.self)) {	/* Corner case ... */
				log.warn(selfName + ": Self-subscription?");
				return;
			}

			Info stored = tracked.get(event.ref);
			if (stored == null) {
				log.info(selfName + ": Subscribing " + event.ref);
				stored = new Info();
				startRound(event.ref, stored);
				tracked.put(event.ref, stored);
			} else {
				stored.refcount ++;
			}
		}
	};

	private Handler<FdetPort.Unsubscribe> handleUnsubscribe = new Handler<FdetPort.Unsubscribe>() {
		@Override
		public void handle(FdetPort.Unsubscribe event) {
			if (event.ref.equals(conf.self)) { 	/* Corner case ... */
				log.warn(selfName + ": Self-unsubscription?");
				return;
			}
			Info stored = tracked.get(event.ref);
			if (stored == null) return;
			if (stored.refcount > 1) {
				stored.refcount --;
			} else {
				log.info(selfName + ": Unsubscribing " + event.ref);
				trigger(new CancelTimeout(stored.timeoutId), timerPort);
				tracked.remove(event.ref);
			}
		}
	};

	private Handler<DetectorTimeout> handleTimeout = new Handler<DetectorTimeout>() {
		@Override
		public void handle(DetectorTimeout event) {
			Info stored = tracked.get(event.ref);
			assert stored != null;
			
			if (!stored.alive) {
				if (!stored.dead) {
					trigger(new FdetPort.Dead(event.ref), servicePort);
					stored.dead = true;
				}
			}
			startRound(event.ref, stored);
		}
	};
	
	private Handler<Ping> handlePing = new Handler<Ping>() {
		@Override
		public void handle(Ping event) {
			Address from = event.getSource();
			Info stored = tracked.get(from);
			if (event.answerExpected) {
				sendPing(from, false); // XXX s/false/stored != null/?
			}
			if (stored != null) {
				if (stored.dead) {
					/* We signaled the peer as dead, so we need to withdraw our claim. */
					trigger(new FdetPort.Undead(from), servicePort);
					stored.dead = false;
				}
				stored.alive = true;
			}
		}
	};
}

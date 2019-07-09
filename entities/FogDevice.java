package org.fog.entities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.math3.util.Pair;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.power.PowerDatacenter;
import org.cloudbus.cloudsim.power.PowerHost;
import org.cloudbus.cloudsim.power.models.PowerModel;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.sdn.overbooking.BwProvisionerOverbooking;
import org.cloudbus.cloudsim.sdn.overbooking.PeProvisionerOverbooking;
import org.fog.application.AppEdge;
import org.fog.application.AppLoop;
import org.fog.application.AppModule;
import org.fog.application.Application;
import org.fog.policy.AppModuleAllocationPolicy;
import org.fog.scheduler.StreamOperatorScheduler;
import org.fog.scheduler.TupleScheduler;
import org.fog.utils.Config;
import org.fog.utils.FogEvents;
import org.fog.utils.FogUtils;
import org.fog.utils.Logger;
import org.fog.utils.ModuleLaunchConfig;
import org.fog.utils.NetworkUsageMonitor;
import org.fog.utils.TimeKeeper;

public class FogDevice extends PowerDatacenter {
	protected Queue<Tuple> northTupleQueue;
	protected Queue<Pair<Tuple, Integer>> southTupleQueue;

	protected List<String> activeApplications;

	protected Map<String, Application> applicationMap;
	protected Map<String, List<String>> appToModulesMap;
	protected Map<Integer, Double> childToLatencyMap;

	protected Map<Integer, Integer> cloudTrafficMap;

	protected double lockTime;

	/**
	 * ID of the parent Fog Device
	 */
	protected int parentId;

	/**
	 * ID of the Controller
	 */
	protected int controllerId;
	/**
	 * IDs of the children Fog devices
	 */
	protected List<Integer> childrenIds;

	protected Map<Integer, List<String>> childToOperatorsMap;

	/**
	 * Flag denoting whether the link southwards from this FogDevice is busy
	 */
	protected boolean isSouthLinkBusy;

	/**
	 * Flag denoting whether the link northwards from this FogDevice is busy
	 */
	protected boolean isNorthLinkBusy;

	protected double uplinkBandwidth;
	protected double downlinkBandwidth;
	protected double uplinkLatency;
	protected List<Pair<Integer, Double>> associatedActuatorIds;

	protected double energyConsumption;
	protected double lastUtilizationUpdateTime;
	protected double lastUtilization;
	private int level;

	protected double ratePerMips;

	protected double totalCost;

	protected Map<String, Map<String, Integer>> moduleInstanceCount;

	// Custom declarations
	protected Vm smallestVm;

	private static final int REPLICATION_FACTOR = 200;

	public static class Point2D {

		private double x;
		private double y;

		public Point2D(double x, double y) {
			this.x = x;
			this.y = y;
		}

		public Point2D() {
			// TODO Auto-generated constructor stub
		}

		private double getDistance(Point2D other) {
			return Math.sqrt(Math.pow(this.x - other.x, 2) + Math.pow(this.y - other.y, 2));
		}

		public int getNearestPointIndex(List<Point2D> points) {
			int index = -1;
			double minDist = Double.MAX_VALUE;
			for (int i = 0; i < points.size(); i++) {
				double dist = this.getDistance(points.get(i));
				if (dist < minDist) {
					minDist = dist;
					index = i;
				}
			}
			return index;
		}

		public static Point2D getMean(List<Point2D> points) {
			double accumX = 0;
			double accumY = 0;
			if (points.size() == 0)
				return new Point2D(accumX, accumY);
			for (Point2D point : points) {
				accumX += point.x;
				accumY += point.y;
			}
			return new Point2D(accumX / points.size(), accumY / points.size());
		}

		@Override
		public String toString() {
			return "[" + this.x + "," + this.y + "]";
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null || !(obj.getClass() != Point2D.class)) {
				return false;
			}
			Point2D other = (Point2D) obj;
			return this.x == other.x && this.y == other.y;
		}

		public static List<Point2D> getDataset(double x, double y) throws Exception {
			List<Point2D> dataset = new ArrayList<>();

			for (int i = 0; i < REPLICATION_FACTOR; i++) {
				Point2D point = new Point2D(x, y);
				dataset.add(point);
			}
			return dataset;
		}

		public static List<Point2D> initializeRandomCenters(int n, int lowerBound, int upperBound) {
			List<Point2D> centers = new ArrayList<>(n);
			for (int i = 0; i < n; i++) {
				double x = (double) (Math.random() * (upperBound - lowerBound) + lowerBound);
				double y = (double) (Math.random() * (upperBound - lowerBound) + lowerBound);
				Point2D point = new Point2D(x, y);
				centers.add(point);
			}
			return centers;
		}

		public static List<Point2D> getNewCenters(List<Point2D> dataset, List<Point2D> centers) {
			List<List<Point2D>> clusters = new ArrayList<>(centers.size());
			for (int i = 0; i < centers.size(); i++) {
				clusters.add(new ArrayList<Point2D>());
			}
			for (Point2D data : dataset) {
				int index = data.getNearestPointIndex(centers);
				clusters.get(index).add(data);
			}
			List<Point2D> newCenters = new ArrayList<>(centers.size());
			for (List<Point2D> cluster : clusters) {
				newCenters.add(Point2D.getMean(cluster));
			}
			return newCenters;
		}

		public static double getDistance(List<Point2D> oldCenters, List<Point2D> newCenters) {
			double accumDist = 0;
			for (int i = 0; i < oldCenters.size(); i++) {
				double dist = oldCenters.get(i).getDistance(newCenters.get(i));
				accumDist += dist;
			}
			return accumDist;
		}

		public static List<Point2D> kmeans(List<Point2D> centers, List<Point2D> dataset, int k) {
			boolean converged;
			do {
				List<Point2D> newCenters = getNewCenters(dataset, centers);
				double dist = getDistance(centers, newCenters);
				centers = newCenters;
				converged = dist == 0;
			} while (!converged);
			return centers;
		}
	}

	public FogDevice(String name, FogDeviceCharacteristics characteristics, VmAllocationPolicy vmAllocationPolicy,
			List<Storage> storageList, double schedulingInterval, double uplinkBandwidth, double downlinkBandwidth,
			double uplinkLatency, double ratePerMips) throws Exception {
		super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval);
		setCharacteristics(characteristics);
		setVmAllocationPolicy(vmAllocationPolicy);
		setLastProcessTime(0.0);
		setStorageList(storageList);
		setVmList(new ArrayList<Vm>());
		setSchedulingInterval(schedulingInterval);
		setUplinkBandwidth(uplinkBandwidth);
		setDownlinkBandwidth(downlinkBandwidth);
		setUplinkLatency(uplinkLatency);
		setRatePerMips(ratePerMips);
		setAssociatedActuatorIds(new ArrayList<Pair<Integer, Double>>());
		for (Host host : getCharacteristics().getHostList()) {
			host.setDatacenter(this);
		}
		setActiveApplications(new ArrayList<String>());
		// If this resource doesn't have any PEs then no useful at all
		if (getCharacteristics().getNumberOfPes() == 0) {
			throw new Exception(
					super.getName() + " : Error - this entity has no PEs. Therefore, can't process any Cloudlets.");
		}
		// stores id of this class
		getCharacteristics().setId(super.getId());

		applicationMap = new HashMap<String, Application>();
		appToModulesMap = new HashMap<String, List<String>>();
		northTupleQueue = new LinkedList<Tuple>();
		southTupleQueue = new LinkedList<Pair<Tuple, Integer>>();
		setNorthLinkBusy(false);
		setSouthLinkBusy(false);

		setChildrenIds(new ArrayList<Integer>());
		setChildToOperatorsMap(new HashMap<Integer, List<String>>());

		this.cloudTrafficMap = new HashMap<Integer, Integer>();

		this.lockTime = 0;

		this.energyConsumption = 0;
		this.lastUtilization = 0;
		setTotalCost(0);
		setModuleInstanceCount(new HashMap<String, Map<String, Integer>>());
		setChildToLatencyMap(new HashMap<Integer, Double>());
	}

	public FogDevice(String name, long mips, int ram, double uplinkBandwidth, double downlinkBandwidth,
			double ratePerMips, PowerModel powerModel) throws Exception {
		super(name, null, null, new LinkedList<Storage>(), 0);

		List<Pe> peList = new ArrayList<Pe>();

		// 3. Create PEs and add these into a list.
		peList.add(new Pe(0, new PeProvisionerOverbooking(mips))); // need to store Pe id and MIPS Rating

		int hostId = FogUtils.generateEntityId();
		long storage = 1000000; // host storage
		int bw = 10000;

		PowerHost host = new PowerHost(hostId, new RamProvisionerSimple(ram), new BwProvisionerOverbooking(bw), storage,
				peList, new StreamOperatorScheduler(peList), powerModel);

		List<Host> hostList = new ArrayList<Host>();
		hostList.add(host);

		setVmAllocationPolicy(new AppModuleAllocationPolicy(hostList));

		String arch = Config.FOG_DEVICE_ARCH;
		String os = Config.FOG_DEVICE_OS;
		String vmm = Config.FOG_DEVICE_VMM;
		double time_zone = Config.FOG_DEVICE_TIMEZONE;
		double cost = Config.FOG_DEVICE_COST;
		double costPerMem = Config.FOG_DEVICE_COST_PER_MEMORY;
		double costPerStorage = Config.FOG_DEVICE_COST_PER_STORAGE;
		double costPerBw = Config.FOG_DEVICE_COST_PER_BW;

		FogDeviceCharacteristics characteristics = new FogDeviceCharacteristics(arch, os, vmm, host, time_zone, cost,
				costPerMem, costPerStorage, costPerBw);

		setCharacteristics(characteristics);

		setLastProcessTime(0.0);
		setVmList(new ArrayList<Vm>());
		setUplinkBandwidth(uplinkBandwidth);
		setDownlinkBandwidth(downlinkBandwidth);
		setUplinkLatency(uplinkLatency);
		setAssociatedActuatorIds(new ArrayList<Pair<Integer, Double>>());
		for (Host host1 : getCharacteristics().getHostList()) {
			host1.setDatacenter(this);
		}
		setActiveApplications(new ArrayList<String>());
		if (getCharacteristics().getNumberOfPes() == 0) {
			throw new Exception(
					super.getName() + " : Error - this entity has no PEs. Therefore, can't process any Cloudlets.");
		}

		getCharacteristics().setId(super.getId());

		applicationMap = new HashMap<String, Application>();
		appToModulesMap = new HashMap<String, List<String>>();
		northTupleQueue = new LinkedList<Tuple>();
		southTupleQueue = new LinkedList<Pair<Tuple, Integer>>();
		setNorthLinkBusy(false);
		setSouthLinkBusy(false);

		setChildrenIds(new ArrayList<Integer>());
		setChildToOperatorsMap(new HashMap<Integer, List<String>>());

		this.cloudTrafficMap = new HashMap<Integer, Integer>();

		this.lockTime = 0;

		this.energyConsumption = 0;
		this.lastUtilization = 0;
		setTotalCost(0);
		setChildToLatencyMap(new HashMap<Integer, Double>());
		setModuleInstanceCount(new HashMap<String, Map<String, Integer>>());
	}

	/**
	 * Overrides this method when making a new and different type of resource. <br>
	 * <b>NOTE:</b> You do not need to override {@link #body()} method, if you use
	 * this method.
	 * 
	 * @pre $none
	 * @post $none
	 */
	protected void registerOtherEntity() {

	}

	@Override
	protected void processOtherEvent(SimEvent ev) {
		switch (ev.getTag()) {
		case FogEvents.TUPLE_ARRIVAL:
			processTupleArrival(ev);
			break;
		case FogEvents.LAUNCH_MODULE:
			processModuleArrival(ev);
			break;
		case FogEvents.RELEASE_OPERATOR:
			processOperatorRelease(ev);
			break;
		case FogEvents.SENSOR_JOINED:
			processSensorJoining(ev);
			break;
		case FogEvents.SEND_PERIODIC_TUPLE:
			sendPeriodicTuple(ev);
			break;
		case FogEvents.APP_SUBMIT:
			processAppSubmit(ev);
			break;
		case FogEvents.UPDATE_NORTH_TUPLE_QUEUE:
			updateNorthTupleQueue();
			break;
		case FogEvents.UPDATE_SOUTH_TUPLE_QUEUE:
			updateSouthTupleQueue();
			break;
		case FogEvents.ACTIVE_APP_UPDATE:
			updateActiveApplications(ev);
			break;
		case FogEvents.ACTUATOR_JOINED:
			processActuatorJoined(ev);
			break;
		case FogEvents.LAUNCH_MODULE_INSTANCE:
			updateModuleInstanceCount(ev);
			break;
		case FogEvents.RESOURCE_MGMT:
			manageResources(ev);
		default:
			break;
		}
	}

	/**
	 * Perform miscellaneous resource management tasks
	 * 
	 * @param ev
	 */
	private void manageResources(SimEvent ev) {
		updateEnergyConsumption();
		send(getId(), Config.RESOURCE_MGMT_INTERVAL, FogEvents.RESOURCE_MGMT);
	}

	/**
	 * Updating the number of modules of an application module on this device
	 * 
	 * @param ev instance of SimEvent containing the module and no of instances
	 */
	private void updateModuleInstanceCount(SimEvent ev) {
		ModuleLaunchConfig config = (ModuleLaunchConfig) ev.getData();
		String appId = config.getModule().getAppId();
		if (!moduleInstanceCount.containsKey(appId))
			moduleInstanceCount.put(appId, new HashMap<String, Integer>());
		moduleInstanceCount.get(appId).put(config.getModule().getName(), config.getInstanceCount());
		System.out.println(getName() + " Creating " + config.getInstanceCount() + " instances of module "
				+ config.getModule().getName());
	}

	private AppModule getModuleByName(String moduleName) {
		AppModule module = null;
		for (Vm vm : getHost().getVmList()) {
			if (((AppModule) vm).getName().equals(moduleName)) {
				module = (AppModule) vm;
				break;
			}
		}
		return module;
	}

	/**
	 * Sending periodic tuple for an application edge. Note that for multiple
	 * instances of a single source module, only one tuple is sent DOWN while
	 * instanceCount number of tuples are sent UP.
	 * 
	 * @param ev SimEvent instance containing the edge to send tuple on
	 */
	private void sendPeriodicTuple(SimEvent ev) {
		AppEdge edge = (AppEdge) ev.getData();
		String srcModule = edge.getSource();
		AppModule module = getModuleByName(srcModule);

		if (module == null)
			return;

		int instanceCount = module.getNumInstances();
		/*
		 * Since tuples sent through a DOWN application edge are anyways broadcasted,
		 * only UP tuples are replicated
		 */
		for (int i = 0; i < ((edge.getDirection() == Tuple.UP) ? instanceCount : 1); i++) {
			// System.out.println(CloudSim.clock()+" : Sending periodic tuple
			// "+edge.getTupleType());
			Tuple tuple = applicationMap.get(module.getAppId()).createTuple(edge, getId(), module.getId());
			updateTimingsOnSending(tuple);
			sendToSelf(tuple);
		}
		send(getId(), edge.getPeriodicity(), FogEvents.SEND_PERIODIC_TUPLE, edge);
	}

	protected void processActuatorJoined(SimEvent ev) {
		int actuatorId = ev.getSource();
		double delay = (double) ev.getData();
		getAssociatedActuatorIds().add(new Pair<Integer, Double>(actuatorId, delay));
	}

	protected void updateActiveApplications(SimEvent ev) {
		Application app = (Application) ev.getData();
		getActiveApplications().add(app.getAppId());
	}

	public String getOperatorName(int vmId) {
		for (Vm vm : this.getHost().getVmList()) {
			if (vm.getId() == vmId)
				return ((AppModule) vm).getName();
		}
		return null;
	}

	/**
	 * Update cloudet processing without scheduling future events.
	 * 
	 * @return the double
	 */
	protected double updateCloudetProcessingWithoutSchedulingFutureEventsForce() {
		double currentTime = CloudSim.clock();
		double minTime = Double.MAX_VALUE;
		double timeDiff = currentTime - getLastProcessTime();
		double timeFrameDatacenterEnergy = 0.0;

		for (PowerHost host : this.<PowerHost>getHostList()) {
			Log.printLine();

			double time = host.updateVmsProcessing(currentTime); // inform VMs to update processing
			if (time < minTime) {
				minTime = time;
			}

			Log.formatLine("%.2f: [Host #%d] utilization is %.2f%%", currentTime, host.getId(),
					host.getUtilizationOfCpu() * 100);
		}

		if (timeDiff > 0) {
			Log.formatLine("\nEnergy consumption for the last time frame from %.2f to %.2f:", getLastProcessTime(),
					currentTime);

			for (PowerHost host : this.<PowerHost>getHostList()) {
				double previousUtilizationOfCpu = host.getPreviousUtilizationOfCpu();
				double utilizationOfCpu = host.getUtilizationOfCpu();
				double timeFrameHostEnergy = host.getEnergyLinearInterpolation(previousUtilizationOfCpu,
						utilizationOfCpu, timeDiff);
				timeFrameDatacenterEnergy += timeFrameHostEnergy;

				Log.printLine();
				Log.formatLine("%.2f: [Host #%d] utilization at %.2f was %.2f%%, now is %.2f%%", currentTime,
						host.getId(), getLastProcessTime(), previousUtilizationOfCpu * 100, utilizationOfCpu * 100);
				Log.formatLine("%.2f: [Host #%d] energy is %.2f W*sec", currentTime, host.getId(), timeFrameHostEnergy);
			}

			Log.formatLine("\n%.2f: Data center's energy is %.2f W*sec\n", currentTime, timeFrameDatacenterEnergy);
		}

		setPower(getPower() + timeFrameDatacenterEnergy);

		checkCloudletCompletion();

		/** Remove completed VMs **/
		/**
		 * Change made by HARSHIT GUPTA
		 */
		/*
		 * for (PowerHost host : this.<PowerHost> getHostList()) { for (Vm vm :
		 * host.getCompletedVms()) { getVmAllocationPolicy().deallocateHostForVm(vm);
		 * getVmList().remove(vm); Log.printLine("VM #" + vm.getId() +
		 * " has been deallocated from host #" + host.getId()); } }
		 */

		Log.printLine();

		setLastProcessTime(currentTime);
		return minTime;
	}

	protected void checkCloudletCompletion() {
		boolean cloudletCompleted = false;
		List<? extends Host> list = getVmAllocationPolicy().getHostList();
		for (int i = 0; i < list.size(); i++) {
			Host host = list.get(i);
			for (Vm vm : host.getVmList()) {
				while (vm.getCloudletScheduler().isFinishedCloudlets()) {
					Cloudlet cl = vm.getCloudletScheduler().getNextFinishedCloudlet();
					if (cl != null) {

						cloudletCompleted = true;
						Tuple tuple = (Tuple) cl;
						TimeKeeper.getInstance().tupleEndedExecution(tuple);
						Application application = getApplicationMap().get(tuple.getAppId());
						Logger.debug(getName(), "Completed execution of tuple " + tuple.getCloudletId() + "on "
								+ tuple.getDestModuleName());
						List<Tuple> resultantTuples = application.getResultantTuples(tuple.getDestModuleName(), tuple,
								getId(), vm.getId());
						for (Tuple resTuple : resultantTuples) {
							resTuple.setModuleCopyMap(new HashMap<String, Integer>(tuple.getModuleCopyMap()));
							resTuple.getModuleCopyMap().put(((AppModule) vm).getName(), vm.getId());
							updateTimingsOnSending(resTuple);
							sendToSelf(resTuple);
						}
						sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);
					}
				}
			}
		}
		if (cloudletCompleted)
			updateAllocatedMips(null);
	}

	protected void updateTimingsOnSending(Tuple resTuple) {
		// TODO ADD CODE FOR UPDATING TIMINGS WHEN A TUPLE IS GENERATED FROM A
		// PREVIOUSLY RECIEVED TUPLE.
		// WILL NEED TO CHECK IF A NEW LOOP STARTS AND INSERT A UNIQUE TUPLE ID TO IT.
		String srcModule = resTuple.getSrcModuleName();
		String destModule = resTuple.getDestModuleName();
		for (AppLoop loop : getApplicationMap().get(resTuple.getAppId()).getLoops()) {
			if (loop.hasEdge(srcModule, destModule) && loop.isStartModule(srcModule)) {
				int tupleId = TimeKeeper.getInstance().getUniqueId();
				resTuple.setActualTupleId(tupleId);
				if (!TimeKeeper.getInstance().getLoopIdToTupleIds().containsKey(loop.getLoopId()))
					TimeKeeper.getInstance().getLoopIdToTupleIds().put(loop.getLoopId(), new ArrayList<Integer>());
				TimeKeeper.getInstance().getLoopIdToTupleIds().get(loop.getLoopId()).add(tupleId);
				TimeKeeper.getInstance().getEmitTimes().put(tupleId, CloudSim.clock());

				// Logger.debug(getName(),
				// "\tSENDING\t"+tuple.getActualTupleId()+"\tSrc:"+srcModule+"\tDest:"+destModule);

			}
		}
	}

	protected int getChildIdWithRouteTo(int targetDeviceId) {
		for (Integer childId : getChildrenIds()) {
			if (targetDeviceId == childId)
				return childId;
			if (((FogDevice) CloudSim.getEntity(childId)).getChildIdWithRouteTo(targetDeviceId) != -1)
				return childId;
		}
		return -1;
	}

	protected int getChildIdForTuple(Tuple tuple) {
		if (tuple.getDirection() == Tuple.ACTUATOR) {
			int gatewayId = ((Actuator) CloudSim.getEntity(tuple.getActuatorId())).getGatewayDeviceId();
			return getChildIdWithRouteTo(gatewayId);
		}
		return -1;
	}

	// START OF KMeans ALGORITHM IMPLEMENTATION
	protected void updateAllocatedMipsK(String incomingOperator) {
		// KMEAN variables
		List<Point2D> dataset = null;
		List<Point2D> centers = null;
		Map<Integer, Tuple> k_t = new HashMap<>();
		Map<Integer, Vm> k_v = new HashMap<>();
		List<Point2D> d_vm = null;
		List<Point2D> c_vm = null;
		Point2D p = new Point2D();
		int k =200;

		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		getHost().getVmScheduler().deallocatePesForAllVms();
		for (final Vm vm : getHost().getVmList()) {
			if (vm.getCloudletScheduler().runningCloudlets() > 0
					|| ((AppModule) vm).getName().equals(incomingOperator)) {

				TupleScheduler cs = (TupleScheduler) vm.getCloudletScheduler();
				List<ResCloudlet> cloudlets = cs.getCloudletExecList();

				for (ResCloudlet cl : cloudlets) {
					Cloudlet cloudlet = cl.getCloudlet();
					Tuple tuple = (Tuple) cloudlet;
					tuples.add(tuple);
				}
			}
		}
		// BEGIN KMEAN PART FOR CLOUDLETS
		for (int i = 0; i < tuples.size(); i++) {
			try {
				dataset = Point2D.getDataset(tuples.get(i).getExecStartTime(), tuples.get(i).getCloudletFileSize());
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			centers = Point2D.initializeRandomCenters(k, 0, 100000);

			Point2D.kmeans(centers, dataset, k);
			Point2D.getDistance(centers, dataset);
			k_t.put(p.getNearestPointIndex(centers), tuples.get(i));
			Point2D.getNewCenters(dataset, centers);

		} // END KMEAN PART FOR CLOUDLETS

		// BEGIN KMEAN PART FOR CLOUDLETS
		for (int i = 0; i < getHost().getVmList().size(); i++) {

			try {

				d_vm = Point2D.getDataset(getHost().getVmList().get(i).getRam(),
						getHost().getVmList().get(i).getCurrentRequestedMaxMips());

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			c_vm = Point2D.initializeRandomCenters(k, 0, 100000);
			Point2D.kmeans(c_vm, d_vm, k);
			Point2D.getDistance(c_vm, d_vm);
			k_v.put(p.getNearestPointIndex(c_vm), getHost().getVmList().get(i));
			Point2D.getNewCenters(d_vm, c_vm);
		}
		// END KMEAN PART FOR VM

		// BEGIN SORT IN DESCENDING ORDER FOR TUPLES
		Map<Integer, Tuple> k_t_r = new TreeMap<>(Collections.reverseOrder());
		k_t_r.putAll(k_t);
		// END SORT IN DESCENDING ORDER FOR TUPLES

		// BEGIN SORT IN DESCENDING ORDER FOR VM
		Map<Integer, Vm> k_v_r = new TreeMap<>(Collections.reverseOrder());
		k_v_r.putAll(k_v);

		// END SORT IN DESCENDING ORDER FOR VM

		// ASSIGN TUPLES TO VM TO DO
		Set<Integer> commonKey = k_v_r.keySet();
		commonKey.retainAll(k_t_r.keySet());
		Map<Vm, Tuple> map = new HashMap<>();
		List<Tuple> tpl = new ArrayList<>();
		List<Vm> vml = new ArrayList<>();

		for (Integer key : commonKey) {
			if (key != null) {
				map.put(k_v.get(key), k_t.get(key));
				vml.add(k_v.get(key));
				tpl.add(k_t.get(key));
			}
		}
		// WHAT IS NEXT SIR!!!
		//System.out.println("vm size : "+vml.size()+" ==> tuple size: "+tpl.size());

		if (!tpl.isEmpty()) {
			for (Tuple tuple : tpl) {
				for (Vm sVm : vml) {
					if (sVm.getId() == tuple.getVmId()) {
						getHost().getVmScheduler().allocatePesForVm(sVm, new ArrayList<Double>() {
							protected static final long serialVersionUID = 1L;
							{
								
								add(sVm.getMips());
							}
						});
						break;
					} else {
						getHost().getVmScheduler().allocatePesForVm(sVm, new ArrayList<Double>() {

							protected static final long serialVersionUID = 1L;
							{
								add(0.0);
							}
						});
					}
				}
			}
		}

		updateEnergyConsumption();

	}

	/*
	 * public static Map<Vm, Tuple> pair(Map<Integer, Vm> vms, Map<Integer, Tuple>
	 * tps) { Set<Integer> commonKey = vms.keySet();
	 * commonKey.retainAll(tps.keySet());
	 * 
	 * Map<Vm, Tuple> map = new HashMap<>();
	 * 
	 * for (Integer key: commonKey) { map.put(vms.get(key), tps.get(key)); }
	 * 
	 * return map; }
	 */
	// END Of KMeans ALGORITHM IMPLEMENTATION
	// START OF FIRST COME FIRST SERVED ALGORITHM IMPLEMENTATION
	protected void updateAllocatedMips(String incomingOperator) {

		getHost().getVmScheduler().deallocatePesForAllVms();
		for (final Vm vm : getHost().getVmList()) {
			if (vm.getCloudletScheduler().runningCloudlets() > 0
					|| ((AppModule) vm).getName().equals(incomingOperator)) {

				getHost().getVmScheduler().allocatePesForVm(vm, new ArrayList<Double>() {
					protected static final long serialVersionUID = 1L;
					{
						add((double) getHost().getTotalMips());
					}
				});
			} else {
				getHost().getVmScheduler().allocatePesForVm(vm, new ArrayList<Double>() {

					protected static final long serialVersionUID = 1L;
					{

						add(0.0);
					}

				});
			}

		}
		updateEnergyConsumption();

	}
	// END OF FIRST COME FIRST SERVED ALGORITHM IMPLEMENTATION

	// START OF SHORTEST JOB FIRST ALGORITHM IMPLEMENTATION
	protected void updateAllocatedMipsSJF(String incomingOperator) {

		List<Tuple> sortedList = new ArrayList<Tuple>();
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();

		getHost().getVmScheduler().deallocatePesForAllVms();
		for (final Vm vm : getHost().getVmList()) {
			if (vm.getCloudletScheduler().runningCloudlets() > 0
					|| ((AppModule) vm).getName().equals(incomingOperator)) {

				TupleScheduler cs = (TupleScheduler) vm.getCloudletScheduler();
				List<ResCloudlet> cloudlets = cs.getCloudletExecList();

				for (ResCloudlet cl : cloudlets) {
					Cloudlet cloudlet = cl.getCloudlet();
					Tuple tuple = (Tuple) cloudlet;

					tuples.add(tuple);
				}

				int totalTuples = tuples.size();

				for (int i = 0; i < totalTuples; i++) {
					Tuple smallestTuple = (Tuple) tuples.get(0);
					for (Tuple checkTuple : tuples) {
						if (smallestTuple.getCloudletLength() > checkTuple.getCloudletLength()) {
							smallestTuple = checkTuple;
						}
					}

					sortedList.add(smallestTuple);
					tuples.remove(smallestTuple);

				}

				if (!sortedList.isEmpty()) {
					Tuple tuple = sortedList.get(0);
					for (Vm sVm : getHost().getVmList()) {
						if (sVm.getId() == tuple.getVmId()) {
							smallestVm = sVm;
							break;
						}
					}
				}

				if (smallestVm != null) {

					getHost().getVmScheduler().allocatePesForVm(vm, new ArrayList<Double>() {
						protected static final long serialVersionUID = 1L;
						{
							// add((double) getHost().getTotalMips());
							add(smallestVm.getMips());
						}
					});
				} else {
					getHost().getVmScheduler().allocatePesForVm(vm, new ArrayList<Double>() {

						protected static final long serialVersionUID = 1L;
						{

							add(0.0);
						}
					});
				}

			} else {
				getHost().getVmScheduler().allocatePesForVm(vm, new ArrayList<Double>() {
					protected static final long serialVersionUID = 1L;
					{
						add(0.0);
					}
				});
			}

		}

		updateEnergyConsumption();

	}
	// END OF SHORTEST JOB FIRST ALGORITHM IMPLEMENTATION

	// START OF GENERALIZED PRIORITY ALGORITHM IMPLEMENTATION
	protected void updateAllocatedMipsGP(String incomingOperator) {// prioritising tuples with high high execution time

		List<Tuple> sortedList = new ArrayList<Tuple>();
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();

		getHost().getVmScheduler().deallocatePesForAllVms();
		for (final Vm vm : getHost().getVmList()) {
			if (vm.getCloudletScheduler().runningCloudlets() > 0
					|| ((AppModule) vm).getName().equals(incomingOperator)) {
				TupleScheduler cs = (TupleScheduler) vm.getCloudletScheduler();
				List<ResCloudlet> cloudlets = cs.getCloudletExecList();

				for (ResCloudlet cl : cloudlets) {
					Cloudlet cloudlet = cl.getCloudlet();
					Tuple tuple = (Tuple) cloudlet;
					tuples.add(tuple);

				}

				int totalTuples = tuples.size();
				for (int i = 0; i < totalTuples; i++) {
					Tuple hpTuple = (Tuple) tuples.get(0);
					for (Tuple checkTuple : tuples) {
						if ((hpTuple.getExecStartTime() < checkTuple.getExecStartTime())
								&& (hpTuple.getCloudletOutputSize() < checkTuple.getCloudletOutputSize())) {
							hpTuple = checkTuple;
						}
					}
					sortedList.add(hpTuple);
					tuples.remove(hpTuple);

				}

				if (!sortedList.isEmpty()) {
					Tuple tuple = sortedList.get(0);
					for (Vm sVm : getHost().getVmList()) {
						if (sVm.getId() == tuple.getVmId()) {
							smallestVm = sVm;
							break;
						}
					}
				}

				if (smallestVm != null) {

					getHost().getVmScheduler().allocatePesForVm(vm, new ArrayList<Double>() {
						protected static final long serialVersionUID = 1L;
						{
							// add((double) getHost().getTotalMips());
							add(smallestVm.getMips());
						}
					});
				} else {
					getHost().getVmScheduler().allocatePesForVm(vm, new ArrayList<Double>() {

						protected static final long serialVersionUID = 1L;
						{

							add(0.0);
						}
					});
				}

			} else {
				getHost().getVmScheduler().allocatePesForVm(vm, new ArrayList<Double>() {
					protected static final long serialVersionUID = 1L;
					{
						add(0.0);
					}
				});
			}

		}

		updateEnergyConsumption();

	}
	// END OF GENERALIZED PRIORITY ALGORITHM IMPLEMENTATION

	// START OF RR ALGORITHM IMPLEMENTATION
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected void updateAllocatedMipsRR(String incomingOperator) {

		// List<Tuple> sortedList = new ArrayList<Tuple>();
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		@SuppressWarnings("unused")
		ArrayList<Double> wt = new <Double>ArrayList();
		@SuppressWarnings("unused")
		ArrayList<Double> bt = new <Double>ArrayList();
		ArrayList<Double> rem_bt = new ArrayList<Double>();

		double time = 0;
		// double pworkTime = 0;
		int quantum = 10;

		getHost().getVmScheduler().deallocatePesForAllVms();
		for (final Vm vm : getHost().getVmList()) {
			if (vm.getCloudletScheduler().runningCloudlets() > 0
					|| ((AppModule) vm).getName().equals(incomingOperator)) {
				TupleScheduler cs = (TupleScheduler) vm.getCloudletScheduler();
				List<ResCloudlet> cloudlets = cs.getCloudletExecList();

				for (ResCloudlet cl : cloudlets) {
					Cloudlet cloudlet = cl.getCloudlet();
					Tuple tuple = (Tuple) cloudlet;

					tuples.add(tuple);
				}

				for (int i = 0; i < tuples.size(); i++) {
					wt.add(tuples.get(i).getWaitingTime());
					bt.add(tuples.get(i).getExecStartTime());

				}
				rem_bt.addAll(bt);

				while (true) {
					boolean done = true;
					for (int i = 0; i < tuples.size(); i++) {
						if (rem_bt.get(i) > 0) {

							done = false; // There is a pending process

							if (rem_bt.get(i) > quantum) { // Increase the value of t i.e. shows
															// how much time a process has been processed
								time += quantum;

								// Decrease the burst_time of current process
								// by quantum
								double dbtime = rem_bt.get(i) - quantum;
								rem_bt.add(i, dbtime);
							}
							// If burst time is smaller than or equal to
							// quantum. Last cycle for this process
							else {
								// Increase the value of t i.e. shows
								// how much time a process has been processed
								time = time + rem_bt.get(i);

								// Waiting time is current time minus time
								// used by this process

								wt.add(i, time - bt.get(i));
								// As the process gets fully executed
								// make its remaining burst time = 0
								// System.out.println("Burst = " + bt.get(i));
								// System.out.println("Remaining = " + rem_bt.get(i));
								// System.out.println("Waiting Time = " + (time - bt.get(i)));
								rem_bt.add(i, 0.0);
								tuples.get(i).setExecStartTime(bt.get(i));
								tuples.get(i).setSubmissionTime(bt.get(i) - wt.get(i));

							}
						}
					}
					// If all processes are done
					if (done == true)
						break;
				}

				if (!tuples.isEmpty()) {
					for (Tuple tuple : tuples) {
						for (Vm sVm : getHost().getVmList()) {
							if (sVm.getId() == tuple.getVmId()) {
								getHost().getVmScheduler().allocatePesForVm(vm, new ArrayList<Double>() {
									protected static final long serialVersionUID = 1L;
									{
										add(sVm.getMips());
									}
								});
								break;
							}
						}
					}
				}

			} else {
				getHost().getVmScheduler().allocatePesForVm(vm, new ArrayList<Double>() {

					protected static final long serialVersionUID = 1L;
					{

						add(0.0);
					}

				});
			}

		}

		updateEnergyConsumption();

	}
	// END OF RR ALGORITHM IMPLEMENTATION

	private void updateEnergyConsumption() {
		double totalMipsAllocated = 0;
		for (final Vm vm : getHost().getVmList()) {
			AppModule operator = (AppModule) vm;
			operator.updateVmProcessing(CloudSim.clock(),
					getVmAllocationPolicy().getHost(operator).getVmScheduler().getAllocatedMipsForVm(operator));
			totalMipsAllocated += getHost().getTotalAllocatedMipsForVm(vm);
		}

		double timeNow = CloudSim.clock();
		double currentEnergyConsumption = getEnergyConsumption();
		double newEnergyConsumption = currentEnergyConsumption
				+ (timeNow - lastUtilizationUpdateTime) * getHost().getPowerModel().getPower(lastUtilization);
		setEnergyConsumption(newEnergyConsumption);

		/*
		 * if(getName().equals("d-0")){ System.out.println("------------------------");
		 * System.out.println("Utilization = "+lastUtilization);
		 * System.out.println("Power = "+getHost().getPowerModel().getPower(
		 * lastUtilization)); System.out.println(timeNow-lastUtilizationUpdateTime); }
		 */

		double currentCost = getTotalCost();
		double newcost = currentCost
				+ (timeNow - lastUtilizationUpdateTime) * getRatePerMips() * lastUtilization * getHost().getTotalMips();
		setTotalCost(newcost);

		lastUtilization = Math.min(1, totalMipsAllocated / getHost().getTotalMips());
		lastUtilizationUpdateTime = timeNow;
	}

	protected void processAppSubmit(SimEvent ev) {
		Application app = (Application) ev.getData();
		applicationMap.put(app.getAppId(), app);
	}

	protected void addChild(int childId) {
		if (CloudSim.getEntityName(childId).toLowerCase().contains("sensor"))
			return;
		if (!getChildrenIds().contains(childId) && childId != getId())
			getChildrenIds().add(childId);
		if (!getChildToOperatorsMap().containsKey(childId))
			getChildToOperatorsMap().put(childId, new ArrayList<String>());
	}

	protected void updateCloudTraffic() {
		int time = (int) CloudSim.clock() / 1000;
		if (!cloudTrafficMap.containsKey(time))
			cloudTrafficMap.put(time, 0);
		cloudTrafficMap.put(time, cloudTrafficMap.get(time) + 1);
	}

	protected void sendTupleToActuator(Tuple tuple) {
		/*
		 * for(Pair<Integer, Double> actuatorAssociation : getAssociatedActuatorIds()){
		 * int actuatorId = actuatorAssociation.getFirst(); double delay =
		 * actuatorAssociation.getSecond(); if(actuatorId == tuple.getActuatorId()){
		 * send(actuatorId, delay, FogEvents.TUPLE_ARRIVAL, tuple); return; } } int
		 * childId = getChildIdForTuple(tuple); if(childId != -1) sendDown(tuple,
		 * childId);
		 */
		for (Pair<Integer, Double> actuatorAssociation : getAssociatedActuatorIds()) {
			int actuatorId = actuatorAssociation.getFirst();
			double delay = actuatorAssociation.getSecond();
			String actuatorType = ((Actuator) CloudSim.getEntity(actuatorId)).getActuatorType();
			if (tuple.getDestModuleName().equals(actuatorType)) {
				send(actuatorId, delay, FogEvents.TUPLE_ARRIVAL, tuple);
				return;
			}
		}
		for (int childId : getChildrenIds()) {
			sendDown(tuple, childId);
		}
	}

	int numClients = 0;

	protected void processTupleArrival(SimEvent ev) {
		Tuple tuple = (Tuple) ev.getData();

		if (getName().equals("cloud")) {
			updateCloudTraffic();
		}

		/*
		 * if(getName().equals("d-0") && tuple.getTupleType().equals("_SENSOR")){
		 * System.out.println(++numClients); }
		 */
		Logger.debug(getName(),
				"Received tuple " + tuple.getCloudletId() + "with tupleType = " + tuple.getTupleType() + "\t| Source : "
						+ CloudSim.getEntityName(ev.getSource()) + "|Dest : "
						+ CloudSim.getEntityName(ev.getDestination()));
		send(ev.getSource(), CloudSim.getMinTimeBetweenEvents(), FogEvents.TUPLE_ACK);

		if (FogUtils.appIdToGeoCoverageMap.containsKey(tuple.getAppId())) {
		}

		if (tuple.getDirection() == Tuple.ACTUATOR) {
			sendTupleToActuator(tuple);
			return;
		}

		if (getHost().getVmList().size() > 0) {
			final AppModule operator = (AppModule) getHost().getVmList().get(0);
			if (CloudSim.clock() > 0) {
				getHost().getVmScheduler().deallocatePesForVm(operator);
				getHost().getVmScheduler().allocatePesForVm(operator, new ArrayList<Double>() {
					protected static final long serialVersionUID = 1L;
					{
						add((double) getHost().getTotalMips());
					}
				});
			}
		}

		if (getName().equals("cloud") && tuple.getDestModuleName() == null) {
			sendNow(getControllerId(), FogEvents.TUPLE_FINISHED, null);
		}

		if (appToModulesMap.containsKey(tuple.getAppId())) {
			if (appToModulesMap.get(tuple.getAppId()).contains(tuple.getDestModuleName())) {
				int vmId = -1;
				for (Vm vm : getHost().getVmList()) {
					if (((AppModule) vm).getName().equals(tuple.getDestModuleName()))
						vmId = vm.getId();
				}
				if (vmId < 0 || (tuple.getModuleCopyMap().containsKey(tuple.getDestModuleName())
						&& tuple.getModuleCopyMap().get(tuple.getDestModuleName()) != vmId)) {
					return;
				}
				tuple.setVmId(vmId);
				// Logger.error(getName(), "Executing tuple for operator " + moduleName);

				updateTimingsOnReceipt(tuple);

				executeTuple(ev, tuple.getDestModuleName());
			} else if (tuple.getDestModuleName() != null) {
				if (tuple.getDirection() == Tuple.UP)
					sendUp(tuple);
				else if (tuple.getDirection() == Tuple.DOWN) {
					for (int childId : getChildrenIds())
						sendDown(tuple, childId);
				}
			} else {
				sendUp(tuple);
			}
		} else {
			if (tuple.getDirection() == Tuple.UP)
				sendUp(tuple);
			else if (tuple.getDirection() == Tuple.DOWN) {
				for (int childId : getChildrenIds())
					sendDown(tuple, childId);
			}
		}
	}

	protected void updateTimingsOnReceipt(Tuple tuple) {
		Application app = getApplicationMap().get(tuple.getAppId());
		String srcModule = tuple.getSrcModuleName();
		String destModule = tuple.getDestModuleName();
		List<AppLoop> loops = app.getLoops();
		for (AppLoop loop : loops) {
			if (loop.hasEdge(srcModule, destModule) && loop.isEndModule(destModule)) {
				Double startTime = TimeKeeper.getInstance().getEmitTimes().get(tuple.getActualTupleId());
				if (startTime == null)
					break;
				if (!TimeKeeper.getInstance().getLoopIdToCurrentAverage().containsKey(loop.getLoopId())) {
					TimeKeeper.getInstance().getLoopIdToCurrentAverage().put(loop.getLoopId(), 0.0);
					TimeKeeper.getInstance().getLoopIdToCurrentNum().put(loop.getLoopId(), 0);
				}
				double currentAverage = TimeKeeper.getInstance().getLoopIdToCurrentAverage().get(loop.getLoopId());
				int currentCount = TimeKeeper.getInstance().getLoopIdToCurrentNum().get(loop.getLoopId());
				double delay = CloudSim.clock() - TimeKeeper.getInstance().getEmitTimes().get(tuple.getActualTupleId());
				TimeKeeper.getInstance().getEmitTimes().remove(tuple.getActualTupleId());
				double newAverage = (currentAverage * currentCount + delay) / (currentCount + 1);
				TimeKeeper.getInstance().getLoopIdToCurrentAverage().put(loop.getLoopId(), newAverage);
				TimeKeeper.getInstance().getLoopIdToCurrentNum().put(loop.getLoopId(), currentCount + 1);
				break;
			}
		}
	}

	protected void processSensorJoining(SimEvent ev) {
		send(ev.getSource(), CloudSim.getMinTimeBetweenEvents(), FogEvents.TUPLE_ACK);
	}

	protected void executeTuple(SimEvent ev, String moduleName) {
		Logger.debug(getName(), "Executing tuple on module " + moduleName);
		Tuple tuple = (Tuple) ev.getData();

		AppModule module = getModuleByName(moduleName);

		if (tuple.getDirection() == Tuple.UP) {
			String srcModule = tuple.getSrcModuleName();
			if (!module.getDownInstanceIdsMaps().containsKey(srcModule))
				module.getDownInstanceIdsMaps().put(srcModule, new ArrayList<Integer>());
			if (!module.getDownInstanceIdsMaps().get(srcModule).contains(tuple.getSourceModuleId()))
				module.getDownInstanceIdsMaps().get(srcModule).add(tuple.getSourceModuleId());

			int instances = -1;
			for (String _moduleName : module.getDownInstanceIdsMaps().keySet()) {
				instances = Math.max(module.getDownInstanceIdsMaps().get(_moduleName).size(), instances);
			}
			module.setNumInstances(instances);
		}

		TimeKeeper.getInstance().tupleStartedExecution(tuple);
		updateAllocatedMips(moduleName);
		processCloudletSubmit(ev, false);
		updateAllocatedMips(moduleName);
		/*
		 * for(Vm vm : getHost().getVmList()){ Logger.error(getName(),
		 * "MIPS allocated to "+((AppModule)vm).getName()+" = "+getHost().
		 * getTotalAllocatedMipsForVm(vm)); }
		 */
	}

	protected void processModuleArrival(SimEvent ev) {
		AppModule module = (AppModule) ev.getData();
		String appId = module.getAppId();
		if (!appToModulesMap.containsKey(appId)) {
			appToModulesMap.put(appId, new ArrayList<String>());
		}
		appToModulesMap.get(appId).add(module.getName());
		processVmCreate(ev, false);
		if (module.isBeingInstantiated()) {
			module.setBeingInstantiated(false);
		}

		initializePeriodicTuples(module);

		module.updateVmProcessing(CloudSim.clock(),
				getVmAllocationPolicy().getHost(module).getVmScheduler().getAllocatedMipsForVm(module));
	}

	private void initializePeriodicTuples(AppModule module) {
		String appId = module.getAppId();
		Application app = getApplicationMap().get(appId);
		List<AppEdge> periodicEdges = app.getPeriodicEdges(module.getName());
		for (AppEdge edge : periodicEdges) {
			send(getId(), edge.getPeriodicity(), FogEvents.SEND_PERIODIC_TUPLE, edge);
		}
	}

	protected void processOperatorRelease(SimEvent ev) {
		this.processVmMigrate(ev, false);
	}

	protected void updateNorthTupleQueue() {
		if (!getNorthTupleQueue().isEmpty()) {
			Tuple tuple = getNorthTupleQueue().poll();
			sendUpFreeLink(tuple);
		} else {
			setNorthLinkBusy(false);
		}
	}

	protected void sendUpFreeLink(Tuple tuple) {
		double networkDelay = tuple.getCloudletFileSize() / getUplinkBandwidth();
		setNorthLinkBusy(true);
		send(getId(), networkDelay, FogEvents.UPDATE_NORTH_TUPLE_QUEUE);
		send(parentId, networkDelay + getUplinkLatency(), FogEvents.TUPLE_ARRIVAL, tuple);
		NetworkUsageMonitor.sendingTuple(getUplinkLatency(), tuple.getCloudletFileSize());
	}

	protected void sendUp(Tuple tuple) {
		if (parentId > 0) {
			if (!isNorthLinkBusy()) {
				sendUpFreeLink(tuple);
			} else {
				northTupleQueue.add(tuple);
			}
		}
	}

	protected void updateSouthTupleQueue() {
		if (!getSouthTupleQueue().isEmpty()) {
			Pair<Tuple, Integer> pair = getSouthTupleQueue().poll();
			sendDownFreeLink(pair.getFirst(), pair.getSecond());
		} else {
			setSouthLinkBusy(false);
		}
	}

	protected void sendDownFreeLink(Tuple tuple, int childId) {
		double networkDelay = tuple.getCloudletFileSize() / getDownlinkBandwidth();
		// Logger.debug(getName(), "Sending tuple with tupleType =
		// "+tuple.getTupleType()+" DOWN");
		setSouthLinkBusy(true);
		double latency = getChildToLatencyMap().get(childId);
		send(getId(), networkDelay, FogEvents.UPDATE_SOUTH_TUPLE_QUEUE);
		send(childId, networkDelay + latency, FogEvents.TUPLE_ARRIVAL, tuple);
		NetworkUsageMonitor.sendingTuple(latency, tuple.getCloudletFileSize());
	}

	protected void sendDown(Tuple tuple, int childId) {
		if (getChildrenIds().contains(childId)) {
			if (!isSouthLinkBusy()) {
				sendDownFreeLink(tuple, childId);
			} else {
				southTupleQueue.add(new Pair<Tuple, Integer>(tuple, childId));
			}
		}
	}

	protected void sendToSelf(Tuple tuple) {
		send(getId(), CloudSim.getMinTimeBetweenEvents(), FogEvents.TUPLE_ARRIVAL, tuple);
	}

	public PowerHost getHost() {
		return (PowerHost) getHostList().get(0);
	}

	public int getParentId() {
		return parentId;
	}

	public void setParentId(int parentId) {
		this.parentId = parentId;
	}

	public List<Integer> getChildrenIds() {
		return childrenIds;
	}

	public void setChildrenIds(List<Integer> childrenIds) {
		this.childrenIds = childrenIds;
	}

	public double getUplinkBandwidth() {
		return uplinkBandwidth;
	}

	public void setUplinkBandwidth(double uplinkBandwidth) {
		this.uplinkBandwidth = uplinkBandwidth;
	}

	public double getUplinkLatency() {
		return uplinkLatency;
	}

	public void setUplinkLatency(double uplinkLatency) {
		this.uplinkLatency = uplinkLatency;
	}

	public boolean isSouthLinkBusy() {
		return isSouthLinkBusy;
	}

	public boolean isNorthLinkBusy() {
		return isNorthLinkBusy;
	}

	public void setSouthLinkBusy(boolean isSouthLinkBusy) {
		this.isSouthLinkBusy = isSouthLinkBusy;
	}

	public void setNorthLinkBusy(boolean isNorthLinkBusy) {
		this.isNorthLinkBusy = isNorthLinkBusy;
	}

	public int getControllerId() {
		return controllerId;
	}

	public void setControllerId(int controllerId) {
		this.controllerId = controllerId;
	}

	public List<String> getActiveApplications() {
		return activeApplications;
	}

	public void setActiveApplications(List<String> activeApplications) {
		this.activeApplications = activeApplications;
	}

	public Map<Integer, List<String>> getChildToOperatorsMap() {
		return childToOperatorsMap;
	}

	public void setChildToOperatorsMap(Map<Integer, List<String>> childToOperatorsMap) {
		this.childToOperatorsMap = childToOperatorsMap;
	}

	public Map<String, Application> getApplicationMap() {
		return applicationMap;
	}

	public void setApplicationMap(Map<String, Application> applicationMap) {
		this.applicationMap = applicationMap;
	}

	public Queue<Tuple> getNorthTupleQueue() {
		return northTupleQueue;
	}

	public void setNorthTupleQueue(Queue<Tuple> northTupleQueue) {
		this.northTupleQueue = northTupleQueue;
	}

	public Queue<Pair<Tuple, Integer>> getSouthTupleQueue() {
		return southTupleQueue;
	}

	public void setSouthTupleQueue(Queue<Pair<Tuple, Integer>> southTupleQueue) {
		this.southTupleQueue = southTupleQueue;
	}

	public double getDownlinkBandwidth() {
		return downlinkBandwidth;
	}

	public void setDownlinkBandwidth(double downlinkBandwidth) {
		this.downlinkBandwidth = downlinkBandwidth;
	}

	public List<Pair<Integer, Double>> getAssociatedActuatorIds() {
		return associatedActuatorIds;
	}

	public void setAssociatedActuatorIds(List<Pair<Integer, Double>> associatedActuatorIds) {
		this.associatedActuatorIds = associatedActuatorIds;
	}

	public double getEnergyConsumption() {
		return energyConsumption;
	}

	public void setEnergyConsumption(double energyConsumption) {
		this.energyConsumption = energyConsumption;
	}

	public Map<Integer, Double> getChildToLatencyMap() {
		return childToLatencyMap;
	}

	public void setChildToLatencyMap(Map<Integer, Double> childToLatencyMap) {
		this.childToLatencyMap = childToLatencyMap;
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

	public double getRatePerMips() {
		return ratePerMips;
	}

	public void setRatePerMips(double ratePerMips) {
		this.ratePerMips = ratePerMips;
	}

	public double getTotalCost() {
		return totalCost;
	}

	public void setTotalCost(double totalCost) {
		this.totalCost = totalCost;
	}

	public Map<String, Map<String, Integer>> getModuleInstanceCount() {
		return moduleInstanceCount;
	}

	public void setModuleInstanceCount(Map<String, Map<String, Integer>> moduleInstanceCount) {
		this.moduleInstanceCount = moduleInstanceCount;
	}
}
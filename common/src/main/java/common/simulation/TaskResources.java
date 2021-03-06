package common.simulation;

public class TaskResources {

	public final int numCpus;
	public final int memoryInMbs;

	public TaskResources(int numCpus, int memoryInMbs) {
		this.numCpus = numCpus;
		this.memoryInMbs = memoryInMbs;
	}
	
	public int getNumCpus() {
		return numCpus;
	}
	
	public int getMemoryInMbs() {
		return memoryInMbs;
	}

}
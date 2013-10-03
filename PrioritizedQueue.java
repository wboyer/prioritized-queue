import java.util.*;
import java.util.concurrent.*;

/*
 * PriorityQueueManager maintains multiple work queues, each with its own ThreadPoolExecutor,
 * which together function as one queue.
 * 
 * Queued tasks are identified by keys.  An attempt to submit a task, when a task with the same
 * key is already queued, will result only in the first task's priority being incremented and,
 * optionally, additional "instructions" being stored for use by the task when it eventually runs.
 * 
 * As its priority increases, a task may be moved to a higher-priority queue to have a better
 * chance of running sooner.
 * 
 */
class Test
{
	public abstract class TaskFactory
	{
		public abstract Runnable newTask(TaskDetails details);
		public abstract Runnable newTask(List<TaskDetails> details);
	}
	
	public class ExampleTaskFactory extends TaskFactory
	{		
		public Runnable newTask(TaskDetails details)
		{
			return new ExampleTask(details);
		}

		public Runnable newTask(List<TaskDetails> details)
		{
			return new ExampleBatchTask(details);
		}
}
	
	public class ExampleTask implements Runnable
	{
		private TaskDetails details;
		
		ExampleTask(TaskDetails details)
		{
			this.details = details;
		}
		
		public void run()
		{
			try {
				Thread.sleep(1000);
			}
			catch(Exception e) {};

			log("task for " + details.key + " finished");
		}
	}

	public class ExampleBatchTask implements Runnable
	{
		private ArrayList<String> keys;
		
		ExampleBatchTask(Collection<TaskDetails> details)
		{
			this.keys = new ArrayList<String>();
			
			Iterator<TaskDetails> iter = details.iterator();
			while (iter.hasNext())
				this.keys.add(iter.next().key);
		}
		
		public void run()
		{
			try {
				Thread.sleep(1000);
			}
			catch(Exception e) {};

			log("task for " + keys + " finished");
		}
	}

	public class TaskDetails
	{
		public String key;
		public int priority;
		public ArrayList<String> instructions;
		
		TaskDetails(String key)
		{
			this.key = key;
		}

		synchronized public void add(int priority, String instructions)
		{
			this.priority += priority;

			if (instructions != null) {
				if (this.instructions == null)
					this.instructions = new ArrayList<String>();
				this.instructions.add(instructions);
			}
		}

		synchronized public void add(TaskDetails details)
		{
			this.priority += details.priority;

			if (details.instructions != null) {
				if (this.instructions == null)
					this.instructions = new ArrayList<String>();
				this.instructions.addAll(details.instructions);
			}
		}

		synchronized public TaskDetails clear()
		{
			TaskDetails details = new TaskDetails(key);

			details.priority = this.priority;
			this.priority = 0;

			details.instructions = this.instructions;
			this.instructions = null;
			
			return details;
		}
	}

	public class PriorityQueueManager
	{
		private TaskFactory taskFactory;
		private Hashtable<String, TaskStatus> taskMap;

		private ArrayList<ThreadPoolExecutor> queues;
		private int numQueues;

		private int queueIndexLogBase;
		private int minTimeBetweenThreadRuns;
		private int minTimeBetweenRuns;
		private int batchSize;
		
		public PriorityQueueManager(TaskFactory taskFactory, int queueIndexLogBase, int minTimeBetweenThreadRuns, int minTimeBetweenRuns, int batchSize)
		{
			this.taskFactory = taskFactory;

			this.queueIndexLogBase = queueIndexLogBase;
			this.minTimeBetweenThreadRuns = minTimeBetweenThreadRuns;
			this.minTimeBetweenRuns = minTimeBetweenRuns;
			this.batchSize = batchSize;

			queues = new ArrayList<ThreadPoolExecutor>();
			taskMap = new Hashtable<String, TaskStatus>();
		}
		
		public void addQueue(int threadPoolSize)
		{
			queues.add((ThreadPoolExecutor)Executors.newFixedThreadPool(threadPoolSize));
			numQueues++;
		}

		synchronized public void submitTask(String key)
		{
			submitTask(key, 1, null);
		}

		synchronized public void submitTask(String key, int priority)
		{
			submitTask(key, priority, null);
		}

		synchronized public void submitTask(String key, int priority, String instructions)
		{
			TaskStatus entry = taskMap.get(key);

			if (entry == null) {
				entry = new TaskStatus(key, this);
				taskMap.put(key, entry);
			}

			entry.addDetails(priority, instructions);

			enqueueTask(entry);
		}

		synchronized private void enqueueTask(TaskStatus entry)
		{
			if (entry.details.priority == 0) {
				taskMap.remove(entry.details.key);
				return;
			}
			
			int queueIndex = (int)Math.floor(Math.log(entry.details.priority) / Math.log(queueIndexLogBase));
			if (queueIndex >= numQueues)
				queueIndex = numQueues - 1;
			
			entry.enqueue(queues.get(queueIndex));
		}

		private ThreadLocal<Long> timeOfLastThreadRun = new ThreadLocal<Long>() {
			@Override protected Long initialValue() {
                return new Long(0);
			}
        };

        private Long timeOfLastRun = new Long(0);
        
        public void waitBeforeRunningTask()
		{
			if (minTimeBetweenThreadRuns > 0) {
				long timeUntilNextRun = minTimeBetweenThreadRuns - (System.currentTimeMillis() - timeOfLastThreadRun.get());
				if (timeUntilNextRun > 0)
					try {
						log("thread-waiting for " + timeUntilNextRun);
						Thread.sleep(timeUntilNextRun);
					}
					catch(Exception e) {};

				timeOfLastThreadRun.set(System.currentTimeMillis());
			}
			
			if (minTimeBetweenRuns > 0)
				synchronized (timeOfLastRun)
				{
					long timeUntilNextRun = minTimeBetweenRuns - (System.currentTimeMillis() - timeOfLastRun);
					if (timeUntilNextRun > 0)
						try {
							log("waiting for " + timeUntilNextRun);
							Thread.sleep(timeUntilNextRun);
						}
						catch(Exception e) {};

					timeOfLastRun = System.currentTimeMillis();
				}
		}
		
		private class TaskStatus implements Runnable
		{
			private TaskDetails details;
			
			private ThreadPoolExecutor currentQueue;
			private boolean running;

			private PriorityQueueManager queueManager;

			TaskStatus(String key, PriorityQueueManager queueManager)
			{
				this.details = new TaskDetails(key);
				this.queueManager = queueManager;
			}
			
			public void addDetails(int priority, String instructions)
			{
				details.add(priority, instructions);
			}

			public void addDetails(TaskDetails details)
			{
				details.add(details);
			}

			public TaskDetails clearDetails()
			{
				return details.clear();
			}
			
			synchronized public void enqueue(ThreadPoolExecutor queue)
			{
				if (running)
					return;

				if (currentQueue == null)
					log("added (" + details.key + ", " + details.priority + ") to queue " + queue.hashCode());
				else
					if (currentQueue != queue)
						if (currentQueue.remove(this))
							log("move (" + details.key + ", " + details.priority + ") from queue " + currentQueue.hashCode() + " to " + queue.hashCode() + " succeeded");
						else {
							log("move (" + details.key + ", " + details.priority + ") from queue " + currentQueue.hashCode() + " to " + queue.hashCode() + " failed");
							return;
						}

				currentQueue = queue;

				queue.execute(this);
			}

			synchronized private boolean dequeue()
			{
				if (currentQueue == null) {
					// This should never happen, but it does.  ThreadPoolExecutor sometimes
					// runs tasks multiple times.  It seems only to happen to tasks that we've
					// attempted to remove().

					//log("dequeue() for " + key + " failed");
					return false;
				}

				currentQueue.remove(this);
				currentQueue = null;

				running = true;
				
				return true;
			}

			synchronized private void requeue()
			{
				running = false;
				queueManager.enqueueTask(this);
			}
			
			synchronized private void waitBeforeRunning()
			{
				queueManager.waitBeforeRunningTask();				
			}

			public void run()
			{
				ThreadPoolExecutor queue = currentQueue;

				if ((queue == null) || !dequeue())
					return;

				waitBeforeRunning();
				
				TaskDetails details = clearDetails();

				if (queueManager.batchSize > 1)
				{
					ArrayList<TaskStatus> batchedTasks = new ArrayList<TaskStatus>();
					batchedTasks.add(this);

					ArrayList<TaskDetails> batchedDetails = new ArrayList<TaskDetails>();
					batchedDetails.add(details);

					Object[] tasks = queue.getQueue().toArray();
					for (int i = 0; (i < tasks.length) && (batchedTasks.size() < queueManager.batchSize); i++)
					{
						TaskStatus task = (TaskStatus)tasks[i];
						if (!task.dequeue())
							continue;
						
						batchedTasks.add(task);
						batchedDetails.add(task.clearDetails());
					}
					
					try
					{
						queueManager.taskFactory.newTask(batchedDetails).run();
					}
					catch (Exception e)
					{
						for (int i = 0; i < batchedTasks.size(); i++)
							((TaskStatus)batchedTasks.get(i)).addDetails(batchedDetails.get(i));
					}
					finally
					{
						for (int i = 0; i < batchedTasks.size(); i++)
							((TaskStatus)batchedTasks.get(i)).requeue();
					}
				}
				else
					try
					{
						queueManager.taskFactory.newTask(details).run();
					}
					catch (Exception e)
					{
						addDetails(details);
					}
					finally
					{
						requeue();
					}
			}
		}
	}
		
	private void run()
	{
		ExampleTaskFactory taskFactory = new ExampleTaskFactory();
		
		PriorityQueueManager queueManager = new PriorityQueueManager(taskFactory, 3, 1500, 600, 1);
		queueManager.addQueue(1);
		queueManager.addQueue(1);
		queueManager.addQueue(1);
		queueManager.addQueue(1);
		
		int i;
		
		for (i = 0; i < 100; i++)
			queueManager.submitTask("uri1");
		for (i = 0; i < 10; i++)
			queueManager.submitTask("uri" + i);
		for (i = 0; i < 10; i++)
			queueManager.submitTask("uri" + i);
		for (i = 0; i < 5000; i++)
			queueManager.submitTask("uri2");
		for (i = 0; i < 5000; i++)
			queueManager.submitTask("uri3");
		for (i = 0; i < 5000; i++)
			queueManager.submitTask("uri4");
		for (i = 0; i < 10; i++)
			queueManager.submitTask("uri9");
		for (i = 0; i < 500000; i++)
			queueManager.submitTask("uri1");

		log("submissions finished");
}

	public static void main(String[] argv)
	{
		new Test().run();
	}

	public static void log(String msg)
	{
		System.out.println(msg + " on thread " + Thread.currentThread().getId() + " at " + System.currentTimeMillis());
	}
}

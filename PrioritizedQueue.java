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
	public class TaskDetails
	{
		public int priority;
		public ArrayList<String> instructions;
		
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
			TaskDetails details = new TaskDetails();

			details.priority = this.priority;
			this.priority = 0;

			details.instructions = this.instructions;
			this.instructions = null;
			
			return details;
		}
	}

	public abstract class TaskFactory
	{
		TaskFactory() {}
		
		public abstract Runnable newTask(String key, TaskDetails details);
		public abstract Runnable newBatchTask(Collection<String> keys, Collection<TaskDetails> details);
	}
	
	public class ExampleTaskFactory extends TaskFactory
	{
		ExampleTaskFactory() {}
		
		public Runnable newTask(String key, TaskDetails details)
		{
			return new ExampleTask(key, details);
		}

		public Runnable newBatchTask(Collection<String> keys, Collection<TaskDetails> details)
		{
			return new ExampleBatchTask(keys, details);
		}
}
	
	public class ExampleTask implements Runnable
	{
		private String key;
		private Collection<?> instructions;
		
		ExampleTask(String key, TaskDetails details)
		{
			this.key = key;
			this.instructions = details.instructions;
		}
		
		public void run()
		{
			try {
				Thread.sleep(1000);
			}
			catch(Exception e) {};

			log("task for " + key + " with instructions " + instructions + " finished");
		}
	}

	public class ExampleBatchTask implements Runnable
	{
		private Collection<String> keys;
		private Collection<TaskDetails> details;
		
		ExampleBatchTask(Collection<String> keys, Collection<TaskDetails> details)
		{
			this.keys = keys;
			this.details = details;
		}
		
		public void run()
		{
			try {
				Thread.sleep(1000);
			}
			catch(Exception e) {};

			log("task for " + keys + " with details " + details + " finished");
		}
	}

	public class PriorityQueueManager
	{
		private TaskFactory taskFactory;
		private Hashtable<String, TaskStatus> taskMap;

		private ArrayList<ThreadPoolExecutor> queues;
		private int numQueues;

		private int minTimeBetweenThreadRuns;
		private int batchSize;
		
		public PriorityQueueManager(TaskFactory taskFactory, int minTimeBetweenThreadRuns, int batchSize)
		{
			this.taskFactory = taskFactory;
			this.minTimeBetweenThreadRuns = minTimeBetweenThreadRuns;
			this.batchSize = batchSize;

			queues = new ArrayList<ThreadPoolExecutor>();
			taskMap = new Hashtable<String, TaskStatus>();
		}
		
		public void addQueue(int threadPoolSize)
		{
			queues.add((ThreadPoolExecutor)Executors.newFixedThreadPool(threadPoolSize));
			numQueues++;
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
				taskMap.remove(entry.key);
				return;
			}
			
			int queueIndex = (int)Math.floor(Math.log(entry.details.priority) / Math.log(2));
			if (queueIndex >= numQueues)
				queueIndex = numQueues - 1;
			
			entry.enqueue(queues.get(queueIndex));
		}

		private ThreadLocal<Long> timeLastRun = new ThreadLocal<Long>() {
			@Override protected Long initialValue() {
                return new Long(0);
			}
        };

        public void waitBeforeRunningTask()
		{
			if (minTimeBetweenThreadRuns <= 0)
				return;

			long timeUntilNextRun = minTimeBetweenThreadRuns - (System.currentTimeMillis() - timeLastRun.get());
			if (timeUntilNextRun > 0)
				try {
					log("waiting for " + timeUntilNextRun);
					Thread.sleep(timeUntilNextRun);
				}
				catch(Exception e) {};

			timeLastRun.set(System.currentTimeMillis());
		}
		
		private class TaskStatus implements Runnable
		{
			private String key;
			private TaskDetails details;
			
			private ThreadPoolExecutor currentQueue;
			private boolean running;

			private PriorityQueueManager queueManager;

			TaskStatus(String key, PriorityQueueManager queueManager)
			{
				this.key = key;
				this.details = new TaskDetails();

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
					log("added (" + key + ", " + details.priority + ") to queue " + queue.hashCode());
				else
					if (currentQueue != queue)
						if (currentQueue.remove(this))
							log("move (" + key + ", " + details.priority + ") from queue " + currentQueue.hashCode() + " to " + queue.hashCode() + " succeeded");
						else {
							log("move (" + key + ", " + details.priority + ") from queue " + currentQueue.hashCode() + " to " + queue.hashCode() + " failed");
							return;
						}

				currentQueue = queue;

				queue.execute(this);
			}

			synchronized private boolean dequeue()
			{
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

					ArrayList<String> batchedKeys = new ArrayList<String>();
					batchedKeys.add(key);

					ArrayList<TaskDetails> batchedDetails = new ArrayList<TaskDetails>();
					batchedDetails.add(details);

					Object[] tasks = queue.getQueue().toArray();
					for (int i = 0; (i < tasks.length) && (batchedKeys.size() < queueManager.batchSize); i++)
					{
						TaskStatus task = (TaskStatus)tasks[i];
						if (!task.dequeue())
							continue;
						
						batchedTasks.add(task);
						batchedKeys.add(task.key);

						details = task.clearDetails();
						batchedDetails.add(details);
					}
					
					try
					{
						queueManager.taskFactory.newBatchTask(batchedKeys, batchedDetails).run();
					}
					catch (Exception e)
					{
						for (int i = 0; i < batchedKeys.size(); i++)
							((TaskStatus)batchedTasks.get(i)).addDetails(batchedDetails.get(i));
					}
					finally
					{
						for (int i = 0; i < batchedKeys.size(); i++)
							((TaskStatus)batchedTasks.get(i)).requeue();
					}
				}
				else
					try
					{
						queueManager.taskFactory.newTask(key, details).run();
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
		
		PriorityQueueManager queueManager = new PriorityQueueManager(taskFactory, 1500, 1);
		queueManager.addQueue(1);
		queueManager.addQueue(1);
		queueManager.addQueue(1);
		queueManager.addQueue(1);
		
		int i;
		
		for (i = 0; i < 100; i++)
			queueManager.submitTask("uri1", 1);
		for (i = 0; i < 10; i++)
			queueManager.submitTask("uri" + i, 1);
		for (i = 0; i < 10; i++)
			queueManager.submitTask("uri" + i, 1);
		for (i = 0; i < 5000; i++)
			queueManager.submitTask("uri2", 1);
		for (i = 0; i < 5000; i++)
			queueManager.submitTask("uri3", 1);
		for (i = 0; i < 5000; i++)
			queueManager.submitTask("uri4", 1);
		for (i = 0; i < 10; i++)
			queueManager.submitTask("uri9", 1);
		for (i = 0; i < 500000; i++)
			queueManager.submitTask("uri1", 1);

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

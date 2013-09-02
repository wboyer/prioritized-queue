import java.util.*;
import java.util.concurrent.*;

/*
 * PriorityQueueManager maintains multiple work queues, each with its own ThreadPoolExecutor.
 * 
 * Queued tasks are identified by keys.  An attempt to submit a task, when a task with the same
 * key is already queued, will result only in first task's priority being incremented.
 * 
 * As its priority increases, a task may be moved to a higher-priority queue to have a better
 * chance of running sooner.
 * 
 */
class Test
{
	public abstract class TaskFactory
	{
		TaskFactory() {}
		
		public abstract Runnable newTask(String key);
		public abstract Runnable newBulkTask(Collection<String> keys);
	}
	
	public class ExampleTaskFactory extends TaskFactory
	{
		ExampleTaskFactory() {}
		
		public Runnable newTask(String key)
		{
			return new ExampleTask(key);
		}

		public Runnable newBulkTask(Collection<String> keys)
		{
			return new ExampleBulkTask(keys);
		}
	}
	
	public class ExampleTask implements Runnable
	{
		private String key;
		
		ExampleTask(String key)
		{
			this.key = key;
		}
		
		public void run()
		{
			try {
				Thread.sleep(1000);
			}
			catch(Exception e) {};

			System.out.println("task for " + key + " finished on thread " + Thread.currentThread().getId() + ".");
		}
	}
	
	public class ExampleBulkTask implements Runnable
	{
		private Collection<String> keys;
		
		ExampleBulkTask(Collection<String> keys)
		{
			this.keys = keys;
		}
		
		public void run()
		{
			try {
				Thread.sleep(1000);
			}
			catch(Exception e) {};
			
			System.out.println("bulk task for " + keys + " finished on thread " + Thread.currentThread().getId() + ".");
		}
	}
	
	public class PriorityQueueManager
	{
		public TaskFactory taskFactory;
		public Hashtable<String, TaskMapEntry> taskMap;

		private ArrayList<ThreadPoolExecutor> queues;
		private int numQueues;
		
		PriorityQueueManager(TaskFactory taskFactory)
		{
			this.taskFactory = taskFactory;
			queues = new ArrayList<ThreadPoolExecutor>();
			taskMap = new Hashtable<String, TaskMapEntry>();
		}
		
		public void addQueue(int threadPoolSize)
		{
			queues.add((ThreadPoolExecutor)Executors.newFixedThreadPool(threadPoolSize));
			numQueues++;
		}

		synchronized public void submitTask(String key)
		{
			TaskMapEntry entry = taskMap.get(key);

			if (entry == null) {
				entry = new TaskMapEntry(key, this);
				taskMap.put(key, entry);
			}

			entry.priority++;

			enqueueTask(entry);
		}
		
		private void enqueueTask(TaskMapEntry entry)
		{
			if (entry.priority == 0) {
				taskMap.remove(entry.key);
				return;
			}
			
			int queueIndex = (int)Math.floor(Math.log(entry.priority) / Math.log(2));
			if (queueIndex >= numQueues)
				queueIndex = numQueues - 1;
			
			entry.enqueue(queues.get(queueIndex));
		}

		synchronized public Collection<String> runBulkTask(Collection<String> keys, TaskFactory taskFactory)
		{
			Iterator<String> iterator = keys.iterator();

			ArrayList<String> eligibleKeys = new ArrayList<String>();
			
			while (iterator.hasNext()) {
				String key = iterator.next();
				TaskMapEntry entry = taskMap.get(key);

				if (entry == null) {
					entry = new TaskMapEntry(key, this);
					taskMap.put(key, entry);
					eligibleKeys.add(key);
				}
				else
					if (entry.dequeue()) {
						entry.priority = 0;
						eligibleKeys.add(key);
					}
			}

			try {
				taskFactory.newBulkTask(eligibleKeys).run();
			}
			catch (Exception e) {
				// handle the exception
			}
			finally {
				iterator = eligibleKeys.iterator();

				while (iterator.hasNext())
					taskMap.get(iterator.next()).requeue();
			}
			
			return eligibleKeys;
		}
		
		private class TaskMapEntry implements Runnable
		{
			public String key;
			public int priority;
			public Runnable task;
			public ThreadPoolExecutor currentQueue;

			public PriorityQueueManager queueManager;

			TaskMapEntry(String key, PriorityQueueManager queueManager)
			{
				this.key = key;
				this.queueManager = queueManager;
			}
			
			public void enqueue(ThreadPoolExecutor queue)
			{
				if (task == null) {
					task = queueManager.taskFactory.newTask(key);
					currentQueue = queue;
					queue.execute(this);
				}
				else
					synchronized (this)
					{
						if ((currentQueue != null) && (currentQueue != queue)) {
							System.out.print("move " + key + " from " + currentQueue + " to " + queue);
							if (currentQueue.remove(this)) {
								currentQueue = queue;
								queue.execute(this);
								System.out.println(" succeeded");
							}
							else
								System.out.println(" failed");
						}
					}
			}

			public boolean dequeue()
			{
				synchronized (this)
				{
					if (currentQueue == null) {
						System.out.println("dequeue for " + key + " failed");
						return false;
					}
					
					currentQueue.remove(this);
					currentQueue = null;
				}
				return true;
			}

			public void requeue()
			{
				synchronized (this)
				{
					task = null;
					queueManager.enqueueTask(this);
				}
			}
			
			public void run()
			{
				if (!dequeue())
					return;
				
				int initialPriority = priority;
				
				try {
					task.run();
					priority -= initialPriority;
				}
				catch (Exception e) {
					// handle the exception
				}
				finally {
					requeue();
				}
			}
		}
	}
		
	private void run()
	{
		ExampleTaskFactory taskFactory = new ExampleTaskFactory();
		
		PriorityQueueManager queueManager = new PriorityQueueManager(taskFactory);
		queueManager.addQueue(1);
		queueManager.addQueue(1);
		queueManager.addQueue(1);
		queueManager.addQueue(1);
		
		int i;
		
		for (i = 0; i < 5; i++)
			queueManager.submitTask("uri1");
		for (i = 0; i < 10; i++)
			queueManager.submitTask("uri" + i);
		for (i = 0; i < 10; i++)
			queueManager.submitTask("uri" + i);
		for (i = 0; i < 10; i++)
			queueManager.submitTask("uri2");
		for (i = 0; i < 10; i++)
			queueManager.submitTask("uri9");
		for (i = 0; i < 500000; i++)
			queueManager.submitTask("uri1");
		
		System.out.println("submitted");

		ArrayList<String> keys = new ArrayList<String>();
		keys.add("uri1");
		keys.add("uri5");
		keys.add("uri7");
		keys.add("uri8");
		queueManager.runBulkTask(keys, taskFactory);

		keys.add("uri6");
		queueManager.runBulkTask(keys, taskFactory);
}

	public static void main(String[] argv)
	{
		Test test = new Test();
		test.run();
	}
}

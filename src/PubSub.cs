using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace pubsub
{
    public class Channel
    {
        public string Path { get; private set; }
        public int[] HashPath { get; private set; }

        public Channel(string path)
        {
            this.Path = path;
            Build();
        }

        private void Build()
        {
            var items = Path.Split('.');
            HashPath = new int[items.Length];
            
            for(var i = 0; i < items.Length; i++)
            {
                HashPath[i] = items[i].GetHashCode();
            }
        }
    }
    public class Node
    {
        private ConcurrentDictionary<int, Node> Children { get; set; }
        // since there's no 'ConcurrentSet' in .Net 4.5, use 'ConcurrentDictionary<T,T>' instead.
        private ConcurrentDictionary<object, object> Subscribers { get; set; }

        private Node Parent { get; set; }
        private int Depth { get; set; }
        private int Key { get; set; }

        private long RefCount;
        private object SyncRoot { get; set; }

        private static readonly int Asterisk = "*".GetHashCode();

        public Node(Node parent, int key, int depth)
        {
            this.Parent = parent;
            this.Key = key;
            this.Depth = depth;
            this.Children = new ConcurrentDictionary<int, Node>();
            this.Subscribers = new ConcurrentDictionary<object, object>();
            this.RefCount = 0;
            this.SyncRoot = new object();
        }

        public void Add(int[] hashPath, object subscriber)
        {
            if (hashPath.Length < Depth)
                throw new InvalidOperationException();

            if(hashPath.Length == Depth + 1)
            {
                IncRefCount();

                // In this case, subscriber already exists in the collection.
                if (!Subscribers.TryAdd(subscriber, subscriber))
                {
                    DecRefCount();

                    throw new InvalidOperationException();
                }
            }
            else
            {
                IncRefCount();

                int key = hashPath[Depth + 1];
                Node node;
                if (!Children.TryGetValue(key, out node))
                {
                    // 'lock' must be placed here,
                    //  cause there is a potential problem which makes a orpahned node.
                    lock (SyncRoot)
                    {
                        // double-checking for reducing the overheads from 'lock'
                        if(!Children.TryGetValue(key, out node))
                        {
                            node = new Node(this, key, Depth + 1);

                            Children.TryAdd(key, node);
                        }
                    }
                }

                node.Add(hashPath, subscriber);
            }
        }
        public bool Remove(int[] hashPath, object subscriber)
        {
            if (hashPath.Length < Depth)
                throw new InvalidOperationException();

            if (hashPath.Length == Depth + 1)
            {
                object obj;
                if (Subscribers.TryRemove(subscriber, out obj))
                {
                    DecRefCount();

                    return true;
                }

                return false;
            }
            else
            {
                int key = hashPath[Depth + 1];
                Node node;
                if (Children.TryGetValue(key, out node))
                {
                    if (node.Remove(hashPath, subscriber))
                    {
                        DecRefCount();

                        return true;
                    }

                    return false;
                }
            }

            return false;
        }

        public IEnumerable<object> QuerySubscribers(int[] hashPath)
        {
            if (hashPath.Length < Depth)
                throw new InvalidOperationException();

            if (hashPath.Length == Depth + 1)
            {
                foreach (var item in Subscribers)
                    yield return item.Key;
            }
            else
            {
                int key = hashPath[Depth + 1];
                Node node;

                if(key == Asterisk)
                {
                    foreach(var child in Children)
                    {
                        foreach (var item in child.Value.QuerySubscribers(hashPath))
                            yield return item;
                    }
                }
                else
                {
                    if (!Children.TryGetValue(key, out node))
                    {
                        yield break;
                    }

                    foreach (var item in node.QuerySubscribers(hashPath))
                        yield return item;
                }
            }
        }

        private void IncRefCount()
        {
            Interlocked.Increment(ref RefCount);
        }
        private void DecRefCount()
        {
            if (Interlocked.Decrement(ref RefCount) == 0)
            {
                RemoveFromParent();
            }
        }
        private void RemoveFromParent()
        {
            // This point, we need another 'lock' here.
            // In case,
            //    1. The 'RefCount' has decreased and becomes zero.
            //    2. 'RemoveFromParent' will be called
            //    3. Swich to the another thread.
            //    4. The thread will 'Add' a new subsrbier.
            //    5. 'RefCount' will be one or above.
            //    6. Swich back to current context.
            //    7. 'RefCount' is not zero, but it will perform destruction routine.
            lock (SyncRoot)
            {
                if (RefCount > 0)
                    return;

                Node node;
                if (!Parent?.Children.TryRemove(Key, out node) ?? false)
                {
                    throw new InvalidOperationException();
                }
                Parent = null;
            }
        }

        public void Print()
        {
            for(int i=0;i< Depth;i++)
                Console.Write("  ");
            Console.WriteLine($"Node({Key}) - {RefCount}");


            foreach (var child in Subscribers)
            {
                for (int i = 0; i < Depth + 1; i++)
                    Console.Write("  ");
                Console.WriteLine($"Subscriber");
            }
            foreach (var child in Children)
                child.Value.Print();
        }
    }
    public class Pubsub
    {
        private Node Root { get; set; }
        private ThreadLocal<Dictionary<string, WeakReference<Channel>>> PathCache;

        public Pubsub()
        {
            this.Root = new Node(null, 0, -1);
            
            // Each threads has its own thread-local-cache storage. 
            this.PathCache = new ThreadLocal<Dictionary<string, WeakReference<Channel>>>(
                ()=>{
                    return new Dictionary<string, WeakReference<Channel>>();
                });
        }

        private Channel GetChannelFromPath(string path)
        {
            WeakReference<Channel> weakChannel;
            Channel channel;
            var localPathCache = PathCache.Value;

            if(localPathCache.TryGetValue(path, out weakChannel))
            {
                if(weakChannel.TryGetTarget(out channel))
                {
                    // Best case, target already cached and alive.
                    return channel;
                }

                // Second case, the GC collected the target object.
                // We should re-create cache.
                channel = new Channel(path);
                weakChannel.SetTarget(channel);
                return channel;
            }

            channel = new Channel(path);
            localPathCache.Add(path, new WeakReference<Channel>(channel));
            return channel;
        }

        public void Subscribe(string path, object subscriber)
        {
            Root.Add(GetChannelFromPath(path).HashPath, subscriber);
        }
        public void Unsubscribe(string path, object subscriber)
        {
            Root.Remove(GetChannelFromPath(path).HashPath, subscriber);
        }
        public IEnumerable<object> QuerySubscribers(string path)
        {
            foreach (var subscriber in Root.QuerySubscribers(GetChannelFromPath(path).HashPath))
                yield return subscriber;
        }
        public void Publish(string path, object msg)
        {
            // ffff
            foreach (var subscriber in Root.QuerySubscribers(GetChannelFromPath(path).HashPath))
                Console.WriteLine(subscriber);
        }
        public void Print()
        {
            Root.Print();
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            Pubsub p = new Pubsub();

            /*
            List<object> obj = new List<object>();

            for (int i = 0; i < 4000000; i++)
                obj.Add(new object());

            for(int i = 0; i < 4; i++)
            {
                var n = i * 100000;
                var t = new Thread(() =>
                {
                    for(int j= n;j<n+100000;j++)
                    {
                        p.Subscribe("a.b.c", obj[j]);
                    }
                });
                t.Start();
            }
            for (int i = 0; i < 4; i++)
            {
                var n = i * 100000;
                var t = new Thread(() =>
                {
                    for (int j = n; j < n + 100000; j++)
                    {
                        p.Unsubscribe("a.b.c", obj[j]);
                    }
                });
                t.Start();
            }
            */

            var obj2 = new object();
            //p.Subscribe("a.b.c", new object());
            p.Subscribe("a.b.c", obj2);
            p.Subscribe("a.b.d", obj2);
            p.Subscribe("a.b.e", obj2);
            p.Subscribe("a.b.e", new object());
            ///p.Unsubscribe("a.b.c", obj2);

            p.Publish("a.b.*", "A");

            p.Print();

            p.Unsubscribe("a.b.e", obj2);
            p.Unsubscribe("a.b.c", obj2);

            p.Print();

            System.Collections.Queue q = new System.Collections.Queue();
        }
    }
}

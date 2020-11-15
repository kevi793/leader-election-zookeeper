import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;

public class LeaderElection implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT_IN_MS = 3000;
    private static final String ELECTION_NAMESPACE = "/election";
    private final Semaphore semaphore = new Semaphore(0);
    private ZooKeeper zooKeeper;
    private String currentZnodeName;

    public static void main(String[] args) throws IOException, InterruptedException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZookeeper();
        leaderElection.volunteerForLeadership();
        leaderElection.electLeader();
        leaderElection.run();
        leaderElection.close();
        System.out.println("Disconnected from zookeeper, exiting application.");
    }

    public void volunteerForLeadership() throws InterruptedException {
        createElectionNamespace();
        String znodePrefix = ELECTION_NAMESPACE + "/c_";

        while (true) {
            try {
                String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                System.out.println("znode name: " + znodeFullPath);
                this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
                break;
            } catch (KeeperException | InterruptedException ex) {
                System.out.println("Could not volunteer for leadership. Trying again.");
            }
        }
    }

    private void createElectionNamespace() throws InterruptedException {
        while (true) {
            try {
                zooKeeper.create(ELECTION_NAMESPACE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                System.out.println("Election namespace created successfully.");
                break;
            } catch (KeeperException e) {
                if (e.code() == KeeperException.Code.NODEEXISTS) {
                    System.out.println("Election namespace already exists.");
                    break;
                }
            }
        }
    }

    public void electLeader() {
        while (true) {
            try {
                List<String> children = this.zooKeeper.getChildren(ELECTION_NAMESPACE, false);
                Collections.sort(children);
                if (children.get(0).equals(this.currentZnodeName)) {
                    System.out.println("I am the leader");
                } else {
                    System.out.println("I am not the leader. Following: " + children.get(0));
                    int index = children.indexOf(this.currentZnodeName);
                    setupWatchOnImmediateLeader(children.get(index - 1));
                }
                break;
            } catch (KeeperException | InterruptedException e) {
                System.out.println("Error in fetching children of election namespace. Message is: " + e.getMessage());
                System.out.println("Trying again.");
            }
        }
    }

    private void setupWatchOnImmediateLeader(String targetNodeToWatch) {
        this.zooKeeper.getData(ELECTION_NAMESPACE + "/" + targetNodeToWatch,
                watchedEvent -> {
                    if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
                        System.out.println(targetNodeToWatch + " got deleted. Triggering leader election.");
                        LeaderElection.this.electLeader();
                    }
                }, (rc, path, ctx, bytes, stat) -> {
                    switch (KeeperException.Code.get(rc)) {
                        case OK:
                            System.out.println("Watch set for " + targetNodeToWatch);
                            break;
                        case CONNECTIONLOSS:
                            LeaderElection.this.setupWatchOnImmediateLeader(targetNodeToWatch);
                            break;
                        case NONODE:
                            LeaderElection.this.electLeader();
                            break;
                        default:
                            System.out.println("Unknown error in setting up watch. Trying again.");
                            LeaderElection.this.setupWatchOnImmediateLeader(targetNodeToWatch);
                    }
                }, null);
    }

    public void connectToZookeeper() throws IOException, InterruptedException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT_IN_MS, this);
        this.semaphore.acquire();
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.None) {
            if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                System.out.println("Successfully connected to zookeeper.");
                this.semaphore.release();
            } else {
                synchronized (zooKeeper) {
                    System.out.println("Disconnected from zookeeper event.");
                    zooKeeper.notifyAll();
                }
            }
        }
    }
}

(ns
  ^{:author "Nikita Prokopov"
    :doc "Cluster control as a library.

    TERMINOLOGY

    Peer is a worker that participates in task distribution. Usually (but not
    necessarily) single JVM corresponds to a single peer.

    Job is a process that needs to be executed.

    Cluster is a set of peers executing set of jobs. Ensemble guarantees that
    each job will be assigned to one and only one peer at a time and that peers
    will evenly distribute jobs between them.

    Peer may have a role (basically, a regexp), which means that it will not 
    execute every job, but only those jobs which match this regexp.

    Players is a name for a subset of peers that may (potentially) execute
    particular job (in other words, have appropriate role). Potentially,
    a set of players for each job may be unique.

    Each peer has a weight (default is 100) signifying the amount of work it wants
    to handle, in proportion to other peers.


    USAGE FROM PEERS

    On application start, join cluster by connecting to Zookeeper:

        (let [cluster (ensemble/join-cluster {
                        :url   \"zk://zookeeper.ec:2181\"
                        :ns    \"/echo/ensemble\"
                        :role  \"pipeline\\..*\"
                        :name  \"node32\"
                        :vnodes [100 200 300]
                        :weight  200 })]

    Everything except `:url` is optional. If peer name is omitted, hostname
    will be used.

    After joining, get list of jobs assigned to this peer:

        (let [jobs (peer-jobs cluster)]

    `peer-jobs` returns a ref whose state will always contain up-to date list
    of jobs designated to run on current peer (in fact, map of job-name => data):

        => @jobs
        {
          \"twitter-slurp\" { :login ... :password ... :accounts ...  } 
          \"validate\"      { :objects [...] }
          ...
        }

    To get notifications on job list changes, use clojure's `add-watch`:

        (add-watch jobs :state-change
          (fn [_ _ old new]
            (stop-all old)
            (start-all new)))

    When peer wants to leave cluster (usually on shutdown), call

        (leave-cluster cluster)


    JOB MANAGMENT

    To schedule new tasks and cancel old ones, use `sync-jobs`:

        (sync-jobs cluster {
          :job1 some-data
          :job2 other-data })

    Or, if you're doing it not from a connected peer, use regular zk handle:

        (sync-nodes zk \"/jobs\" {
          :job1 some-data
          :job2 other-data })

    Both `sync-jobs` and `sync-nodes` will compare the list of jobs provided with
    those which are being currently executed, stop jobs that are no longer
    listed, start new ones, and update the changed ones.


    ALGORITHM

    Jobs are distributed between players using consistent hashing. Each peer
    maintains its own set of vnodes, in amount proportional to its weight. For
    each job, a ring is a set of vnodes from all its players. Job is assigned
    to one of the players in that ring basing on job name's hash. Note that a job's
    data is not considered when assigning a job.

    Every peer constantly monitors the list of jobs and list of peers. Whenever one
    of them changes, it recalculates jobs distribution basing on updated
    information, takes new jobs that are now assigned to it and leaves those which
    are now assigned to another peer.


    IMPLEMENTATION DETAILS

    In Zookeeper, peers are stored under '/peers' znode:

        /peers
          /peer-name-1
            {:vnodes [...], :role #\".*\"}
          /peer-name-2
            {:vnodes [...], :role #\".*\"}

    Jobs are stored under '/jobs' znode:

        /jobs
          /job1
            ... arbitrary job data ...
          /job2
            ... arbitrary job data ...

    Peers nodes are ephemeral, so if one peer is down, the others will notice the
    loss immediately. There's no inter-peer communication in Ensemble, every
    peer monitors shared Zookeeper state for changes."}

  com.aboutecho.ensemble

  (:require
    [clojure.set :as set]
    [clojure.string :as string]
    [com.aboutecho.ensemble.util :as util]
    [com.aboutecho.ensemble.zookeeper :as zookeeper])
  (:import
    [java.util Collections List]
    [java.net InetAddress]
    [org.apache.zookeeper KeeperException$NoNodeException KeeperException$NodeExistsException KeeperException$ConnectionLossException]))


;; Helpers

(defn- serialize [data] (when data (.getBytes (pr-str data))))
(defn- deserialize [^bytes bytes] (when bytes (read-string (String. bytes))))

(defn- hostname []
  (.getHostName (InetAddress/getLocalHost)))

;; Low-level zk operations

(defn children-data
  "Wrapper around zookeeper/children-data which sorts the nodes and deserializes data"
  [zk path & opts]
  (->> (apply zookeeper/children-data zk path opts)
    (util/map-vals deserialize )
    (into (sorted-map))))

(defn sync-nodes
  "Compares current state of `root` znode and `new-children-data` provided,
   then eliminates the difference"
  [zk root new-children-data]
  (zookeeper/create-all zk root :persistent? true)
  (let [old  (children-data zk root)
        diff (util/map-diff old new-children-data)]
    (doseq [node (concat (keys (:delete diff)) (keys (:update diff)))]
      (util/ignore [KeeperException$NoNodeException]
        (zookeeper/delete zk (zookeeper/as-path root node))))
    (doseq [[node data] (merge (:add diff) (:update diff))]
      (util/ignore [KeeperException$NoNodeException KeeperException$NodeExistsException]
        (zookeeper/create zk (zookeeper/as-path root node) :persistent? true :data (serialize data))))))

(defn- react [tree zk event]
  ;; if nodes were modified during `react` call, it means that we have another
  ;; react in agent's queue already so it's ok to supress any single `react` call
  (util/ignore [KeeperException$NoNodeException KeeperException$NodeExistsException]
    (when (zookeeper/exists? zk "/" :watch? true)
      (let [groups (zookeeper/children zk "/" :watch? true)]
        (into {}
          (for [group groups]
            [(keyword group) (children-data zk (zookeeper/as-path group) :watch? true)]))))))

(defn subscribe
  "Returns a ref whose state always reflects the current state of \"/\" znode.
   Current implementation always digs only two levels deep, and re-reads
   everything on each change"
  [zk]
  (let [tree (agent {} :error-mode :continue)]
    (zookeeper/add-watcher zk
      (fn [event]
        (send tree react zk event)))
    (zookeeper/add-conn-watcher zk
      (fn [state] 
        (case state
          :connected   (send tree react zk nil)
          :suspended   :noop
          :lost        (send tree (constantly nil))
          :reconnected (send tree react zk nil))))
    (send tree react zk nil)
    tree))

;; Consistent hashing

(defn vnodes
  "vnodes generator, assumes 2^16 ring size"
  [n]
  (vec (sort (repeatedly n #(rand-int 65536)))))

(defn- short-hash
  "Hash value in a range 0..2^16"
  [s]
  ;; SHA-1 used for uniform value distribution
  (bit-and (->> (.hashString (com.google.common.hash.Hashing/sha1) s) (.asInt)) 0xffff))

(defn closest-before 
  "Given list of ints, finds left boundary of range where `el` lies"
  [coll el]
  (let [idx (Collections/binarySearch coll el compare)]
    (cond 
      (= -1 idx) -1
      (neg? idx) (coll (- -2 idx))
      :else      (coll idx))))

(defn responsible
  "Returns index of peer in `ring` responsible for `hash`.
   For range 0..first-vnode, last peer in ring is always responsible"
  [hash ^List ring]
  (let [found (apply max-key #(closest-before % hash) ring)]
    (.lastIndexOf ring found)))


;; All puzzle together

(defn sync-jobs [cluster jobs]
  (sync-nodes (:zk cluster) "/jobs" jobs))

(defn players [job peers]
  (->> peers
    (filter (fn [[peer data]] (and (:role data)
                                   (re-matches (:role data) job))))
    (mapv first)))

(defn responsible-peer [obj peers]
  (let [ring  (mapv (comp :vnodes second) peers)
        names (mapv first peers)]
    (when-not (empty? names)
      (names (responsible (short-hash obj) ring)))))

(defn jobs-distribution
  "Map of { peer => { job => job-data }}"
  [peers jobs]
  (->> jobs
    (group-by
      (fn [[job job-data]]
        (let [players-peers (into (sorted-map)
                              (select-keys peers (players job peers)))]
          (responsible-peer job players-peers))))
    (util/map-vals #(into {} %))))

(defn jobs-distribution-live [cluster]
  (util/transformed (:tree cluster)
    (fn [{:keys [peers jobs]}]
      (when-not (or (empty? peers) (empty? jobs))
        (jobs-distribution peers jobs)))))

(defn peer-jobs
  "Current live view of peer's jobs as reference to { job => job-data }.
   At each moment, (deref) of this reference is peer's jobs at the moment."
  [cluster]
  (util/transformed (:tree cluster)
    (fn [{:keys [peers jobs]}]
      (when-not (or (empty? peers) (empty? jobs))
        ((jobs-distribution peers jobs) (:name cluster))))))

(defn- join [zk name & [opts]]
  (let [data {:role     (:role opts #".*")
              :vnodes   (or (:vnodes opts) (vnodes (:weight opts 100)))
              :hostname (hostname)}
        node [(zookeeper/as-path "/peers" name) :persistent? false :data (serialize data)]]
    (zookeeper/add-watcher zk
      (fn [event]
        (when (and (= (:state event) :sync-connected)
                   (:path event)
                   (.startsWith (:path event) "/peers"))
          (util/ignore [KeeperException$NodeExistsException IllegalStateException]
            (apply zookeeper/create-all zk node)))))
    (zookeeper/add-conn-watcher zk
      (fn [state]
        (when (#{:reconnected :connected} state)
          (apply zookeeper/create-all zk node))))
    (util/ignore [KeeperException$ConnectionLossException IllegalStateException]
      (apply zookeeper/create-all zk node))))

(defn join-cluster
  "Register current peer in a cluster, subscribe to notifications.
   `opts` include:
     :url    string  Zookeeper cluster url
     :name   string  This peer's name. Defaults to hostname
     :role   regexp  Pattern for job names that may be assigned to this peer. Defaults to .*
     :weight int     Relative weight of peer in ring, defaults to 100
     :vnodes [short] Specific vector of vnodes (loaded from per-node storage?), 0..2^16"
  [& [opts]]
  (let [zk   (util/apply-map zookeeper/connect opts)
        tree (subscribe zk)
        name (or (:name opts)
                 (System/getenv "ENSEMBLE_PEER")
                 (-> (hostname) (string/split #"\.") first))]
    (join zk name opts)
    { :name  name
      :zk    zk
      :tree  tree }))

(defn leave-cluster
  "Unregister current peer in a cluster"
  [cluster]
  (zookeeper/close (:zk cluster))
  (send (:tree cluster) (constantly nil)))

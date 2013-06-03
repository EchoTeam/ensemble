(ns com.aboutecho.ensemble.zookeeper
  (:require
    [clojure.string :as string]
    [com.aboutecho.ensemble.util :as util])
  (:import
    [java.net InetSocketAddress BindException]
    [com.netflix.curator.retry RetryNTimes]
    [com.netflix.curator.framework.api CuratorEventType CuratorListener]
    [com.netflix.curator.framework CuratorFramework CuratorFrameworkFactory]
    [com.netflix.curator.framework.state ConnectionStateListener]
    [org.apache.zookeeper Watcher ZooDefs$Ids CreateMode WatchedEvent
                          KeeperException$NoNodeException KeeperException$NodeExistsException]
    [com.netflix.curator.test TestingServer]))

;; Helpers

(defn- event->map [^WatchedEvent event]
  (when event
    { :event (util/enum->keyword (.getType event))
      :state (util/enum->keyword (.getState event))
      :path  (.getPath event) } ))

(defn- as-listener [f]
  (reify CuratorListener
    (eventReceived [this curator e]
      (when (= (.getType e) CuratorEventType/WATCHED)
        (f (event->map (.getWatchedEvent e)))))))

(defn- as-watcher [f]
  (reify Watcher
    (process [this e]
      (f (event->map e)))))

(defn- as-connection-listener [f]
  (reify ConnectionStateListener
    (stateChanged [this curator state]
      (f (util/enum->keyword state)))))

(defn- setup-watches [^CuratorFramework curator {:keys [watcher watch?]}]
  (-> curator
    (#(if watch? (.watched %) %))
    (#(if watcher (.usingWatcher % (as-watcher watcher)) %))))

(defn as-path [& tokens]
  (->> tokens
    (remove nil?)
    (map name)
    (remove string/blank?)
    (string/join "/")
    (#(if (.startsWith % "/") % (str "/" %)))))

;; Operations

(defn exists? [curator path & {:as opts}]
  (boolean
    (-> (.checkExists curator)
      (setup-watches opts)
      (.forPath path))))

(defn create [curator path & {:keys [data sequential? persistent?]}]
  (when-not (exists? curator path)
    (-> (.create curator)
      (.withMode (if sequential?
                   (if persistent? CreateMode/PERSISTENT_SEQUENTIAL CreateMode/EPHEMERAL_SEQUENTIAL)
                   (if persistent? CreateMode/PERSISTENT CreateMode/EPHEMERAL)))
      (.withACL ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (.forPath path data))))

(defn delete [curator path & {:as opts}]
  (-> (.delete curator)
    (.forPath path)))

(defn children [curator path & {:as opts}]
  (-> (.getChildren curator)
    (setup-watches opts)
    (.forPath path)
    (#(if (empty? %) nil (vec %)))))

(defn data [curator path & {:as opts}]
  (when (util/apply-map exists? curator path opts) ;; pass watches
    (util/ignore [KeeperException$NoNodeException]
      (-> (.getData curator)
        (setup-watches opts)
        (.forPath path)
        (#(if (and % (pos? (alength %))) % nil))))))

(defn create-all [curator path & {:as opts}]
  ;; TODO Curator.creatingParentsIfNeeded
  (let [tokens (string/split path #"/")
        path   (apply as-path tokens)]
    (doseq [i (range 1 (count tokens))
            :let [path (apply as-path (subvec tokens 0 i))]]
      (util/ignore [KeeperException$NodeExistsException]
        (create curator path :persistent? true)))
    (util/ignore [KeeperException$NodeExistsException]
      (util/apply-map create curator path opts))))

(defn children-data [curator path & {:as opts}]
  (when (util/apply-map exists? curator path opts) ;; pass watches
    (let [children (util/apply-map children curator path opts)]
      (into {}
        (for [child children]
          [child (util/apply-map data curator (as-path path child) opts)])))))

(defn add-watcher
  "Watch for node changes events. On each event, `watcher` will be called with one arg:
    { :event :node-created | :node-deleted | :node-data-changed | :node-children-changed
      :state usually, :sync-connected
      :path  string What node was changed
    }"
  [curator watcher]
  (.addListener (.getCuratorListenable curator) (as-listener watcher)))

(defn add-conn-watcher
  "Watch for connection changes events. You HAVE to handle :lost, :suspended and :reconnect states"
  [curator watcher]
  (.addListener (.getConnectionStateListenable curator) (as-connection-listener watcher)))

(defn connect
  "`opts` include:
    :url string       Zookeeper host:port
    :ns  string       All zookeeper paths will be prepended with this string
    :session-timeout  In ms
    :conn-timeout     In ms"
  [& {:keys [url ns session-timeout conn-timeout]
      :or   { session-timeout 5000
              conn-timeout    10000 }}]
  (let [url (util/parse-uri url)
        url (str (:host url) ":" (:port url))
        url (if ns (str url "," ns) url) 
        curator (CuratorFrameworkFactory/newClient
                  url
                  session-timeout
                  conn-timeout
                  (RetryNTimes. 1 1000))]
    (.start curator)
    curator))

(defn close
  "Idempotent way to close curator"
  [curator]
  (when-not (= (util/enum->keyword (.getState curator)) :stopped)
    (.close curator)))


;; In-memory server

(defn server [& {:keys [dir port] :or {port -1}}]
  (proxy [TestingServer clojure.lang.ILookup] [port (clojure.java.io/as-file dir)]
    (valAt
      ([key] (.valAt this key nil))
      ([key not-found]
        (case key
          :port (.getPort this)
          :url  (str "zk://" (.getConnectString this)))))))

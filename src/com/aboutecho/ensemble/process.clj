(ns
  ^{:author "Nikita Prokopov"
    :doc "Threads library with supervising and resource management.
    
    Process is a thread that can report its current liveness status
    (via heartbeats) and has a scope.

    Supervisor is an obj with its own resource scope and underlying process
    which is monitored and restarted in case of death / stalling / exit.

    Scope is a simple thing: you associate process or supervisor with a
    set of fns (finalizers) and they will be called when process or supervisor
    are stopped. For example, to monitor rabbitmq queue:

        (let [q (rmq/create-queue ...)]
          (spawn
            (fn [] ...consume q...)
            { :finalizers [
                #(rmq/close q)
              ] }))

    Process can also add finalizers \"from inside\":

        (spawn
          (fn []
            (let [q (rmq/create-queue ...)]
              (add-finalizers [ #(rmq/close q) ])
              ... consume q ...))))

    Now, if spawned thread dies, is considered stalled or is stopped,
    finalizers will be called, freeing up all resources allocated.

    Both spawn and supervise support event listeners passed in options. Listener is
    one argument function invoked in appropriate process lifecycle time. Supported
    listeners are start, stalled and kill:

        (spawn #(do-some-work)
           { :name \"My-Job\"
             :listeners
                   {:start   (fn [opts] (logging/debug \"Starting process \" (:name opts)))
                    :stalled (fn [opts] (logging/debug \"Stalled process \" (:name opts)))
                    :kill    (fn [opts] (logging/debug \"Killed process \" (:name opts)))}})"}
  com.aboutecho.ensemble.process
  (:require
    [clojure.tools.logging :as logging]
    [com.aboutecho.ensemble.util :as util])
  (:import
    [clojure.lang IMeta]
    [java.util Timer TimerTask]))

;; Timed tasks

(def ^Timer shared-timer (Timer. true))
(def supervise-thread-pool (util/fixed-thread-pool "ensemble/supervise-thread-pool" 1))

(defmacro as-timer-task ^Timer [& body]
 `(proxy [TimerTask] []
    (run []
      (try
        ~@body
        (catch InterruptedException e# (throw e#))
        (catch Exception e#
          (logging/error e# "Error in scheduled task"))))))

(defmacro after
  "Schedule body to be executed after `delay` ms. Returns 
   TimerTask which can be (.cancel) -ed if not yet started.
   Body should complete quickly, because all scheduled tasks
   share the same background thread."
  [^Long delay & body]
 `(let [task# (as-timer-task ~@body)]
    (.schedule shared-timer task# ~delay)
    task#))

(defmacro every
  "Schedule body to be executed every `period` ms. Returns TimerTask which can
   be (.cancel) -ed. Body should complete quickly, because all scheduled tasks
   share the same background thread."
  [^Long period & body]
 `(let [task# (as-timer-task ~@body)]
    (.schedule shared-timer task# 0 ~period)
    task#))

(def purger (every (* 5 1000) (.purge shared-timer)))

;; Processes

(def ^:private process-id (atom 0))

(defn current
  "Returns process from which this function was called"
  ^Thread []
  (Thread/currentThread))

(defn add-finalizers
  "Register in process metadata a set of callback fns to be called when this
   process finishes (for any reason)"
  ([close-fns] (add-finalizers (current) close-fns))
  ([process close-fns]
    (when-not (empty? close-fns)
      (swap! (:finalizers (meta process)) concat close-fns))))

(defn add-finalizer
  ([close-fn]
    (add-finalizers [close-fn]))
  ([obj fn & args]
    (add-finalizers [#(apply fn obj args)])
    obj))

(def ^:dynamic *process*)

(defn heartbeat
  "To be called from inside process to indicate it is still alive. Heartbeats
   are used by supervisor to determine if underlying process is stalled or not"
  []
  (let [process *process*]
    (when-let [meta (meta process)]
      (when (or (.isInterrupted process)
                @(:stopped meta))
        (throw (InterruptedException.)))
      (reset! (:heartbeat meta) (System/currentTimeMillis)))))

(defn- trigger-listener [key opts]
  (when-let [f (get-in opts [:listeners key])]
    (try (f opts)
         (catch Exception e (logging/error e (str "Failed running listener " key " on " opts))))))

(defn stop-process [^Thread process]
  (when process
    (trigger-listener :kill (meta process))
    (reset! (:stopped (meta process)) true)
    (.interrupt process)))

(defn finalize [finalizers]
  (doseq [close-fn (reverse finalizers)]
    (try
      (close-fn)
      (catch Exception e#
        (logging/error e# "Error in finalizer")))))

(defn- process-name ^String [name]
  (str (or name "proc") "-" (swap! process-id inc)))

(defn- process-fn ^Runnable [f opts]
  (bound-fn []
    (try
      (logging/debug "Process" (.getName (current)) "started")
      (add-finalizers (:finalizers opts))
      (binding [*process* (current)]
        (f))
      (logging/debug "Process" (.getName (current)) "completed")
      (catch InterruptedException e
        (logging/debug "Process" (.getName (current)) "stopped"))
      (catch Exception e
        (when (:log? (ex-data e) true)
          (logging/error e "Process" (.getName (current)) "died")))
      (finally
        (finalize @(:finalizers (meta (current))))))))

(defn spawn
  "Creates new process. Options are:
    :name        Process name (useful for logging, debugging)
    :finalizers  On-close callbacks to be assigned to this process once started
    :daemon      Should this process prevent JVM exit? Defaults to false"
  [f & [opts]]
  (let [meta {:finalizers (atom [])
              :heartbeat  (atom (System/currentTimeMillis))
              :listeners  (:listeners opts)
              :name       (:name opts)
              :stopped    (atom false)}
        name (process-name (:name opts))]
    (trigger-listener :start meta)
    (doto
      (proxy [Thread IMeta] [(process-fn f opts) name]
        (meta [] meta))
      (.setDaemon (:daemon opts false))
      (.start))))


;; Supervisor

(defn- stopped? [^Thread process threshold]
  (cond
    (nil? process)           :not-started
    (not (.isAlive process)) :not-alive
    (nil? threshold)         nil
    (>= (System/currentTimeMillis) (+ threshold @(:heartbeat (meta process)))) :stalled
    :else                    nil))

(defn- stop-ll [supervisor]
  (let [{:keys [^Thread process
                finalizers
                ^TimerTask timer]} supervisor]
    (when process
      (logging/debug "Stopping process" (.getName process)))
    (when timer (.cancel timer))
    (stop-process process)
    (finalize finalizers)
    (-> supervisor
      (assoc :running false)
      (dissoc :process :finalizers :timer))))

(defn- restart-ll [supervisor reason]
  (let [{:keys [fun name ^Thread process]} supervisor
        opts {:name      (str name "/proc")
              :listeners (:listeners supervisor)
              :daemon    (:daemon supervisor false)}]
    (if process
      (logging/debug "Process" (.getName process) "is" (str reason ", restarting"))
      (logging/debug "Process for" (:name supervisor) "is" (str reason ", starting")))
    (when (= :stalled reason)
      (trigger-listener :stalled opts))
    (stop-process process)
    (assoc supervisor
      :process (spawn fun opts))))

(defn- check-ll [{:keys [running process threshold] :as supervisor}]
  (if running
    (if-let [reason (stopped? process threshold)]
      (restart-ll supervisor reason)
      supervisor)
    (stop-ll supervisor)))

(def ^:private supervisor-id (atom 0))

(defn- sup-name [name]
  (str (or name "sup") "-" (swap! supervisor-id inc)))

(defn supervise
  "Runs f in a secure environment, checking for occasional death/exits/stalls
   and restarting as needed.

   Opts are:
     :name       Template for underlying process names
     :threshold  Max interval between heartbeats before process is considered
                 stalled, ms
     :finalizers  Set of finalizers to be associated with this supervisor (not
                 with underlying processes!). Will be cleared out only after
                 the whole supervisor stopped."
  [f & [opts]]
  (let [opts       (update-in opts [:name] sup-name)
        supervisor (agent (assoc opts :fun f :running true)
                     :error-handler #(logging/error %2 "Supervisor agent failed"))]
    (send-via supervise-thread-pool supervisor assoc
      :timer (every (min 1000 (:threshold opts 1000))
               (send-via supervise-thread-pool supervisor check-ll)))))

(defn stop-supervisor [supervisor]
  (send-via supervise-thread-pool supervisor stop-ll))


(ns com.aboutecho.ensemble.util
  (:require
    [clojure.set :as set]
    [clojure.string :as string])
  (:import
    [java.util.concurrent Executors ThreadFactory]))

(def thread-number (atom 0))

(defn thread-factory [pool-name]
  (reify ThreadFactory
    (newThread [this runnable]
      (let [name (str "pool:" pool-name " thread:" (swap! thread-number inc))]
        (doto
          (Thread. runnable name)
          (.setDaemon true))))))

(defn fixed-thread-pool [pool-name size]
  (Executors/newFixedThreadPool size (thread-factory pool-name)))

(def enum->keyword 
  (memoize
    (fn [^Enum enum]
      (-> (.name enum)
        (string/replace #"([a-z])([A-Z])" "$1-$2")
        string/lower-case
        keyword))))

(defn apply-map [f & args]
  (apply f (concat (butlast args) (mapcat identity (last args)))))

(defmacro ignore [exceptions & body]
  (let [catches (map (fn [e] `(catch ~e e#)) exceptions)]
   `(try
      ~@body
      (catch InterruptedException e# (throw e#))
      ~@catches)))

(defn parse-uri
  ([uri] (parse-uri uri {}))
  ([uri defaults]
    (let [u (java.net.URI. uri)
          [user passwd] (if-let [ui (.getUserInfo u)] (string/split ui #":") [nil nil])]
      (merge-with #(if %2 %2 %1) defaults
        {:host   (.getHost u)
         :port   (when (not= -1 (.getPort u)) (.getPort u))
         :path   (.getPath u)
         :user   user
         :passwd passwd}))))

(defn map-vals [f m]
  (into {}
    (map (fn [[k v]] [k (f v)]) m)))

(defn map-diff [m1 m2]
  {:add    (select-keys m2 (set/difference (set (keys m2)) (set (keys m1))))
   :delete (select-keys m1 (set/difference (set (keys m1)) (set (keys m2))))
   :update (into {} (for [[k v] m2 :when (not= (get m1 k v) v)] [k v]))})

(defn transformed
  "Returns a ref that will contain 'live view' of another ref with `f` applied,
   recalculated as original one changes."
  [src f]
  (let [trgt (agent nil :error-mode :continue)
        upd  (fn [_ val] (f val))]
    (send trgt upd @src)
    (add-watch src trgt
      (fn [_ _ old new]
        (when (not= old new)
          (send trgt upd new))))
    trgt))

(defmacro wait
  "Usable for tests where no direct synchronization is possible. Instead of:

      (Thread/sleep 1000)
      (is (= @f :ok))

  write this:

      (util/wait 1000 (= @f :ok))

  It will test the condition every 10ms and pass through right after the moment
  condition renders true. If it's still falsy after 1000ms of testing, it fails
  the assertion at this point."
  [threshold body]
 `(let [start# (System/currentTimeMillis)]
    (loop []
      (if (or ~body
              (> (- (System/currentTimeMillis) start#) ~threshold))
        (do (clojure.test/is ~body))
        (do (Thread/sleep 10) (recur))))))

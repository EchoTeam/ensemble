(defproject com.aboutecho.ensemble/ensemble "0.1.1"
  :description "Cluster control as a library."
  :url "http://github.com/echoteam/ensemble"
  :license {:name "The BSD 2-Clause License"
            :url "http://opensource.org/licenses/bsd-license.php"
            :distribution :repo}
  :dependencies [
    [org.clojure/clojure "1.5.1"]
    [org.clojure/tools.logging "0.2.6"]
    [com.netflix.curator/curator-framework "1.3.3"]
    [com.netflix.curator/curator-test "1.3.3"]
  ])

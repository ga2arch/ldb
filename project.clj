(defproject ldb "0.0.1-SNAPSHOT"
  :description "ldb"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.lmdbjava/lmdbjava "0.6.3"]
                 [net.openhft/zero-allocation-hashing "0.9"]
                 [org.clojure/data.fressian "0.2.1"]
                 [criterium "0.4.4"]
                 [clj-http "3.9.1"]
                 [org.clojure/tools.namespace "0.3.0-alpha4"]
                 [org.clojure/spec.alpha "0.2.176"]
                 [com.cognitect/anomalies "0.1.12"]]

  :main ^:skip-aot core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
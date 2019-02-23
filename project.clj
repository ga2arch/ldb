(defproject ldb "0.0.1-SNAPSHOT"
  :description "ldb"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.lmdbjava/lmdbjava "0.6.3"]
                 [org.clojure/data.fressian "0.2.1"]
                 [criterium "0.4.4"]
                 [org.clojure/tools.namespace "0.3.0-alpha4"]
                 [org.clojure/spec.alpha "0.2.176"]
                 [org.clojure/core.unify "0.5.7"]]
  :main ^:skip-aot core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
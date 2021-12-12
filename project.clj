(defproject gather "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.json "2.4.0"]
                 [org.clojure/core.async "1.3.610"]
                 [org.clojure/tools.cli "1.0.206"]
                 [org.clojure/tools.logging "1.2.1"]
                 [org.clojure/tools.namespace "0.2.11"]
                 [org.clojure/java.jdbc "0.7.11"]
                 [org.clojure/data.csv "1.0.0"]
                 [seancorfield/next.jdbc "1.1.588"]
                 [proto-repl "0.3.1"]
                 [proto-repl-charts "0.3.1"]
                 [aleph "0.4.7-alpha5"]
                 [compojure "1.6.2"]
                 [com.github.housepower/clickhouse-native-jdbc "2.6.2"]
                 [com.layerware/hugsql "0.5.1"]
                 [us.2da/hugsql-adapter-clickhouse-native-jdbc "1.0.1"]
                 [hikari-cp "2.13.0"]
                 [org.slf4j/slf4j-log4j12 "1.7.32"]
                 [org.slf4j/slf4j-api "1.7.32"]]
  :jvm-opts ["-Dclojure.tools.logging.factory=clojure.tools.logging.impl/log4j-factory"]
  :main ^:skip-aot gather.core
  :target-path "target/%s"
  :profiles
  {:uberjar {:aot :all
             :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}
   :dev {:source-paths ["dev" "src" "test"]
         :dependencies [[org.clojure/tools.namespace "0.2.11"]]}})

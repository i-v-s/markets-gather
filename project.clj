(defproject gather "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [
    [org.clojure/clojure "1.10.1"]
    [org.clojure/data.json "1.0.0"]
    [org.clojure/core.async "1.3.610"]
    [proto-repl "0.3.1"]
    [proto-repl-charts "0.3.1"]
    [org.clojure/tools.namespace "0.2.11"]
    [aleph "0.4.7-alpha5"]
    [compojure "1.6.2"]
    [com.layerware/hugsql "0.5.1"]
    [us.2da/hugsql-adapter-clickhouse-native-jdbc "1.0.1"]
  ]
  :main ^:skip-aot gather.core
  :target-path "target/%s"
  :profiles
  {
    :uberjar {:aot :all
              :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}
    :dev {:source-paths ["dev" "src" "test"]
          :dependencies [[org.clojure/tools.namespace "0.2.11"]]}
  }
)
